use std::{
    any, fmt,
    sync::{
        Arc,
        atomic::{self, AtomicBool},
    },
};

use async_trait::async_trait;
use tokio::{
    sync::{
        Mutex, OwnedSemaphorePermit, RwLock, Semaphore, SemaphorePermit, broadcast, mpsc, oneshot,
    },
    task::{self, JoinSet},
};
use tracing::Instrument;

pub mod context;
pub mod events;
pub mod routed;
pub mod router_handle;

pub use context::RouterCtx;
pub use events::{EventReceiver, EventSender};
pub use router_handle::RouterHandle;

mod private {
    use async_trait::async_trait;
    use std::{any, fmt};

    use crate::{RouterBase, RouterCtx};

    #[async_trait]
    pub trait RouterPriv: RouterBase + Sized {
        type InternalEvent: Send + fmt::Debug;
        type Meta: RouterMeta<RouterState = Self>;

        async fn new(meta: Self::Meta, ctx: RouterCtx<Self>) -> Self;

        #[allow(unused)]
        fn handle_internal_event(&self, internal: Self::InternalEvent) {
            unimplemented!("{}", any::type_name::<Self>());
        }
    }

    pub trait RouterMeta: fmt::Debug {
        type RouterState: RouterBase + RouterPriv<Meta = Self> + Send + Sync;
    }
}

pub use private::{RouterMeta, RouterPriv};

#[async_trait]
pub trait RouterBase: Send + Sync + 'static {
    type Event: Send + fmt::Debug;

    type Notif: Send + fmt::Debug;
    type ShutdownResult: Send + fmt::Debug;

    #[allow(unused)]
    fn handle_notification(&mut self, notif: Self::Notif) {
        unimplemented!("{}", any::type_name::<Self>());
    }

    async fn shutdown(self) -> Self::ShutdownResult;
}

#[tracing::instrument(skip(cmd_rx, state_handle, circuit_breaker))]
async fn cmd_rx_loop<State>(
    mut cmd_rx: mpsc::Receiver<Box<dyn routed::AnyCall<State>>>,
    state_handle: Arc<RwLock<State>>,
    circuit_breaker: Arc<Mutex<Arc<Semaphore>>>,
    mut permits_rx: broadcast::Receiver<()>,
    permits_tx: broadcast::Sender<()>,
    mut shutdown_cmd_rx: broadcast::Receiver<()>,
) where
    State: RouterBase + Send + Sync + 'static,
{
    loop {
        tokio::select! {
            // TODO: swap ordering of guard acquisition and cmd dequeueing from rx channel?
            // needed if notifs should preempt rx of cmds
            Some(routed) = cmd_rx.recv()=>{
                tracing::trace!("received routed call {:?}", routed);
                tracing::trace!("acquired circuit breaker lock");

                loop {
                    let guard = circuit_breaker.lock().await;
                    if let Ok(permit) = guard.clone().try_acquire_owned() {
                        tracing::trace!("acquired permit");

                        #[cfg(not(feature = "route_spans"))]
                        let span = tracing::Span::current();
                        #[cfg(feature = "route_spans")]
                        let span = routed.span().clone();

                        let run_with_permit =
                            |permit: OwnedSemaphorePermit,
                                state_handle: Arc<RwLock<State>>,
                                routed: Box<dyn routed::AnyCall<State>>,
                                permits_tx: broadcast::Sender<()>| async move {
                                routed.run(state_handle).instrument(span).await;
                                permits_tx
                                    .send(())
                                    .expect("unable to send permit availability broadcast");
                                drop(permit);
                            };

                        tokio::spawn(run_with_permit(
                            permit,
                            state_handle.clone(),
                            routed,
                            permits_tx.clone(),
                        ));
                        break;
                    }
                    tracing::trace!("no permits to acquire - awaiting permit availability");
                    match permits_rx.recv().await {
                        Ok(()) |
                        Err(broadcast::error::RecvError::Lagged(_)) => (),
                        other => panic!(
                            "unable to receive permit availability broadcast: {other:?}",
                        ),
                    }
                }
            },
            shutdown = shutdown_cmd_rx.recv()=> {
                shutdown.expect("unable to receive internal shutdown broadcast");
                return;
            }
        }
    }
}

#[tracing::instrument(skip(notif_rx, state_handle, circuit_breaker))]
async fn notif_rx_loop<State>(
    mut notif_rx: mpsc::Receiver<<State as RouterBase>::Notif>,
    n_concurrent: u32,
    state_handle: Arc<RwLock<State>>,
    circuit_breaker: Arc<Mutex<Arc<Semaphore>>>,
    permits_tx: broadcast::Sender<()>,
    mut shutdown_cmd_rx: broadcast::Receiver<()>,
) where
    State: RouterBase + Send + Sync + 'static,
{
    loop {
        tokio::select! {Some(notif) = notif_rx.recv()=> {
            tracing::trace!("received notifiction {:?}", notif);
            let guard = circuit_breaker.lock().await;
            tracing::trace!(
                "holding circuit breaker lock & waiting for all outstanding requests to complete"
            );

            {
                let _permits = guard
                    .acquire_many(n_concurrent)
                    .await
                    .expect("unable to acquire permits for notif");
                tracing::trace!("acquired all permits");

                let mut state = state_handle.write().await;
                // FUTURE: add documentation for the handle_notification() method describing that you can get yourself in trouble here
                task::block_in_place(move || {state.handle_notification(notif)});
            }
            permits_tx
                .send(())
                .expect("unable to send permit availability broadcast");
        },

            shutdown= shutdown_cmd_rx.recv() => {
                shutdown.expect("unable to receive internal shutdown broadcast");
                return;
            }
        }
    }
}

#[tracing::instrument(skip(internal_event_rx, state_handle, circuit_breaker))]
async fn internal_event_rx_loop<State>(
    mut internal_event_rx: mpsc::Receiver<<State as RouterPriv>::InternalEvent>,
    state_handle: Arc<RwLock<State>>,
    circuit_breaker: Arc<Mutex<Arc<Semaphore>>>,
    mut permits_rx: broadcast::Receiver<()>,
    permits_tx: broadcast::Sender<()>,
    mut shutdown_cmd_rx: broadcast::Receiver<()>,
) where
    State: RouterBase + RouterPriv + Send + Sync + 'static,
{
    loop {
        tokio::select! {Some(internal_event) = internal_event_rx.recv()=> {
                loop {
                    let guard = circuit_breaker.lock().await;
                    match guard.try_acquire() {
                        Ok(permit) => {
                            tracing::trace!("acquired permit");

                            let run_with_permit =
                                |_permit: SemaphorePermit<'_>,
                                 state_handle: Arc<RwLock<State>>,
                                 permits_tx: broadcast::Sender<()>| async move {
                                    let state = state_handle.read().await;
                                    state.handle_internal_event(internal_event);
                                    drop(state);
                                    permits_tx
                                        .send(())
                                        .expect("unable to send permit availability broadcast");
                                };

                            tokio::spawn(run_with_permit(
                                permit,
                                state_handle.clone(),
                                permits_tx.clone(),
                            ));
                            break;
                        }
                        Err(_) => {
                            permits_rx
                                .recv()
                                .await
                                .expect("unable to receive permit availability broadcast");
                        }
                    }
                }
            },

                shutdown= shutdown_cmd_rx.recv() => {
                    shutdown.expect("unable to receive internal shutdown broadcast");
                    return;
                }

        }
    }
}

async fn shutdown_handler<State>(
    state_handle: Arc<RwLock<State>>,
    shutdown_cmd_rx: oneshot::Receiver<()>,
    shutdown_cmd_done_tx: oneshot::Sender<<State as RouterBase>::ShutdownResult>,
    internal_shutdown_tx: broadcast::Sender<()>,
    alive: Arc<AtomicBool>,
    worker_loops: JoinSet<()>,
) where
    State: Router,
{
    shutdown_cmd_rx
        .await
        .expect("unable to receive shutdown message");
    alive.store(false, atomic::Ordering::Relaxed);
    internal_shutdown_tx
        .send(())
        .expect("unable to transmit internal shutdown message");
    worker_loops.join_all().await;
    tracing::debug!("all worker loops joined");
    let state: State = Arc::try_unwrap(state_handle)
        .unwrap_or_else(|_| panic!("unable to take singular ownership over state during shutdown"))
        .into_inner();
    let result = state.shutdown().await;
    shutdown_cmd_done_tx
        .send(result)
        .expect("unable to send shutdown done response");
}

#[async_trait]
pub trait Router: RouterBase + RouterPriv {
    // type Handle: From<RouterHandle<Self>> + Clone;

    #[tracing::instrument(level = "debug", skip(n_concurrent, meta_args))]
    async fn spawn_from_meta(
        meta_args: impl Into<Self::Meta> + fmt::Debug + Send,
        n_concurrent: u32,
    ) -> (
        RouterHandle<Self>,
        EventReceiver<<Self as RouterBase>::Event>,
    ) {
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let (notif_tx, notif_rx) = mpsc::channel(32);

        let (event_tx, event_rx) = mpsc::channel(32);
        let (internal_event_tx, internal_event_rx) = mpsc::channel(32);

        let (permits_tx, permits_rx) = broadcast::channel(1);

        let (shutdown_cmd_tx, shutdown_cmd_rx) = oneshot::channel();
        let (shutdown_cmd_done_tx, shutdown_cmd_done_rx) = oneshot::channel();
        let (internal_shutdown_tx, internal_shutdown_rx) = broadcast::channel(1);
        let alive = Arc::new(AtomicBool::new(true));

        let handle = RouterHandle::<Self>::new(
            cmd_tx,
            notif_tx.clone(),
            shutdown_cmd_tx,
            shutdown_cmd_done_rx,
            alive.clone(),
        );

        let state = Self::new(
            meta_args.into(),
            RouterCtx::new(event_tx, internal_event_tx, notif_tx),
        )
        .await;

        // inner arc so that we can take back sole ownership of state object during shutdown
        let state_handle = Arc::new(RwLock::new(state));
        let circuit_breaker = Arc::new(Mutex::new(Arc::new(Semaphore::new(n_concurrent as usize))));

        let mut worker_loops = JoinSet::new();

        let _cmd_abort = worker_loops.spawn(
            cmd_rx_loop(
                cmd_rx,
                state_handle.clone(),
                circuit_breaker.clone(),
                permits_rx,
                permits_tx.clone(),
                internal_shutdown_rx,
            )
            .instrument(tracing::Span::current()),
        );

        let _notif_abort = worker_loops.spawn(
            notif_rx_loop(
                notif_rx,
                n_concurrent,
                state_handle.clone(),
                circuit_breaker.clone(),
                permits_tx.clone(),
                internal_shutdown_tx.subscribe(),
            )
            .instrument(tracing::Span::current()),
        );

        let _internal_event_abort = worker_loops.spawn(
            internal_event_rx_loop(
                internal_event_rx,
                state_handle.clone(),
                circuit_breaker,
                permits_tx.subscribe(),
                permits_tx,
                internal_shutdown_tx.subscribe(),
            )
            .instrument(tracing::Span::current()),
        );

        tokio::spawn(
            shutdown_handler(
                state_handle,
                shutdown_cmd_rx,
                shutdown_cmd_done_tx,
                internal_shutdown_tx,
                alive.clone(),
                worker_loops,
            )
            .instrument(tracing::Span::current()),
        );

        (handle, EventReceiver::new(event_rx, alive))
    }
}

use std::sync::{
    Arc,
    atomic::{self, AtomicBool},
};

use tokio::{
    sync::{
        Mutex, OwnedSemaphorePermit, RwLock, Semaphore, SemaphorePermit, broadcast, mpsc, oneshot,
    },
    task::{self, JoinSet},
};
use tracing::Instrument;

use crate::{Router, RouterBase, RouterPriv, routed};

#[tracing::instrument(skip(cmd_rx, state_handle, circuit_breaker))]
pub(crate) async fn cmd_rx_loop<State>(
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
pub(crate) async fn notif_rx_loop<State>(
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
pub(crate) async fn internal_event_rx_loop<State>(
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

pub(crate) async fn shutdown_handler<State>(
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

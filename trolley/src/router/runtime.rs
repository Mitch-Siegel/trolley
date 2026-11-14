use std::sync::Arc;

use tokio::{
    sync::{RwLock, broadcast, mpsc, oneshot},
    task::JoinSet,
};
use tracing::Instrument;

use crate::{Router, RouterBase, RouterPriv, events, routed, router::Handle};

mod concurrency;

use concurrency::Concurrency;

#[cfg(test)]
mod tests;

#[tracing::instrument(skip(cmd_rx, state_handle, concurrency))]
async fn cmd_rx_loop<State>(
    mut cmd_rx: mpsc::Receiver<Box<dyn routed::AnyCall<State>>>,
    concurrency: Arc<Concurrency>,
    state_handle: Arc<RwLock<State>>,
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
                let permit = concurrency.acquire_owned().await;
                tracing::trace!("acquired circuit breaker lock");

                #[cfg(not(feature = "route_spans"))]
                let span = tracing::Span::current();
                #[cfg(feature = "route_spans")]
                let span = routed.span().clone();

                let state_handle = state_handle.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    routed.run(state_handle).instrument(span).await;
                });
            },
            shutdown = shutdown_cmd_rx.recv()=> {
                shutdown.expect("unable to receive internal shutdown broadcast");
                return;
            }
        }
    }
}

#[tracing::instrument(skip(notif_rx, shared_state, concurrency))]
async fn notif_rx_loop<State>(
    mut notif_rx: mpsc::Receiver<<State as RouterBase>::Notif>,
    shared_state: Arc<RwLock<State>>,
    concurrency: Arc<Concurrency>,
    mut shutdown_cmd_rx: broadcast::Receiver<()>,
) where
    State: RouterBase + Send + Sync + 'static,
{
    loop {
        tokio::select! {Some(notif) = notif_rx.recv()=> {
            tracing::trace!("received notifiction {:?}", notif);
            // hold circuit breaker over entire notif processing to block dequeue of any incoming commands or internal events
            let _permit = concurrency.acquire_all().await;

            let mut state = shared_state.write().await;
            state.handle_notification(notif);
        }
        shutdown = shutdown_cmd_rx.recv() => {
            shutdown.expect("unable to receive internal shutdown broadcast");
            return;
        }
        }
    }
}

#[tracing::instrument(skip(internal_event_rx, shared_state, concurrency))]
async fn internal_event_rx_loop<State>(
    mut internal_event_rx: mpsc::Receiver<<State as RouterPriv>::InternalEvent>,
    shared_state: Arc<RwLock<State>>,
    concurrency: Arc<Concurrency>,
    mut shutdown_cmd_rx: broadcast::Receiver<()>,
) where
    State: RouterBase + RouterPriv + Send + Sync + 'static,
{
    loop {
        tokio::select! {Some(internal_event) = internal_event_rx.recv()=> {
                    let permit = concurrency.acquire_owned().await;

                    let shared_state = shared_state.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        let state = shared_state.read().await;
                        state.handle_internal_event(internal_event);
                    });
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
    shutdown_cmd_rx: oneshot::Receiver<oneshot::Sender<<State as RouterBase>::ShutdownResult>>,
    internal_shutdown_tx: broadcast::Sender<()>,
    alive: Arc<()>,
    worker_loops: JoinSet<()>,
) where
    State: Router,
{
    let shutdown_done_tx = shutdown_cmd_rx
        .await
        .expect("unable to receive shutdown message");
    drop(alive);
    internal_shutdown_tx
        .send(())
        .expect("unable to transmit internal shutdown message");
    worker_loops.join_all().await;
    tracing::debug!("all worker loops joined");
    let state: State = Arc::try_unwrap(state_handle)
        .ok() // for code coverage :)
        .expect("unable to take singular ownership over state during shutdown")
        .into_inner();
    let result = state.shutdown().await;
    shutdown_done_tx
        .send(result)
        .expect("unable to send shutdown done response");
}

/// # Panics if `n_permits` exceeds `Semaphore::MAX_PERMITS`
pub(crate) fn start<State>(
    state: State,
    n_permits: u32,
    notif_tx: mpsc::Sender<<State as RouterBase>::Notif>,
    notif_rx: mpsc::Receiver<<State as RouterBase>::Notif>,
    event_rx: mpsc::Receiver<<State as RouterBase>::Event>,
    internal_event_rx: mpsc::Receiver<<State as RouterPriv>::InternalEvent>,
) -> (
    Handle<State>,
    events::Receiver<<State as RouterBase>::Event>,
)
where
    State: Router,
{
    let (cmd_tx, cmd_rx) = mpsc::channel(32);

    let (shutdown_cmd_tx, shutdown_cmd_rx) = oneshot::channel();
    let (internal_shutdown_tx, internal_shutdown_rx) = broadcast::channel(1);
    let alive = Arc::new(());

    let concurrency = Arc::new(Concurrency::new(n_permits));

    let mut worker_loops = JoinSet::new();

    let state_arc = Arc::new(RwLock::new(state));

    worker_loops.spawn(cmd_rx_loop(
        cmd_rx,
        concurrency.clone(),
        state_arc.clone(),
        internal_shutdown_tx.subscribe(),
    ));
    worker_loops.spawn(internal_event_rx_loop(
        internal_event_rx,
        state_arc.clone(),
        concurrency.clone(),
        internal_shutdown_tx.subscribe(),
    ));
    worker_loops.spawn(notif_rx_loop(
        notif_rx,
        state_arc.clone(),
        concurrency,
        internal_shutdown_rx,
    ));

    let final_weak = Arc::downgrade(&alive);

    let shutdown_join_handle = tokio::spawn(shutdown_handler(
        state_arc,
        shutdown_cmd_rx,
        internal_shutdown_tx,
        alive,
        worker_loops,
    ))
    .instrument(tracing::Span::current());
    drop(shutdown_join_handle);

    let handle = Handle::<State>::new(cmd_tx, notif_tx, shutdown_cmd_tx, final_weak.clone());

    let events = events::Receiver::new(event_rx, final_weak);

    (handle, events)
}

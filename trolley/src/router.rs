use std::{
    any, fmt,
    sync::{Arc, atomic::AtomicBool},
};

use async_trait::async_trait;
use tokio::{
    sync::{Mutex, RwLock, Semaphore, broadcast, mpsc, oneshot},
    task::JoinSet,
};
use tracing::Instrument;

use crate::events;

mod context;
mod handle;
mod runtime;

pub use context::Ctx;
pub use handle::Handle;

mod private {
    use async_trait::async_trait;
    use std::{any, fmt};

    use crate::{RouterBase, router};

    #[async_trait]
    pub trait RouterPriv: RouterBase + Sized {
        type InternalEvent: Send + fmt::Debug;
        type Meta: RouterMeta<RouterState = Self>;

        async fn new(meta: Self::Meta, ctx: router::Ctx<Self>) -> Self;

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
#[async_trait]
pub trait Router: RouterBase + RouterPriv {
    #[tracing::instrument(level = "debug", skip(n_concurrent, meta_args))]
    async fn spawn_from_meta(
        meta_args: impl Into<Self::Meta> + fmt::Debug + Send,
        n_concurrent: u32,
    ) -> (Handle<Self>, events::Receiver<<Self as RouterBase>::Event>) {
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let (notif_tx, notif_rx) = mpsc::channel(32);

        let (event_tx, event_rx) = mpsc::channel(32);
        let (internal_event_tx, internal_event_rx) = mpsc::channel(32);

        let (permits_tx, permits_rx) = broadcast::channel(1);

        let (shutdown_cmd_tx, shutdown_cmd_rx) = oneshot::channel();
        let (shutdown_cmd_done_tx, shutdown_cmd_done_rx) = oneshot::channel();
        let (internal_shutdown_tx, internal_shutdown_rx) = broadcast::channel(1);
        let alive = Arc::new(AtomicBool::new(true));

        let handle = Handle::<Self>::new(
            cmd_tx,
            notif_tx.clone(),
            shutdown_cmd_tx,
            shutdown_cmd_done_rx,
            alive.clone(),
        );

        let state = Self::new(
            meta_args.into(),
            Ctx::new(event_tx, internal_event_tx, notif_tx),
        )
        .await;

        // inner arc so that we can take back sole ownership of state object during shutdown
        let state_handle = Arc::new(RwLock::new(state));
        let circuit_breaker = Arc::new(Mutex::new(Arc::new(Semaphore::new(n_concurrent as usize))));

        let mut worker_loops = JoinSet::new();

        let _cmd_abort = worker_loops.spawn(
            runtime::cmd_rx_loop(
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
            runtime::notif_rx_loop(
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
            runtime::internal_event_rx_loop(
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
            runtime::shutdown_handler(
                state_handle,
                shutdown_cmd_rx,
                shutdown_cmd_done_tx,
                internal_shutdown_tx,
                alive.clone(),
                worker_loops,
            )
            .instrument(tracing::Span::current()),
        );

        (handle, events::Receiver::new(event_rx, alive))
    }
}

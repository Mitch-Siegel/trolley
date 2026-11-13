use std::{any, fmt};

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::events;

mod context;
pub mod handle;
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
        let (notif_tx, notif_rx) = mpsc::channel(32);
        let (event_tx, event_rx) = mpsc::channel(32);
        let (internal_event_tx, internal_event_rx) = mpsc::channel(32);
        runtime::start(
            Self::new(
                meta_args.into(),
                Ctx::new(event_tx, internal_event_tx, notif_tx.clone()),
            )
            .await,
            n_concurrent,
            notif_tx,
            notif_rx,
            event_rx,
            internal_event_rx,
        )
    }
}

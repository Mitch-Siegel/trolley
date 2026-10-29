use tokio::sync::mpsc;

use crate::router::{EventSender, RouterBase, RouterPriv};

#[derive(Debug)]
pub struct RouterCtx<State>
where
    State: RouterBase + RouterPriv,
{
    event_tx: mpsc::Sender<<State as RouterBase>::Event>,
    internal_event_tx: mpsc::Sender<<State as RouterPriv>::InternalEvent>,
    notif_tx: mpsc::Sender<<State as RouterBase>::Notif>,
}

impl<State> RouterCtx<State>
where
    State: RouterBase + RouterPriv,
{
    pub fn new(
        event_tx: mpsc::Sender<<State as RouterBase>::Event>,
        internal_event_tx: mpsc::Sender<<State as RouterPriv>::InternalEvent>,
        notif_tx: mpsc::Sender<<State as RouterBase>::Notif>,
    ) -> Self {
        Self {
            event_tx,
            internal_event_tx,
            notif_tx,
        }
    }

    fn expect_send<T>(result: Result<(), mpsc::error::SendError<T>>) {
        match result {
            Ok(()) => (),
            Err(e) => {
                tracing::error!(
                    "unable to send event: {}\nSomething has gone badly wrong transmitting {}",
                    e,
                    std::any::type_name::<T>(),
                );
                panic!(
                    "unable to send event: {}\nSomething has gone badly wrong transmitting {}",
                    e,
                    std::any::type_name::<T>(),
                );
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn send_event(&self, event: <State as RouterBase>::Event) {
        Self::expect_send(self.event_tx.send(event).await);
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn send_internal_event(&self, internal_event: <State as RouterPriv>::InternalEvent) {
        Self::expect_send(self.internal_event_tx.blocking_send(internal_event));
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn notify(
        &self,
        notif: <State as RouterBase>::Notif,
    ) -> Result<(), tokio::sync::mpsc::error::TrySendError<<State as RouterBase>::Notif>> {
        self.notif_tx.try_send(notif)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn send_internal_event_async(
        &self,
        internal_event: <State as RouterPriv>::InternalEvent,
    ) -> Result<(), tokio::sync::mpsc::error::TrySendError<<State as RouterPriv>::InternalEvent>>
    {
        self.internal_event_tx.try_send(internal_event)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn notify_async(&self, notif: <State as RouterBase>::Notif) {
        Self::expect_send(self.notif_tx.send(notif).await);
    }

    pub fn internal_events(&self) -> EventSender<<State as RouterPriv>::InternalEvent> {
        EventSender::from(self.internal_event_tx.clone())
    }
}

impl<State> Clone for RouterCtx<State>
where
    State: RouterBase + RouterPriv,
{
    fn clone(&self) -> Self {
        Self {
            event_tx: self.event_tx.clone(),
            internal_event_tx: self.internal_event_tx.clone(),
            notif_tx: self.notif_tx.clone(),
        }
    }
}

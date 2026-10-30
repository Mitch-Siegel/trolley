use std::any;

use tokio::sync::mpsc;

use crate::router::{EventSender, RouterBase, RouterPriv};

#[derive(Debug)]
pub struct RouterCtx<State>
where
    State: RouterBase + RouterPriv,
{
    events: mpsc::Sender<<State as RouterBase>::Event>,
    internal_events: mpsc::Sender<<State as RouterPriv>::InternalEvent>,
    notifs: mpsc::Sender<<State as RouterBase>::Notif>,
}

impl<State> RouterCtx<State>
where
    State: RouterBase + RouterPriv,
{
    #[must_use]
    pub const fn new(
        events: mpsc::Sender<<State as RouterBase>::Event>,
        internal_events: mpsc::Sender<<State as RouterPriv>::InternalEvent>,
        notifs: mpsc::Sender<<State as RouterBase>::Notif>,
    ) -> Self {
        Self {
            events,
            internal_events,
            notifs,
        }
    }

    fn expect_send<T>(result: Result<(), mpsc::error::SendError<T>>) {
        match result {
            Ok(()) => (),
            Err(e) => {
                tracing::error!(
                    "unable to send event: {}\nSomething has gone badly wrong transmitting {}",
                    e,
                    any::type_name::<T>(),
                );
                panic!(
                    "unable to send event: {}\nSomething has gone badly wrong transmitting {}",
                    e,
                    any::type_name::<T>(),
                );
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn send_event(&self, event: <State as RouterBase>::Event) {
        Self::expect_send(self.events.send(event).await);
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn send_internal_event(&self, internal_event: <State as RouterPriv>::InternalEvent) {
        Self::expect_send(self.internal_events.blocking_send(internal_event));
    }

    /// Send a notification to the router
    ///
    /// # Errors
    ///
    /// Returns an error if the notification channel is full
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn notify(
        &self,
        notif: <State as RouterBase>::Notif,
    ) -> Result<(), mpsc::error::TrySendError<<State as RouterBase>::Notif>> {
        self.notifs.try_send(notif)
    }

    /// Send an internal event to the router
    ///
    /// # Errors
    ///
    /// Returns an error if the internal event channel is full
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn send_internal_event_async(
        &self,
        internal_event: <State as RouterPriv>::InternalEvent,
    ) -> Result<(), mpsc::error::TrySendError<<State as RouterPriv>::InternalEvent>> {
        self.internal_events.try_send(internal_event)
    }

    // TODO: panics (docs)
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn notify_async(&self, notif: <State as RouterBase>::Notif) {
        Self::expect_send(self.notifs.send(notif).await);
    }

    // TODO: panics (docs)
    #[must_use]
    pub fn internal_events(&self) -> EventSender<<State as RouterPriv>::InternalEvent> {
        EventSender::from(self.internal_events.clone())
    }
}

impl<State> Clone for RouterCtx<State>
where
    State: RouterBase + RouterPriv,
{
    fn clone(&self) -> Self {
        Self {
            events: self.events.clone(),
            internal_events: self.internal_events.clone(),
            notifs: self.notifs.clone(),
        }
    }
}

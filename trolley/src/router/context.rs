use tokio::sync::mpsc;

use crate::{
    events,
    router::{RouterBase, RouterPriv},
};

#[derive(Debug)]
pub struct Ctx<State>
where
    State: RouterBase + RouterPriv,
{
    events: mpsc::Sender<<State as RouterBase>::Event>,
    internal_events: mpsc::Sender<<State as RouterPriv>::InternalEvent>,
    notifs: mpsc::Sender<<State as RouterBase>::Notif>,
}

impl<State> Ctx<State>
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

    async fn send<T>(tx: &mpsc::Sender<T>, msg: T) {
        tx.send(msg)
            .await
            .expect("internal rx loop closed unexpectedly");
    }

    fn try_send<T>(tx: &mpsc::Sender<T>, msg: T) -> Result<(), T> {
        match tx.try_send(msg) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(msg)) => Err(msg),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                panic!("internal rx loop closed unexpectedly")
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn send_event(&self, event: <State as RouterBase>::Event) {
        Self::send(&self.events, event).await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn try_send_event(
        &self,
        event: <State as RouterBase>::Event,
    ) -> Result<(), <State as RouterBase>::Event> {
        Self::try_send(&self.events, event)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn send_internal_event(&self, internal_event: <State as RouterPriv>::InternalEvent) {
        Self::send(&self.internal_events, internal_event).await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn try_send_internal_event(
        &self,
        internal_event: <State as RouterPriv>::InternalEvent,
    ) -> Result<(), <State as RouterPriv>::InternalEvent> {
        Self::try_send(&self.internal_events, internal_event)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn notify(&self, notif: <State as RouterBase>::Notif) {
        Self::send(&self.notifs, notif).await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn try_notify(
        &self,
        notif: <State as RouterBase>::Notif,
    ) -> Result<(), <State as RouterBase>::Notif> {
        Self::try_send(&self.notifs, notif)
    }

    #[must_use]
    pub fn internal_events(&self) -> events::Sender<<State as RouterPriv>::InternalEvent> {
        events::Sender::from(self.internal_events.clone())
    }
}

impl<State> Clone for Ctx<State>
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

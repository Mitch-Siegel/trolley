use std::{
    fmt, marker,
    sync::{Arc, Weak},
};

use tokio::sync::{RwLock, mpsc, oneshot};
use tracing::instrument;

use crate::{
    Service, ServiceMut,
    routed::{self, Timeout},
    router::{Router, RouterBase},
};

struct ShutdownChannels<State>
where
    State: Router,
{
    shutdown_tx: oneshot::Sender<()>,
    shutdown_done_rx: oneshot::Receiver<<State as RouterBase>::ShutdownResult>,
}
pub struct Handle<State>
where
    State: Router,
{
    // FUTURE: monitoring/diagnostics:
    // FUTURE: call queue depth monitoring
    // FUTURE: notif queue depth monitoring
    // FUTURE: internal event queue depth monitoring
    // FUTURE: event queue depth monitoring
    // FUTURE: time statistics (per call type and total?)
    cmd_tx: mpsc::Sender<Box<dyn routed::AnyCall<State>>>,
    notif_tx: mpsc::Sender<<State as RouterBase>::Notif>,
    shutdown_channels: Arc<RwLock<Option<ShutdownChannels<State>>>>,
    alive: Weak<()>,
    _state: marker::PhantomData<State>,
}

impl<State> Handle<State>
where
    State: Router,
{
    pub(crate) fn new(
        cmd_tx: mpsc::Sender<Box<dyn routed::AnyCall<State>>>,
        notif_tx: mpsc::Sender<<State as RouterBase>::Notif>,
        shutdown_tx: oneshot::Sender<()>,
        shutdown_done_rx: oneshot::Receiver<<State as RouterBase>::ShutdownResult>,
        alive: Weak<()>,
    ) -> Self {
        Self {
            cmd_tx,
            notif_tx,
            shutdown_channels: Arc::new(RwLock::new(Some(ShutdownChannels {
                shutdown_tx,
                shutdown_done_rx,
            }))),
            alive,
            _state: marker::PhantomData,
        }
    }
}

impl<State> Clone for Handle<State>
where
    State: Router,
{
    fn clone(&self) -> Self {
        Self {
            cmd_tx: self.cmd_tx.clone(),
            notif_tx: self.notif_tx.clone(),
            shutdown_channels: self.shutdown_channels.clone(),
            alive: self.alive.clone(),
            _state: marker::PhantomData,
        }
    }
}

#[derive(Debug)]
pub enum NotifyError {
    RouterShutdown,
}

#[derive(Debug)]
pub enum TryNotifyError<T> {
    RouterShutdown,
    Full(T),
}

impl<State> Handle<State>
where
    State: Router,
{
    pub async fn shutdown(self) -> <State as RouterBase>::ShutdownResult {
        let _guard = self.alive.upgrade().expect("router was already shut down");

        let ShutdownChannels {
            shutdown_tx,
            shutdown_done_rx,
        } = self
            .shutdown_channels
            .write()
            .await
            .take()
            .expect("shutdown already in progress through another handle");

        shutdown_tx
            .send(())
            .expect("unable to transmit shutdown message to router");

        drop(_guard);

        shutdown_done_rx
            .await
            .expect("unable to receive shutdown result from router")
    }

    async fn call_routed<Output>(
        &self,
        routed: Box<dyn routed::AnyCall<State>>,
        rsp_rx: oneshot::Receiver<Result<Output, routed::Error>>,
    ) -> Result<Output, routed::Error>
    where
        Output: fmt::Debug,
    {
        let _guard = self.alive.upgrade().ok_or(routed::Error::RouterShutdown)?;

        self.cmd_tx.send(routed).await.unwrap();
        rsp_rx.await.map_err(routed::Error::RecvError)?
    }

    pub async fn notify(&self, notif: <State as RouterBase>::Notif) -> Result<(), NotifyError> {
        let _guard = self.alive.upgrade().ok_or(NotifyError::RouterShutdown)?;

        self.notif_tx
            .send(notif)
            .await
            .expect("internal rx loop closed unexpectedly");
        Ok(())
    }

    pub fn try_notify(
        &self,
        notif: <State as RouterBase>::Notif,
    ) -> Result<(), TryNotifyError<<State as RouterBase>::Notif>> {
        let Some(_guard) = self.alive.upgrade() else {
            return Err(TryNotifyError::RouterShutdown);
        };

        match self.notif_tx.try_send(notif) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                panic!("internal rx loop closed unexpectedly")
            }
            Err(mpsc::error::TrySendError::Full(msg)) => Err(TryNotifyError::Full(msg)),
        }
    }

    /// Note that timeout is the *total* duration allowed for both acquiring the state handle *and* executing the call
    #[instrument(level = "trace")]
    pub async fn call<Input>(
        &self,
        input: Input,
        timeout: Timeout,
    ) -> Result<<State as Service<Input>>::Output, routed::Error>
    where
        State: Service<Input> + fmt::Debug + Send + Sync + 'static,
        Input: fmt::Debug + Send + 'static,
        <State as Service<Input>>::Output: Send,
    {
        let (call, rsp_rx) = routed::Const::new(input, timeout);
        self.call_routed(Box::new(call), rsp_rx).await
    }

    #[instrument(level = "trace")]
    pub async fn call_mut<Input>(
        &self,
        input: Input,
        timeout: Timeout,
    ) -> Result<<State as ServiceMut<Input>>::Output, routed::Error>
    where
        State: ServiceMut<Input> + fmt::Debug + Send + Sync + 'static,
        Input: fmt::Debug + Send + 'static,
        <State as ServiceMut<Input>>::Output: Send,
    {
        let (call, rsp_rx) = routed::Mut::new(input, timeout);
        self.call_routed(Box::new(call), rsp_rx).await
    }
}

impl<State> fmt::Debug for Handle<State>
where
    State: Router,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "router handle")
    }
}

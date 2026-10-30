use std::{
    fmt, marker,
    sync::{
        Arc,
        atomic::{self, AtomicBool},
    },
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
    alive: Arc<AtomicBool>,
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
        alive: Arc<AtomicBool>,
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

impl<State> Handle<State>
where
    State: Router,
{
    pub async fn shutdown(self) -> <State as RouterBase>::ShutdownResult {
        assert!(
            self.alive.load(atomic::Ordering::Relaxed),
            "shutdown called on router handle for which router was already shut down"
        );
        // FIXME: TOCTOU for alive check
        let ShutdownChannels {
            shutdown_tx,
            shutdown_done_rx,
        } = self.shutdown_channels.write().await.take().unwrap();

        shutdown_tx
            .send(())
            .expect("unable to transmit shutdown message to router");

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
        if !self.alive.load(atomic::Ordering::Relaxed) {
            return Err(routed::Error::RouterShutdown);
        }
        // FIXME: TOCTOU for alive check

        self.cmd_tx.send(routed).await.unwrap();
        rsp_rx.await.map_err(routed::Error::RecvError)?
    }

    pub fn notify(
        &self,
        notif: <State as RouterBase>::Notif,
    ) -> Result<(), mpsc::error::SendError<<State as RouterBase>::Notif>> {
        #[allow(clippy::manual_assert)]
        if !self.alive.load(atomic::Ordering::Relaxed) {
            // FUTURE: return error here
            panic!("router was shut down")
        }
        // FIXME: TOCTOU for alive check
        self.notif_tx.blocking_send(notif)
    }

    pub async fn notify_async(
        &self,
        notif: <State as RouterBase>::Notif,
    ) -> Result<(), mpsc::error::SendError<<State as RouterBase>::Notif>> {
        #[allow(clippy::manual_assert)]
        if !self.alive.load(atomic::Ordering::Relaxed) {
            // FUTURE: return error here
            panic!("router was shut down")
        }
        // FIXME: TOCTOU for alive check
        self.notif_tx.send(notif).await
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
        if !self.alive.load(atomic::Ordering::Relaxed) {
            return Err(routed::Error::RouterShutdown);
        }

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
        if !self.alive.load(atomic::Ordering::Relaxed) {
            return Err(routed::Error::RouterShutdown);
        }

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

use std::{
    fmt, marker,
    sync::{Arc, atomic::AtomicBool},
};

use tokio::sync::{RwLock, mpsc, oneshot};
use tracing::instrument;

use crate::{
    Service, ServiceMut,
    router::{
        Router, RouterBase,
        routed::{self, RoutedCallError},
    },
};

struct ShutdownChannels<State>
where
    State: Router,
{
    shutdown_tx: oneshot::Sender<()>,
    shutdown_done_rx: oneshot::Receiver<<State as RouterBase>::ShutdownResult>,
}
pub struct RouterHandle<State>
where
    State: Router,
{
    cmd_tx: mpsc::Sender<Box<dyn routed::AnyCall<State>>>,
    notif_tx: mpsc::Sender<<State as RouterBase>::Notif>,
    shutdown_channels: Arc<RwLock<Option<ShutdownChannels<State>>>>,
    alive: Arc<AtomicBool>,
    _state: marker::PhantomData<State>,
}

impl<State> RouterHandle<State>
where
    State: Router,
{
    pub fn new(
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

impl<State> Clone for RouterHandle<State>
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

impl<State> RouterHandle<State>
where
    State: Router,
{
    pub async fn shutdown(self) -> <State as RouterBase>::ShutdownResult {
        if !self.alive.load(std::sync::atomic::Ordering::Relaxed) {
            panic!("shutdown called on router handle for which router was already shut down");
        }
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
        rsp_rx: oneshot::Receiver<Result<Output, RoutedCallError>>,
    ) -> Result<Output, RoutedCallError>
    where
        Output: fmt::Debug,
    {
        self.cmd_tx.send(routed).await.unwrap();
        let result = rsp_rx.await.expect("unable to rx routed call result");
        _pop_global_call!(State, format!("{:?}", result));
        result
    }

    pub async fn notify(&self, notif: <State as RouterBase>::Notif) {
        self.notif_tx.send(notif).await.expect("unable to tx notif");
    }

    /// Note that timeout is the *total* duration allowed for both acquiring the state handle *and* executing the call
    #[instrument(level = "trace")]
    pub async fn call<Input>(
        &self,
        input: Input,
        timeout: routed::RoutedCallTimeout,
    ) -> Result<<State as Service<Input>>::Output, RoutedCallError>
    where
        State: Service<Input> + fmt::Debug + Send + Sync + 'static,
        Input: fmt::Debug + Send + 'static,
        <State as Service<Input>>::Output: Send,
    {
        _push_global_call!(State, format!("{:?}", input));
        let (call, rsp_rx) = routed::Const::new(input, timeout);
        self.call_routed(Box::new(call), rsp_rx).await
    }

    #[instrument(level = "trace")]
    pub async fn call_mut<Input>(
        &self,
        input: Input,
        timeout: routed::RoutedCallTimeout,
    ) -> Result<<State as ServiceMut<Input>>::Output, RoutedCallError>
    where
        State: ServiceMut<Input> + fmt::Debug + Send + Sync + 'static,
        Input: fmt::Debug + Send + 'static,
        <State as ServiceMut<Input>>::Output: Send,
    {
        _push_global_call!(State, format!("{:?}", input));
        let (call, rsp_rx) = routed::Mut::new(input, timeout);
        self.call_routed(Box::new(call), rsp_rx).await
    }
}

impl<State> fmt::Debug for RouterHandle<State>
where
    State: Router,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "router handle")
    }
}

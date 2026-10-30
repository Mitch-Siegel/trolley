use std::{error, fmt, marker, sync::Arc};

use async_trait::async_trait;
use tokio::{
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard, oneshot},
    time::{self, Duration, Instant, timeout},
};
use tracing::{Instrument, Span};

use crate::{Service, ServiceMut};

/// Timeouts
// returns: Ok((output, ms_remaining)) or Err(Elapsed)
// TODO: make this a macro?
async fn await_with_timeout<F>(
    f: F,
    duration: Duration,
) -> Result<(<F as Future>::Output, Duration), time::error::Elapsed>
where
    F: Future,
{
    let start = Instant::now();
    match timeout(duration, f).await {
        Ok(result) => {
            let end = Instant::now();
            let remaining = duration
                .checked_sub(end - start)
                .expect("overflow in timeout calculation");
            Ok((result, remaining))
        }
        Err(e) => Err(e),
    }
}

#[async_trait]
pub trait AnyCall<State>: fmt::Debug + Send {
    async fn run(self: Box<Self>, state_handle: Arc<RwLock<State>>);

    fn span(&self) -> Span;
}

#[derive(Debug, Copy, Clone)]
pub enum RoutedCallTimeout {
    Single(Duration),
    Explicit {
        state_timeout: Duration,
        exec_timeout: Duration,
    },
}

#[derive(Debug)]
pub struct RoutedInner<Input, Output> {
    pub(crate) input: Input,
    pub(crate) rsp_tx: oneshot::Sender<Result<Output, RoutedCallError>>,
    pub(crate) timeout: RoutedCallTimeout,
    #[cfg(feature = "route_spans")]
    pub(crate) parent_span: tracing::Span,
}

impl<Input, Output> RoutedInner<Input, Output> {
    pub const fn new(
        input: Input,
        rsp_tx: oneshot::Sender<Result<Output, RoutedCallError>>,
        timeout: RoutedCallTimeout,
    ) -> Self {
        Self {
            input,
            rsp_tx,
            timeout,
            #[cfg(feature = "route_spans")]
            parent_span: Span::current(),
        }
    }
}

async fn acquire_state_read<State>(
    state_handle: &RwLock<State>,
    timeout: Duration,
) -> Result<(RwLockReadGuard<'_, State>, Duration), RoutedCallError>
where
    State: Sync + Send,
{
    if let Ok((state, remaining)) = await_with_timeout(state_handle.read(), timeout).await {
        Ok((state, remaining))
    } else {
        tracing::error!("timed out waiting for read lock on state handle");
        Err(RoutedCallError::Timeout(Timeout::RwLockTimeout(timeout)))
    }
}

async fn acquire_state_write<State>(
    state_handle: &RwLock<State>,
    timeout: Duration,
) -> Result<(RwLockWriteGuard<'_, State>, Duration), RoutedCallError>
where
    State: Sync + Send,
{
    if let Ok((state, remaining)) = await_with_timeout(state_handle.write(), timeout).await {
        Ok((state, remaining))
    } else {
        tracing::error!("timed out waiting for write lock on state handle");
        Err(RoutedCallError::Timeout(Timeout::RwLockTimeout(timeout)))
    }
}

async fn await_call_with_timeout<Output>(
    f: impl Future<Output = Output>,
    lock_time: Duration,
    timeout: Duration,
) -> Result<Output, RoutedCallError> {
    match await_with_timeout(f, timeout).await {
        Ok((output, _)) => Ok(output),
        Err(_e) => {
            let timeout_err = Timeout::ExecuteTimeout(lock_time, timeout);
            tracing::error!(%timeout_err);
            Err(RoutedCallError::Timeout(timeout_err))
        }
    }
}

#[derive(Debug)]
pub struct Const<State, Req>
where
    Req: fmt::Debug,
    State: Service<Req>,
{
    inner: RoutedInner<Req, <State as Service<Req>>::Output>,
    _state: marker::PhantomData<State>,
}

pub type RoutedCallResult<State, Input> =
    Result<<State as Service<Input>>::Output, RoutedCallError>;
pub type RoutedMutCallResult<State, Input> =
    Result<<State as ServiceMut<Input>>::Output, RoutedCallError>;

impl<State, Input> Const<State, Input>
where
    Input: fmt::Debug,
    State: Service<Input>,
{
    pub fn new(
        input: Input,
        timeout: RoutedCallTimeout,
    ) -> (Self, oneshot::Receiver<RoutedCallResult<State, Input>>) {
        let (rsp_tx, rsp_rx) = oneshot::channel();
        let routed = Self {
            inner: RoutedInner::new(input, rsp_tx, timeout),
            _state: marker::PhantomData,
        };
        (routed, rsp_rx)
    }
}

#[async_trait]
impl<State, Req> AnyCall<State> for Const<State, Req>
where
    Req: fmt::Debug + Send,
    State: Service<Req> + Send + Sync,
    <State as Service<Req>>::Output: Send,
{
    async fn run(self: Box<Self>, state_handle: Arc<RwLock<State>>) {
        let (state, lock_time, exec_timeout) = match self.inner.timeout {
            RoutedCallTimeout::Single(t) => match acquire_state_read(&state_handle, t).await {
                Ok((state, remaining)) => {
                    let lock_time = t
                        .checked_sub(remaining)
                        .expect("lock time exceeded total timeout time");
                    tracing::trace!(
                        "acquired state read handle in {:?}, {:?} remaining to execute call",
                        lock_time,
                        remaining
                    );
                    (state, lock_time, remaining)
                }
                Err(e) => {
                    self.inner
                        .rsp_tx
                        .send(Err(e))
                        .expect("unable to respond to routed call");
                    return;
                }
            },
            RoutedCallTimeout::Explicit {
                state_timeout,
                exec_timeout,
            } => match acquire_state_read(&state_handle, state_timeout).await {
                Ok((state, remaining)) => {
                    let lock_time = state_timeout
                        .checked_sub(remaining)
                        .expect("lock time exceeded state timeout time");
                    tracing::trace!(
                        "acquired state read handle in {:?}, {:?} to execute call",
                        lock_time,
                        state_timeout
                    );
                    (state, lock_time, exec_timeout)
                }
                Err(e) => {
                    self.inner
                        .rsp_tx
                        .send(Err(e))
                        .expect("unable to respond to routed call");
                    return;
                }
            },
        };

        let span = self.span();
        let result = await_call_with_timeout(
            state.call(self.inner.input).instrument(span),
            lock_time,
            exec_timeout,
        )
        .await;

        self.inner
            .rsp_tx
            .send(result)
            .expect("unable to respond to routed call");
    }

    fn span(&self) -> tracing::Span {
        #[cfg(feature = "route_spans")]
        return self.inner.parent_span.clone();
        #[cfg(not(feature = "route_spans"))]
        return tracing::Span::current();
    }
}

#[derive(Debug)]
pub struct Mut<State, Input>
where
    Input: fmt::Debug,
    State: ServiceMut<Input>,
{
    inner: RoutedInner<Input, <State as ServiceMut<Input>>::Output>,
    _state: marker::PhantomData<State>,
}

impl<State, Input> Mut<State, Input>
where
    Input: fmt::Debug,
    State: ServiceMut<Input>,
{
    pub fn new(
        input: Input,
        timeout: RoutedCallTimeout,
    ) -> (Self, oneshot::Receiver<RoutedMutCallResult<State, Input>>) {
        let (rsp_tx, rsp_rx) = oneshot::channel();
        let routed = Self {
            inner: RoutedInner::new(input, rsp_tx, timeout),
            _state: marker::PhantomData,
        };
        (routed, rsp_rx)
    }
}

#[async_trait]
impl<State, Input> AnyCall<State> for Mut<State, Input>
where
    Input: fmt::Debug + Send,
    State: ServiceMut<Input> + Send + Sync,
    <State as ServiceMut<Input>>::Output: Send,
{
    async fn run(self: Box<Self>, state_handle: Arc<RwLock<State>>) {
        let (mut state, lock_time, exec_timeout) = match self.inner.timeout {
            RoutedCallTimeout::Single(t) => match acquire_state_write(&state_handle, t).await {
                Ok((state, remaining)) => {
                    let lock_time = t
                        .checked_sub(remaining)
                        .expect("lock time exceeded total timeout time");
                    tracing::trace!(
                        "acquired state write handle in {:?}, {:?} remaining to execute call",
                        lock_time,
                        remaining
                    );
                    (state, lock_time, remaining)
                }
                Err(e) => {
                    self.inner
                        .rsp_tx
                        .send(Err(e))
                        .expect("unable to respond to routed call");
                    return;
                }
            },
            RoutedCallTimeout::Explicit {
                state_timeout,
                exec_timeout,
            } => match acquire_state_write(&state_handle, state_timeout).await {
                Ok((state, remaining)) => {
                    let lock_time = state_timeout
                        .checked_sub(remaining)
                        .expect("lock time exceeded state timeout time");
                    tracing::trace!(
                        "acquired state write handle in {:?}, {:?} to execute call",
                        lock_time,
                        state_timeout
                    );
                    (state, lock_time, exec_timeout)
                }
                Err(e) => {
                    self.inner
                        .rsp_tx
                        .send(Err(e))
                        .expect("unable to respond to routed call");
                    return;
                }
            },
        };

        let span = self.span();
        let result = await_call_with_timeout(
            state.call(self.inner.input).instrument(span),
            lock_time,
            exec_timeout,
        )
        .await;

        self.inner
            .rsp_tx
            .send(result)
            .expect("unable to respond to routed call");
    }

    fn span(&self) -> Span {
        #[cfg(feature = "route_spans")]
        return self.inner.parent_span.clone();
        #[cfg(not(feature = "route_spans"))]
        return tracing::Span::current();
    }
}

/// automatic wrapping of services with construction-time type erasure

#[derive(Debug)]
pub enum Timeout {
    RwLockTimeout(Duration),
    ExecuteTimeout(Duration, Duration),
}

impl error::Error for Timeout {}

impl fmt::Display for Timeout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RwLockTimeout(duration) => {
                write!(f, "timed out waiting for rwlock on state ({duration:?})")
            }
            Self::ExecuteTimeout(lock_duration, execution_duration) => write!(
                f,
                "timed out executing call ({lock_duration:?} spent acquiring lock, {execution_duration:?} spent in call)",
            ),
        }
    }
}

#[derive(Debug)]
pub enum RoutedCallError {
    Timeout(Timeout),
    RouterShutdown,
    RecvError(oneshot::error::RecvError),
}

impl fmt::Display for RoutedCallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout(t) => write!(f, "{t}"),
            Self::RouterShutdown => write!(f, "router was shut down"),
            Self::RecvError(e) => write!(f, "unable to receive response: {e}"),
        }
    }
}

impl error::Error for RoutedCallError {}

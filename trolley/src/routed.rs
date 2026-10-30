use std::{fmt, marker, sync::Arc};

use async_trait::async_trait;
use tokio::{
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard, oneshot},
    time::{self, Duration, Instant, timeout},
};
use tracing::{Instrument, Span};

use crate::{Service, ServiceMut};

mod error;

pub use error::Error;

#[derive(Debug, Copy, Clone)]
pub enum Timeout {
    Single(Duration),
    Explicit {
        state_timeout: Duration,
        exec_timeout: Duration,
    },
}

impl Timeout {
    #[must_use]
    pub const fn state_timeout(self) -> Duration {
        match self {
            Self::Single(duration) => duration,
            Self::Explicit { state_timeout, .. } => state_timeout,
        }
    }
}

/// Timeouts
// returns: Ok((output, ms_remaining)) or Err(Elapsed)
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
pub(crate) trait AnyCall<State>: fmt::Debug + Send {
    async fn run(self: Box<Self>, state_handle: Arc<RwLock<State>>);

    fn span(&self) -> Span;
}

#[derive(Debug)]
struct RoutedInner<Input, Output> {
    pub input: Input,
    pub rsp_tx: oneshot::Sender<Result<Output, Error>>,
    pub timeout: Timeout,
    #[cfg(feature = "route_spans")]
    pub parent_span: tracing::Span,
}

impl<Input, Output> RoutedInner<Input, Output> {
    const fn new(
        input: Input,
        rsp_tx: oneshot::Sender<Result<Output, Error>>,
        timeout: Timeout,
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
) -> Result<(RwLockReadGuard<'_, State>, Duration), Error>
where
    State: Sync + Send,
{
    if let Ok((state, remaining)) = await_with_timeout(state_handle.read(), timeout).await {
        Ok((state, remaining))
    } else {
        tracing::error!("timed out waiting for read lock on state handle");
        Err(Error::RwLockTimeout(timeout))
    }
}

async fn acquire_state_write<State>(
    state_handle: &RwLock<State>,
    duration: Duration,
) -> Result<(RwLockWriteGuard<'_, State>, Duration), Error>
where
    State: Sync + Send,
{
    if let Ok((state, remaining)) = await_with_timeout(state_handle.write(), duration).await {
        Ok((state, remaining))
    } else {
        tracing::error!("timed out waiting for write lock on state handle");
        Err(Error::RwLockTimeout(duration))
    }
}

async fn await_call_with_timeout<Output>(
    f: impl Future<Output = Output>,
    lock_time: Duration,
    duration: Duration,
) -> Result<Output, Error> {
    match await_with_timeout(f, duration).await {
        Ok((output, _)) => Ok(output),
        Err(_e) => {
            let timeout_err = Error::ExecuteTimeout(lock_time, duration);
            tracing::error!(%timeout_err);
            Err(timeout_err)
        }
    }
}

/// Acquires state guard within the timeout, taking the future of the in-flight acquisition
/// Returns `(guard, time spent acquiring the lock, time available to execute the call)`.
async fn acquire_guard_within_timeout<Guard>(
    acquire: impl Future<Output = Result<(Guard, Duration), Error>>,
    timeout: Timeout,
) -> Result<(Guard, Duration, Duration), Error> {
    let (guard, remaining) = acquire.await?;
    let state_timeout = timeout.state_timeout();
    let lock_time = state_timeout
        .checked_sub(remaining)
        .expect("lock time exceeded state timeout time");

    let exec_timeout = match timeout {
        Timeout::Single(_) => remaining,
        Timeout::Explicit { exec_timeout, .. } => exec_timeout,
    };

    tracing::trace!(
        "acquired state guard in {:?}, {:?} to execute call",
        lock_time,
        exec_timeout
    );
    Ok((guard, lock_time, exec_timeout))
}

#[derive(Debug)]
pub(crate) struct Const<State, Req>
where
    Req: fmt::Debug,
    State: Service<Req>,
{
    inner: RoutedInner<Req, <State as Service<Req>>::Output>,
    _state: marker::PhantomData<State>,
}

pub(crate) type RoutedCallResult<State, Input> = Result<<State as Service<Input>>::Output, Error>;
pub(crate) type RoutedMutCallResult<State, Input> =
    Result<<State as ServiceMut<Input>>::Output, Error>;

impl<State, Input> Const<State, Input>
where
    Input: fmt::Debug,
    State: Service<Input>,
{
    pub(crate) fn new(
        input: Input,
        timeout: Timeout,
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
        let (state, lock_time, exec_timeout) = match acquire_guard_within_timeout(
            acquire_state_read(&state_handle, self.inner.timeout.state_timeout()),
            self.inner.timeout,
        )
        .await
        {
            Ok(acquired) => acquired,
            Err(e) => {
                self.inner
                    .rsp_tx
                    .send(Err(e))
                    .expect("unable to respond to routed call");
                return;
            }
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
pub(crate) struct Mut<State, Input>
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
    pub(crate) fn new(
        input: Input,
        timeout: Timeout,
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
        let (mut state, lock_time, exec_timeout) = match acquire_guard_within_timeout(
            acquire_state_write(&state_handle, self.inner.timeout.state_timeout()),
            self.inner.timeout,
        )
        .await
        {
            Ok(acquired) => acquired,
            Err(e) => {
                self.inner
                    .rsp_tx
                    .send(Err(e))
                    .expect("unable to respond to routed call");
                return;
            }
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

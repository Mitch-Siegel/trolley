use std::fmt;

use tokio::time::Duration;

use crate::{Router, Service, ServiceMut, routed, router};

#[derive(Debug, Copy, Clone)]
pub struct CallCtx<'a, State>
where
    State: Router,
{
    handle: &'a router::Handle<State>,
    timeout: routed::Timeout,
}

impl<'a, State> CallCtx<'a, State>
where
    State: Router,
{
    #[must_use]
    pub const fn single_timeout(handle: &'a router::Handle<State>, timeout: Duration) -> Self {
        Self {
            handle,
            timeout: routed::Timeout::Single(timeout),
        }
    }

    #[must_use]
    pub const fn explicit_timeouts(
        handle: &'a router::Handle<State>,
        state_timeout: Duration,
        exec_timeout: Duration,
    ) -> Self {
        Self {
            handle,
            timeout: routed::Timeout::Explicit {
                state_timeout,
                exec_timeout,
            },
        }
    }

    #[must_use]
    pub const fn with_retries(self, retries: u32) -> RetryCallCtx<'a, State> {
        RetryCallCtx {
            handle: self.handle,
            timeout: self.timeout,
            retries,
        }
    }

    pub async fn call<Input>(
        &self,
        input: Input,
    ) -> Result<<State as Service<Input>>::Output, routed::Error>
    where
        State: Service<Input>,
        Input: fmt::Debug + Send + 'static,
        <State as Service<Input>>::Output: Send,
    {
        self.handle.call(input, self.timeout).await
    }

    pub async fn call_mut<Input>(
        &self,
        input: Input,
    ) -> Result<<State as ServiceMut<Input>>::Output, routed::Error>
    where
        State: ServiceMut<Input>,
        Input: fmt::Debug + Send + 'static,
        <State as ServiceMut<Input>>::Output: Send,
    {
        self.handle.call_mut(input, self.timeout).await
    }
}

#[derive(Debug)]
pub struct RetryCallCtx<'a, State>
where
    State: Router,
{
    handle: &'a router::Handle<State>,
    timeout: routed::Timeout,
    retries: u32,
}
impl<State> RetryCallCtx<'_, State>
where
    State: Router,
{
    pub async fn call<Input>(
        &self,
        input: Input,
    ) -> Result<<State as Service<Input>>::Output, routed::Error>
    where
        State: Service<Input>,
        Input: Clone + fmt::Debug + Send + Sync + 'static,
        <State as Service<Input>>::Output: Send,
    {
        loop {
            let try_output = self.handle.call(input.clone(), self.timeout).await;
            match &try_output {
                Ok(_) | Err(routed::Error::RouterShutdown) => break try_output,
                Err(_) => {
                    if self.retries > 0 {
                        continue;
                    }
                    break try_output;
                }
            }
        }
    }

    pub async fn call_mut<Input>(
        &self,
        input: Input,
    ) -> Result<<State as ServiceMut<Input>>::Output, routed::Error>
    where
        State: ServiceMut<Input>,
        Input: Clone + fmt::Debug + Send + Sync + 'static,
        <State as ServiceMut<Input>>::Output: Send,
    {
        loop {
            let try_output = self.handle.call_mut(input.clone(), self.timeout).await;
            match &try_output {
                Ok(_) | Err(routed::Error::RouterShutdown) => break try_output,
                Err(_) => {
                    if self.retries > 0 {
                        continue;
                    }
                    break try_output;
                }
            }
        }
    }
}

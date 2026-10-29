#[cfg(feature = "build")]
use quote::quote;
#[cfg(feature = "build")]
use std::{fs, path};

pub mod interface;
pub mod router;
pub mod traits;

pub use router::routed::{RoutedCallError, RoutedCallTimeout, Timeout};
pub use router::{
    EventReceiver, EventSender, Router, RouterBase, RouterCtx, RouterHandle, RouterMeta, RouterPriv,
};
pub use trolley_macros::{Router, interface, router_meta};

pub use traits::{InfallibleService, InfallibleServiceMut, Service, ServiceMut};

pub mod error {
    pub use crate::router::routed::Timeout;
}

#[macro_export]
macro_rules! include_generated {
    ($filename:literal) => {
        #[cfg(not(feature = "build"))]
        include!(concat!(env!("OUT_DIR"), $filename))
    };
}

#[cfg(feature = "build")]
pub fn generate_call_ctx(relative_out_path: &path::Path) {
    let generated = quote! {
        #[derive(Debug, Copy, Clone)]
        pub struct CallCtx<'a, State>
        where State: ::trolley::Router
        {
            handle: &'a ::trolley::RouterHandle<State>,
            timeout: ::trolley::RoutedCallTimeout,
        }

        impl<'a, State> CallCtx<'a, State>
        where State: ::trolley::Router
        {
            pub fn single_timeout(handle: &'a ::trolley::RouterHandle<State>, timeout: ::tokio::time::Duration) -> Self {
                Self {
                    handle,
                    timeout: ::trolley::RoutedCallTimeout::Single(timeout),
                }
            }

            pub fn explicit_timeouts(handle: &'a ::trolley::RouterHandle<State>, state_timeout: ::tokio::time::Duration, exec_timeout: ::tokio::time::Duration) -> Self {
                Self {
                    handle,
                    timeout: ::trolley::RoutedCallTimeout::Explicit{state_timeout, exec_timeout},
                }
            }

            pub fn with_retries(self, retries: u32) -> RetryCallCtx<'a, State> {
                RetryCallCtx {
                    handle: self.handle,
                    timeout: self.timeout,
                    retries
                }
            }

            pub fn panicky(self) -> PanickyCallCtx<'a, State> {
                PanickyCallCtx {
                    handle: self.handle,
                    timeout: self.timeout,
                }
            }

            pub async fn call<Input>(
                &self,
                input: Input
            ) -> ::std::result::Result<<State as ::trolley::Service<Input>>::Output, ::trolley::RoutedCallError>
            where
                State: ::trolley::Service<Input>,
                Input: ::std::fmt::Debug + Send + 'static,
                <State as ::trolley::Service<Input>>::Output: Send,
                {
                    self.handle.call(input, self.timeout).await
                }

            pub async fn call_mut<Input>(
                &self,
                input: Input
            ) -> ::std::result::Result<<State as ::trolley::ServiceMut<Input>>::Output, ::trolley::RoutedCallError>
            where
                State: ::trolley::ServiceMut<Input>,
                Input: ::std::fmt::Debug + Send + 'static,
                <State as ::trolley::ServiceMut<Input>>::Output: Send,
                {
                    self.handle.call_mut(input, self.timeout).await
                }
            }

        #[derive(Debug)]
        pub struct RetryCallCtx<'a, State>
        where State: ::trolley::Router
        {
            handle: &'a ::trolley::RouterHandle<State>,
            timeout: ::trolley::RoutedCallTimeout,
            retries: u32,
        }
        impl<'a, State> RetryCallCtx<'a, State>
        where State: ::trolley::Router
        {
            pub async fn call<Input>(
                &self,
                input: Input
            ) -> ::std::result::Result<<State as ::trolley::Service<Input>>::Output, ::trolley::RoutedCallError>
            where
                State: ::trolley::Service<Input>,
                Input: Clone + ::std::fmt::Debug + Send + Sync + 'static,
                <State as ::trolley::Service<Input>>::Output: Send,
                {
                loop {
                    let try_output =  self.handle.call(input.clone(), self.timeout).await;
                    match &try_output {
                        Ok(_) => break try_output,
                        Err(::trolley::RoutedCallError::RouterShutdown) => break try_output,
                        Err(_) => {
                            if self.retries > 0 {
                                continue;
                            } else {
                                break try_output;
                            }
                        }
                    }
                }
            }

            pub async fn call_mut<Input>(
                &self,
                input: Input
            ) -> ::std::result::Result<<State as ::trolley::ServiceMut<Input>>::Output, ::trolley::RoutedCallError>
            where
                State: ::trolley::ServiceMut<Input>,
                Input: Clone + ::std::fmt::Debug + Send + Sync + 'static,
                <State as ::trolley::ServiceMut<Input>>::Output: Send,
                {
                loop {
                    let try_output =  self.handle.call_mut(input.clone(), self.timeout).await;
                    match &try_output {
                        Ok(_) => break try_output,
                        Err(::trolley::RoutedCallError::RouterShutdown) => break try_output,
                        Err(_) => {
                            if self.retries > 0 {
                                continue;
                            } else {
                                break try_output;
                            }
                        }
                    }
                }
            }
        }

        #[derive(Debug)]
        pub struct PanickyCallCtx<'a, State>
        where State: ::trolley::Router
        {
            handle: &'a ::trolley::RouterHandle<State>,
            timeout: ::trolley::RoutedCallTimeout,
        }
        impl<'a, State> PanickyCallCtx<'a, State>
        where State: ::trolley::Router
        {
            pub async fn call<Input>(&self, input: Input) -> <State as ::trolley::Service<Input>>::Output
            where
                State: ::trolley::Service<Input>,
                Input: ::std::fmt::Debug + Send + 'static,
                <State as ::trolley::Service<Input>>::Output: Send,
                {
                    self.handle.call(input, self.timeout).await.unwrap()
                }

            pub async fn call_mut<Input>(
                &self,
                input: Input
            ) -> <State as ::trolley::ServiceMut<Input>>::Output
            where
                State: ::trolley::ServiceMut<Input>,
                Input: ::std::fmt::Debug + Send + 'static,
                <State as ::trolley::ServiceMut<Input>>::Output: Send,
                {
                    self.handle.call_mut(input, self.timeout).await.unwrap()
                }
        }
    };

    fs::write(
        std::path::PathBuf::from(std::env::var("OUT_DIR").expect("unable to get OUT_DIR"))
            .join(relative_out_path),
        generated.to_string(), // prettyplease::unparse(&syn::parse_file(&generated.to_string()).unwrap()),
    )
    .expect("failed to write generated file");
}

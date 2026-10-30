#[cfg(feature = "build")]
use proc_macro2::TokenStream;
#[cfg(feature = "build")]
use quote::quote;
#[cfg(feature = "build")]
use std::{env, fs, path};

pub mod events;
pub mod interface;
pub mod routed;
pub mod router;
pub mod traits;

pub use router::{Router, RouterBase, RouterMeta, RouterPriv};
pub use trolley_macros::{Router, interface, router_meta};

pub use traits::{Service, ServiceMut};

#[macro_export]
macro_rules! include_generated {
    ($filename:literal) => {
        #[cfg(not(feature = "build"))]
        include!(concat!(env!("OUT_DIR"), $filename))
    };
}

#[cfg(feature = "build")]
fn generate_call_ctx() -> TokenStream {
    quote! {
    #[derive(Debug, Copy, Clone)]
    pub struct CallCtx<'a, State>
    where State: ::trolley::Router
    {
        handle: &'a ::trolley::router::Handle<State>,
        timeout: ::trolley::routed::Timeout,
    }

    impl<'a, State> CallCtx<'a, State>
    where State: ::trolley::Router
    {
        #[must_use]
        pub const fn single_timeout(handle: &'a ::trolley::router::Handle<State>, timeout: ::tokio::time::Duration) -> Self {
            Self {
                handle,
                timeout: ::trolley::routed::Timeout::Single(timeout),
            }
        }

        #[must_use]
        pub const fn explicit_timeouts(handle: &'a ::trolley::router::Handle<State>, state_timeout: ::tokio::time::Duration, exec_timeout: ::tokio::time::Duration) -> Self {
            Self {
                handle,
                timeout: ::trolley::routed::Timeout::Explicit{state_timeout, exec_timeout},
            }
        }

        #[must_use]
        pub const fn with_retries(self, retries: u32) -> RetryCallCtx<'a, State> {
            RetryCallCtx {
                handle: self.handle,
                timeout: self.timeout,
                retries
            }
        }

        pub async fn call<Input>(
            &self,
            input: Input
        ) -> ::std::result::Result<<State as ::trolley::Service<Input>>::Output, ::trolley::routed::Error>
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
        ) -> ::std::result::Result<<State as ::trolley::ServiceMut<Input>>::Output, ::trolley::routed::Error>
        where
            State: ::trolley::ServiceMut<Input>,
            Input: ::std::fmt::Debug + Send + 'static,
            <State as ::trolley::ServiceMut<Input>>::Output: Send,
            {
                self.handle.call_mut(input, self.timeout).await
            }
        }
    }
}

#[cfg(feature = "build")]
fn generate_retry_call_ctx() -> TokenStream {
    quote! {
    #[derive(Debug)]
    pub struct RetryCallCtx<'a, State>
    where State: ::trolley::Router
    {
        handle: &'a ::trolley::router::Handle<State>,
        timeout: ::trolley::routed::Timeout,
        retries: u32,
    }
    impl<State> RetryCallCtx<'_, State>
    where State: ::trolley::Router
    {
        pub async fn call<Input>(
            &self,
            input: Input
        ) -> ::std::result::Result<<State as ::trolley::Service<Input>>::Output, ::trolley::routed::Error>
        where
            State: ::trolley::Service<Input>,
            Input: Clone + ::std::fmt::Debug + Send + Sync + 'static,
            <State as ::trolley::Service<Input>>::Output: Send,
            {
            loop {
                let try_output =  self.handle.call(input.clone(), self.timeout).await;
                match &try_output {
                    Ok(_) |
                    Err(::trolley::routed::Error::RouterShutdown) => break try_output,
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
            input: Input
        ) -> ::std::result::Result<<State as ::trolley::ServiceMut<Input>>::Output, ::trolley::routed::Error>
        where
            State: ::trolley::ServiceMut<Input>,
            Input: Clone + ::std::fmt::Debug + Send + Sync + 'static,
            <State as ::trolley::ServiceMut<Input>>::Output: Send,
            {
            loop {
                let try_output =  self.handle.call_mut(input.clone(), self.timeout).await;
                match &try_output {
                    Ok(_) |
                    Err(::trolley::routed::Error::RouterShutdown) => break try_output,
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
    }
}

#[cfg(feature = "build")]
pub fn generate_call_ctxs(relative_out_path: &path::Path) {
    let regular = generate_call_ctx();
    let retry = generate_retry_call_ctx();

    let generated = quote! {
        #regular
        #retry
    };

    fs::write(
        path::PathBuf::from(env::var("OUT_DIR").expect("unable to get OUT_DIR"))
            .join(relative_out_path),
        generated.to_string(), // prettyplease::unparse(&syn::parse_file(&generated.to_string()).unwrap()),
    )
    .expect("failed to write generated file");
}

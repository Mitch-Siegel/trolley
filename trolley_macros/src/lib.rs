use proc_macro::TokenStream;

mod router;
mod service;

#[proc_macro_attribute]
pub fn interface(attribute_args: TokenStream, item: TokenStream) -> TokenStream {
    service::interface(&attribute_args.into(), item.into(), true).into()
}

#[proc_macro_derive(Router, attributes(router_events, params))]
pub fn router(item: TokenStream) -> TokenStream {
    router::router(item.into()).into()
}

#[proc_macro_attribute]
pub fn router_meta(attribute_args: TokenStream, item: TokenStream) -> TokenStream {
    router::router_meta(attribute_args.into(), item.into()).into()
}

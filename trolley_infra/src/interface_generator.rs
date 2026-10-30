use std::path;

use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::spanned::Spanned;

use crate::{ServiceMethodInfo, collect_service_impls, extract_method_info};

pub struct GenericsTokenStreams {
    pub generic_params: Vec<TokenStream>,
    pub impl_generics: TokenStream,
    pub ty_generics: TokenStream,
    pub maybe_where_clause: Option<TokenStream>,
}

impl From<syn::Generics> for GenericsTokenStreams {
    fn from(value: syn::Generics) -> Self {
        let (impl_generics, ty_generics, maybe_where_clause) = value.split_for_impl();
        Self {
            generic_params: value.params.iter().map(ToTokens::to_token_stream).collect(),
            impl_generics: impl_generics.to_token_stream(),
            ty_generics: ty_generics.to_token_stream(),
            maybe_where_clause: maybe_where_clause.map(ToTokens::to_token_stream),
        }
    }
}

pub struct ServiceInterface {
    pub self_ty: TokenStream,
    // generics including wheres from the interface impl blocks
    // allows generated code to reproduce any bounds from the router type
    pub generics: GenericsTokenStreams,
    pub methods: Vec<ServiceMethodInfo>,
}

/// Extracts service interfaces from the given paths.
#[must_use]
pub fn extract_interfaces(paths: &[&path::Path]) -> Vec<ServiceInterface> {
    let impls: Vec<(String, syn::ItemImpl)> = paths
        .iter()
        .flat_map(|path| collect_service_impls(path))
        .collect();

    let mut interfaces = Vec::new();

    for (src_file, implementation) in impls {
        let mut methods = Vec::new();
        for item in &implementation.items {
            let method = match item {
                syn::ImplItem::Fn(method) => method,
                other => panic!(
                    "unexpected item in service impl {:?}:{:?}",
                    src_file,
                    other.span().start()
                ),
            };

            // TODO: point to a specific source location
            methods.push(extract_method_info(method).unwrap_or_else(|_| {
                panic!(
                    "failed to extract method info ({}:{}:{})",
                    method.span().file(),
                    method.span().start().line,
                    method.span().start().column,
                )
            }));
        }

        let self_ty = &implementation.self_ty;

        interfaces.push(ServiceInterface {
            self_ty: quote! {#self_ty},
            generics: implementation.generics.into(),
            methods,
        });
    }

    interfaces
}

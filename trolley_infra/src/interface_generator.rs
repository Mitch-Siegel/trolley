use std::path;

use proc_macro2::TokenStream;
use quote::quote;
use syn::spanned::Spanned;

use crate::{ServiceMethodInfo, collect_service_impls, extract_method_info};

pub struct ServiceInterface {
    pub self_ty: TokenStream,
    // generics including wheres from the interface impl blocks
    // allows generated code to reproduce any bounds from the router type
    pub generics: syn::Generics,
    pub methods: Vec<ServiceMethodInfo>,
}

pub fn extract_interfaces(paths: &[&path::Path]) -> Vec<ServiceInterface> {
    let impls: Vec<(String, syn::ItemImpl)> = paths
        .iter()
        .flat_map(|path| collect_service_impls(path))
        .collect();

    let mut interfaces = Vec::new();

    for (src_file, implementation) in impls {
        let mut methods = Vec::new();
        for item in implementation.items.iter() {
            let method = match item {
                syn::ImplItem::Fn(method) => method,
                _other => panic!(
                    "unexpected item in service impl {:?}:{:?}",
                    src_file,
                    _other.span().start()
                ),
            };

            // TODO: point to a specific source location
            methods.push(extract_method_info(method).expect(&format!(
                "failed to extract method info ({}:{}:{})",
                method.span().file(),
                method.span().start().line,
                method.span().start().column,
            )));
        }

        let self_ty = &implementation.self_ty;

        interfaces.push(ServiceInterface {
            self_ty: quote! {#self_ty},
            generics: implementation.generics.clone(),
            methods,
        })
    }

    interfaces
}

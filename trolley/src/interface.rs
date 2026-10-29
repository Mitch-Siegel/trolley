use std::{collections::HashMap, fs, path};

use quote::{format_ident, quote};

pub fn generator(
    paths: &[&path::Path],
    relative_out_path: &path::Path,
    call_ctx_module_crate_path: &[&str],
) {
    let ctx_path_idents: Vec<proc_macro2::Ident> = call_ctx_module_crate_path
        .iter()
        .map(|segment| format_ident!("{}", segment))
        .collect();
    let call_ctx = quote! {
    crate::#( #ctx_path_idents )::*::CallCtx
        };
    // let retry_call_ctx = quote! {
    // crate::#( #ctx_path_idents )::*::RetryCallCtx
    //     };
    let panicky_call_ctx = quote! {
    crate::#( #ctx_path_idents )::*::PanickyCallCtx
        };

    let interfaces = trolley_infra::interface_generator::extract_interfaces(paths);

    let mut generated = Vec::new();

    let mut generated_methods = Vec::new();

    let mut router_types = HashMap::new();

    for trolley_infra::interface_generator::ServiceInterface {
        self_ty,
        generics,
        methods,
    } in interfaces
    {
        let (_, ty_generics, where_clause) = generics.split_for_impl();
        let generic_params: Vec<proc_macro2::TokenStream> = generics
            .params
            .iter()
            .map(|param| quote! {#param})
            .collect();

        for trolley_infra::ServiceMethodInfo {
            name,
            sig_ident_span: _,
            mutness,
            asyncness: _,
            args,
            maybe_ret,
            struct_name,
        } in methods
        {
            let (arg_names, arg_types): (Vec<proc_macro2::Ident>, Vec<proc_macro2::TokenStream>) =
                args.into_iter().unzip();

            let router_call_method = if mutness {
                format_ident!("call_mut")
            } else {
                format_ident!("call")
            };

            let ret_ty = match maybe_ret {
                Some(ty) => quote! {#ty},
                None => quote! {()},
            };

            let router_trait = if mutness {
                quote! {::trolley::ServiceMut}
            } else {
                quote! {::trolley::Service}
            };

            // fire the entire struct (65% more struct per struct)
            let full_struct_target = quote! { #struct_name #ty_generics };

            // expression for entire struct with name, generic params, and param names
            let full_struct_expr = if generic_params.is_empty() {
                quote! { #struct_name { #( #arg_names ),* } }
            } else {
                quote! { #struct_name::<#( #generic_params ),*> { #( #arg_names ),* } }
            };

            // actual site at which methods are implemented by name for various call contexts
            // FUTURE: retry call ctx actually implement names, but only for arg types that implement clone?
            let call_ctx_method: proc_macro2::TokenStream = quote! {
                impl<'a #(, #generic_params)*> #call_ctx<'a, #self_ty> #where_clause {
                    pub async fn #name(&self, #( #arg_names: #arg_types ),* ) ->
                        ::std::result::Result<#ret_ty, ::trolley::RoutedCallError>
                    where #self_ty: #router_trait<#full_struct_target, Output = #ret_ty>
                    {
                        let input = #full_struct_expr;
                        self.#router_call_method(input).await
                    }
                }

                // impl<'a> #retry_call_ctx<'a, #self_ty> {
                //     pub async fn #method_name(&self, #( #arg_names: #arg_types ),* ) ->
                //         ::std::result::Result<#ret_ty, ::trolley::RoutedCallError>
                //     where #self_ty: #router_trait<#full_struct_target, Output = #ret_ty>
                //     {
                //         let input = #full_struct_expr;
                //         self.#router_call_method(input).await
                //     }
                // }

                impl<'a #(, #generic_params)*> #panicky_call_ctx<'a, #self_ty> #where_clause {
                    pub async fn #name(&self, #( #arg_names: #arg_types ),* ) ->
                        #ret_ty
                    where #self_ty: #router_trait<#full_struct_target, Output = #ret_ty>
                    {
                        let input = #full_struct_expr;
                        self.#router_call_method(input).await
                    }
                }
            };

            generated_methods.push(call_ctx_method);
        }

        // needs to be after the method loop, impl_generics/ty_generics/where_clause borrow from generics
        router_types.insert(self_ty.to_string(), generics);
    }

    // TODO: un-hack the proc macro fields registry to pass params directly to spawn()
    for (ty_string, generics) in router_types {
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        let tokens: proc_macro2::TokenStream = ty_string
            .parse()
            .expect("Invalid token string for router type");

        let mut base_segments = Vec::new();
        let mut has_generics = false;

        for token in tokens {
            if !has_generics {
                if let proc_macro2::TokenTree::Punct(ref p) = token {
                    if p.as_char() == '<' {
                        has_generics = true;
                    }
                }
            }

            if !has_generics {
                base_segments.push(token);
            }
        }

        let last_ident = base_segments
            .iter()
            .rev()
            .find_map(|token| {
                if let proc_macro2::TokenTree::Ident(ident) = token {
                    Some(ident.clone())
                } else {
                    None
                }
            })
            .expect("Unable to find a valid base identifier in router type");

        let handle_type_name = format_ident!("{}Handle", last_ident);
        let base_path_stream: proc_macro2::TokenStream = base_segments.into_iter().collect();

        // be explicit. be very explicit.
        let generated_handle = quote! {
            impl #impl_generics #base_path_stream #ty_generics #where_clause {
                pub async fn spawn( meta: <Self as ::trolley::RouterPriv>::Meta, n_concurrent: u32) -> (#handle_type_name #ty_generics, ::trolley::EventReceiver<<#base_path_stream #ty_generics as ::trolley::RouterBase>::Event>) {
                    let (raw_handle, event_rx) =
                        <Self as ::trolley::Router>::spawn_from_meta(meta, n_concurrent).await;

                    (#handle_type_name::from(raw_handle), event_rx)
                }
            }

            pub struct #handle_type_name #ty_generics (::trolley::RouterHandle<#base_path_stream #ty_generics>) #where_clause;

            impl #impl_generics ::core::clone::Clone for #handle_type_name #ty_generics #where_clause {
                fn clone(&self) -> Self {
                    Self(self.0.clone())
                }
            }

            impl #impl_generics ::core::fmt::Debug for #handle_type_name #ty_generics #where_clause {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                    f.debug_tuple(stringify!(#handle_type_name))
                     .finish_non_exhaustive()
                }
            }

            impl #impl_generics From<::trolley::RouterHandle<#base_path_stream #ty_generics>> for #handle_type_name #ty_generics #where_clause {
                fn from(value: ::trolley::RouterHandle<#base_path_stream #ty_generics>) -> Self {
                    Self(value)
                }
            }

            impl #impl_generics #handle_type_name #ty_generics #where_clause {
                pub async fn shutdown(self) -> <#base_path_stream #ty_generics as ::trolley::RouterBase>::ShutdownResult {
                    self.0.shutdown().await
                }

                pub fn with_timeout<'a>(&'a self, timeout: tokio::time::Duration) -> #call_ctx<'a, #base_path_stream #ty_generics> {
                    #call_ctx::single_timeout(&self.0, timeout)
                }

                pub fn with_explicit_timeouts<'a>(&'a self, state_timeout: tokio::time::Duration, exec_timeout: tokio::time::Duration) -> #call_ctx<'a, #base_path_stream #ty_generics> {
                    #call_ctx::explicit_timeouts(&self.0, state_timeout, exec_timeout)
                }
            }
        };

        generated.push(generated_handle);
    }

    for method in generated_methods {
        generated.push(method)
    }

    let generated = quote! {
            #(
            #generated
            )*
    };

    fs::write(
        std::path::PathBuf::from(std::env::var("OUT_DIR").expect("unable to get OUT_DIR"))
            .join(relative_out_path),
        generated.to_string(),
        // prettyplease::unparse(&syn::parse_file(&generated.to_string()).unwrap()),
    )
    .expect("failed to write generated file");
}

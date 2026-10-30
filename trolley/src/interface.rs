use std::{collections::HashMap, env, fs, path};

use quote::{format_ident, quote};

use trolley_infra::interface_generator::{self, GenericsTokenStreams};

fn generate_call_ctx(
    ctx_path_idents: &[proc_macro2::Ident],
) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
    let call_ctx = quote! {
    crate::#( #ctx_path_idents )::*::CallCtx
    };

    let retry_call_ctx = quote! {
    crate::#( #ctx_path_idents )::*::RetryCallCtx
    };

    (call_ctx, retry_call_ctx)
}

fn split_base_and_ident_from_ty_string(
    ty_string: &str,
) -> (proc_macro2::TokenStream, proc_macro2::Ident) {
    let tokens: proc_macro2::TokenStream = ty_string
        .parse()
        .expect("Invalid token string for router type");

    let mut base_segments = Vec::new();
    let mut has_generics = false;

    for token in tokens {
        if !has_generics
            && matches!(token, proc_macro2::TokenTree::Punct(ref p) if p.as_char() == '<')
        {
            has_generics = true;
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

    let base_path_stream: proc_macro2::TokenStream = base_segments.into_iter().collect();

    (base_path_stream, last_ident)
}

fn generate_handle(
    generics: &GenericsTokenStreams,
    base_path_stream: &proc_macro2::TokenStream,
    handle_type_name: &proc_macro2::Ident,
    call_ctx: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let GenericsTokenStreams {
        generic_params: _,
        impl_generics,
        ty_generics,
        maybe_where_clause,
    } = generics;

    // be explicit. be very explicit.
    let generated_handle = quote! {
        impl #impl_generics #base_path_stream #ty_generics #maybe_where_clause {
            pub async fn spawn( meta: <Self as ::trolley::RouterPriv>::Meta, n_concurrent: u32) -> (#handle_type_name #ty_generics, ::trolley::events::Receiver<<#base_path_stream #ty_generics as ::trolley::RouterBase>::Event>) {
                let (raw_handle, event_rx) =
                    <Self as ::trolley::Router>::spawn_from_meta(meta, n_concurrent).await;

                (#handle_type_name::from(raw_handle), event_rx)
            }
        }

        pub struct #handle_type_name #ty_generics (::trolley::router::Handle<#base_path_stream #ty_generics>) #maybe_where_clause;

        impl #impl_generics ::core::clone::Clone for #handle_type_name #ty_generics #maybe_where_clause {
            fn clone(&self) -> Self {
                Self(self.0.clone())
            }
        }

        impl #impl_generics ::core::fmt::Debug for #handle_type_name #ty_generics #maybe_where_clause {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.debug_tuple(stringify!(#handle_type_name))
                 .finish_non_exhaustive()
            }
        }

        impl #impl_generics From<::trolley::router::Handle<#base_path_stream #ty_generics>> for #handle_type_name #ty_generics #maybe_where_clause {
            fn from(value: ::trolley::router::Handle<#base_path_stream #ty_generics>) -> Self {
                Self(value)
            }
        }

        impl #impl_generics #handle_type_name #ty_generics #maybe_where_clause {
            pub async fn shutdown(self) -> <#base_path_stream #ty_generics as ::trolley::RouterBase>::ShutdownResult {
                self.0.shutdown().await
            }

            pub fn notify(&self, notif: <#base_path_stream #ty_generics as ::trolley::RouterBase>::Notif) ->
                ::core::result::Result<(), ::tokio::sync::mpsc::error::SendError::<<#base_path_stream #ty_generics as ::trolley::RouterBase>::Notif>> {
                self.0.notify(notif)
            }

            pub async fn notify_async(&self, notif: <#base_path_stream #ty_generics as ::trolley::RouterBase>::Notif) ->
                ::core::result::Result<(), ::tokio::sync::mpsc::error::SendError::<<#base_path_stream #ty_generics as ::trolley::RouterBase>::Notif>> {
                self.0.notify_async(notif).await
            }

            #[must_use]
            pub const fn with_timeout<'a>(&'a self, timeout: tokio::time::Duration) -> #call_ctx<'a, #base_path_stream #ty_generics> {
                #call_ctx::single_timeout(&self.0, timeout)
            }

            #[must_use]
            pub const fn with_explicit_timeouts<'a>(&'a self, state_timeout: tokio::time::Duration, exec_timeout: tokio::time::Duration) -> #call_ctx<'a, #base_path_stream #ty_generics> {
                #call_ctx::explicit_timeouts(&self.0, state_timeout, exec_timeout)
            }
        }
    };

    generated_handle
}

fn generate_call_ctx_method(
    generics: &GenericsTokenStreams,
    call_ctx: &proc_macro2::TokenStream,
    _retry_call_ctx: &proc_macro2::TokenStream,
    self_ty: &proc_macro2::TokenStream,
    method_info: trolley_infra::ServiceMethodInfo,
) -> proc_macro2::TokenStream {
    let trolley_infra::ServiceMethodInfo {
        name,
        sig_ident_span: _,
        mutness,
        asyncness: _,
        args,
        maybe_ret,
        struct_name,
    } = method_info;

    let GenericsTokenStreams {
        generic_params,
        impl_generics: _,
        ty_generics,
        maybe_where_clause,
    } = generics;

    let (arg_names, arg_types): (Vec<proc_macro2::Ident>, Vec<proc_macro2::TokenStream>) =
        args.into_iter().unzip();

    let router_call_method = if mutness {
        format_ident!("call_mut")
    } else {
        format_ident!("call")
    };

    let ret_ty = maybe_ret.map_or_else(|| quote! {()}, |ty| quote! {#ty});

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
    quote! {
        impl<'a #(, #generic_params)*> #call_ctx<'a, #self_ty> #maybe_where_clause {
            pub async fn #name(&self, #( #arg_names: #arg_types ),* ) ->
                ::std::result::Result<#ret_ty, ::trolley::routed::Error>
            where #self_ty: #router_trait<#full_struct_target, Output = #ret_ty>
            {
                let input = #full_struct_expr;
                self.#router_call_method(input).await
            }
        }

        // impl<'a> #retry_call_ctx<'a, #self_ty> where #full_struct_target: Clone {
        //     pub async fn #name(&self, #( #arg_names: #arg_types ),* ) ->
        //         ::std::result::Result<#ret_ty, ::trolley::routed::Error>
        //     where #self_ty: #router_trait<#full_struct_target, Output = #ret_ty>
        //     {
        //         let input = #full_struct_expr;
        //         self.#router_call_method(input).await
        //     }
        // }
    }
}

pub fn generator(
    paths: &[&path::Path],
    relative_out_path: &path::Path,
    call_ctx_module_crate_path: &[&str],
) {
    let ctx_path_idents: Vec<proc_macro2::Ident> = call_ctx_module_crate_path
        .iter()
        .map(|segment| format_ident!("{}", segment))
        .collect();
    let (call_ctx, retry_call_ctx) = generate_call_ctx(&ctx_path_idents);

    let interfaces = interface_generator::extract_interfaces(paths);

    let mut generated_stream = Vec::new();
    let mut generated_methods = Vec::new();

    // mapping from service type name to generics for the router type itself
    let mut router_types: HashMap<String, GenericsTokenStreams> = HashMap::new();

    for interface_generator::ServiceInterface {
        self_ty,
        generics,
        methods,
    } in interfaces
    {
        for method_info in methods {
            let call_ctx_method = generate_call_ctx_method(
                &generics,
                &call_ctx,
                &retry_call_ctx,
                &self_ty,
                method_info,
            );

            generated_methods.push(call_ctx_method);
        }

        // needs to be after the method loop, impl_generics/ty_generics/where_clause borrow from generics
        router_types.insert(self_ty.to_string(), generics);
    }

    // TODO: un-hack the proc macro fields registry to pass params directly to spawn()
    for (ty_string, generics) in router_types {
        let (base_path_stream, last_ident) = split_base_and_ident_from_ty_string(&ty_string);

        let handle_type_name = format_ident!("{}Handle", last_ident);

        let generated_handle =
            generate_handle(&generics, &base_path_stream, &handle_type_name, &call_ctx);

        generated_stream.push(generated_handle);
    }

    for method in generated_methods {
        generated_stream.push(method);
    }

    let generated = quote! {
            #(
            #generated_stream
            )*
    };

    fs::write(
        path::PathBuf::from(env::var("OUT_DIR").expect("unable to get OUT_DIR"))
            .join(relative_out_path),
        // generated.to_string(),
        prettyplease::unparse(&syn::parse_file(&generated.to_string()).unwrap()),
    )
    .expect("failed to write generated file");
}

use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::{parse2, spanned::Spanned};

use trolley_infra::{ServiceMethodInfo, extract_method_info, interface_generator};

struct GeneratedMethod {
    _registry_method: TokenStream,
    call_params_struct: TokenStream,
    params_and_impl: TokenStream,
}

fn generate_method(
    self_ty: &syn::Type,
    generics: &interface_generator::GenericsTokenStreams,
    info: ServiceMethodInfo,
) -> GeneratedMethod {
    let ServiceMethodInfo {
        name: method_name,
        sig_ident_span,
        args,
        mutness,
        asyncness,
        maybe_ret,
        struct_name,
    } = info;

    let interface_generator::GenericsTokenStreams {
        generic_params: _,
        ty_generics,
        impl_generics,
        maybe_where_clause,
    } = generics;

    let maybe_mut = if mutness { Some(quote! {mut}) } else { None };

    let maybe_await = if asyncness {
        Some(quote! {.await})
    } else {
        None
    };

    let implemented_trait = if info.mutness {
        syn::Ident::new("ServiceMut", sig_ident_span)
    } else {
        syn::Ident::new("Service", sig_ident_span)
    };

    let (fields, types): (Vec<proc_macro2::Ident>, Vec<TokenStream>) = args.into_iter().unzip();

    let router_call_method = if mutness {
        syn::Ident::new("call_mut", sig_ident_span)
    } else {
        syn::Ident::new("call", sig_ident_span)
    };

    let call_params_struct = quote! {#[derive(Debug)]
    pub struct #struct_name #impl_generics #maybe_where_clause {
        #( pub #fields: #types ),*
    }};

    let (registry_method, params_and_impl) = {
        let return_type = maybe_ret.map_or_else(|| quote! {()}, |ty| quote! {#ty});

        let registry_method = quote! {
            pub async fn #method_name(&self, #( #fields: #types, )* timeout: ::tokio::time::Duration) ->
                ::std::result::Result<#return_type, ::trolley::router::routed::RoutedCallError>
            {
                let input = #struct_name { #( #fields ),* };
                self.0.#router_call_method(input, timeout).await
            }
        };

        let service_trait_impl = quote! {
            #[async_trait::async_trait]
            impl #impl_generics ::trolley::#implemented_trait<#struct_name #ty_generics> for #self_ty #maybe_where_clause {
                type Output = #return_type;

                async fn call (&#maybe_mut self, input: #struct_name #ty_generics) -> #return_type {
                    let #struct_name { #( #fields ),* } = input;
                    self.#method_name( #( #fields),* )#maybe_await
                }
            }
        };

        (registry_method, service_trait_impl)
    };

    GeneratedMethod {
        _registry_method: registry_method,
        call_params_struct,
        params_and_impl,
    }
}

// FUTURE: ensure this attriute only attached to one implementation for struct
pub(crate) fn interface(attr: &TokenStream, item: TokenStream, _allow_async: bool) -> TokenStream {
    if !attr.is_empty() {
        return quote_spanned!(attr.span() => compile_error!("service attribute does not take any arguments"));
    }

    let implementation: syn::ItemImpl = match parse2(item) {
        Ok(parsed) => parsed,
        Err(e) => return e.to_compile_error(),
    };

    // ensure inherent impl, not trait
    if let Some((path, _for)) = &implementation.trait_ {
        let mut pretty_path = format!("{}", quote! {#path});

        pretty_path.retain(|c| !c.is_whitespace());
        let error = format!(
            "service attribute may only be placed on inherent impl (found trait {})",
            // FIXME: this doesn't preserve source text, but path.source_text() doesn't give expected output (e.g. 'std::fmt::Debug' just gives 'std')
            quote! {#path}
        );
        return quote_spanned!(implementation.span()=> const _: () = {compile_error!(#error);};);
    }

    // filter methods, ensure only public
    let mut public_methods = implementation
        .items
        .iter()
        .filter_map(|i| match i {
            syn::ImplItem::Fn(fun) => {
                if matches!(fun.vis, syn::Visibility::Public(_)) {
                    Some(fun)
                } else {
                    None
                }
            }
            _ => None,
        })
        .peekable();

    let mut private_methods = implementation.items.iter().filter_map(|i| match i {
        syn::ImplItem::Fn(fun) => {
            if matches!(fun.vis, syn::Visibility::Restricted(_))
                || matches!(fun.vis, syn::Visibility::Inherited)
            {
                Some(fun)
            } else {
                None
            }
        }
        _ => None,
    });

    if private_methods.next().is_some() {
        return quote_spanned! { implementation.span() =>
            const _: () = { compile_error!("service interface may not contain non-public methods");};
        };
    }

    if public_methods.peek().is_none() {
        return quote_spanned! { implementation.span() =>
            const _: () = { compile_error!("service interface must have at least one public method");};
        };
    }

    // collect info from every method
    let mut method_infos = Vec::<ServiceMethodInfo>::new();
    for method in public_methods {
        let method_info = match extract_method_info(method) {
            Ok(info) => info,
            Err(e) => return e,
        };
        method_infos.push(method_info);
    }

    let self_ty = &*implementation.self_ty;

    let generics: interface_generator::GenericsTokenStreams =
        implementation.generics.clone().into();

    let mut impl_quotes: Vec<TokenStream> = Vec::new();
    // let mut registry: Vec<TokenStream> = Vec::new();

    // generate for each method:
    // - quoted callers with same signature as the original method
    // - argument struct containing same fields as method
    // - implementation of corresponding service trait for the type
    for info in method_infos {
        let GeneratedMethod {
            _registry_method: _,
            call_params_struct,
            params_and_impl,
        } = generate_method(self_ty, &generics, info);

        // registry.push(registry_method);
        impl_quotes.push(call_params_struct);
        impl_quotes.push(params_and_impl);
    }

    // emit:
    // - the original implementation
    // - registry macro that emits the callers for router handle to implement
    // - implementations of the service traits for every method on the type

    quote! {

        #implementation

        #( #impl_quotes )*
    }
}

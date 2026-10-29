use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::{parse2, spanned::Spanned};

// FUTURE: ensure this attriute only attached to one implementation for struct
pub fn interface(attr: TokenStream, item: TokenStream, _allow_async: bool) -> TokenStream {
    if !attr.is_empty() {
        return quote_spanned!(attr.span() => compile_error!("service attribute does not take any arguments"));
    }

    let implementation: syn::ItemImpl = match parse2(item.clone()) {
        Ok(parsed) => parsed,
        Err(e) => return e.to_compile_error(),
    };

    // ensure inherent impl, not trait
    if let Some((_not, path, _for)) = &implementation.trait_ {
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
    let public_methods: Vec<&syn::ImplItemFn> = implementation
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
        .collect();

    let private_methods: Vec<&syn::ImplItemFn> = implementation
        .items
        .iter()
        .filter_map(|i| match i {
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
        })
        .collect();

    if !private_methods.is_empty() {
        return quote_spanned! { implementation.span() =>
            const _: () = { compile_error!("service interface may not contain non-public methods");};
        };
    }

    if public_methods.is_empty() {
        return quote_spanned! { implementation.span() =>
            const _: () = { compile_error!("service interface must have at least one public method");};
        };
    }

    // collect info from every method
    let mut method_infos = Vec::<trolley_infra::ServiceMethodInfo>::new();
    for method in public_methods {
        let method_info = match trolley_infra::extract_method_info(method) {
            Ok(info) => info,
            Err(e) => return e,
        };
        method_infos.push(method_info);
    }

    let self_ty = &*implementation.self_ty;

    let (impl_generics, ty_generics, maybe_where_clause) = implementation.generics.split_for_impl();

    let mut impl_quotes: Vec<TokenStream> = Vec::new();
    let mut registry: Vec<TokenStream> = Vec::new();

    // generate for each method:
    // - quoted callers with same signature as the original method
    // - argument struct containing same fields as method
    // - implementation of corresponding service trait for the type
    for info in method_infos {
        let trolley_infra::ServiceMethodInfo {
            name: method_name,
            sig_ident_span,
            args,
            mutness,
            asyncness,
            maybe_ret,
            struct_name,
        } = info;

        let maybe_mut = if mutness { Some(quote! {mut}) } else { None };

        let maybe_await = if asyncness {
            Some(quote! {.await})
        } else {
            None
        };

        let implemented_trait = match info.mutness {
            true => syn::Ident::new("ServiceMut", sig_ident_span),
            false => syn::Ident::new("Service", sig_ident_span),
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
            let return_type = match maybe_ret {
                Some(ty) => quote! {#ty},
                None => quote! {()},
            };

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

        registry.push(registry_method);
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

use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::{parse2, spanned::Spanned};

pub fn router(item: TokenStream) -> TokenStream {
    let input: syn::DeriveInput = match parse2(item) {
        Ok(parsed) => parsed,
        Err(e) => return e.to_compile_error(),
    };

    let syn::DeriveInput {
        attrs: _,
        vis: _,
        ident,
        generics,
        data: _,
    } = &input;

    // if !generics.params.is_empty() {
    //     return quote_spanned! {generics.span()=> const _: () = {compile_error!("router derive macro doesn't support generics");}};
    // }

    let (impl_generics, type_generics, wheres) = generics.split_for_impl();

    quote! {
        impl #impl_generics ::trolley::Router for #ident #type_generics
        #wheres{
        }

    }
}

pub fn router_meta(attribute_args: TokenStream, item: TokenStream) -> TokenStream {
    let router_type: syn::Type = match parse2(attribute_args) {
        Ok(parsed) => parsed,
        Err(e) => return e.to_compile_error(),
    };

    let parsed_item: syn::Item = match parse2(item) {
        Ok(parsed) => parsed,
        Err(e) => return e.to_compile_error(),
    };

    let struct_ty = match parsed_item {
        syn::Item::Struct(s) => s,
        other => {
            return quote_spanned! {other.span()=> parse_error!("router_meta attribute only supports struct declarations")};
        }
    };

    let (impl_generics, ty_generics, maybe_where_clause) = struct_ty.generics.split_for_impl();

    let fields: Vec<(&syn::Ident, &syn::Type)> =
        match trolley_infra::extract_struct_fields(&struct_ty) {
            Ok(extracted) => extracted,
            Err(e) => return e,
        };
    let (field_names, field_types): (Vec<&syn::Ident>, Vec<&syn::Type>) =
        fields.into_iter().unzip();
    let struct_name = &struct_ty.ident;

    quote! {
        #struct_ty

        impl #impl_generics trolley::RouterMeta for #struct_name #ty_generics #maybe_where_clause {
            type RouterState = #router_type;
        }

        impl #impl_generics #struct_name #ty_generics #maybe_where_clause {
            pub fn new( #( #field_names: #field_types ),* ) -> Self {
                Self {
                    #( #field_names ),*
                }
            }
        }
    }
}

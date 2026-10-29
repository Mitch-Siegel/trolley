use convert_case::Casing;
use proc_macro2::{Span, TokenStream};
use quote::{quote, quote_spanned};
use syn::spanned::Spanned;

use std::{fs, path};

pub mod interface_generator;

pub struct ServiceMethodInfo {
    // pub method: &'a syn::ImplItemFn,
    pub name: TokenStream,
    pub sig_ident_span: Span,
    pub mutness: bool,
    pub asyncness: bool,
    pub args: Vec<(proc_macro2::Ident, proc_macro2::TokenStream)>,
    pub maybe_ret: Option<syn::Type>,
    pub struct_name: syn::Ident,
}

fn extract_info_from_arg(arg: &syn::FnArg) -> Result<(syn::Ident, syn::Type), TokenStream> {
    match arg {
        syn::FnArg::Typed(pat_type) => match *pat_type.pat.clone() {
            syn::Pat::Ident(pat_ident) => Ok((pat_ident.ident, *pat_type.ty.clone())),
            _ => Err(quote_spanned!(arg.span()=> compile_error!("unsupported argument pattern");)),
        },
        _ => Err(
            quote_spanned!(arg.span()=> compile_error!("all arguments after first must be typed");),
        ),
    }
}

fn extract_info_from_first_arg(arg: &syn::FnArg) -> Result<bool, TokenStream> {
    match arg {
        syn::FnArg::Receiver(receiver) => {
            if receiver.reference.is_none() {
                Err(
                    quote_spanned!(arg.span()=>compile_error!("first argument must be &self or &mut self");),
                )
            } else {
                Ok(receiver.mutability.is_some())
            }
        }
        _ => Err(
            quote_spanned!(arg.span()=> compile_error!("first argument must be &self or &mut self");),
        ),
    }
}

pub fn extract_method_info(method: &syn::ImplItemFn) -> Result<ServiceMethodInfo, TokenStream> {
    let method_name = &method.sig.ident;

    let mut inputs = method.sig.inputs.iter();
    let mutness = match inputs.next() {
        Some(arg) => extract_info_from_first_arg(arg),
        None => Err(
            quote_spanned!(method.sig.inputs.span()=>compile_error!("first argument must be &self or &mut self");),
        ),
    }?;

    // extract args, skip &self
    let mut syn_args = Vec::<(syn::Ident, syn::Type)>::new();
    for arg in inputs {
        syn_args.push(extract_info_from_arg(arg)?);
    }

    let args: Vec<(proc_macro2::Ident, proc_macro2::TokenStream)> = syn_args
        .into_iter()
        .map(|(ident, ty)| (ident, quote! {#ty}))
        .collect();

    // generate struct name from method name
    let struct_name_string = method_name.to_string().to_case(convert_case::Case::Pascal);
    let struct_name = syn::Ident::new(&struct_name_string, method.sig.ident.span());

    // extract output and error types
    let maybe_ret: Option<syn::Type> = match &method.sig.output {
        syn::ReturnType::Type(_, ty) => match ty.as_ref() {
            syn::Type::Path(_) => Some(*ty.clone()),
            syn::Type::Tuple(_) => Some(*ty.clone()),
            _ => {
                return Err(quote_spanned!(ty.span() => compile_error!("unsupported return type")));
            }
        },
        syn::ReturnType::Default => None,
    };

    let method_name = &method.sig.ident;

    Ok(ServiceMethodInfo {
        name: quote! {#method_name},
        sig_ident_span: method.sig.ident.span(),
        args,
        mutness,
        asyncness: method.sig.asyncness.is_some(),
        maybe_ret,
        struct_name,
    })
}

pub fn extract_struct_fields(
    struct_ty: &syn::ItemStruct,
) -> Result<Vec<(&syn::Ident, &syn::Type)>, TokenStream> {
    let mut fields = Vec::new();

    for f in &struct_ty.fields {
        if !matches!(f.vis, syn::Visibility::Public(_)) {
            return Err(
                quote_spanned! {f.span()=> compile_error!("router metadata struct fields must be public")},
            );
        }
        match &f.ident {
            Some(ident) => {
                fields.push((ident, &f.ty));
            }
            None => {
                return Err(
                    quote_spanned! {f.ident.span()=> compile_error!("router metadata struct fields must be named")},
                );
            }
        }
    }

    Ok(fields)
}

// find if an attribute is *exactly* trolley::interface
fn has_service_interface_attribute(attrs: &[syn::Attribute]) -> bool {
    for attr in attrs {
        let path = &attr.path();
        if path.segments.len() != 2 {
            continue;
        }

        let mut segments = path.segments.iter();
        match (segments.next(), segments.next()) {
            (
                Some(syn::PathSegment { ident: first, .. }),
                Some(syn::PathSegment { ident: second, .. }),
            ) => {
                if first == "trolley" && second == "interface" {
                    return true;
                }
            }
            _other => continue,
        }
    }

    false
}

pub fn collect_service_impls(path: &path::Path) -> Vec<(String, syn::ItemImpl)> {
    let mut impls = Vec::new();

    if path.is_dir() {
        for entry in fs::read_dir(path).expect("failed to read directory") {
            let entry = entry.expect("failed to ready directory entry");
            let path = entry.path();
            impls.extend(collect_service_impls(&path));
        }
    } else if path.extension().map(|e| e == "rs").unwrap_or(false) {
        let source =
            fs::read_to_string(path).unwrap_or_else(|_| panic!("failed to read file {:?}", path));
        let source = match syn::parse_file(&source) {
            Ok(parsed) => parsed,
            Err(e) => {
                let span = e.span();
                let start = span.start();
                panic!("failed to parse file: {} @ {:?}:{:?}", e, path, start);
            }
        };

        for item in source.items {
            match item {
                syn::Item::Impl(i) => {
                    if has_service_interface_attribute(&i.attrs) {
                        impls.push((path.to_str().unwrap().into(), i));
                    }
                }
                _ => continue,
            };
        }
    }

    impls
}

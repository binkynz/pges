//! Generates the `Event` impl.

use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};

use crate::input;

/// The impl, plus one serde helper pair per struct variant, inside an anonymous `const` block.
///
/// Generated locals start with `__pges_`, so that an upcaster path such as `data` names the user's item and
/// not a parameter of `decode`.
pub(crate) fn event_impl(event: &input::EventEnum) -> TokenStream {
    let krate = &event.krate;
    let ident = &event.ident;
    let category = &event.category;
    let names = event.variants.iter().map(|variant| &variant.name);
    let helpers = event
        .variants
        .iter()
        .map(|variant| serde_helpers(krate, variant));
    let type_name_arms = event.variants.iter().map(|variant| {
        let pattern = wildcard_pattern(variant);
        let name = &variant.name;
        quote! { #pattern => #name }
    });
    let schema_version_arms = event.variants.iter().map(|variant| {
        let pattern = wildcard_pattern(variant);
        let version = variant.version;
        quote! {
            #pattern => {
                const __PGES_VERSION: #krate::event::SchemaVersion =
                    #krate::__private::schema_version(#version);
                __PGES_VERSION
            }
        }
    });
    let encode_arms = event
        .variants
        .iter()
        .map(|variant| encode_arm(krate, event.codec, variant));
    let decode_arms = event
        .variants
        .iter()
        .map(|variant| decode_arm(krate, event.codec, variant));

    quote! {
        const _: () = {
            #(#helpers)*

            #[automatically_derived]
            impl #krate::event::Event for #ident {
                const CATEGORY: #krate::category::CategoryName =
                    #krate::__private::category(#category);
                const TYPES: &'static [&'static str] = &[#(#names),*];

                fn type_name(&self) -> &'static str {
                    match self {
                        #(#type_name_arms,)*
                    }
                }

                fn schema_version(&self) -> #krate::event::SchemaVersion {
                    match self {
                        #(#schema_version_arms,)*
                    }
                }

                fn encode(
                    &self,
                    out: &mut ::std::vec::Vec<u8>,
                ) -> ::core::result::Result<(), #krate::event::CodecError> {
                    match self {
                        #(#encode_arms,)*
                    }
                }

                fn decode(
                    __pges_type_name: &str,
                    __pges_schema_version: #krate::event::SchemaVersion,
                    __pges_data: &[u8],
                ) -> ::core::result::Result<
                    #krate::event::Decoded<Self>,
                    #krate::event::CodecError,
                > {
                    let __pges_event = match (__pges_type_name, __pges_schema_version.get()) {
                        #(#decode_arms,)*
                        _ => {
                            return ::core::result::Result::Ok(#krate::event::Decoded::Unknown {
                                type_name: ::std::borrow::ToOwned::to_owned(__pges_type_name),
                                schema_version: __pges_schema_version,
                            });
                        }
                    };

                    ::core::result::Result::Ok(#krate::event::Decoded::Event(__pges_event))
                }
            }
        };
    }
}

fn codec_module(krate: &syn::Path, codec: input::Codec) -> TokenStream {
    match codec {
        input::Codec::Json => quote! { #krate::codec::json },
    }
}

fn upcaster_type(krate: &syn::Path, codec: input::Codec) -> TokenStream {
    match codec {
        input::Codec::Json => quote! { #krate::codec::json::Upcaster },
    }
}

/// `#[serde(crate = "...")]` takes the path as a string: `<krate>::__private::serde`.
fn serde_crate(krate: &syn::Path) -> syn::LitStr {
    let mut path = String::new();
    if krate.leading_colon.is_some() {
        path.push_str("::");
    }
    for (index, segment) in krate.segments.iter().enumerate() {
        if index > 0 {
            path.push_str("::");
        }
        path.push_str(&segment.ident.to_string());
    }
    path.push_str("::__private::serde");

    syn::LitStr::new(&path, Span::call_site())
}

fn wildcard_pattern(variant: &input::EventVariant) -> TokenStream {
    let ident = &variant.ident;

    match &variant.shape {
        input::Shape::Struct(_) => quote! { Self::#ident { .. } },
        input::Shape::Newtype => quote! { Self::#ident(..) },
    }
}

fn encode_helper(variant: &input::EventVariant) -> syn::Ident {
    format_ident!("__PgesEncode{}", variant.ident)
}

fn decode_helper(variant: &input::EventVariant) -> syn::Ident {
    format_ident!("__PgesDecode{}", variant.ident)
}

/// Bindings for a struct variant's fields that cannot shadow the generated function's parameters.
fn field_bindings(fields: &[input::StructField]) -> Vec<syn::Ident> {
    (0..fields.len())
        .map(|index| format_ident!("__pges_field_{}", index))
        .collect()
}

/// A struct variant's fields as a borrowed struct to encode and an owned struct to decode, so the stored
/// shape is the object of the fields, whatever the Rust variant is called.
fn serde_helpers(krate: &syn::Path, variant: &input::EventVariant) -> TokenStream {
    let input::Shape::Struct(fields) = &variant.shape else {
        return TokenStream::new();
    };

    let encode = encode_helper(variant);
    let decode = decode_helper(variant);
    let serde = serde_crate(krate);
    let field_idents: Vec<&syn::Ident> = fields.iter().map(|field| &field.ident).collect();
    let field_types: Vec<&syn::Type> = fields.iter().map(|field| &field.ty).collect();
    let field_attrs: Vec<&Vec<syn::Attribute>> =
        fields.iter().map(|field| &field.serde_attrs).collect();
    let lifetime = if fields.is_empty() {
        TokenStream::new()
    } else {
        quote! { <'pges> }
    };

    // A field the serde attributes skip when encoding is never read from the encode struct.
    quote! {
        #[derive(#krate::__private::serde::Serialize)]
        #[serde(crate = #serde)]
        #[allow(dead_code)]
        struct #encode #lifetime {
            #( #(#field_attrs)* #field_idents: &'pges #field_types, )*
        }

        #[derive(#krate::__private::serde::Deserialize)]
        #[serde(crate = #serde)]
        struct #decode {
            #( #(#field_attrs)* #field_idents: #field_types, )*
        }
    }
}

fn encode_arm(
    krate: &syn::Path,
    codec: input::Codec,
    variant: &input::EventVariant,
) -> TokenStream {
    let ident = &variant.ident;
    let name = &variant.name;
    let module = codec_module(krate, codec);

    match &variant.shape {
        input::Shape::Struct(fields) => {
            let encode = encode_helper(variant);
            let field_idents: Vec<&syn::Ident> = fields.iter().map(|field| &field.ident).collect();
            let bindings = field_bindings(fields);
            quote! {
                Self::#ident { #(#field_idents: #bindings),* } => #module::encode(
                    #name,
                    &#encode { #(#field_idents: #bindings),* },
                    out,
                )
            }
        }
        input::Shape::Newtype => quote! {
            Self::#ident(__pges_inner) => #module::encode(#name, __pges_inner, out)
        },
    }
}

/// One arm for every version of the variant: the current version decodes directly, older ones run the tail
/// of the upcaster chain that starts at their version.
fn decode_arm(
    krate: &syn::Path,
    codec: input::Codec,
    variant: &input::EventVariant,
) -> TokenStream {
    let ident = &variant.ident;
    let name = &variant.name;
    let module = codec_module(krate, codec);
    let upcaster = upcaster_type(krate, codec);
    let upcasters = &variant.upcasters;
    let current = variant.version;
    let versions = if current == 1 {
        quote! { 1 }
    } else {
        quote! { 1..=#current }
    };

    let decode = quote! {
        {
            let __pges_upcasters: &[#upcaster] = &[#(#upcasters),*];
            #module::decode_upcast(
                __pges_type_name,
                __pges_schema_version,
                __pges_data,
                &__pges_upcasters[::core::primitive::usize::from(__pges_schema_version.get() - 1)..],
            )?
        }
    };
    let construct = match &variant.shape {
        input::Shape::Struct(fields) => {
            let helper = decode_helper(variant);
            let field_idents: Vec<&syn::Ident> = fields.iter().map(|field| &field.ident).collect();
            quote! {
                {
                    let __pges_decoded: #helper = #decode;
                    Self::#ident { #(#field_idents: __pges_decoded.#field_idents),* }
                }
            }
        }
        input::Shape::Newtype => quote! { Self::#ident(#decode) },
    };

    quote! { (#name, #versions) => #construct }
}

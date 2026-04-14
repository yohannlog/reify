use quote::quote;
use syn::{Attribute, Data, DeriveInput, Lit};

use crate::helpers::to_snake_case;

pub(crate) fn impl_db_enum(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let enum_name = &input.ident;

    let variants = match &input.data {
        Data::Enum(data) => &data.variants,
        _ => {
            return Err(syn::Error::new_spanned(
                input,
                "DbEnum can only be derived on enums",
            ));
        }
    };

    let mut variant_idents = Vec::new();
    let mut variant_strs = Vec::new();

    for variant in variants {
        if !variant.fields.is_empty() {
            return Err(syn::Error::new_spanned(
                variant,
                "DbEnum variants must be unit variants (no fields)",
            ));
        }

        let ident = &variant.ident;
        let db_name = parse_db_enum_rename(&variant.attrs)?
            .unwrap_or_else(|| to_snake_case(&ident.to_string()));

        variant_idents.push(ident.clone());
        variant_strs.push(db_name);
    }

    let num_variants = variant_idents.len();

    let as_str_arms = variant_idents
        .iter()
        .zip(variant_strs.iter())
        .map(|(ident, s)| {
            quote! { #enum_name::#ident => #s }
        });

    let from_str_arms = variant_idents
        .iter()
        .zip(variant_strs.iter())
        .map(|(ident, s)| {
            quote! { #s => Some(#enum_name::#ident) }
        });

    let variant_str_refs: Vec<&str> = variant_strs.iter().map(|s| s.as_str()).collect();

    Ok(quote! {
        impl reify_core::DbEnum for #enum_name {
            fn variants() -> &'static [&'static str] {
                static VARIANTS: [&str; #num_variants] = [#(#variant_str_refs),*];
                &VARIANTS
            }

            fn as_str(&self) -> &'static str {
                match self {
                    #(#as_str_arms,)*
                }
            }

            fn from_str(s: &str) -> Option<Self> {
                match s {
                    #(#from_str_arms,)*
                    _ => None,
                }
            }
        }

        impl reify_core::value::IntoValue for #enum_name {
            fn into_value(self) -> reify_core::Value {
                reify_core::Value::String(reify_core::DbEnum::as_str(&self).to_owned())
            }
        }

        impl reify_core::value::FromValue for #enum_name {
            fn from_value(val: reify_core::Value) -> Result<Self, String> {
                match &val {
                    reify_core::Value::String(s) => {
                        <#enum_name as reify_core::DbEnum>::from_str(s).ok_or_else(|| {
                            format!(
                                "unknown enum variant '{}', expected one of {:?}",
                                s,
                                <#enum_name as reify_core::DbEnum>::variants()
                            )
                        })
                    }
                    reify_core::Value::Null => Err("expected enum value, got NULL".to_string()),
                    other => Err(format!("expected string for enum, got {:?}", other)),
                }
            }
        }
    })
}

fn parse_db_enum_rename(attrs: &[Attribute]) -> syn::Result<Option<String>> {
    for attr in attrs {
        if !attr.path().is_ident("db_enum") {
            continue;
        }
        let mut rename = None;
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                let value = meta.value()?;
                let lit: Lit = value.parse()?;
                if let Lit::Str(s) = lit {
                    rename = Some(s.value());
                }
                Ok(())
            } else {
                Err(meta.error(format!(
                    "unknown `db_enum` attribute `{}`; expected `rename`",
                    meta.path
                        .get_ident()
                        .map_or("?".to_string(), |i| i.to_string())
                )))
            }
        })?;
        if rename.is_some() {
            return Ok(rename);
        }
    }
    Ok(None)
}

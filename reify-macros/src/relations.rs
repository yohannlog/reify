use quote::quote;
use syn::{Attribute, DeriveInput, Path};

use crate::helpers::MetaExt;

#[derive(Debug)]
enum RelKind {
    HasMany,
    HasOne,
    BelongsTo,
}

#[derive(Debug)]
struct ParsedRelation {
    kind: RelKind,
    name: String,
    target: Path,
    foreign_key: String,
    local_key: Option<String>,
}

pub(crate) fn impl_relations(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let struct_name = &input.ident;

    // Extract field names for validation
    let field_names: Vec<String> = match &input.data {
        syn::Data::Struct(data) => match &data.fields {
            syn::Fields::Named(named) => named
                .named
                .iter()
                .map(|f| f.ident.as_ref().unwrap().to_string())
                .collect(),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    };

    let relations = parse_relations_attr(&input.attrs, struct_name, &field_names)?;

    if relations.is_empty() {
        return Ok(quote! {});
    }

    let methods = relations.iter().map(|rel| {
        let method_name = syn::Ident::new(&rel.name, proc_macro2::Span::call_site());
        let target = &rel.target;
        let rel_name = &rel.name;
        let fk = &rel.foreign_key;

        match rel.kind {
            RelKind::HasMany => {
                let from_col = rel.local_key.as_deref().unwrap_or("id");
                quote! {
                    pub fn #method_name() -> reify_core::Relation<#struct_name, #target> {
                        reify_core::Relation::new(
                            #rel_name,
                            reify_core::RelationType::HasMany,
                            #from_col,
                            #fk,
                        )
                    }
                }
            }
            RelKind::HasOne => {
                let from_col = rel.local_key.as_deref().unwrap_or("id");
                quote! {
                    pub fn #method_name() -> reify_core::Relation<#struct_name, #target> {
                        reify_core::Relation::new(
                            #rel_name,
                            reify_core::RelationType::HasOne,
                            #from_col,
                            #fk,
                        )
                    }
                }
            }
            RelKind::BelongsTo => {
                let to_col = rel.local_key.as_deref().unwrap_or("id");
                quote! {
                    pub fn #method_name() -> reify_core::Relation<#struct_name, #target> {
                        reify_core::Relation::new(
                            #rel_name,
                            reify_core::RelationType::BelongsTo,
                            #fk,
                            #to_col,
                        )
                    }
                }
            }
        }
    });

    Ok(quote! {
        impl #struct_name {
            #(#methods)*
        }
    })
}

fn parse_relations_attr(
    attrs: &[Attribute],
    struct_name: &syn::Ident,
    field_names: &[String],
) -> syn::Result<Vec<ParsedRelation>> {
    let mut result = Vec::new();

    for attr in attrs {
        if !attr.path().is_ident("relations") {
            continue;
        }

        attr.parse_nested_meta(|rel_meta| {
            let kind = if rel_meta.path.is_ident("has_many") {
                RelKind::HasMany
            } else if rel_meta.path.is_ident("has_one") {
                RelKind::HasOne
            } else if rel_meta.path.is_ident("belongs_to") {
                RelKind::BelongsTo
            } else {
                return Err(rel_meta.error("expected `has_many`, `has_one`, or `belongs_to`"));
            };

            let mut name: Option<String> = None;
            let mut target: Option<Path> = None;
            let mut foreign_key: Option<String> = None;
            let mut local_key: Option<String> = None;

            rel_meta.parse_nested_meta(|inner| {
                if inner.path.is_ident("foreign_key") {
                    foreign_key = Some(inner.parse_str_value()?);
                } else if inner.path.is_ident("local_key") {
                    local_key = Some(inner.parse_str_value()?);
                } else if inner.path.is_ident("name") {
                    name = Some(inner.parse_str_value()?);
                } else if inner.path.is_ident("model") {
                    let value = inner.value()?;
                    target = Some(value.parse::<Path>()?);
                } else {
                    let rel_name = inner
                        .path
                        .get_ident()
                        .ok_or_else(|| {
                            inner.error("expected a simple identifier for relation name")
                        })?
                        .to_string();
                    name = Some(rel_name);
                    inner.input.parse::<syn::Token![:]>()?;
                    target = Some(inner.input.parse::<Path>()?);
                }
                Ok(())
            })?;

            let name = name.ok_or_else(|| rel_meta.error("missing relation name"))?;
            let target = target.ok_or_else(|| rel_meta.error("missing target type"))?;
            let foreign_key =
                foreign_key.ok_or_else(|| rel_meta.error(r#"missing `foreign_key = "col"`"#))?;

            // Validate that `belongs_to` foreign_key exists as a field on this struct.
            // For `has_many`/`has_one`, the foreign_key is on the *target* struct,
            // so we can't validate it here (would require cross-crate type info).
            // We validate `local_key` for all relation kinds when explicitly provided.
            if matches!(kind, RelKind::BelongsTo) && !field_names.contains(&foreign_key) {
                return Err(rel_meta.error(format!(
                    "`belongs_to` foreign_key `{foreign_key}` does not exist on `{struct_name}`; \
                     available fields: {}",
                    field_names.join(", ")
                )));
            }

            if let Some(ref lk) = local_key
                && !field_names.contains(lk)
            {
                return Err(rel_meta.error(format!(
                    "`local_key = \"{lk}\"` does not exist on `{struct_name}`; \
                     available fields: {}",
                    field_names.join(", ")
                )));
            }

            result.push(ParsedRelation {
                kind,
                name,
                target,
                foreign_key,
                local_key,
            });
            Ok(())
        })?;
    }

    Ok(result)
}

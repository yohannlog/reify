use quote::quote;
use syn::{Attribute, DeriveInput, Lit, Path};

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
    let relations = parse_relations_attr(&input.attrs)?;

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

fn parse_relations_attr(attrs: &[Attribute]) -> syn::Result<Vec<ParsedRelation>> {
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
                    let value = inner.value()?;
                    let lit: Lit = value.parse()?;
                    if let Lit::Str(s) = lit {
                        foreign_key = Some(s.value());
                    }
                } else if inner.path.is_ident("local_key") {
                    let value = inner.value()?;
                    let lit: Lit = value.parse()?;
                    if let Lit::Str(s) = lit {
                        local_key = Some(s.value());
                    }
                } else if inner.path.is_ident("name") {
                    let value = inner.value()?;
                    let lit: Lit = value.parse()?;
                    if let Lit::Str(s) = lit {
                        name = Some(s.value());
                    }
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

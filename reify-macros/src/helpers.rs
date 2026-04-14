use quote::quote;
use syn::{Attribute, Lit};

#[derive(Default)]
pub(crate) struct ColumnAttrs {
    pub primary_key: bool,
    pub auto_increment: bool,
    pub unique: bool,
    pub nullable: bool,
    pub index: bool,
    pub default: Option<String>,
    pub sql_type: Option<String>,
    pub computed: Option<String>,
    pub computed_rust: bool,
    pub creation_timestamp: bool,
    pub update_timestamp: bool,
    pub timestamp_source: Option<String>,
    pub check: Option<String>,
    pub references: Option<String>,
    pub on_delete: Option<String>,
    pub on_update: Option<String>,
    pub validate: Option<String>,
}

pub(crate) fn parse_column_attrs(attrs: &[Attribute]) -> syn::Result<ColumnAttrs> {
    let mut result = ColumnAttrs::default();
    for attr in attrs {
        if !attr.path().is_ident("column") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("primary_key") {
                result.primary_key = true;
            } else if meta.path.is_ident("auto_increment") {
                result.auto_increment = true;
            } else if meta.path.is_ident("unique") {
                result.unique = true;
            } else if meta.path.is_ident("nullable") {
                result.nullable = true;
            } else if meta.path.is_ident("index") {
                result.index = true;
            } else if meta.path.is_ident("default") {
                let value = meta.value()?;
                let lit: Lit = value.parse()?;
                if let Lit::Str(s) = lit {
                    result.default = Some(s.value());
                }
            } else if meta.path.is_ident("sql_type") {
                let value = meta.value()?;
                let lit: Lit = value.parse()?;
                if let Lit::Str(s) = lit {
                    result.sql_type = Some(s.value());
                }
            } else if meta.path.is_ident("computed") {
                let value = meta.value()?;
                let lit: Lit = value.parse()?;
                if let Lit::Str(s) = lit {
                    result.computed = Some(s.value());
                }
            } else if meta.path.is_ident("computed_rust") {
                result.computed_rust = true;
            } else if meta.path.is_ident("creation_timestamp") {
                result.creation_timestamp = true;
            } else if meta.path.is_ident("update_timestamp") {
                result.update_timestamp = true;
            } else if meta.path.is_ident("source") {
                let value = meta.value()?;
                let lit: Lit = value.parse()?;
                if let Lit::Str(s) = lit {
                    result.timestamp_source = Some(s.value());
                }
            } else if meta.path.is_ident("check") {
                let value = meta.value()?;
                let lit: Lit = value.parse()?;
                if let Lit::Str(s) = lit {
                    result.check = Some(s.value());
                }
            } else if meta.path.is_ident("references") {
                let value = meta.value()?;
                let lit: Lit = value.parse()?;
                if let Lit::Str(s) = lit {
                    result.references = Some(s.value());
                }
            } else if meta.path.is_ident("on_delete") {
                let value = meta.value()?;
                let lit: Lit = value.parse()?;
                if let Lit::Str(s) = lit {
                    result.on_delete = Some(s.value());
                }
            } else if meta.path.is_ident("on_update") {
                let value = meta.value()?;
                let lit: Lit = value.parse()?;
                if let Lit::Str(s) = lit {
                    result.on_update = Some(s.value());
                }
            } else if meta.path.is_ident("validate") {
                let content;
                syn::parenthesized!(content in meta.input);
                let tokens: proc_macro2::TokenStream = content.parse()?;
                result.validate = Some(tokens.to_string());
            } else {
                return Err(meta.error(format!(
                    "unknown `column` attribute `{}`; expected one of: \
primary_key, auto_increment, unique, nullable, index, default, sql_type, \
computed, computed_rust, creation_timestamp, update_timestamp, source, \
check, references, on_delete, on_update, validate",
                    meta.path
                        .get_ident()
                        .map_or("?".to_string(), |i| i.to_string())
                )));
            }
            Ok(())
        })?;
    }
    Ok(result)
}

pub(crate) fn to_snake_case(s: &str) -> String {
    let mut result = String::with_capacity(s.len() + 4);
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(ch.to_lowercase().next().unwrap());
        } else {
            result.push(ch);
        }
    }
    result
}

pub(crate) fn unwrap_option_type(ty: &syn::Type) -> (bool, &syn::Type) {
    if let syn::Type::Path(type_path) = ty {
        if let Some(seg) = type_path.path.segments.last() {
            if seg.ident == "Option" {
                if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
                    if let Some(syn::GenericArgument::Type(inner)) = args.args.first() {
                        return (true, inner);
                    }
                }
            }
        }
    }
    (false, ty)
}

pub(crate) fn rust_type_to_sql_type(ty: &syn::Type) -> proc_macro2::TokenStream {
    let type_str = quote!(#ty).to_string().replace(' ', "");
    match type_str.as_str() {
        "i16" => quote! { reify_core::schema::SqlType::SmallInt },
        "i32" => quote! { reify_core::schema::SqlType::Integer },
        "i64" => quote! { reify_core::schema::SqlType::BigInt },
        "f32" => quote! { reify_core::schema::SqlType::Float },
        "f64" => quote! { reify_core::schema::SqlType::Double },
        "bool" => quote! { reify_core::schema::SqlType::Boolean },
        "String" | "&str" => quote! { reify_core::schema::SqlType::Text },
        "Vec<u8>" => quote! { reify_core::schema::SqlType::Bytea },
        "chrono::DateTime<chrono::Utc>" | "DateTime<Utc>" => {
            quote! { reify_core::schema::SqlType::Timestamptz }
        }
        "chrono::NaiveDateTime" | "NaiveDateTime" => {
            quote! { reify_core::schema::SqlType::Timestamp }
        }
        "chrono::NaiveDate" | "NaiveDate" => {
            quote! { reify_core::schema::SqlType::Date }
        }
        "chrono::NaiveTime" | "NaiveTime" => {
            quote! { reify_core::schema::SqlType::Time }
        }
        "uuid::Uuid" | "Uuid" => quote! { reify_core::schema::SqlType::Uuid },
        "serde_json::Value" | "JsonValue" => {
            quote! { reify_core::schema::SqlType::Jsonb }
        }
        _ => quote! { reify_core::schema::SqlType::Text },
    }
}

pub(crate) fn parse_sql_type_string(s: &str) -> proc_macro2::TokenStream {
    let upper = s.trim().to_uppercase();

    if let Some(inner) = upper
        .strip_prefix("VARCHAR(")
        .and_then(|r| r.strip_suffix(')'))
    {
        if let Ok(len) = inner.trim().parse::<u32>() {
            return quote! { reify_core::schema::SqlType::Varchar(#len) };
        }
    }

    if let Some(inner) = upper
        .strip_prefix("CHAR(")
        .and_then(|r| r.strip_suffix(')'))
    {
        if let Ok(len) = inner.trim().parse::<u32>() {
            return quote! { reify_core::schema::SqlType::Char(#len) };
        }
    }

    let decimal_inner = upper
        .strip_prefix("DECIMAL(")
        .or_else(|| upper.strip_prefix("NUMERIC("))
        .and_then(|r| r.strip_suffix(')'));
    if let Some(inner) = decimal_inner {
        let parts: Vec<&str> = inner.split(',').collect();
        if parts.len() == 2 {
            if let (Ok(p), Ok(sc)) = (parts[0].trim().parse::<u8>(), parts[1].trim().parse::<u8>())
            {
                return quote! { reify_core::schema::SqlType::Decimal(#p, #sc) };
            }
        }
    }

    quote! { reify_core::schema::SqlType::Custom(#s) }
}

use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Attribute, Lit};

/// Helper trait to extract a string value from a `meta.value()` call.
/// Reduces boilerplate in attribute parsing.
pub(crate) trait MetaExt {
    /// Parse `= "value"` and return the string, or error if not a string literal.
    fn parse_str_value(&self) -> syn::Result<String>;
}

impl MetaExt for syn::meta::ParseNestedMeta<'_> {
    fn parse_str_value(&self) -> syn::Result<String> {
        let value = self.value()?;
        let lit: Lit = value.parse()?;
        match lit {
            Lit::Str(s) => Ok(s.value()),
            _ => Err(syn::Error::new_spanned(lit, "expected a string literal")),
        }
    }
}

/// Parse a string literal from a ParseStream (for use inside parenthesized content).
pub(crate) fn parse_str_lit(input: ParseStream) -> syn::Result<String> {
    let lit: Lit = input.parse()?;
    match lit {
        Lit::Str(s) => Ok(s.value()),
        _ => Err(syn::Error::new_spanned(lit, "expected a string literal")),
    }
}

#[derive(Default)]
pub(crate) struct ColumnAttrs {
    pub primary_key: bool,
    pub auto_increment: bool,
    pub unique: bool,
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
    pub validate: Option<proc_macro2::TokenStream>,
    /// Rule names parsed from `validate(...)` (e.g. `["email", "length"]`).
    /// Kept alongside the raw token stream so consumers (rustdoc generation,
    /// Option-nullability checks) don't have to re-parse the tokens.
    pub validate_rule_names: Vec<String>,
    /// Override the column name in the database (e.g. `#[column(name = "user_ip")]`).
    pub name: Option<String>,
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
            } else if meta.path.is_ident("index") {
                result.index = true;
            } else if meta.path.is_ident("default") {
                result.default = Some(meta.parse_str_value()?);
            } else if meta.path.is_ident("sql_type") {
                result.sql_type = Some(meta.parse_str_value()?);
            } else if meta.path.is_ident("computed") {
                result.computed = Some(meta.parse_str_value()?);
            } else if meta.path.is_ident("computed_rust") {
                result.computed_rust = true;
            } else if meta.path.is_ident("creation_timestamp") {
                result.creation_timestamp = true;
            } else if meta.path.is_ident("update_timestamp") {
                result.update_timestamp = true;
            } else if meta.path.is_ident("source") {
                let val = meta.parse_str_value()?;
                if val != "db" && val != "vm" {
                    return Err(syn::Error::new(
                        proc_macro2::Span::call_site(),
                        format!(
                            "invalid `source` value {:?}; expected \"db\" or \"vm\"",
                            val
                        ),
                    ));
                }
                result.timestamp_source = Some(val);
            } else if meta.path.is_ident("check") {
                result.check = Some(meta.parse_str_value()?);
            } else if meta.path.is_ident("references") {
                result.references = Some(meta.parse_str_value()?);
            } else if meta.path.is_ident("on_delete") {
                result.on_delete = Some(meta.parse_str_value()?);
            } else if meta.path.is_ident("on_update") {
                result.on_update = Some(meta.parse_str_value()?);
            } else if meta.path.is_ident("name") {
                result.name = Some(meta.parse_str_value()?);
            } else if meta.path.is_ident("validate") {
                let content;
                syn::parenthesized!(content in meta.input);
                // Parse into a comma-separated list of meta items so we can
                // catch typos early (3.5). Each element must start with a
                // known validator rule name — unknown names are rejected
                // with a span-anchored error instead of being forwarded
                // blindly to `#[validate(...)]` and only failing at the
                // user's next build of their own crate.
                let rules = content.parse_terminated(syn::Meta::parse, syn::Token![,])?;
                for rule in &rules {
                    let name = rule.path().get_ident().map(|i| i.to_string());
                    let known = matches!(
                        name.as_deref(),
                        // Built-in rules as of validator 0.20.
                        Some(
                            "email"
                                | "url"
                                | "length"
                                | "range"
                                | "regex"
                                | "contains"
                                | "does_not_contain"
                                | "must_match"
                                | "required"
                                | "required_nested"
                                | "non_control_character"
                                | "phone"
                                | "credit_card"
                                | "ip"
                                | "ip_v4"
                                | "ip_v6"
                                | "custom"
                                | "nested"
                                | "skip_on_field_errors"
                        )
                    );
                    if !known {
                        return Err(syn::Error::new_spanned(
                            rule.path(),
                            format!(
                                "unknown validator rule `{}`; expected one of: \
email, url, length, range, regex, contains, does_not_contain, must_match, \
required, required_nested, non_control_character, phone, credit_card, ip, \
ip_v4, ip_v6, custom, nested, skip_on_field_errors",
                                name.as_deref().unwrap_or("?")
                            ),
                        ));
                    }
                }
                // Reconstruct the rule list as tokens so validator's
                // `#[validate(...)]` derive receives the exact spelling the
                // user wrote (including any `custom = "fn_path"` arguments).
                let rules_tokens: Vec<proc_macro2::TokenStream> =
                    rules.iter().map(|r| quote::quote!(#r)).collect();
                result.validate = Some(quote::quote! { #(#rules_tokens),* });
                // Parsed rule names are kept so per-field rustdoc and
                // Option-nullability checks can inspect them.
                result.validate_rule_names = rules
                    .iter()
                    .filter_map(|r| r.path().get_ident().map(|i| i.to_string()))
                    .collect();
            } else {
                return Err(meta.error(format!(
                    "unknown `column` attribute `{}`; expected one of: \
primary_key, auto_increment, unique, index, default, sql_type, \
computed, computed_rust, creation_timestamp, update_timestamp, source, \
check, references, on_delete, on_update, validate, name",
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
        // Only recognise the canonical `Option` paths — `Option<T>`,
        // `std::option::Option<T>`, or `core::option::Option<T>`. A user
        // alias `type Option<T> = Vec<T>` in some other path would not
        // match and therefore cannot trick the macro into treating a
        // non-Option as nullable.
        if type_path.qself.is_none() {
            let segs: Vec<String> = type_path
                .path
                .segments
                .iter()
                .map(|s| s.ident.to_string())
                .collect();
            let path_matches = matches!(
                segs.iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .as_slice(),
                ["Option"] | ["std", "option", "Option"] | ["core", "option", "Option"]
            );
            if path_matches {
                if let Some(seg) = type_path.path.segments.last() {
                    if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
                        if let Some(syn::GenericArgument::Type(inner)) = args.args.first() {
                            return (true, inner);
                        }
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
        // ── PostgreSQL array types ─────────────────────────────────────
        "Vec<i16>" => quote! {
            reify_core::schema::SqlType::Array(Box::new(reify_core::schema::SqlType::SmallInt))
        },
        "Vec<i32>" => quote! {
            reify_core::schema::SqlType::Array(Box::new(reify_core::schema::SqlType::Integer))
        },
        "Vec<i64>" => quote! {
            reify_core::schema::SqlType::Array(Box::new(reify_core::schema::SqlType::BigInt))
        },
        "Vec<f32>" => quote! {
            reify_core::schema::SqlType::Array(Box::new(reify_core::schema::SqlType::Float))
        },
        "Vec<f64>" => quote! {
            reify_core::schema::SqlType::Array(Box::new(reify_core::schema::SqlType::Double))
        },
        "Vec<bool>" => quote! {
            reify_core::schema::SqlType::Array(Box::new(reify_core::schema::SqlType::Boolean))
        },
        "Vec<String>" => quote! {
            reify_core::schema::SqlType::Array(Box::new(reify_core::schema::SqlType::Text))
        },
        "Vec<uuid::Uuid>" | "Vec<Uuid>" => quote! {
            reify_core::schema::SqlType::Array(Box::new(reify_core::schema::SqlType::Uuid))
        },
        "Vec<serde_json::Value>" | "Vec<JsonValue>" => quote! {
            reify_core::schema::SqlType::Array(Box::new(reify_core::schema::SqlType::Jsonb))
        },
        // ── Complex types (PostgreSQL / MySQL) ───────────────────────────
        "Point" | "reify::types::Point" | "reify_core::types::Point" => {
            quote! { reify_core::schema::SqlType::Point }
        }
        "Inet" | "reify::types::Inet" | "reify_core::types::Inet" => {
            quote! { reify_core::schema::SqlType::Inet }
        }
        "Cidr" | "reify::types::Cidr" | "reify_core::types::Cidr" => {
            quote! { reify_core::schema::SqlType::Cidr }
        }
        "MacAddr" | "reify::types::MacAddr" | "reify_core::types::MacAddr" => {
            quote! { reify_core::schema::SqlType::MacAddr }
        }
        "Interval" | "reify::types::Interval" | "reify_core::types::Interval" => {
            quote! { reify_core::schema::SqlType::Interval }
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

    // ── Complex types ────────────────────────────────────────────────
    match upper.as_str() {
        "POINT" => return quote! { reify_core::schema::SqlType::Point },
        "INET" => return quote! { reify_core::schema::SqlType::Inet },
        "CIDR" => return quote! { reify_core::schema::SqlType::Cidr },
        "MACADDR" => return quote! { reify_core::schema::SqlType::MacAddr },
        "INTERVAL" => return quote! { reify_core::schema::SqlType::Interval },
        "BYTEA" | "BLOB" => return quote! { reify_core::schema::SqlType::Bytea },
        "UUID" => return quote! { reify_core::schema::SqlType::Uuid },
        "JSONB" | "JSON" => return quote! { reify_core::schema::SqlType::Jsonb },
        "TEXT" => return quote! { reify_core::schema::SqlType::Text },
        "BOOLEAN" | "BOOL" => return quote! { reify_core::schema::SqlType::Boolean },
        "SMALLINT" | "INT2" => return quote! { reify_core::schema::SqlType::SmallInt },
        "INTEGER" | "INT" | "INT4" => return quote! { reify_core::schema::SqlType::Integer },
        "BIGINT" | "INT8" => return quote! { reify_core::schema::SqlType::BigInt },
        "REAL" | "FLOAT4" | "FLOAT" => return quote! { reify_core::schema::SqlType::Float },
        "DOUBLE PRECISION" | "FLOAT8" | "DOUBLE" => {
            return quote! { reify_core::schema::SqlType::Double }
        }
        "DATE" => return quote! { reify_core::schema::SqlType::Date },
        "TIME" => return quote! { reify_core::schema::SqlType::Time },
        "TIMESTAMP" => return quote! { reify_core::schema::SqlType::Timestamp },
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => {
            return quote! { reify_core::schema::SqlType::Timestamptz }
        }
        "SERIAL" => return quote! { reify_core::schema::SqlType::Serial },
        "BIGSERIAL" => return quote! { reify_core::schema::SqlType::BigSerial },
        _ => {}
    }

    quote! { reify_core::schema::SqlType::Custom(#s) }
}

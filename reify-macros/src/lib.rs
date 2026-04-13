use proc_macro::TokenStream;
use quote::quote;
use syn::{parse::Parse, parse_macro_input, Attribute, Data, DeriveInput, Fields, Lit, Path};

/// Derive macro that implements `Table` and generates typed column constants + query builder helpers.
///
/// # Usage
/// ```ignore
/// #[derive(Table)]
/// #[table(name = "users")]
/// pub struct User {
///     #[column(primary_key, auto_increment)]
///     pub id: i64,
///     #[column(unique, index)]
///     pub email: String,
/// }
/// ```
///
/// ## Composite indexes
///
/// ```ignore
/// #[derive(Table)]
/// #[table(
///     name = "users",
///     index(columns("email", "role")),
///     index(columns("email", "role"), unique, name = "idx_email_role"),
/// )]
/// pub struct User { /* ... */ }
/// ```
#[proc_macro_derive(Table, attributes(table, column))]
pub fn derive_table(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match impl_table(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

// ── #[derive(Relations)] ────────────────────────────────────────────

/// Derive macro that reads `#[relations(...)]` attributes on a struct and
/// generates typed relation factory methods.
///
/// # Syntax
///
/// ```ignore
/// #[derive(Table, Relations)]
/// #[table(name = "users")]
/// #[relations(
///     has_many(posts:   Post,    foreign_key = "user_id"),
///     has_one( profile: Profile, foreign_key = "user_id"),
///     belongs_to(team: Team,    foreign_key = "team_id"),
/// )]
/// pub struct User {
///     pub id:      i64,
///     pub team_id: i64,
/// }
/// ```
///
/// Each entry generates a `pub fn <name>() -> Relation<Self, Target>` method
/// on the struct.
///
/// - `has_many` / `has_one`: `from_col` defaults to `"id"`, `to_col` = `foreign_key`.
/// - `belongs_to`: `from_col` = `foreign_key`, `to_col` defaults to `"id"`.
#[proc_macro_derive(Relations, attributes(relations))]
pub fn derive_relations(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match impl_relations(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

// ── Parsed relation entry ────────────────────────────────────────────

#[derive(Debug)]
enum RelKind {
    HasMany,
    HasOne,
    BelongsTo,
}

#[derive(Debug)]
struct ParsedRelation {
    kind: RelKind,
    /// Method name, e.g. `posts`.
    name: String,
    /// Target type path, e.g. `Post`.
    target: Path,
    /// The foreign-key column name.
    foreign_key: String,
    /// Optional explicit local column override.
    local_key: Option<String>,
}

fn impl_relations(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let struct_name = &input.ident;
    let relations = parse_relations_attr(&input.attrs)?;

    if relations.is_empty() {
        // No #[relations(...)] — emit nothing.
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

/// Parse `#[relations(has_many(name: Type, foreign_key = "col"), ...)]`.
fn parse_relations_attr(attrs: &[Attribute]) -> syn::Result<Vec<ParsedRelation>> {
    let mut result = Vec::new();

    for attr in attrs {
        if !attr.path().is_ident("relations") {
            continue;
        }

        attr.parse_nested_meta(|rel_meta| {
            // rel_meta.path is `has_many` / `has_one` / `belongs_to`
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
                // First positional-style item: `name: Type`
                // syn parses `name: Type` as a path `name` followed by `: Type`
                // We handle it as a named key `name = "..."`  OR as the
                // first ident:path pair.
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
                    // Treat as `relation_name: TargetType` shorthand.
                    // The ident is the relation name; after `:` comes the type path.
                    let rel_name = inner
                        .path
                        .get_ident()
                        .ok_or_else(|| {
                            inner.error("expected a simple identifier for relation name")
                        })?
                        .to_string();
                    name = Some(rel_name);
                    // Consume `: Type`
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

/// Derive macro that implements `DbEnum` + `IntoValue` for a unit enum.
///
/// Variants are lowercased by default (`Admin` → `"admin"`).
/// Use `#[db_enum(rename = "custom_name")]` to override.
///
/// # Usage
/// ```ignore
/// #[derive(DbEnum, Debug, Clone, PartialEq)]
/// pub enum Role {
///     Admin,
///     Member,
///     Guest,
/// }
///
/// #[derive(DbEnum, Debug, Clone, PartialEq)]
/// pub enum Status {
///     Active,
///     #[db_enum(rename = "on_hold")]
///     OnHold,
///     Archived,
/// }
/// ```
#[proc_macro_derive(DbEnum, attributes(db_enum))]
pub fn derive_db_enum(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match impl_db_enum(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

// ── Parsed index from #[table(index(...))] ──────────────────────────

struct ParsedIndex {
    columns: Vec<String>,
    unique: bool,
    name: Option<String>,
    predicate: Option<String>,
}

// ── Parsed table attribute ──────────────────────────────────────────

struct TableAttr {
    name: String,
    indexes: Vec<ParsedIndex>,
    audit: bool,
}

fn impl_table(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let struct_name = &input.ident;

    // Parse #[table(name = "...", index(...))] attribute
    let table_attr = parse_table_attr(&input.attrs)?;
    let table_name = &table_attr.name;

    // Extract fields
    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named) => &named.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    input,
                    "Table derive requires named fields",
                ))
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                input,
                "Table derive only works on structs",
            ))
        }
    };

    let mut col_names = Vec::new();
    let mut col_idents = Vec::new();
    let mut col_types = Vec::new();
    let mut value_conversions = Vec::new();
    let mut single_col_indexes: Vec<String> = Vec::new();
    let mut update_ts_vm_cols: Vec<String> = Vec::new();

    let mut col_defs_tokens: Vec<proc_macro2::TokenStream> = Vec::new();

    for field in fields.iter() {
        let ident = field.ident.as_ref().unwrap();
        let ty = &field.ty;
        let name_str = ident.to_string();

        col_names.push(name_str.clone());
        col_idents.push(ident.clone());
        col_types.push(ty.clone());

        let col_attrs = parse_column_attrs(&field.attrs);
        if col_attrs.index {
            single_col_indexes.push(name_str.clone());
        }

        // Track VM-source update_timestamp columns for override generation
        if col_attrs.update_timestamp && col_attrs.timestamp_source.as_deref() != Some("db") {
            update_ts_vm_cols.push(name_str.clone());
        }

        // Determine if this is a VM-source timestamp (inject Utc::now())
        let is_vm_timestamp = (col_attrs.creation_timestamp || col_attrs.update_timestamp)
            && col_attrs.timestamp_source.as_deref() != Some("db");

        if is_vm_timestamp {
            value_conversions.push(quote! {
                reify_core::value::IntoValue::into_value(chrono::Utc::now())
            });
        } else {
            value_conversions.push(quote! {
                reify_core::value::IntoValue::into_value(self.#ident.clone())
            });
        }

        // Determine SqlType and nullable from the Rust type + attributes
        let (is_option, inner_ty) = unwrap_option_type(ty);
        let is_nullable = col_attrs.nullable || is_option;
        let sql_type_token = if let Some(ref custom) = col_attrs.sql_type {
            let custom_str: &str = custom;
            quote! { reify_core::schema::SqlType::Custom(#custom_str) }
        } else if col_attrs.primary_key && col_attrs.auto_increment {
            quote! { reify_core::schema::SqlType::BigSerial }
        } else {
            rust_type_to_sql_type(inner_ty)
        };

        let is_pk = col_attrs.primary_key;
        let is_auto = col_attrs.auto_increment;
        let is_unique = col_attrs.unique;
        let is_index = col_attrs.index;

        // For db-source timestamps, auto-set default to NOW() if not explicitly provided
        let is_db_source = col_attrs.timestamp_source.as_deref() == Some("db");
        let default_token = match &col_attrs.default {
            Some(d) => quote! { Some(#d.to_string()) },
            None if is_db_source => quote! { Some("NOW()".to_string()) },
            None => quote! { None },
        };

        let computed_token = if let Some(ref expr) = col_attrs.computed {
            quote! { Some(reify_core::schema::ComputedColumn::Stored(#expr.to_string())) }
        } else if col_attrs.computed_rust {
            quote! { Some(reify_core::schema::ComputedColumn::Virtual) }
        } else {
            quote! { None }
        };

        // Timestamp kind & source tokens
        let timestamp_kind_token = if col_attrs.creation_timestamp {
            quote! { Some(reify_core::schema::TimestampKind::Creation) }
        } else if col_attrs.update_timestamp {
            quote! { Some(reify_core::schema::TimestampKind::Update) }
        } else {
            quote! { None }
        };

        let timestamp_source_token = if is_db_source {
            quote! { reify_core::schema::TimestampSource::Db }
        } else {
            quote! { reify_core::schema::TimestampSource::Vm }
        };

        col_defs_tokens.push(quote! {
            reify_core::schema::ColumnDef {
                name: #name_str,
                sql_type: #sql_type_token,
                primary_key: #is_pk,
                auto_increment: #is_auto,
                unique: #is_unique,
                index: #is_index,
                nullable: #is_nullable,
                default: #default_token,
                computed: #computed_token,
                timestamp_kind: #timestamp_kind_token,
                timestamp_source: #timestamp_source_token,
            }
        });
    }

    let col_name_strs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();
    let num_cols = col_names.len();

    // Generate Column constants
    let column_consts =
        col_idents
            .iter()
            .zip(col_types.iter())
            .zip(col_names.iter())
            .map(|((ident, ty), name)| {
                quote! {
                    #[allow(non_upper_case_globals)]
                    pub const #ident: reify_core::Column<#struct_name, #ty> = reify_core::Column::new(#name);
                }
            });

    // Generate IndexDef tokens for single-column indexes from #[column(index)]
    let single_index_tokens = single_col_indexes.iter().map(|col_name| {
        let auto_name = format!("idx_{}_{}", table_name, col_name);
        quote! {
            reify_core::IndexDef {
                name: Some(#auto_name.to_string()),
                columns: vec![#col_name],
                unique: false,
                kind: reify_core::IndexKind::BTree,
                  predicate: None,
            }
        }
    });

    // Generate IndexDef tokens for composite indexes from #[table(index(...))]
    let composite_index_tokens = table_attr.indexes.iter().map(|idx| {
        let cols = &idx.columns;
        let unique = idx.unique;
        let name_token = match &idx.name {
            Some(n) => quote! { Some(#n.to_string()) },
            None => {
                let sep = "_";
                let auto_name = format!("idx_{}_{}", table_name, cols.join(sep));
                quote! { Some(#auto_name.to_string()) }
            }
        };
        let predicate_token = match &idx.predicate {
            Some(p) => quote! { Some(#p.to_string()) },
            None => quote! { None },
        };

        quote! {
            reify_core::IndexDef {
                  name: #name_token,
                columns: vec![#(#cols),*],
                unique: #unique,
                kind: reify_core::IndexKind::BTree,
                predicate: #predicate_token,
            }
        }
    });

    let all_index_tokens = single_index_tokens.chain(composite_index_tokens);

    // ── Optional Auditable impl ─────────────────────────────────────
    let audit_impl = if table_attr.audit {
        let audit_table_name = format!("{}_audit", table_name);
        quote! {
            impl reify_core::audit::Auditable for #struct_name {
                fn audit_table_name() -> &'static str {
                    #audit_table_name
                }
                fn audit_column_defs() -> Vec<reify_core::schema::ColumnDef> {
                    reify_core::audit::audit_column_defs_for(#audit_table_name)
                }
            }
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        // ── Table trait impl ────────────────────────────────────────
        impl reify_core::Table for #struct_name {
            fn table_name() -> &'static str {
                #table_name
            }

            fn column_names() -> &'static [&'static str] {
                static COLS: [&str; #num_cols] = [#(#col_name_strs),*];
                &COLS
            }

            fn into_values(&self) -> Vec<reify_core::Value> {
                vec![#(#value_conversions),*]
            }

            fn column_defs() -> Vec<reify_core::schema::ColumnDef> {
                vec![#(#col_defs_tokens),*]
            }

            fn indexes() -> Vec<reify_core::IndexDef> {
                vec![#(#all_index_tokens),*]
            }

            fn update_timestamp_columns() -> Vec<&'static str> {
                vec![#(#update_ts_vm_cols),*]
            }
        }

        // ── Column constants + query builder helpers ────────────────
        impl #struct_name {
            #(#column_consts)*

            pub fn find() -> reify_core::SelectBuilder<#struct_name> {
                reify_core::SelectBuilder::new()
            }

            pub fn insert(val: &#struct_name) -> reify_core::InsertBuilder<#struct_name> {
                reify_core::InsertBuilder::new(val)
            }

            pub fn insert_many(models: &[#struct_name]) -> reify_core::InsertManyBuilder<#struct_name> {
                reify_core::InsertManyBuilder::new(models)
            }

            pub fn update() -> reify_core::UpdateBuilder<#struct_name> {
                reify_core::UpdateBuilder::new()
            }

            pub fn delete() -> reify_core::DeleteBuilder<#struct_name> {
                reify_core::DeleteBuilder::new()
            }
        }
    };

    Ok(quote! { #expanded #audit_impl })
}

/// Parse `#[table(name = "users", index(...), ...)]`
fn parse_table_attr(attrs: &[Attribute]) -> syn::Result<TableAttr> {
    for attr in attrs {
        if !attr.path().is_ident("table") {
            continue;
        }

        let mut table_name = None;
        let mut indexes = Vec::new();
        let mut audit = false;

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                let value = meta.value()?;
                let lit: Lit = value.parse()?;
                if let Lit::Str(s) = lit {
                    table_name = Some(s.value());
                }
            } else if meta.path.is_ident("audit") {
                audit = true;
            } else if meta.path.is_ident("index") {
                let mut columns = Vec::new();
                let mut unique = false;
                let mut name = None;
                let mut predicate = None;

                meta.parse_nested_meta(|inner| {
                    if inner.path.is_ident("columns") {
                        let content;
                        syn::parenthesized!(content in inner.input);
                        let lits = content.parse_terminated(Lit::parse, syn::Token![,])?;
                        for lit in lits {
                            if let Lit::Str(s) = lit {
                                columns.push(s.value());
                            }
                        }
                    } else if inner.path.is_ident("unique") {
                        unique = true;
                    } else if inner.path.is_ident("name") {
                        let value = inner.value()?;
                        let lit: Lit = value.parse()?;
                        if let Lit::Str(s) = lit {
                            name = Some(s.value());
                        }
                    } else if inner.path.is_ident("predicate") {
                        let value = inner.value()?;
                        let lit: Lit = value.parse()?;
                        if let Lit::Str(s) = lit {
                            predicate = Some(s.value());
                        }
                    }
                    Ok(())
                })?;

                indexes.push(ParsedIndex {
                    columns,
                    unique,
                    name,
                    predicate,
                });
            }
            Ok(())
        })?;

        if let Some(name) = table_name {
            return Ok(TableAttr { name, indexes, audit });
        }
    }
    Err(syn::Error::new(
        proc_macro2::Span::call_site(),
        r#"Missing #[table(name = "...")] attribute"#,
    ))
}

// ── DbEnum derive ──────────────────────────────────────────────────

fn impl_db_enum(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let enum_name = &input.ident;

    let variants = match &input.data {
        Data::Enum(data) => &data.variants,
        _ => {
            return Err(syn::Error::new_spanned(
                input,
                "DbEnum can only be derived on enums",
            ))
        }
    };

    let mut variant_idents = Vec::new();
    let mut variant_strs = Vec::new();

    for variant in variants {
        // Ensure unit variant (no fields)
        if !variant.fields.is_empty() {
            return Err(syn::Error::new_spanned(
                variant,
                "DbEnum variants must be unit variants (no fields)",
            ));
        }

        let ident = &variant.ident;
        let db_name = parse_db_enum_rename(&variant.attrs)
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
    })
}

/// Parse `#[db_enum(rename = "...")]` on a variant.
fn parse_db_enum_rename(attrs: &[Attribute]) -> Option<String> {
    for attr in attrs {
        if !attr.path().is_ident("db_enum") {
            continue;
        }
        let mut rename = None;
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                let value = meta.value()?;
                let lit: Lit = value.parse()?;
                if let Lit::Str(s) = lit {
                    rename = Some(s.value());
                }
            }
            Ok(())
        });
        if rename.is_some() {
            return rename;
        }
    }
    None
}

/// Convert `PascalCase` to `snake_case`.
fn to_snake_case(s: &str) -> String {
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

/// Parsed column attributes from `#[column(...)]`.
#[derive(Default)]
struct ColumnAttrs {
    primary_key: bool,
    auto_increment: bool,
    unique: bool,
    nullable: bool,
    index: bool,
    default: Option<String>,
    sql_type: Option<String>,
    /// DB-generated computed column: `GENERATED ALWAYS AS (expr) STORED`.
    computed: Option<String>,
    /// Rust-side virtual column: not in the DB, computed after fetch.
    computed_rust: bool,
    /// Auto-set on INSERT (like Hibernate's `@CreationTimestamp`).
    creation_timestamp: bool,
    /// Auto-set on INSERT and UPDATE (like Hibernate's `@UpdateTimestamp`).
    update_timestamp: bool,
    /// Source of the current date: `"vm"` (default) or `"db"`.
    timestamp_source: Option<String>,
}

/// Parse `#[column(...)]` attributes using proper `syn` parsing.
fn parse_column_attrs(attrs: &[Attribute]) -> ColumnAttrs {
    let mut result = ColumnAttrs::default();
    for attr in attrs {
        if !attr.path().is_ident("column") {
            continue;
        }
        let _ = attr.parse_nested_meta(|meta| {
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
            }
            Ok(())
        });
    }
    result
}

/// Derive macro that generates `FromRow` and a `select_columns()` helper
/// for a partial model.
#[proc_macro_derive(PartialModel, attributes(partial_model))]
pub fn derive_partial_model(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match impl_partial_model(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn impl_partial_model(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let struct_name = &input.ident;
    let _entity = parse_partial_model_attr(&input.attrs)?;

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named) => &named.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    input,
                    "PartialModel requires named fields",
                ))
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                input,
                "PartialModel only works on structs",
            ))
        }
    };

    let mut col_names: Vec<String> = Vec::new();
    let mut field_idents: Vec<syn::Ident> = Vec::new();
    let mut from_row_arms: Vec<proc_macro2::TokenStream> = Vec::new();

    for field in fields.iter() {
        let ident = field.ident.as_ref().unwrap();
        let ty = &field.ty;
        let name_str = ident.to_string();
        col_names.push(name_str.clone());
        field_idents.push(ident.clone());

        from_row_arms.push(quote! {
            let #ident = match row.get(#name_str) {
                Some(v) => <#ty as reify_core::value::FromValue>::from_value(v.clone())
                    .map_err(|e| reify_core::db::DbError::Conversion(e))?,
                None => return Err(reify_core::db::DbError::Conversion(
                    format!("missing column: {}", #name_str)
                )),
            };
        });
    }

    let num_cols = col_names.len();
    let col_name_strs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();

    Ok(quote! {
        impl reify_core::db::FromRow for #struct_name {
            fn from_row(row: &reify_core::db::Row) -> Result<Self, reify_core::db::DbError> {
                #(#from_row_arms)*
                Ok(Self { #(#field_idents),* })
            }
        }

        impl #struct_name {
            pub fn select_columns() -> &'static [&'static str] {
                static COLS: [&str; #num_cols] = [#(#col_name_strs),*];
                &COLS
            }
        }
    })
}

fn parse_partial_model_attr(attrs: &[Attribute]) -> syn::Result<Option<String>> {
    for attr in attrs {
        if !attr.path().is_ident("partial_model") {
            continue;
        }
        let mut entity = None;
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("entity") {
                let value = meta.value()?;
                let lit: Lit = value.parse()?;
                if let Lit::Str(s) = lit {
                    entity = Some(s.value());
                }
            }
            Ok(())
        })?;
        return Ok(entity);
    }
    Ok(None)
}

// ── Type mapping helpers ────────────────────────────────────────────

/// Unwrap `Option<T>` → `(true, T)`, or return `(false, ty)` unchanged.
fn unwrap_option_type(ty: &syn::Type) -> (bool, &syn::Type) {
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

/// Map a Rust type to the corresponding `SqlType` variant token stream.
fn rust_type_to_sql_type(ty: &syn::Type) -> proc_macro2::TokenStream {
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
        // chrono types
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
        // uuid
        "uuid::Uuid" | "Uuid" => quote! { reify_core::schema::SqlType::Uuid },
        // serde_json
        "serde_json::Value" | "JsonValue" => {
            quote! { reify_core::schema::SqlType::Jsonb }
        }
        // Fallback
        _ => quote! { reify_core::schema::SqlType::Text },
    }
}

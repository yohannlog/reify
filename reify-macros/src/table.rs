use quote::quote;
use syn::{Attribute, Data, DeriveInput, Fields, Lit, parse::Parse};

use crate::helpers::{
    parse_column_attrs, parse_sql_type_string, rust_type_to_sql_type, to_snake_case,
    unwrap_option_type,
};

// ── Parsed index from #[table(index(...))] ──────────────────────────

pub(crate) struct ParsedIndexColumn {
    pub name: String,
    pub desc: bool,
}

pub(crate) struct ParsedIndex {
    pub columns: Vec<ParsedIndexColumn>,
    pub unique: bool,
    pub name: Option<String>,
    pub predicate: Option<String>,
}

// ── Parsed table attribute ──────────────────────────────────────────

pub(crate) struct TableAttr {
    pub name: String,
    pub indexes: Vec<ParsedIndex>,
    pub audit: bool,
    pub dto_skip: Vec<String>,
}

pub(crate) fn impl_table(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let struct_name = &input.ident;

    let table_attr = parse_table_attr(&input.attrs)?;
    let table_name = &table_attr.name;

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named) => &named.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    input,
                    "Table derive requires named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                input,
                "Table derive only works on structs",
            ));
        }
    };

    let mut col_names = Vec::new();
    let mut col_idents = Vec::new();
    let mut col_types = Vec::new();
    let mut value_conversions = Vec::new();
    let mut single_col_indexes: Vec<String> = Vec::new();
    let mut update_ts_vm_cols: Vec<String> = Vec::new();

    let mut col_defs_tokens: Vec<proc_macro2::TokenStream> = Vec::new();

    #[cfg(feature = "dto")]
    let mut dto_fields: Vec<(syn::Ident, syn::Type, Option<String>)> = Vec::new();

    for field in fields.iter() {
        let ident = field.ident.as_ref().unwrap();
        let ty = &field.ty;
        let name_str = ident.to_string();

        col_names.push(name_str.clone());
        col_idents.push(ident.clone());
        col_types.push(ty.clone());

        let col_attrs = parse_column_attrs(&field.attrs)?;
        if col_attrs.index {
            single_col_indexes.push(name_str.clone());
        }

        #[cfg(feature = "dto")]
        {
            let skip = (col_attrs.primary_key && col_attrs.auto_increment)
                || col_attrs.creation_timestamp
                || col_attrs.update_timestamp
                || table_attr.dto_skip.contains(&name_str);
            if !skip {
                dto_fields.push((ident.clone(), ty.clone(), col_attrs.validate.clone()));
            }
        }

        if col_attrs.update_timestamp && col_attrs.timestamp_source.as_deref() != Some("db") {
            update_ts_vm_cols.push(name_str.clone());
        }

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

        let (is_option, inner_ty) = unwrap_option_type(ty);
        let is_nullable = col_attrs.nullable || is_option;
        let sql_type_token = if let Some(ref custom) = col_attrs.sql_type {
            parse_sql_type_string(custom)
        } else if col_attrs.primary_key && col_attrs.auto_increment {
            quote! { reify_core::schema::SqlType::BigSerial }
        } else {
            rust_type_to_sql_type(inner_ty)
        };

        let is_pk = col_attrs.primary_key;
        let is_auto = col_attrs.auto_increment;
        let is_unique = col_attrs.unique;
        let is_index = col_attrs.index;

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

        let check_token = match &col_attrs.check {
            Some(expr) => quote! { Some(#expr.to_string()) },
            None => quote! { None },
        };

        let fk_token = if let Some(ref refs) = col_attrs.references {
            let parts: Vec<&str> = refs.splitn(2, "::").collect();
            let (ref_table_raw, ref_col) = if parts.len() == 2 {
                (parts[0], parts[1])
            } else {
                (refs.as_str(), "id")
            };
            let ref_table = to_snake_case(ref_table_raw);
            let ref_table = if ref_table.ends_with('s') {
                ref_table
            } else {
                format!("{ref_table}s")
            };
            let on_delete_str = col_attrs.on_delete.as_deref().unwrap_or("NO ACTION");
            let on_update_str = col_attrs.on_update.as_deref().unwrap_or("NO ACTION");
            quote! {
                Some(reify_core::schema::ForeignKeyDef {
                    references_table: #ref_table.to_string(),
                    references_column: #ref_col.to_string(),
                    on_delete: reify_core::schema::ForeignKeyAction::from_str(#on_delete_str)
                        .unwrap_or(reify_core::schema::ForeignKeyAction::NoAction),
                    on_update: reify_core::schema::ForeignKeyAction::from_str(#on_update_str)
                        .unwrap_or(reify_core::schema::ForeignKeyAction::NoAction),
                })
            }
        } else {
            quote! { None }
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
                check: #check_token,
                foreign_key: #fk_token,
            }
        });
    }

    let col_name_strs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();
    let num_cols = col_names.len();

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

    let single_index_tokens = single_col_indexes.iter().map(|col_name| {
        let auto_name = format!("idx_{}_{}", table_name, col_name);
        quote! {
            reify_core::IndexDef {
                name: Some(#auto_name.to_string()),
                columns: vec![reify_core::IndexColumnDef::asc(#col_name)],
                unique: false,
                kind: reify_core::IndexKind::BTree,
                predicate: None,
            }
        }
    });

    let composite_index_tokens = table_attr.indexes.iter().map(|idx| {
        let col_tokens: Vec<_> = idx
            .columns
            .iter()
            .map(|c| {
                let name = &c.name;
                if c.desc {
                    quote! { reify_core::IndexColumnDef::desc(#name) }
                } else {
                    quote! { reify_core::IndexColumnDef::asc(#name) }
                }
            })
            .collect();
        let col_names: Vec<&str> = idx.columns.iter().map(|c| c.name.as_str()).collect();
        let unique = idx.unique;
        let name_token = match &idx.name {
            Some(n) => quote! { Some(#n.to_string()) },
            None => {
                let sep = "_";
                let auto_name = format!("idx_{}_{}", table_name, col_names.join(sep));
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
                columns: vec![#(#col_tokens),*],
                unique: #unique,
                kind: reify_core::IndexKind::BTree,
                predicate: #predicate_token,
            }
        }
    });

    let all_index_tokens = single_index_tokens.chain(composite_index_tokens);

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

    let from_row_arms: Vec<proc_macro2::TokenStream> = col_idents
        .iter()
        .zip(col_types.iter())
        .zip(col_names.iter())
        .map(|((ident, ty), name)| {
            quote! {
                let #ident: #ty = match row.get(#name) {
                    Some(v) => <#ty as reify_core::value::FromValue>::from_value(v.clone())
                        .map_err(|e| reify_core::db::DbError::Conversion(
                            format!("column '{}': {}", #name, e)
                        ))?,
                    None => return Err(reify_core::db::DbError::Conversion(
                        format!("missing column: {}", #name)
                    )),
                };
            }
        })
        .collect();

    let from_row_field_names = col_idents.iter().collect::<Vec<_>>();

    let expanded = quote! {
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

        impl reify_core::db::FromRow for #struct_name {
            fn from_row(row: &reify_core::db::Row) -> Result<Self, reify_core::db::DbError> {
                #(#from_row_arms)*
                Ok(Self { #(#from_row_field_names),* })
            }
        }

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

    #[cfg(feature = "dto")]
    let dto_impl = {
        let dto_name = syn::Ident::new(&format!("{}Dto", struct_name), struct_name.span());

        let dto_field_defs: Vec<proc_macro2::TokenStream> = dto_fields
            .iter()
            .map(|(ident, ty, _validate)| {
                quote! { pub #ident: #ty }
            })
            .collect();

        let dto_col_names: Vec<String> =
            dto_fields.iter().map(|(id, _, _)| id.to_string()).collect();
        let dto_col_name_strs: Vec<&str> = dto_col_names.iter().map(|s| s.as_str()).collect();
        let dto_num_cols = dto_col_names.len();

        let dto_value_conversions: Vec<proc_macro2::TokenStream> = dto_fields
            .iter()
            .map(|(ident, _, _)| {
                quote! { reify_core::value::IntoValue::into_value(self.#ident.clone()) }
            })
            .collect();

        #[cfg(feature = "dto-validation")]
        let dto_field_attrs: Vec<proc_macro2::TokenStream> = dto_fields
            .iter()
            .map(|(_, _, validate)| {
                if let Some(rule) = validate {
                    let tokens: proc_macro2::TokenStream = rule.parse().unwrap_or_default();
                    quote! { #[validate(#tokens)] }
                } else {
                    quote! {}
                }
            })
            .collect();

        #[cfg(not(feature = "dto-validation"))]
        let dto_field_attrs: Vec<proc_macro2::TokenStream> =
            dto_fields.iter().map(|_| quote! {}).collect();

        #[cfg(feature = "dto-validation")]
        let dto_derives = quote! { #[derive(Debug, Clone, validator::Validate)] };

        #[cfg(not(feature = "dto-validation"))]
        let dto_derives = quote! { #[derive(Debug, Clone)] };

        quote! {
            #dto_derives
            pub struct #dto_name {
                #(#dto_field_attrs #dto_field_defs,)*
            }

            impl #dto_name {
                pub fn column_names() -> &'static [&'static str] {
                    static COLS: [&str; #dto_num_cols] = [#(#dto_col_name_strs),*];
                    &COLS
                }

                pub fn into_values(&self) -> Vec<reify_core::Value> {
                    vec![#(#dto_value_conversions),*]
                }
            }
        }
    };

    #[cfg(not(feature = "dto"))]
    let dto_impl = quote! {};

    Ok(quote! { #expanded #audit_impl #dto_impl })
}

fn parse_table_attr(attrs: &[Attribute]) -> syn::Result<TableAttr> {
    for attr in attrs {
        if !attr.path().is_ident("table") {
            continue;
        }

        let mut table_name = None;
        let mut indexes = Vec::new();
        let mut audit = false;
        let mut dto_skip = Vec::new();

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                let value = meta.value()?;
                let lit: Lit = value.parse()?;
                if let Lit::Str(s) = lit {
                    table_name = Some(s.value());
                }
            } else if meta.path.is_ident("audit") {
                audit = true;
            } else if meta.path.is_ident("dto") {
                let _ = meta.parse_nested_meta(|inner| {
                    if inner.path.is_ident("skip") {
                        let value = inner.value()?;
                        let lit: Lit = value.parse()?;
                        if let Lit::Str(s) = lit {
                            dto_skip.extend(s.value().split(',').map(|s| s.trim().to_string()));
                        }
                    }
                    Ok(())
                });
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
                                let val = s.value();
                                let (name, desc) = if let Some(base) = val
                                    .strip_suffix(" desc")
                                    .or_else(|| val.strip_suffix(" DESC"))
                                {
                                    (base.to_string(), true)
                                } else if let Some(base) = val
                                    .strip_suffix(" asc")
                                    .or_else(|| val.strip_suffix(" ASC"))
                                {
                                    (base.to_string(), false)
                                } else {
                                    (val, false)
                                };
                                columns.push(ParsedIndexColumn { name, desc });
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
            return Ok(TableAttr {
                name,
                indexes,
                audit,
                dto_skip,
            });
        }
    }
    Err(syn::Error::new(
        proc_macro2::Span::call_site(),
        r#"Missing #[table(name = "...")] attribute"#,
    ))
}

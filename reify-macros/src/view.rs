use quote::quote;
use syn::{Attribute, Data, DeriveInput, Fields, Lit};

use crate::helpers::{
    parse_column_attrs, parse_sql_type_string, rust_type_to_sql_type, unwrap_option_type,
};

struct ViewAttr {
    name: String,
    query: Option<String>,
}

fn parse_view_attr(attrs: &[Attribute]) -> syn::Result<ViewAttr> {
    for attr in attrs {
        if !attr.path().is_ident("view") {
            continue;
        }

        let mut view_name = None;
        let mut query = None;

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                let value = meta.value()?;
                let lit: Lit = value.parse()?;
                if let Lit::Str(s) = lit {
                    view_name = Some(s.value());
                }
            } else if meta.path.is_ident("query") {
                let value = meta.value()?;
                let lit: Lit = value.parse()?;
                if let Lit::Str(s) = lit {
                    query = Some(s.value());
                }
            }
            Ok(())
        })?;

        if let Some(name) = view_name {
            return Ok(ViewAttr { name, query });
        }
    }
    Err(syn::Error::new(
        proc_macro2::Span::call_site(),
        r#"Missing #[view(name = "...")] attribute"#,
    ))
}

pub(crate) fn impl_view(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let struct_name = &input.ident;

    let view_attr = parse_view_attr(&input.attrs)?;
    let view_name = &view_attr.name;

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named) => &named.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    input,
                    "View derive requires named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                input,
                "View derive only works on structs",
            ));
        }
    };

    let mut col_names = Vec::new();
    let mut col_idents = Vec::new();
    let mut col_types = Vec::new();
    let mut col_defs_tokens: Vec<proc_macro2::TokenStream> = Vec::new();

    for field in fields.iter() {
        let ident = field.ident.as_ref().unwrap();
        let ty = &field.ty;
        let name_str = ident.to_string();

        col_names.push(name_str.clone());
        col_idents.push(ident.clone());
        col_types.push(ty.clone());

        let col_attrs = parse_column_attrs(&field.attrs)?;
        let (is_option, inner_ty) = unwrap_option_type(ty);
        let is_nullable = is_option;
        let sql_type_token = if let Some(ref custom) = col_attrs.sql_type {
            parse_sql_type_string(custom)
        } else {
            rust_type_to_sql_type(inner_ty)
        };

        col_defs_tokens.push(quote! {
            reify_core::schema::ColumnDef {
                name: #name_str,
                sql_type: #sql_type_token,
                primary_key: false,
                auto_increment: false,
                unique: false,
                index: false,
                nullable: #is_nullable,
                default: None,
                computed: None,
                timestamp_kind: None,
                timestamp_source: reify_core::schema::TimestampSource::Vm,
                check: None,
                foreign_key: None,
                // Views never carry a soft-delete column themselves; the
                // underlying tables own that semantic. Always emit `false`
                // so the macro stays in sync with `ColumnDef`'s public
                // shape.
                soft_delete: false,
            }
        });
    }

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

    let view_query_impl = if let Some(ref query) = view_attr.query {
        quote! {
            fn view_query() -> reify_core::view::ViewQuery {
                reify_core::view::ViewQuery::Raw(#query.to_string())
            }
        }
    } else {
        quote! {
            fn view_query() -> reify_core::view::ViewQuery {
                reify_core::view::ViewQuery::Raw(String::new())
            }
        }
    };

    Ok(quote! {
        impl reify_core::Table for #struct_name {
            fn table_name() -> &'static str {
                #view_name
            }

            fn column_names() -> &'static [&'static str] {
                static __REIFY_COLS: [&str; #num_cols] = [#(#col_names),*];
                &__REIFY_COLS
            }

            fn as_values(&self) -> Vec<reify_core::Value> {
                vec![]
            }

            fn column_defs() -> Vec<reify_core::schema::ColumnDef> {
                vec![#(#col_defs_tokens),*]
            }
        }

        impl reify_core::view::View for #struct_name {
            fn view_name() -> &'static str {
                #view_name
            }

            #view_query_impl
        }

        impl #struct_name {
            #(#column_consts)*

            pub fn find() -> reify_core::SelectBuilder<#struct_name> {
                reify_core::SelectBuilder::new()
            }
        }
    })
}

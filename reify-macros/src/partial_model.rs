use quote::quote;
use syn::{Attribute, Data, DeriveInput, Fields, Lit};

pub(crate) fn impl_partial_model(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let struct_name = &input.ident;
    let _entity = parse_partial_model_attr(&input.attrs)?;

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named) => &named.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    input,
                    "PartialModel requires named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                input,
                "PartialModel only works on structs",
            ));
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

    Ok(quote! {
        impl reify_core::db::FromRow for #struct_name {
            fn from_row(row: &reify_core::db::Row) -> Result<Self, reify_core::db::DbError> {
                #(#from_row_arms)*
                Ok(Self { #(#field_idents),* })
            }
        }

        impl #struct_name {
            pub fn select_columns() -> &'static [&'static str] {
                static __REIFY_COLS: [&str; #num_cols] = [#(#col_names),*];
                &__REIFY_COLS
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

use proc_macro::TokenStream;
use proc_macro2::{Ident, Span, TokenStream as TokenStream2};
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Result, Type};

#[proc_macro_derive(SystemCatalog)]
pub fn system_catalog_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let expanded = match impl_system_catalog(input) {
        Ok(tokens) => tokens,
        Err(err) => {
            return err.to_compile_error().into();
        }
    };
    TokenStream::from(expanded)
}

fn impl_system_catalog(input: DeriveInput) -> Result<TokenStream2> {
    let data = input.data;
    let field_names_types = if let Data::Struct(data_struct) = data {
        if let Fields::Named(fields) = data_struct.fields {
            fields
                .named
                .iter()
                .cloned()
                .map(|field| {
                    let field_name = field.ident.as_ref().unwrap().clone();
                    let field_type = field.ty.clone();
                    (field_name, field_type)
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    let generated_code = quote! {};
    Ok(generated_code)
}

use proc_macro::TokenStream;
use quote::quote;
use syn::parse_macro_input;
use syn::DeriveInput;

#[proc_macro_derive(StructName)]
pub fn derive_struct_name(item: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(item as DeriveInput);
    let struct_name = &ast.ident;

    let expanded = quote! {
        impl struct_name::StructName for #struct_name {
            fn struct_name() -> &'static str {
                stringify!(#struct_name)
            }
        }
    };
    expanded.into()
}

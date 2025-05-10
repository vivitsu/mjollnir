use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, parse_macro_input};

extern crate proc_macro;

#[proc_macro_attribute]
pub fn main(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);

    let sig = &input_fn.sig;
    let block = &input_fn.block;
    let vis = &input_fn.vis;
    let attrs = &input_fn.attrs;

    if sig.asyncness.is_none() {
        return syn::Error::new_spanned(sig.fn_token, "function must be async")
            .to_compile_error()
            .into();
    }

    let expanded = quote! {
        #(#attrs)*
        #vis fn main() {
            let runtime = match kala::executor::Executor::new() {
                Ok(runtime) => Arc::new(runtime),
                Err(e) => panic!("Could not create runtime: {:?}", e),
            };
            
            kala::executor::enter_runtime_scope(runtime.clone(), move || {
                runtime.block_on(async move {
                    #block
                })
            });
        }
    };

    expanded.into()
}

use proc_macro::TokenStream;
use syn::{parse_macro_input, ItemFn};
use quote::quote;

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
            let runtime = Arc::new(kala::executor::Executor::new());
            
            kala::CURRENT_RUNTIME.with(|slot| {
                *slot.borrow_mut() = Some(runtime.clone());
            });

            runtime.block_on(async move {
                #block
            })
        }
    };
    
    expanded.into()
}

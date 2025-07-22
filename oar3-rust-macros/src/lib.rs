use proc_macro::TokenStream;
use std::sync::atomic::AtomicU32;

use syn::{parse_macro_input, ItemFn};

static COUNTER: AtomicU32 = AtomicU32::new(1);

#[proc_macro_attribute]
pub fn benchmark(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let name = &input.sig.ident;
    let block = &input.block;
    let sig = &input.sig;

    let function_key = name.to_string();
    let function_num = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    let output = quote::quote! {
        pub #sig {
            let start = std::time::Instant::now();
            let result = (|| #block)();
            let duration = start.elapsed();

            let mut metrics = crate::FUNCTION_METRICS.lock().unwrap();
            let entry = metrics.entry((#function_key.to_string(), #function_num)).or_insert((0, std::time::Duration::new(0, 0)));
            entry.0 += 1;
            entry.1 += duration;

            result
        }
    };

    output.into()
}


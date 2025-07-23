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
    let vis = &input.vis;

    let function_key = name.to_string();
    let function_num = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    let output = quote::quote! {
        #vis #sig {
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

#[proc_macro_attribute]
pub fn benchmark_hierarchy(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let name = &input.sig.ident;
    let block = &input.block;
    let sig = &input.sig;
    let vis = &input.vis;

    let function_key = name.to_string();
    let function_num = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    let output = quote::quote! {
        #vis #sig {
            let mut parent = crate::CALL_STACK.with(|stack| stack.borrow().last().cloned().unwrap_or(0));
            let is_recursive = #function_num == parent;
            if !is_recursive {
                crate::CALL_STACK.with(|stack| stack.borrow_mut().push(#function_num));
            }
            let start = std::time::Instant::now();
            let result = (|| #block)();
            let duration = start.elapsed();
            if !is_recursive {
                crate::CALL_STACK.with(|stack| stack.borrow_mut().pop());

                let mut metrics = crate::FUNCTION_METRICS.lock().unwrap();
                let entry = metrics.entry((#function_key.to_string(), #function_num)).or_insert((0, std::time::Duration::new(0, 0)));
                entry.0 += 1;
                entry.1 += duration;

                let mut metrics_hy = crate::FUNCTION_METRICS_HIERARCHY.lock().unwrap();
                let entry = metrics_hy
                    .entry(crate::CALL_STACK.with(|stack| stack.borrow().clone()))
                    .or_insert(std::collections::HashMap::new())
                    .entry((#function_key.to_string(), #function_num))
                    .or_insert((0, std::time::Duration::new(0, 0)));
                entry.0 += 1;
                entry.1 += duration;
            }
            result
        }
    };
    output.into()
}


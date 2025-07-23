use std::cell::RefCell;
use crate::benchmark::benchmarker::{BenchmarkConfig, BenchmarkTarget, WaitingJobsSampleType};
use crate::benchmark::grapher::graph_benchmark_result;
use lazy_static::lazy_static;
use log::LevelFilter;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;
use crate::benchmark::function_benchmark::{print_function_benchmark_results, print_function_benchmark_results_hierarchy};

mod benchmark;
mod models;
mod platform;
mod scheduler;


thread_local! {
    static CALL_STACK: RefCell<Vec<u32>> = RefCell::new(Vec::new());
}
lazy_static! {
    /// (Function name, Function index) -> (Call count, Total duration)
    static ref FUNCTION_METRICS: Mutex<HashMap<(String, u32), (u64, Duration)>> = Mutex::new(HashMap::new());
    /// (function index stack) -> (Function name, Function index, Call count, Total duration)
    static ref FUNCTION_METRICS_HIERARCHY: Mutex<HashMap<Vec<u32>, HashMap<(String, u32), (u64, Duration)>>> = Mutex::new(HashMap::new());
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    env_logger::Builder::new()
        .filter(None, LevelFilter::Info)
        .filter(Some("oar3_rust::scheduler::hierarchy"), LevelFilter::Debug)
        .init();

    let benchmark = BenchmarkConfig {
        target: BenchmarkTarget::Tree,
        sample_type: WaitingJobsSampleType::NodeOnly,
        cache: false,
        averaging: 1,
        res_count: 10_000,
        start: 6,
        end: 6,
        step: 6,
        seed: 22,
        single_thread: false,
    };
    let results = benchmark.benchmark().await;


    print_function_benchmark_results();
    print_function_benchmark_results_hierarchy();
    graph_benchmark_result("6_benchmarked".to_string(), benchmark, results);
}

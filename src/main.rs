use crate::benchmark::benchmarker::{BenchmarkConfig, BenchmarkTarget, WaitingJobsSampleType};
use crate::benchmark::grapher::graph_benchmark_result;
use lazy_static::lazy_static;
use log::LevelFilter;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;
use crate::benchmark::function_benchmark::print_function_benchmark_results;

mod benchmark;
mod models;
mod platform;
mod scheduler;

lazy_static! {
    static ref FUNCTION_METRICS: Mutex<HashMap<(String, u32), (u64, Duration)>> = Mutex::new(HashMap::new());
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    env_logger::Builder::new().filter(None, LevelFilter::Info).init();

    let benchmark = BenchmarkConfig {
        target: BenchmarkTarget::Tree,
        sample_type: WaitingJobsSampleType::NodeOnly,
        cache: true,
        averaging: 5,
        res_count: 10_000,
        start: 0,
        end: 400,
        step: 100,
        seed: 22,
        single_thread: false,
    };
    let results = benchmark.benchmark().await;

    graph_benchmark_result("6_benchmarked".to_string(), benchmark, results);

    print_function_benchmark_results();
}

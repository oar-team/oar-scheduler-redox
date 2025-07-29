mod benchmarker;
mod grapher;
mod python_caller;
mod platform_mock;

use log::LevelFilter;
use oar3_scheduler::auto_bench_fct::{print_bench_fct_hy_results, print_bench_fct_results};
use crate::benchmarker::{BenchmarkConfig, BenchmarkTarget, WaitingJobsSampleType};
use crate::grapher::graph_benchmark_result;

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    env_logger::Builder::new()
        .filter(None, LevelFilter::Info)
        .filter(Some("oar3_rust::scheduler::hierarchy"), LevelFilter::Debug)
        .init();

    let benchmark = BenchmarkConfig {
        target: BenchmarkTarget::Rust,
        sample_type: WaitingJobsSampleType::NodeOnly,
        cache: true,
        averaging: 1,
        res_count: 10_000,
        start: 0,
        end: 500,
        step: 100,
        seed: 26,
        single_thread: true,
    };
    let results = benchmark.benchmark().await;


    print_bench_fct_results();
    print_bench_fct_hy_results();
    graph_benchmark_result("1_ts".to_string(), benchmark, results);
}

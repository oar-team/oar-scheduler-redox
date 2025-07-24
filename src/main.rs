use crate::benchmark::benchmarker::{BenchmarkConfig, BenchmarkTarget, WaitingJobsSampleType};
use crate::benchmark::grapher::graph_benchmark_result;
use auto_bench_fct::{print_bench_fct_hy_results, print_bench_fct_results};
use log::LevelFilter;

mod benchmark;
mod models;
mod platform;
mod scheduler;


#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    env_logger::Builder::new()
        .filter(None, LevelFilter::Info)
        .filter(Some("oar3_rust::scheduler::hierarchy"), LevelFilter::Debug)
        .init();

    let benchmark = BenchmarkConfig {
        target: BenchmarkTarget::Basic,
        sample_type: WaitingJobsSampleType::CoreOnly,
        cache: false,
        averaging: 1,
        res_count: 10_000,
        start: 700,
        end: 700,
        step: 1,
        seed: 26,
        single_thread: false,
    };
    let results = benchmark.benchmark().await;


    print_bench_fct_results();
    print_bench_fct_hy_results();
    graph_benchmark_result("6_benchmarked".to_string(), benchmark, results);
}

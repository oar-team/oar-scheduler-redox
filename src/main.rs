use std::env;
use crate::benchmark::benchmarker::{BenchmarkTarget, WaitingJobsSampleType};
use crate::benchmark::grapher::graph_benchmark_result;
use log::LevelFilter;

mod models;
mod platform;
mod scheduler;
mod benchmark;

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    env_logger::Builder::new().filter(None, LevelFilter::Info).init();

    let averaging = 1;
    let res_count = 10_000;
    let target = BenchmarkTarget::Python(WaitingJobsSampleType::Normal);

    let results = target.benchmark_batch(averaging, res_count, 100, 100, 100).await;
    //graph_benchmark_result("4_quotas".to_string(), target, results);
}

use crate::benchmark::benchmarker::{BenchmarkTarget, WaitingJobsSampleType};
use log::{LevelFilter};
use rand::{Rng, RngCore, SeedableRng};
use crate::benchmark::grapher::graph_benchmark_result;

mod models;
mod platform;
mod scheduler;
mod benchmark;

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() {
    env_logger::Builder::new().filter(None, LevelFilter::Info).init();

    let averaging = 10;
    let res_count = 10_000;
    let target = BenchmarkTarget::Python(WaitingJobsSampleType::Normal);

    let results = target.benchmark_batch(averaging, res_count, 0, 70, 10, 42).await;
    graph_benchmark_result("4_quotas".to_string(), target, results);
}

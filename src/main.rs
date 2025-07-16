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

    let averaging = 10;
    let res_count = 10_000;
    let target = BenchmarkTarget::Basic(WaitingJobsSampleType::HighCacheHits, false);

    let results = target.benchmark_batch(averaging, res_count, 0, 500, 100).await;
    graph_benchmark_result("4_quotas".to_string(), target, results);
}

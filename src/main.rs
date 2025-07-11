use crate::benchmarker::{BenchmarkTarget, WaitingJobsSampleType};
use crate::grapher::graph_benchmark_result;
use log::LevelFilter;

mod benchmarker;
mod grapher;
mod models;
mod platform;
mod scheduler;

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    env_logger::Builder::new().filter(None, LevelFilter::Info).init();

    let averaging = 40;
    let res_count = 10_000;
    let target = BenchmarkTarget::Basic(WaitingJobsSampleType::HighCacheHits, false);

    let results = target.benchmark_batch(averaging, res_count, 0, 1000, 100).await;
    graph_benchmark_result("3_hierarchy".to_string(), target, results);
}

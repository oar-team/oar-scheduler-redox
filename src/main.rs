use crate::benchmark::benchmarker::{BenchmarkTarget, WaitingJobsSampleType};
use log::{LevelFilter};
use rand::{Rng, RngCore, SeedableRng};
use crate::benchmark::grapher::graph_benchmark_result;

mod models;
mod platform;
mod scheduler;
mod benchmark;

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    env_logger::Builder::new().filter(None, LevelFilter::Info).init();

    let averaging = 10;
    let res_count = 10_000;
    let target = BenchmarkTarget::Basic(WaitingJobsSampleType::NodeOnly, false);

    let results = target.benchmark_batch(averaging, res_count, 0, 2000, 200, 22).await;
    graph_benchmark_result("5_noquotas".to_string(), target, results);
}

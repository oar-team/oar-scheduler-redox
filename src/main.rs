use crate::benchmarker::{BenchmarkTarget, WaitingJobsSampleType};
use crate::grapher::graph_benchmark_result;
use log::LevelFilter;

mod models;
mod platform;
mod scheduler;
mod benchmarker;
mod grapher;

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    env_logger::Builder::new().filter(None, LevelFilter::Info).init();

    let averaging = 30;
    let res_count = 10_000;
    let target = BenchmarkTarget::Tree(WaitingJobsSampleType::Normal);

    let results = target.benchmark_batch(averaging, res_count, 0, 800, 100).await;
    graph_benchmark_result(target, results);

}

/*fn test_tree_slotset() {
    let mut ss = TreeSlotSet::from_proc_set(ProcSet::from_iter([1..=10]), 0, 100);
    ss.to_table(true).printstd();

    let m1 = Moldable::new(10, ProcSet::from_iter([1..=5]));
    let m2 = Moldable::new(10, ProcSet::from_iter([3..=7]));

    let node1 = ss.find_node_for_moldable(&m1).unwrap();
    ss.claim_node_for_moldable(node1.node_id(), &m1);
    ss.to_table(true).printstd();

    let node2 = ss.find_node_for_moldable(&m2).unwrap();
    ss.claim_node_for_moldable(node2.node_id(), &m2);
    ss.to_table(true).printstd();
}*/

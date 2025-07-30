mod benchmarker;
mod grapher;
mod python_caller;
mod platform_mock;

use log::{info, LevelFilter};
use oar3_scheduler::auto_bench_fct::{print_bench_fct_hy_results, print_bench_fct_results};
use crate::benchmarker::{get_sample_waiting_jobs, BenchmarkConfig, BenchmarkTarget, WaitingJobsSampleType};
use oar3_scheduler::models::Job;
use oar3_scheduler::platform::PlatformTrait;
use oar3_scheduler::scheduler::kamelot::schedule_cycle;
use crate::grapher::graph_benchmark_result;
use crate::python_caller::schedule_cycle_on_oar_python;
use crate::platform_mock::{PlatformBenchMock, generate_mock_platform_config};

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    env_logger::Builder::new()
        .filter(None, LevelFilter::Info)
        .filter(Some("oar3_rust::scheduler::hierarchy"), LevelFilter::Debug)
        .init();

    // Run the difference detection
    /*let seed_offset = 2_000_000;
    for i in 1..=1000 {
        if(detect_differences(seed_offset + i).await) {
            info!("Difference detected for seed {}", i);
            break;
        }
    }*/


    let benchmark = BenchmarkConfig {
        target: BenchmarkTarget::RustFromPython,
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

async fn detect_differences(seed: u64) -> bool {
    let job_count = 20;
    let res_count = 100;
    let waiting_jobs = get_sample_waiting_jobs(res_count, job_count, WaitingJobsSampleType::CoreOnly, seed);

    // Run Rust scheduler
    let platform_config = generate_mock_platform_config(true, res_count, 20, 5, 5, false);
    let mut rust_platform = PlatformBenchMock::new(platform_config, vec![], waiting_jobs.clone());
    let queues = vec!["default".to_string()];

    schedule_cycle(&mut rust_platform, queues.clone());
    let rust_scheduled_jobs = rust_platform.get_scheduled_jobs().clone();

    // Run Python scheduler
    let platform_config = generate_mock_platform_config(true, res_count, 20, 5, 5, false);  // Generate again
    let mut python_platform = PlatformBenchMock::new(platform_config, vec![], waiting_jobs.clone());

    schedule_cycle_on_oar_python(&mut python_platform, queues, false);
    let python_scheduled_jobs = python_platform.get_scheduled_jobs().clone();

    // Compare results
    if rust_scheduled_jobs.len() != python_scheduled_jobs.len() {
        println!("DIFFERENCE DETECTED: Different number of scheduled jobs!");
        println!("  Rust scheduled: {} jobs", rust_scheduled_jobs.len());
        println!("  Python scheduled: {} jobs", python_scheduled_jobs.len());
        display_job_comparison(&waiting_jobs, &rust_scheduled_jobs, &python_scheduled_jobs);
        return true;
    }

    // Sort jobs by ID for comparison
    let mut rust_jobs_sorted = rust_scheduled_jobs;
    let mut python_jobs_sorted = python_scheduled_jobs;
    rust_jobs_sorted.sort_by_key(|job| job.id);
    python_jobs_sorted.sort_by_key(|job| job.id);

    // Compare each job
    for (rust_job, python_job) in rust_jobs_sorted.iter().zip(python_jobs_sorted.iter()) {
        if rust_job.id != python_job.id {
            println!("DIFFERENCE DETECTED: Job ID mismatch!");
            println!("  Rust job ID: {}", rust_job.id);
            println!("  Python job ID: {}", python_job.id);
            display_job_comparison(&waiting_jobs, &rust_jobs_sorted, &python_jobs_sorted);
            return true;
        }

        let rust_begin = rust_job.begin().unwrap_or(-1);
        let python_begin = python_job.begin().unwrap_or(-1);
        let rust_end = rust_job.end().unwrap_or(-1);
        let python_end = python_job.end().unwrap_or(-1);

        let rust_procset = rust_job.scheduled_data.as_ref().map(|sd| format!("{:?}", sd.proc_set)).unwrap_or("None".to_string());
        let python_procset = python_job.scheduled_data.as_ref().map(|sd| format!("{:?}", sd.proc_set)).unwrap_or("None".to_string());

        if rust_begin != python_begin || rust_end != python_end || rust_procset != python_procset {
            println!("DIFFERENCE DETECTED: Job {} has different scheduling data!", rust_job.id);
            println!("  Rust: begin={}, end={}, procset={}", rust_begin, rust_end, rust_procset);
            println!("  Python: begin={}, end={}, procset={}", python_begin, python_end, python_procset);
            display_job_comparison(&waiting_jobs, &rust_jobs_sorted, &python_jobs_sorted);
            return true;
        }
    }
    false
}

fn display_job_comparison(waiting_jobs: &Vec<Job>, rust_scheduled: &Vec<Job>, python_scheduled: &Vec<Job>) {
    println!("\n=== JOB COMPARISON ===");

    println!("\nOriginal waiting jobs:");
    for job in waiting_jobs {
        println!("  Job {}: walltime={}, request={:?}", job.id, job.moldables[0].walltime, job.moldables[0].requests.0[0].level_nbs);
    }

    println!("\nRust scheduled jobs:");
    for job in rust_scheduled {
        println!("  Job {}: begin={}, end={}, procset={}",
                 job.id,
                 job.begin().unwrap_or(-1),
                 job.end().unwrap_or(-1),
                 job.scheduled_data.as_ref().map(|sd| format!("{:?}", sd.proc_set)).unwrap_or("None".to_string()));
    }

    println!("\nPython scheduled jobs:");
    for job in python_scheduled {
        println!("  Job {}: begin={}, end={}, procset={}",
                 job.id,
                 job.begin().unwrap_or(-1),
                 job.end().unwrap_or(-1),
                 job.scheduled_data.as_ref().map(|sd| format!("{:?}", sd.proc_set)).unwrap_or("None".to_string()));
    }
}

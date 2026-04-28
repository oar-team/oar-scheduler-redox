mod benchmarker;
mod grapher;
mod python_caller;
mod platform_mock;
mod json_export;
use crate::benchmarker::{get_sample_waiting_jobs, ResourceTopology, PrefillLevel, BenchmarkConfig, BenchmarkTarget, WaitingJobsSampleType};
use crate::grapher::graph_benchmark_result;
use crate::platform_mock::PlatformBenchMock;
use crate::python_caller::schedule_cycle_on_oar_python;
use indexmap::IndexMap;
use log::LevelFilter;
use oar_scheduler_core::auto_bench_fct::{print_bench_fct_hy_results, print_bench_fct_results};
use oar_scheduler_core::model::job::Job;
use oar_scheduler_core::platform::PlatformTrait;
use oar_scheduler_core::scheduler::kamelot::schedule_cycle;

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    env_logger::Builder::new()
        .filter(None, LevelFilter::Info)
        .filter(Some("oar3_rust::scheduler::hierarchy"), LevelFilter::Debug)
        .init();

    // Run the difference detection
    /*let seed_offset = 2_000_000;
    for i in 1..=1000 {
        if detect_differences(seed_offset + i).await {
            info!("Difference detected for seed {}", i);
            break;
        }
    }*/

    // --- simple CLI ---
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--help" || a == "-h") {
        println!(
            "Usage:
                oar-scheduler-bench [--all] [--phase-bench] [--single-thread] [--start N] [--end N] [--step N] [--sample NAME]

                Options:
                --all             Run all benchmark suites (rust/python/rust-from-python × all samples)
                --phase-bench     Run fast Rust-only phase benchmark (no graphs, no python)
                --single-thread   Force sequential sampling (no join_all)
                --start N         Start jobs count (default: step)
                --end N           End jobs count (default: 500)
                --step N          Step in jobs count (default: 100)
                --sample NAME     Sample type for --phase-bench:
                                  normal | high-cache-hit | besteffort | node-only | core-only | old-normal | fragmented | hierarchy-heavy | cache-friendly
                --prefill NAME    Background occupancy level:
                                  empty | light | medium | heavy
                --topology NAME   Resource topology:
                                  homogeneous | heterogeneous
                --no-cache        Disable scheduler cache in phase-bench mode
                --nnodes N        Number of nodes in the synthetic graph (default: 4096)
            "
        );
        return;
    }

    let cache_enabled = !args.iter().any(|a| a == "--no-cache");

    let single_thread = args.iter().any(|a| a == "--single-thread");
    let phase_bench = args.iter().any(|a| a == "--phase-bench");

    let nnodes: u32 = get_usize_flag(&args, "--nnodes").map(|x| x as u32).unwrap_or(4096);

    let step: usize = get_usize_flag(&args, "--step").unwrap_or(100);
    let start: usize = get_usize_flag(&args, "--start").unwrap_or(step);
    let end: usize = get_usize_flag(&args, "--end").unwrap_or(500);

    let averaging = 1;
    let seed = 26;

    let prefill = get_prefill_flag(&args).unwrap_or(PrefillLevel::Empty);
    let topology = get_topology_flag(&args).unwrap_or(ResourceTopology::Homogeneous);

    let res_count = platform_mock::estimate_total_cores(topology, nnodes);

    let sample_types = vec![
        WaitingJobsSampleType::Normal,
        WaitingJobsSampleType::HighCacheHit,
        WaitingJobsSampleType::Besteffort,
        WaitingJobsSampleType::NodeOnly,
        WaitingJobsSampleType::CoreOnly,
        WaitingJobsSampleType::OldNormal,
    ];

    let all_targets = vec![
        (BenchmarkTarget::Rust, true),
        (BenchmarkTarget::Rust, false),
        (BenchmarkTarget::Python, true),
        (BenchmarkTarget::RustFromPython, true),
    ];

    if args.iter().any(|a| a == "--all") {
        for sample_type in sample_types {
            for (target, cache) in &all_targets {
                let benchmark = BenchmarkConfig {
                    target: *target,
                    sample_type,
                    cache: *cache,
                    averaging,
                    res_count,
                    start,
                    end,
                    step,
                    seed,
                    single_thread,
                    prefill,
                    topology,
                    nnodes,
                };

                let results = benchmark.benchmark().await;

                print_bench_fct_results();
                print_bench_fct_hy_results();

                let prefix = if single_thread { "all_1t" } else { "all_mt" };
                graph_benchmark_result(prefix.to_string(), benchmark, results);
            }
        }
        return;
    }

    if phase_bench {
        let maybe_sample = get_sample_flag(&args);

        let phase_samples: Vec<WaitingJobsSampleType> = match maybe_sample {
            Some(sample) => vec![sample],
            None => vec![
                WaitingJobsSampleType::NodeOnly,
                WaitingJobsSampleType::Normal,
                WaitingJobsSampleType::HighCacheHit,
                WaitingJobsSampleType::Fragmented,
            ],
        };

        for sample_type in phase_samples {
            let benchmark = BenchmarkConfig {
                target: BenchmarkTarget::Rust,
                sample_type,
                cache: cache_enabled,
                averaging,
                res_count,
                start,
                end,
                step,
                seed,
                single_thread,
                prefill,
                topology,
                nnodes,
            };

            println!(
                "\n=== PHASE BENCH === target=Rust sample={:?} topology={} prefill={} cache={} start={} end={} step={} res_count={} single_thread={}",
                benchmark.sample_type,
                benchmark.topology.as_str(),
                benchmark.prefill.as_str(),
                benchmark.cache,
                benchmark.start,
                benchmark.end,
                benchmark.step,
                benchmark.res_count,
                benchmark.single_thread
            );

            let results = benchmark.benchmark().await;

            print_bench_fct_results();
            print_bench_fct_hy_results();

            let file_name = format!(
                "results/phase_bench_{}_{}_{}_{}_{}_{}-{}-{}_{}.json",
                sample_type_name(benchmark.sample_type),
                benchmark.topology.as_str(),
                benchmark.prefill.as_str(),
                if benchmark.cache { "cache-on" } else { "cache-off" },
                if benchmark.single_thread { "single-thread" } else { "multi-thread" },
                benchmark.start,
                benchmark.end,
                benchmark.step,
                benchmark.nnodes,
            );

            match json_export::save_json(&file_name, "phase-bench", &benchmark, &results) {
                Ok(_) => println!("Saved JSON results to {}", file_name),
                Err(e) => eprintln!("Failed to save JSON results to {}: {}", file_name, e),
            }
        }

        return;
    }

    // Default single benchmark
    let benchmark = BenchmarkConfig {
        target: BenchmarkTarget::RustFromPython,
        sample_type: WaitingJobsSampleType::NodeOnly,
        cache: true,
        averaging,
        res_count,
        start,
        end,
        step,
        seed,
        single_thread,
        prefill,
        topology,
        nnodes,
    };
    let results = benchmark.benchmark().await;

    print_bench_fct_results();
    print_bench_fct_hy_results();
    graph_benchmark_result("default".to_string(), benchmark, results);
}

fn get_usize_flag(args: &[String], name: &str) -> Option<usize> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse::<usize>().ok())
}

fn get_string_flag(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn get_sample_flag(args: &[String]) -> Option<WaitingJobsSampleType> {
    let value = get_string_flag(args, "--sample")?;
    match value.to_ascii_lowercase().as_str() {
        "normal" => Some(WaitingJobsSampleType::Normal),
        "high-cache-hit" => Some(WaitingJobsSampleType::HighCacheHit),
        "besteffort" => Some(WaitingJobsSampleType::Besteffort),
        "node-only" => Some(WaitingJobsSampleType::NodeOnly),
        "core-only" => Some(WaitingJobsSampleType::CoreOnly),
        "old-normal" => Some(WaitingJobsSampleType::OldNormal),
        "fragmented" => Some(WaitingJobsSampleType::Fragmented),
        "hierarchy-heavy" => Some(WaitingJobsSampleType::HierarchyHeavy),
        "cache-friendly" => Some(WaitingJobsSampleType::CacheFriendly),
        _ => {
            eprintln!(
                "Unknown sample type: {}. Supported: normal | high-cache-hit | besteffort | node-only | core-only | old-normal | fragmented | hierarchy-heavy | cache-friendly",
                value
            );
            None
        }
    }
}

#[allow(dead_code)]
async fn detect_differences(seed: u64) -> bool {
    let job_count = 20;
    let nnodes = 16;
    let res_count = platform_mock::estimate_total_cores(ResourceTopology::Homogeneous, nnodes);

    let waiting_jobs = get_sample_waiting_jobs(
        res_count,
        nnodes,
        job_count,
        WaitingJobsSampleType::CoreOnly,
        seed,
    );

    // Run Rust scheduler
    let platform_config = platform_mock::generate_mock_platform_config_for_topology(
        ResourceTopology::Homogeneous,
        true,
        nnodes,
        false,
    );
    let mut rust_platform = PlatformBenchMock::new(platform_config, vec![], waiting_jobs.clone());
    let queues = vec!["default".to_string()];

    schedule_cycle(&mut rust_platform, &queues);
    let rust_scheduled_jobs = rust_platform.get_scheduled_jobs();

    // Run Python scheduler
    let platform_config = platform_mock::generate_mock_platform_config_for_topology(
        ResourceTopology::Homogeneous,
        true,
        nnodes,
        false,
    );
    let mut python_platform = PlatformBenchMock::new(platform_config, vec![], waiting_jobs.clone());

    schedule_cycle_on_oar_python(&mut python_platform, queues, false);
    let python_scheduled_jobs = python_platform.get_scheduled_jobs();

    // Compare results
    if rust_scheduled_jobs.len() != python_scheduled_jobs.len() {
        println!("DIFFERENCE DETECTED: Different number of scheduled jobs!");
        println!("  Rust scheduled: {} jobs", rust_scheduled_jobs.len());
        println!("  Python scheduled: {} jobs", python_scheduled_jobs.len());
        display_job_comparison(&waiting_jobs, &rust_scheduled_jobs, &python_scheduled_jobs);
        return true;
    }

    let mut rust_jobs_sorted = rust_scheduled_jobs;
    let mut python_jobs_sorted = python_scheduled_jobs;
    rust_jobs_sorted.sort_by_key(|job| job.id);
    python_jobs_sorted.sort_by_key(|job| job.id);

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

        let rust_procset = rust_job
            .assignment
            .as_ref()
            .map(|sd| format!("{:?}", sd.resources))
            .unwrap_or("None".to_string());
        let python_procset = python_job
            .assignment
            .as_ref()
            .map(|sd| format!("{:?}", sd.resources))
            .unwrap_or("None".to_string());

        if rust_begin != python_begin || rust_end != python_end || rust_procset != python_procset {
            println!("DIFFERENCE DETECTED: Job {} has different scheduling data!", rust_job.id);
            println!(
                "  Rust: begin={}, end={}, procset={}",
                rust_begin, rust_end, rust_procset
            );
            println!(
                "  Python: begin={}, end={}, procset={}",
                python_begin, python_end, python_procset
            );
            display_job_comparison(&waiting_jobs, &rust_jobs_sorted, &python_jobs_sorted);
            return true;
        }
    }

    false
}

#[allow(dead_code)]
fn display_job_comparison(
    waiting_jobs: &IndexMap<i64, Job>,
    rust_scheduled: &Vec<Job>,
    python_scheduled: &Vec<Job>,
) {
    println!("\n=== JOB COMPARISON ===");

    println!("\nOriginal waiting jobs:");
    for (_job_id, job) in waiting_jobs {
        println!(
            "  Job {}: walltime={}, request={:?}",
            job.id, job.moldables[0].walltime, job.moldables[0].requests.0[0].level_nbs
        );
    }

    println!("\nRust scheduled jobs:");
    for job in rust_scheduled {
        println!(
            "  Job {}: begin={}, end={}, procset={}",
            job.id,
            job.begin().unwrap_or(-1),
            job.end().unwrap_or(-1),
            job.assignment
                .as_ref()
                .map(|sd| format!("{:?}", sd.resources))
                .unwrap_or("None".to_string())
        );
    }

    println!("\nPython scheduled jobs:");
    for job in python_scheduled {
        println!(
            "  Job {}: begin={}, end={}, procset={}",
            job.id,
            job.begin().unwrap_or(-1),
            job.end().unwrap_or(-1),
            job.assignment
                .as_ref()
                .map(|sd| format!("{:?}", sd.resources))
                .unwrap_or("None".to_string())
        );
    }
}

fn sample_type_name(sample: WaitingJobsSampleType) -> &'static str {
    match sample {
        WaitingJobsSampleType::Normal => "normal",
        WaitingJobsSampleType::HighCacheHit => "high-cache-hit",
        WaitingJobsSampleType::Besteffort => "besteffort",
        WaitingJobsSampleType::NodeOnly => "node-only",
        WaitingJobsSampleType::CoreOnly => "core-only",
        WaitingJobsSampleType::OldNormal => "old-normal",
        WaitingJobsSampleType::Fragmented => "fragmented",
        WaitingJobsSampleType::HierarchyHeavy => "hierarchy-heavy",
        WaitingJobsSampleType::CacheFriendly => "cache-friendly",
    }
}

fn get_prefill_flag(args: &[String]) -> Option<PrefillLevel> {
    let value = get_string_flag(args, "--prefill")?;
    match value.to_ascii_lowercase().as_str() {
        "empty" => Some(PrefillLevel::Empty),
        "light" => Some(PrefillLevel::Light),
        "medium" => Some(PrefillLevel::Medium),
        "heavy" => Some(PrefillLevel::Heavy),
        _ => {
            eprintln!("Unknown prefill level: {}. Supported: empty | light | medium | heavy", value);
            None
        }
    }
}

fn get_topology_flag(args: &[String]) -> Option<ResourceTopology> {
    let value = get_string_flag(args, "--topology")?;
    match value.to_ascii_lowercase().as_str() {
        "homogeneous" => Some(ResourceTopology::Homogeneous),
        "heterogeneous" => Some(ResourceTopology::Heterogeneous),
        _ => {
            eprintln!("Unknown topology: {}. Supported: homogeneous | heterogeneous", value);
            None
        }
    }
}
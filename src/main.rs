use crate::models::models::Job;
use crate::platform::{PlatformTest, ResourceSet};
use crate::scheduler::kamelot_basic::schedule_cycle;
use crate::scheduler::slot::ProcSet;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use futures::future::join_all;
use plotters::prelude::*;
use tokio::task::JoinHandle;

mod models;
mod platform;
mod scheduler;

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let averaging = 100;
    let times: Vec<(i32, i32)> = futures::future::join_all(
        (1..=30)
        .map(async |i| {
            let jobs = i * 10;
            let (time, identical) = measure_scheduling_time(averaging, 1_024, jobs).await;
            println!("{} jobs scheduled in {} ms ({}% identical moldables)", jobs, time, (identical as f32 / jobs as f32 * 100.0) as usize);
            (jobs as i32, time as i32)
        })).await;

    let max_x = times.iter().map(|(x, _)| *x).max().unwrap() + 50;
    let max_y = times.iter().map(|(_, y)| *y).max().unwrap() + 100;

    let root_area = SVGBackend::new("scheduler-perf-no-cache.svg", (600, 400)).into_drawing_area();
    root_area.fill(&WHITE).unwrap();
    let mut ctx = ChartBuilder::on(&root_area)
        .margin(5)
        .set_label_area_size(LabelAreaPosition::Left, 40)
        .set_label_area_size(LabelAreaPosition::Bottom, 30)
        .caption("Scheduler performance (ms) by number of jobs (single moldable)", ("sans-serif", 12))
        .build_cartesian_2d(0..max_x, 0..max_y)
        .unwrap();


    ctx.configure_mesh()
        .x_labels(10)
        .y_labels(10)
        .label_style(("sans-serif", 10))
        .x_desc("Number of jobs (single moldable)")
        .y_desc("Average scheduling time (ms)")
        // .axis_desc_style()
        .draw()
        .unwrap();

    ctx.draw_series(LineSeries::new(times, &BLUE)).unwrap();
}

async fn measure_scheduling_time(averaging: usize, res_count: u32, jobs_count: u32) -> (u64, usize) {

    let (time, identical) = futures::future::join_all((0..averaging).map(|_| {
        let res_count_clone = res_count.clone();
        let jobs_count_clone = jobs_count.clone();
        tokio::spawn(async move {
            let resource_set = ResourceSet {
                default_intervals: ProcSet::from_iter([1..=res_count_clone]),
                available_upto: vec![],
            };

            let mut scheduled_jobs: Vec<Job> = vec![];
            let mut waiting_jobs: Vec<Job> = vec![];

            waiting_jobs.append(gen_random_jobs(1_000_000, (jobs_count_clone / 3) as usize, 10, 60, 10, 64, 128, 64, res_count_clone).as_mut());
            waiting_jobs.append(gen_random_jobs(2_000_000, (jobs_count_clone / 3) as usize, 30, 60 * 3, 30, 128, 256, 128, res_count_clone).as_mut());
            waiting_jobs.append(gen_random_jobs(3_000_000, (jobs_count_clone / 3) as usize, 60, 60 * 12, 120, 256, 512, 256, res_count_clone).as_mut());

            // Count number of moldables with the same cache key
            let mut cache = HashSet::new();
            let mut identical = 0;
            for job in waiting_jobs.iter() {
                for moldable in job.moldables.iter() {
                    let key = moldable.get_cache_key();
                    if cache.contains(&key) {
                        identical += 1;
                    } else {
                        cache.insert(key);
                    }
                }
            }

            scheduled_jobs.sort_by_key(|j| j.begin.unwrap());
            let mut platform = PlatformTest::new(resource_set, scheduled_jobs, waiting_jobs);
            let queues = vec!["default".to_string()];

            let time = measure_time(|| schedule_cycle(&mut platform.clone(), queues.clone()));
            (time, identical)
        })
    })).await.into_iter().map(|f| f.unwrap()).collect::<Vec<(u64, usize)>>().into_iter().fold((0, 0), |(total_time, total_identical), (time, identical)| {
        (total_time + time, total_identical + identical)
    });
    (time / averaging as u64, identical / averaging)
}

fn gen_random_jobs(
    offset: usize,
    count: usize,
    duration_min: i64,
    duration_max: i64,
    duration_step: i64,
    res_min: u32,
    res_max: u32,
    res_step: u32,
    res_all_max: u32,
) -> Vec<Job> {
    let mut jobs: Vec<Job> = vec![];
    for i in offset..(offset + count) {
        let duration = rand::random_range((duration_min / duration_step)..=(duration_max / duration_step));
        let res_size = rand::random_range((res_min / res_step)..=(res_max / res_step)) * res_step;
        let res_start = rand::random_range(0..=((res_all_max - res_size) / res_step)) * res_step;
        jobs.push(Job::new_from_proc_set(
            i as u32,
            duration * duration_step,
            ProcSet::from_iter([res_start..=(res_start + res_size)]),
        ));
    }
    jobs
}

fn measure_time<F>(f: F) -> u64
where
    F: FnOnce(),
{
    let start = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    f();
    let end = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    (end.as_millis() - start.as_millis()) as u64
}

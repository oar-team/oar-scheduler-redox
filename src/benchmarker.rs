use crate::models::models::{Job, Moldable, ProcSet};
use crate::platform::{PlatformTest, ResourceSet};
use crate::scheduler::hierarchy::{Hierarchy, HierarchyRequest, HierarchyRequests};
use crate::scheduler::{kamelot_basic, kamelot_tree};
use log::info;
use plotters::data::Quartiles;
use std::cmp::max;
use std::collections::HashSet;
use std::fmt::Display;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub struct BenchmarkResult {
    pub jobs_count: u32,
    pub scheduling_time: u32,
    pub cache_hits: u32,
    pub slot_count: u32,
    pub nodes_count: u32,
}

impl BenchmarkResult {
    pub fn new(jobs_count: u32, scheduling_time: u32, cache_hits: u32, slot_count: u32, nodes_count: u32) -> Self {
        BenchmarkResult {
            jobs_count,
            scheduling_time,
            cache_hits,
            slot_count,
            nodes_count,
        }
    }
}

pub struct BenchmarkAverageResult {
    pub jobs_count: u32,
    pub scheduling_time: BenchmarkMeasurementStatistics,
    pub slot_count: BenchmarkMeasurementStatistics,
    pub cache_hits: BenchmarkMeasurementStatistics,
}

#[allow(dead_code)]
pub struct BenchmarkMeasurementStatistics {
    pub min: u32,
    pub max: u32,
    pub mean: u32,
    pub q1: u32,
    pub q2: u32,
    pub q3: u32,
    pub std_dev: u32,
    pub quartiles: Quartiles,
}
impl From<Vec<u32>> for BenchmarkMeasurementStatistics {
    fn from(mut value: Vec<u32>) -> Self {
        if value.is_empty() {
            return BenchmarkMeasurementStatistics {
                min: 0,
                max: 0,
                mean: 0,
                q1: 0,
                q2: 0,
                q3: 0,
                std_dev: 0,
                quartiles: Quartiles::new::<u32>(&[0]),
            };
        }
        value.sort();
        let mean = value.iter().sum::<u32>() / value.len() as u32;
        BenchmarkMeasurementStatistics {
            min: value.get(0).unwrap().to_owned(),
            max: value.get(value.len() - 1).unwrap().to_owned(),
            mean,
            q1: value.iter().nth(value.len() / 4).unwrap().to_owned(),
            q2: value.iter().nth(value.len() / 2).unwrap().to_owned(),
            q3: value.iter().nth(value.len() * 3 / 4).unwrap().to_owned(),
            std_dev: (value.iter().map(|x| ((*x as i32 - mean as i32) as f64).powi(2)).sum::<f64>() / value.len() as f64) as u32,
            quartiles: Quartiles::new(&value),
        }
    }
}

#[derive(Copy, Clone)]
#[allow(dead_code)]
pub enum WaitingJobsSampleType {
    HighCacheHits,
    Normal,
    NormalMoreIdenticalDurations,
}
impl WaitingJobsSampleType {
    pub fn to_friendly_string(&self) -> String {
        match self {
            WaitingJobsSampleType::HighCacheHits => "High cache hits jobs".to_string(),
            WaitingJobsSampleType::Normal => "Normal jobs".to_string(),
            WaitingJobsSampleType::NormalMoreIdenticalDurations => "Normal with more identical duration jobs".to_string(),
        }
    }
}
impl Display for WaitingJobsSampleType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            WaitingJobsSampleType::HighCacheHits => "HighCacheHits",
            WaitingJobsSampleType::Normal => "Normal",
            WaitingJobsSampleType::NormalMoreIdenticalDurations => "NormalMoreIdenticalDurations",
        }
        .to_string();
        write!(f, "{}", str)
    }
}

impl From<Vec<BenchmarkResult>> for BenchmarkAverageResult {
    fn from(value: Vec<BenchmarkResult>) -> Self {
        BenchmarkAverageResult {
            jobs_count: value.get(0).map(|x| x.jobs_count).unwrap_or(0),
            scheduling_time: value.iter().map(|r| r.scheduling_time).collect::<Vec<u32>>().into(),
            slot_count: value.iter().map(|r| r.slot_count).collect::<Vec<u32>>().into(),
            cache_hits: value.iter().map(|r| r.cache_hits).collect::<Vec<u32>>().into(),
        }
    }
}

#[derive(Copy, Clone)]
pub enum BenchmarkTarget {
    #[allow(dead_code)]
    Basic(WaitingJobsSampleType, bool),
    #[allow(dead_code)]
    Tree(WaitingJobsSampleType),
}

impl BenchmarkTarget {
    pub fn get_sample_type(&self) -> WaitingJobsSampleType {
        match self {
            BenchmarkTarget::Basic(sample_type, _) => sample_type.clone(),
            BenchmarkTarget::Tree(sample_type) => sample_type.clone(),
        }
    }
    pub fn benchmark_file_name(&self, prefix: String) -> String {
        #[cfg(debug_assertions)]
        let profile = "debug";
        #[cfg(not(debug_assertions))]
        let profile = "release";

        let target = match self {
            BenchmarkTarget::Basic(_, true) => "basic-Cache",
            BenchmarkTarget::Basic(_, false) => "basic-NoCache",
            BenchmarkTarget::Tree(_) => "tree",
        };
        format!(
            "./benchmarks/{}_{}_{}-{}.svg",
            prefix,
            profile,
            target,
            self.get_sample_type().to_string()
        )
    }
    pub fn benchmark_friendly_name(&self) -> String {
        #[cfg(debug_assertions)]
        let profile = "Debug";
        #[cfg(not(debug_assertions))]
        let profile = "Release";

        let sample_type_str = self.get_sample_type().to_friendly_string();
        match self {
            BenchmarkTarget::Basic(_, true) => format!(
                "Basic scheduler performance by number of jobs ({}, With cache, {})",
                profile, sample_type_str
            ),
            BenchmarkTarget::Basic(_, false) => format!(
                "Basic scheduler performance by number of jobs ({}, No cache, {})",
                profile, sample_type_str
            ),
            BenchmarkTarget::Tree(_) => format!("Tree scheduler performance by number of jobs ({}, {})", profile, sample_type_str),
        }
        .to_string()
    }
    pub fn sample_type(&self) -> WaitingJobsSampleType {
        match self {
            BenchmarkTarget::Basic(sample_type, _) => sample_type.clone(),
            BenchmarkTarget::Tree(sample_type) => sample_type.clone(),
        }
    }

    pub fn has_cache(&self) -> bool {
        match self {
            BenchmarkTarget::Basic(_, has_cache) => *has_cache,
            BenchmarkTarget::Tree(_) => true,
        }
    }
    pub fn has_nodes(&self) -> bool {
        match self {
            BenchmarkTarget::Basic(_, _) => false,
            BenchmarkTarget::Tree(_) => true,
        }
    }

    pub async fn benchmark_batch(&self, averaging: usize, res_count: u32, start: usize, end: usize, step: usize) -> Vec<BenchmarkAverageResult> {
        futures::future::join_all(((start / step)..=(end / step)).map(async |i| {
            let jobs = i * step;
            let result = self.benchmark(averaging, res_count, jobs).await;
            info!(
                "{} jobs scheduled in {} ms ({}% cache hits, {} slots)",
                result.jobs_count, result.scheduling_time.mean, result.cache_hits.mean, result.slot_count.mean
            );
            result
        }))
        .await
    }

    pub async fn benchmark(&self, averaging: usize, res_count: u32, sample_size: usize) -> BenchmarkAverageResult {
        if sample_size == 0 {
            return vec![].into();
        }
        futures::future::join_all((0..averaging).map(|_| {
            let res_count_clone = res_count.clone();
            let jobs_count = sample_size.clone();
            let target = self.clone();
            let sample_type = self.sample_type();
            tokio::spawn(async move {
                let resource_set = ResourceSet {
                    default_intervals: ProcSet::from_iter([1..=res_count_clone]),
                    available_upto: vec![],
                    hierarchy: Hierarchy::new()
                        .add_partition(
                            "switch".into(),
                            (1..=5)
                                .map(|i| ProcSet::from_iter([(1 + res_count_clone * (i - 1) / 5)..=(res_count_clone * i / 5)]))
                                .collect::<Box<[ProcSet]>>(),
                        )
                        .add_partition(
                            "node".into(),
                            (1..=40)
                                .map(|i| ProcSet::from_iter([(1 + res_count_clone * (i - 1) / 40)..=(res_count_clone * i / 40)]))
                                .collect::<Box<[ProcSet]>>(),
                        )
                        .add_partition(
                            "core".into(),
                            (1..=res_count_clone).map(|i| ProcSet::from_iter([i..=i])).collect::<Box<[ProcSet]>>(),
                        ),
                };

                let waiting_jobs = get_sample_waiting_jobs(res_count_clone, jobs_count, sample_type);
                let cache_hits = count_cache_hits(&waiting_jobs);

                let mut platform = PlatformTest::new(resource_set, vec![], waiting_jobs);
                let queues = vec!["default".to_string()];

                let (scheduling_time, (slot_count, nodes_count)) = measure_time(|| match target {
                    BenchmarkTarget::Basic(_, cache) => (kamelot_basic::schedule_cycle(&mut platform, queues, cache), 0),
                    BenchmarkTarget::Tree(_) => kamelot_tree::schedule_cycle(&mut platform, queues),
                });
                BenchmarkResult::new(
                    jobs_count as u32,
                    scheduling_time,
                    (cache_hits * 100 / jobs_count) as u32,
                    slot_count as u32,
                    nodes_count as u32,
                )
            })
        }))
        .await
        .into_iter()
        .map(|f| f.unwrap())
        .collect::<Vec<BenchmarkResult>>()
        .into()
    }
}

fn get_sample_waiting_jobs(res_count: u32, jobs_count: usize, sample_type: WaitingJobsSampleType) -> Vec<Job> {
    let mut waiting_jobs: Vec<Job> = vec![];
    match sample_type {
        WaitingJobsSampleType::Normal => {
            waiting_jobs.append(gen_random_jobs(1_000_000, jobs_count / 3, 10, 60, 1, 1, 11, 2, 64, 128, 16, res_count).as_mut());
            waiting_jobs.append(gen_random_jobs(2_000_000, jobs_count / 3, 30, 60 * 3, 5, 1, 201, 5, 128, 256, 32, res_count).as_mut());
            waiting_jobs.append(gen_random_jobs(3_000_000, jobs_count / 3, 60, 60 * 12, 15, 10, 500, 10, 256, 512, 64, res_count).as_mut());
        }
        WaitingJobsSampleType::NormalMoreIdenticalDurations => {
            waiting_jobs.append(gen_random_jobs(1_000_000, jobs_count / 3, 10, 60, 5, 1, 11, 2, 0, 128, 16, res_count).as_mut());
            waiting_jobs.append(gen_random_jobs(2_000_000, jobs_count / 3, 30, 60 * 3, 10, 10, 200, 10, 128, 1024, 32, res_count).as_mut());
            waiting_jobs.append(gen_random_jobs(3_000_000, jobs_count / 3, 60, 60 * 12, 20, 20, 500, 20, 256, 1024, 64, res_count).as_mut());
        }
        WaitingJobsSampleType::HighCacheHits => {
            waiting_jobs.append(gen_random_jobs(1_000_000, jobs_count / 3, 10, 60, 1, 1, 11, 2, 64, 128, 64, res_count).as_mut());
            waiting_jobs.append(gen_random_jobs(2_000_000, jobs_count / 3, 30, 60 * 3, 10, 1, 11, 2, 128, 256, 128, res_count).as_mut());
            waiting_jobs.append(gen_random_jobs(3_000_000, jobs_count / 3, 60, 60 * 12, 120, 1, 11, 2, 256, 512, 256, res_count).as_mut());
        }
    }
    waiting_jobs
}
fn gen_random_jobs(
    offset: usize,
    count: usize,
    duration_min: i64,
    duration_max: i64,
    duration_step: i64,
    core_min: u32,
    core_max: u32,
    core_step: u32,
    res_min: u32,
    res_max: u32,
    res_step: u32,
    res_all_max: u32,
) -> Vec<Job> {
    let mut jobs: Vec<Job> = vec![];
    for i in offset..(offset + count) {
        let walltime = rand::random_range((duration_min / duration_step)..=(duration_max / duration_step)) * duration_step;
        let core_count = rand::random_range((core_min / core_step)..=(core_max / core_step)) * core_step;

        let res_size = max(core_count, rand::random_range((res_min / res_step)..=(res_max / res_step)) * res_step);
        let res_start = rand::random_range(1..=((res_all_max - res_size) / res_step)) * res_step;
        let filter_proc_set = ProcSet::from_iter([res_start..=(res_start + res_size)]);
        let moldable = Moldable::new(
            walltime,
            HierarchyRequests::from_requests(vec![HierarchyRequest::new(ProcSet::from_iter([0..=10_000]), vec![("core".into(), core_count)])]),
        );
        jobs.push(Job::new(i as u32, vec![moldable]));
    }
    jobs
}
fn count_cache_hits(waiting_jobs: &Vec<Job>) -> usize {
    let mut cache = HashSet::new();
    let mut cache_hits = 0;
    for job in waiting_jobs.iter() {
        for moldable in job.moldables.iter() {
            let key = moldable.get_cache_key();
            if cache.contains(&key) {
                cache_hits += 1;
            } else {
                cache.insert(key);
            }
        }
    }
    cache_hits
}
fn measure_time<F, R>(f: F) -> (u32, R)
where
    F: FnOnce() -> R,
{
    let start = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let res = f();
    let end = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    ((end.as_millis() - start.as_millis()) as u32, res)
}

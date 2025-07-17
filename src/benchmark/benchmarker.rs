use crate::benchmark::platform_mock;
use crate::benchmark::platform_mock::PlatformBenchMock;
use crate::models::models::{Job, Moldable, ProcSet};
use crate::scheduler::hierarchy::{HierarchyRequest, HierarchyRequests};
use crate::scheduler::{kamelot_basic, kamelot_tree};
use log::info;
use plotters::data::Quartiles;
use range_set_blaze::ValueRef;
use std::collections::HashSet;
use std::fmt::Display;
use std::ops::RangeInclusive;
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
            min: *ValueRef::to_owned(&value.get(0).unwrap()),
            max: ValueRef::to_owned(value.get(value.len() - 1).unwrap()),
            mean,
            q1: ValueRef::to_owned(value.iter().nth(value.len() / 4).unwrap()),
            q2: ValueRef::to_owned(value.iter().nth(value.len() / 2).unwrap()),
            q3: ValueRef::to_owned(value.iter().nth(value.len() * 3 / 4).unwrap()),
            std_dev: (value.iter().map(|x| ((*x as i32 - mean as i32) as f64).powi(2)).sum::<f64>() / value.len() as f64) as u32,
            quartiles: Quartiles::new(&value),
        }
    }
}

#[derive(Copy, Clone)]
#[allow(dead_code)]
pub enum WaitingJobsSampleType {
    Normal,
    Besteffort,
}
impl WaitingJobsSampleType {
    pub fn to_friendly_string(&self) -> String {
        match self {
            WaitingJobsSampleType::Normal => "Normal jobs".to_string(),
            WaitingJobsSampleType::Besteffort => "Besteffort jobs".to_string(),
        }
    }
}
impl Display for WaitingJobsSampleType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            WaitingJobsSampleType::Normal => "Normal",
            WaitingJobsSampleType::Besteffort => "Besteffort",
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

                let waiting_jobs = get_sample_waiting_jobs(res_count_clone, jobs_count, sample_type);
                let cache_hits = count_cache_hits(&waiting_jobs);


                let platform_config = platform_mock::generate_mock_platform_config(target.has_cache(), res_count_clone, 24, 4, 64, true);
                let mut platform = PlatformBenchMock::new(platform_config, vec![], waiting_jobs);
                let queues = vec!["default".to_string()];

                let (scheduling_time, (slot_count, nodes_count)) = measure_time(|| match target {
                    BenchmarkTarget::Basic(_, _) => (kamelot_basic::schedule_cycle(&mut platform, queues), 0),
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
    let mut jobs = match sample_type {
        WaitingJobsSampleType::Normal => RandomJobGenerator {
            count: jobs_count,
            id_offset: 0,
            total_res: res_count,

            walltime_min: 1,
            walltime_max: 24 * 10,
            walltime_step: 1,

            res_min: 1,
            res_max: 10,
            res_step: 1,
            res_type: "cpus".to_string(),
            res_in_single_type: "nodes".to_string(),
        }
        .generate_jobs(),
        WaitingJobsSampleType::Besteffort => RandomJobGenerator {
            count: jobs_count,
            id_offset: 0,
            total_res: res_count,

            walltime_min: 6,
            walltime_max: 24,
            walltime_step: 2,

            res_min: 1,
            res_max: 10,
            res_step: 1,
            res_type: "cores".to_string(),
            res_in_single_type: "cpus".to_string(),
        }
        .generate_jobs(),
    };
    waiting_jobs.append(&mut jobs);
    waiting_jobs
}
struct RandomJobGenerator {
    count: usize,
    id_offset: u32,
    total_res: u32,

    walltime_min: u32,
    walltime_max: u32,
    walltime_step: u32,

    res_min: u32,
    res_max: u32,
    res_step: u32,
    res_type: String,
    res_in_single_type: String,
}
impl RandomJobGenerator {
    fn generate_jobs(&self) -> Vec<Job> {
        let mut jobs: Vec<Job> = Vec::with_capacity(self.count);
        for i in 0..self.count {
            let walltime = self.generate(self.walltime_min, self.walltime_max, self.walltime_step) as i64;
            let res_count = self.generate(self.res_min, self.res_max, self.res_step);

            let request = HierarchyRequest::new(ProcSet::from_iter(1..=self.total_res), vec![
                (self.res_in_single_type.clone().into_boxed_str(), 1),
                (self.res_type.clone().into_boxed_str(), res_count)
            ]);

            let moldable = Moldable::new(walltime, HierarchyRequests::from_requests(vec![request]));
            jobs.push(Job::new(i as u32, "user".to_string(), "project".to_string(), "queue".to_string(), vec!["besteffort".to_string()], vec![moldable]));
        }
        jobs
    }
    fn generate(&self, min: u32, max: u32, step: u32) -> u32 {
        let range = ((max - min) / step) + 1;
        min + rand::random_range(0..range) * step
    }
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

use crate::platform_mock;
use crate::platform_mock::PlatformBenchMock;
use crate::python_caller::schedule_cycle_on_oar_python;
use indexmap::IndexMap;
use log::info;
use oar_scheduler_core::model::job::{Job, JobBuilder, ProcSet, ProcSetCoresOp};
use oar_scheduler_core::platform::PlatformTrait;
use oar_scheduler_core::scheduler::hierarchy::{HierarchyRequest, HierarchyRequests};
use oar_scheduler_core::scheduler::kamelot::schedule_cycle;
use plotters::data::Quartiles;
use rand::prelude::SliceRandom;
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};
use range_set_blaze::ValueRef;
use std::collections::HashSet;
use std::fmt::Display;
use std::ops::RangeInclusive;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub struct BenchmarkResult {
    pub jobs_count: u32,
    pub scheduled_jobs_count: u32,
    pub scheduling_time: u32,
    pub cache_hits: u32,
    pub slot_count: u32,
    pub quotas_hit: u32,
    pub gantt_width: u32,
    pub optimal_gantt_width: u32,
    pub resource_occupation: u32,
}

impl BenchmarkResult {
    pub fn new(
        jobs_count: u32,
        scheduled_jobs_count: u32,
        scheduling_time: u32,
        cache_hits: u32,
        slot_count: u32,
        quotas_hit: u32,
        gantt_width: u32,
        optimal_gantt_width: u32,
    ) -> Self {
        BenchmarkResult {
            jobs_count,
            scheduled_jobs_count,
            scheduling_time,
            cache_hits,
            slot_count,
            quotas_hit,
            gantt_width: gantt_width / 60,
            optimal_gantt_width: optimal_gantt_width / 60,
            resource_occupation: if gantt_width == 0 {
                100
            } else {
                optimal_gantt_width * 100 / gantt_width
            },
        }
    }
}

pub struct BenchmarkAverageResult {
    pub jobs_count: u32,
    pub scheduled_jobs_count: BenchmarkMeasurementStatistics,
    pub scheduling_time: BenchmarkMeasurementStatistics,
    pub slot_count: BenchmarkMeasurementStatistics,
    pub cache_hits: BenchmarkMeasurementStatistics,
    pub quotas_hit: BenchmarkMeasurementStatistics,
    pub gantt_width: BenchmarkMeasurementStatistics,
    pub optimal_gantt_width: BenchmarkMeasurementStatistics,
    pub resource_occupation: BenchmarkMeasurementStatistics,
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

#[derive(Copy, Clone, Debug)]
#[allow(dead_code)]
pub enum WaitingJobsSampleType {
    Normal,
    OldNormal,
    HighCacheHit,
    Besteffort,
    NodeOnly,
    CoreOnly,
}
impl WaitingJobsSampleType {
    pub fn to_friendly_string(&self) -> String {
        match self {
            WaitingJobsSampleType::Normal => "Normal jobs".to_string(),
            WaitingJobsSampleType::OldNormal => "Old normal jobs".to_string(),
            WaitingJobsSampleType::HighCacheHit => "High cache hits jobs".to_string(),
            WaitingJobsSampleType::Besteffort => "Besteffort jobs".to_string(),
            WaitingJobsSampleType::NodeOnly => "Node only jobs".to_string(),
            WaitingJobsSampleType::CoreOnly => "Core only jobs".to_string(),
        }
    }
}
impl Display for WaitingJobsSampleType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            WaitingJobsSampleType::Normal => "Normal",
            WaitingJobsSampleType::OldNormal => "OldNormal",
            WaitingJobsSampleType::HighCacheHit => "HighCacheHits",
            WaitingJobsSampleType::Besteffort => "Besteffort",
            WaitingJobsSampleType::NodeOnly => "NodeOnly",
            WaitingJobsSampleType::CoreOnly => "CoreOnly",
        }
        .to_string();
        write!(f, "{}", str)
    }
}

impl From<Vec<BenchmarkResult>> for BenchmarkAverageResult {
    fn from(value: Vec<BenchmarkResult>) -> Self {
        BenchmarkAverageResult {
            jobs_count: value.get(0).map(|x| x.jobs_count).unwrap_or(0),
            scheduled_jobs_count: value.iter().map(|r| r.scheduled_jobs_count).collect::<Vec<u32>>().into(),
            scheduling_time: value.iter().map(|r| r.scheduling_time).collect::<Vec<u32>>().into(),
            slot_count: value.iter().map(|r| r.slot_count).collect::<Vec<u32>>().into(),
            cache_hits: value.iter().map(|r| r.cache_hits).collect::<Vec<u32>>().into(),
            quotas_hit: value.iter().map(|r| r.quotas_hit).collect::<Vec<u32>>().into(),
            gantt_width: value.iter().map(|r| r.gantt_width).collect::<Vec<u32>>().into(),
            optimal_gantt_width: value.iter().map(|r| r.optimal_gantt_width).collect::<Vec<u32>>().into(),
            resource_occupation: value.iter().map(|r| r.resource_occupation).collect::<Vec<u32>>().into(),
        }
    }
}

#[derive(Copy, Clone)]
pub enum BenchmarkTarget {
    #[allow(dead_code)]
    Rust,
    #[allow(dead_code)]
    Python,
    #[allow(dead_code)]
    RustFromPython,
}

pub struct BenchmarkConfig {
    pub target: BenchmarkTarget,
    pub sample_type: WaitingJobsSampleType,
    pub cache: bool,
    pub averaging: usize,
    pub res_count: u32,
    pub start: usize,
    pub end: usize,
    pub step: usize,
    pub seed: usize,
    pub single_thread: bool,
}

impl BenchmarkConfig {
    pub fn benchmark_file_name(&self, prefix: String) -> String {
        #[cfg(debug_assertions)]
        let profile = "debug";
        #[cfg(not(debug_assertions))]
        let profile = "release";

        let target = match self.target {
            BenchmarkTarget::Rust => {
                if self.cache {
                    "rs"
                } else {
                    "rs[nocache]"
                }
            }
            BenchmarkTarget::Python => "py",
            BenchmarkTarget::RustFromPython => "rp",
        };
        format!("./oar-scheduler-bench/benchmarks/{}_{}_{}-{}.svg", prefix, profile, target, self.sample_type.to_string())
    }
    pub fn benchmark_friendly_name(&self) -> String {
        #[cfg(debug_assertions)]
        let profile = "Debug";
        #[cfg(not(debug_assertions))]
        let profile = "Release";

        let sample_type_str = self.sample_type.to_friendly_string();
        let cache_str = if self.cache { "With cache" } else { "No cache" };
        match self.target {
            BenchmarkTarget::Rust => format!(
                "Rust scheduler performance by number of jobs ({}, {}, {})",
                profile, cache_str, sample_type_str
            ),
            BenchmarkTarget::Python => format!("Python scheduler performance by number of jobs ({}, {})", profile, sample_type_str),
            BenchmarkTarget::RustFromPython => format!("Rust from Python scheduler performance by number of jobs ({}, {})", profile, sample_type_str),
        }
        .to_string()
    }

    pub async fn benchmark(&self) -> Vec<BenchmarkAverageResult> {
        self.run_sampling((self.start / self.step)..=(self.end / self.step), async |i| {
            let jobs = i * self.step;
            let result = self.benchmark_single_size(jobs, self.seed + (i + 1)).await;
            info!(
                "{} of {} jobs scheduled in {} ms ({}% cache hits, {} slots, {}/{}h width ({}% usage), {}% quotas hit)",
                result.scheduled_jobs_count.mean,
                result.jobs_count,
                result.scheduling_time.mean,
                result.cache_hits.mean,
                result.slot_count.mean,
                result.gantt_width.mean,
                result.optimal_gantt_width.mean,
                result.resource_occupation.mean,
                result.quotas_hit.mean
            );
            result
        })
        .await
    }

    async fn run_sampling<R, F, Fut>(&self, range: RangeInclusive<usize>, f: F) -> Vec<R>
    where
        F: Fn(usize) -> Fut,
        Fut: Future<Output = R>,
    {
        if self.single_thread {
            let mut results = Vec::with_capacity(range.end() - range.start() + 1);
            for i in range {
                results.push(f(i).await);
            }
            results
        } else {
            futures::future::join_all(range.map(f)).await
        }
    }

    async fn benchmark_single_size(&self, sample_size: usize, seed: usize) -> BenchmarkAverageResult {
        if sample_size == 0 {
            return vec![].into();
        }
        let new_seed = StdRng::seed_from_u64(seed as u64).next_u64();
        self.run_sampling(1..=self.averaging, |i| {
            let res_count = self.res_count.clone();
            let jobs_count = sample_size.clone();
            let target = self.target;
            let cache = self.cache.clone();
            let sample_type = self.sample_type;
            tokio::spawn(async move {
                let waiting_jobs = get_sample_waiting_jobs(res_count, jobs_count, sample_type, new_seed.wrapping_mul(1 + i as u64));
                let cache_hits = count_cache_hits(&waiting_jobs);

                let platform_config = platform_mock::generate_mock_platform_config(cache, res_count, 24, 4, 64, false);
                let mut platform = PlatformBenchMock::new(platform_config, vec![], waiting_jobs);
                let queues = vec!["default".to_string()];

                let (scheduling_time, slot_count) = match target {
                    BenchmarkTarget::Rust => measure_time(|| schedule_cycle(&mut platform, &queues)),
                    BenchmarkTarget::Python => schedule_cycle_on_oar_python(&mut platform, queues, false),
                    BenchmarkTarget::RustFromPython => schedule_cycle_on_oar_python(&mut platform, queues, true),
                };

                // platform.get_scheduled_jobs().iter().for_each(|j| {
                //     let width = 10;
                //     println!("{}: {:>width$} -> {:>width$} | {}", j.id, j.begin().unwrap(), j.end().unwrap(), j.assignment.clone().unwrap().proc_set);
                // });

                let quotas_hits = platform.get_scheduled_jobs().iter().map(|j| j.quotas_hit_count).sum::<u32>();
                let gantt_width = platform
                    .get_scheduled_jobs()
                    .iter()
                    .map(|j| j.assignment.clone().unwrap().end)
                    .max()
                    .unwrap_or(0);
                let optimal_gantt_width = (platform
                    .get_scheduled_jobs()
                    .iter()
                    .map(|j| j.assignment.clone().unwrap())
                    .map(|sd| sd.proc_set.core_count() as i64 * (sd.end - sd.begin + 1))
                    .sum::<i64>()
                    / res_count as i64) as u32;

                BenchmarkResult::new(
                    jobs_count as u32,
                    platform.get_scheduled_jobs().len() as u32,
                    scheduling_time,
                    (cache_hits * 100 / jobs_count) as u32,
                    slot_count as u32,
                    quotas_hits * 100 / jobs_count as u32,
                    gantt_width as u32,
                    optimal_gantt_width,
                )
            })
        })
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<BenchmarkResult>>()
        .into()
    }
}

pub fn get_sample_waiting_jobs(res_count: u32, jobs_count: usize, sample_type: WaitingJobsSampleType, seed: u64) -> IndexMap<i64, Job> {
    let last_remaining = jobs_count - ((2 * jobs_count / 5) * 2 + (jobs_count / 10));
    let jobs = match sample_type {
        WaitingJobsSampleType::Normal => RandomJobGenerator {
            rand: StdRng::seed_from_u64(seed),
            count: 2 * jobs_count / 5,
            id_offset: 0,
            total_res: res_count,
            job_type: "smalljobs".to_string(),

            walltime_min: 5,
            walltime_max: 120,
            walltime_step: 5,

            res_min: 1,
            res_max: 64,
            res_step: 1,
            res_type: "cores".to_string(),
            res_in_single_type: "nodes".to_string(),
        }
        .merge(RandomJobGenerator {
            rand: StdRng::seed_from_u64(seed + 1),
            count: 2 * jobs_count / 5,
            id_offset: 1_000_000,
            total_res: res_count,
            job_type: "midjobs".to_string(),

            walltime_min: 5,
            walltime_max: 180,
            walltime_step: 5,

            res_min: 1,
            res_max: 32,
            res_step: 1,
            res_type: "cpus".to_string(),
            res_in_single_type: "switches".to_string(),
        })
        .merge(RandomJobGenerator {
            rand: StdRng::seed_from_u64(seed + 2),
            count: jobs_count / 10,
            id_offset: 2_000_000,
            total_res: res_count,
            job_type: "longrun".to_string(),

            walltime_min: 2 * 60,
            walltime_max: 14 * 60,
            walltime_step: 15,

            res_min: 1,
            res_max: 8,
            res_step: 1,
            res_type: "nodes".to_string(),
            res_in_single_type: "switches".to_string(),
        })
        .merge(RandomJobGenerator {
            rand: StdRng::seed_from_u64(seed + 3),
            count: last_remaining,
            id_offset: 3_000_000,
            total_res: res_count,
            job_type: "longrun".to_string(),

            walltime_min: 8 * 60,
            walltime_max: 14 * 60,
            walltime_step: 30,

            res_min: 4,
            res_max: 24,
            res_step: 2,
            res_type: "nodes".to_string(),
            res_in_single_type: "switches".to_string(),
        })
        .generate_jobs(),
        WaitingJobsSampleType::HighCacheHit => RandomJobGenerator {
            rand: StdRng::seed_from_u64(seed),
            count: 2 * jobs_count / 5,
            id_offset: 0,
            total_res: res_count,
            job_type: "smalljobs".to_string(),

            walltime_min: 15,
            walltime_max: 120,
            walltime_step: 15,

            res_min: 8,
            res_max: 64,
            res_step: 8,
            res_type: "cores".to_string(),
            res_in_single_type: "nodes".to_string(),
        }
        .merge(RandomJobGenerator {
            rand: StdRng::seed_from_u64(seed + 1),
            count: 2 * jobs_count / 5,
            id_offset: 1_000_000,
            total_res: res_count,
            job_type: "midjobs".to_string(),

            walltime_min: 30,
            walltime_max: 120,
            walltime_step: 15,

            res_min: 4,
            res_max: 16,
            res_step: 4,
            res_type: "cpus".to_string(),
            res_in_single_type: "switches".to_string(),
        })
        .merge(RandomJobGenerator {
            rand: StdRng::seed_from_u64(seed + 2),
            count: jobs_count / 10,
            id_offset: 2_000_000,
            total_res: res_count,
            job_type: "longrun".to_string(),

            walltime_min: 2 * 60,
            walltime_max: 14 * 60,
            walltime_step: 4 * 60,

            res_min: 1,
            res_max: 8,
            res_step: 1,
            res_type: "nodes".to_string(),
            res_in_single_type: "switches".to_string(),
        })
        .merge(RandomJobGenerator {
            rand: StdRng::seed_from_u64(seed + 3),
            count: last_remaining,
            id_offset: 3_000_000,
            total_res: res_count,
            job_type: "longrun".to_string(),

            walltime_min: 10 * 60,
            walltime_max: 14 * 60,
            walltime_step: 4 * 60,

            res_min: 8,
            res_max: 24,
            res_step: 8,
            res_type: "nodes".to_string(),
            res_in_single_type: "switches".to_string(),
        })
        .generate_jobs(),
        WaitingJobsSampleType::Besteffort => RandomJobGenerator {
            rand: StdRng::seed_from_u64(seed),
            count: jobs_count,
            id_offset: 0,
            total_res: res_count,
            job_type: "besteffort".to_string(),

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
        WaitingJobsSampleType::NodeOnly => RandomJobGenerator {
            rand: StdRng::seed_from_u64(seed),
            count: jobs_count,
            id_offset: 0,
            total_res: res_count,
            job_type: "nodeonly".to_string(),

            walltime_min: 60,
            walltime_max: 60 * 24,
            walltime_step: 60,

            res_min: 1,
            res_max: 39,
            res_step: 1,
            res_type: "nodes".to_string(),
            res_in_single_type: "".to_string(),
        }
        .generate_jobs(),
        WaitingJobsSampleType::CoreOnly => RandomJobGenerator {
            rand: StdRng::seed_from_u64(seed),
            count: jobs_count,
            id_offset: 0,
            total_res: res_count,
            job_type: "nodeonly".to_string(),

            walltime_min: 60,
            walltime_max: 60 * 24,
            walltime_step: 60,

            res_min: 1 * 64,
            res_max: 39 * 64,
            res_step: 1,
            res_type: "cores".to_string(),
            res_in_single_type: "switches".to_string(),
        }
        .generate_jobs(),
        WaitingJobsSampleType::OldNormal => RandomJobGenerator {
            rand: StdRng::seed_from_u64(seed),
            count: jobs_count,
            id_offset: 0,
            total_res: res_count,
            job_type: "smalljobs".to_string(),

            walltime_min: 10,
            walltime_max: 60 * 24,
            walltime_step: 1,

            res_min: 1,
            res_max: 1000,
            res_step: 1,
            res_type: "cores".to_string(),
            res_in_single_type: "".to_string(),
        }
        .generate_jobs(),
    };
    jobs.into_iter()
        .map(|j| (j.id, j))
        .collect::<IndexMap<i64, Job>>()
}
struct RandomJobGeneratorMerged {
    generators: Vec<RandomJobGenerator>,
}
impl RandomJobGeneratorMerged {
    fn merge(&mut self, generator: RandomJobGenerator) -> &mut Self {
        self.generators.push(generator);
        self
    }
    fn generate_jobs(&mut self) -> Vec<Job> {
        let mut jobs: Vec<Job> = Vec::new();
        let mut last_seed = 0;
        for generator in self.generators.iter_mut() {
            jobs.append(&mut generator.generate_jobs());
            last_seed = generator.rand.next_u64();
        }
        // shuffle the jobs to mix them up
        jobs.shuffle(&mut StdRng::seed_from_u64(last_seed));
        jobs
    }
}
struct RandomJobGenerator {
    rand: StdRng,
    count: usize,
    id_offset: i64,
    total_res: u32,
    job_type: String,

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
    fn generate_jobs(&mut self) -> Vec<Job> {
        let mut jobs: Vec<Job> = Vec::with_capacity(self.count);
        for i in 0..self.count {
            let walltime = self.generate(self.walltime_min, self.walltime_max, self.walltime_step) as i64;
            let res_count = self.generate(self.res_min, self.res_max, self.res_step);

            let hierarchy_req = if self.res_in_single_type == "" {
                vec![(self.res_type.clone().into_boxed_str(), res_count)]
            } else {
                vec![
                    (self.res_in_single_type.clone().into_boxed_str(), 1),
                    (self.res_type.clone().into_boxed_str(), res_count),
                ]
            };

            let request = HierarchyRequest::new(ProcSet::from_iter(1..=self.total_res), hierarchy_req);
            jobs.push(
                JobBuilder::new(i as i64 + self.id_offset)
                    .moldable_auto(i as i64 + self.id_offset, walltime, HierarchyRequests::from_requests(vec![request]))
                    .add_type_key(self.job_type.clone().into())
                    .build(),
            );
        }
        jobs
    }
    fn generate(&mut self, min: u32, max: u32, step: u32) -> u32 {
        let range = ((max - min) / step) + 1;
        min + self.rand.random_range(0..range) * step
    }
    fn merge(self, other: RandomJobGenerator) -> RandomJobGeneratorMerged {
        RandomJobGeneratorMerged {
            generators: vec![self, other],
        }
    }
}

fn count_cache_hits(waiting_jobs: &IndexMap<i64, Job>) -> usize {
    let mut cache = HashSet::new();
    let mut cache_hits = 0;
    for (_job_id, job) in waiting_jobs.iter() {
        for moldable in job.moldables.iter() {
            if cache.contains(&moldable.cache_key) {
                cache_hits += 1;
            } else {
                cache.insert(moldable.cache_key.clone());
            }
        }
    }
    cache_hits
}
pub fn measure_time<F, R>(f: F) -> (u32, R)
where
    F: FnOnce() -> R,
{
    let start = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

    let res = f();

    let end = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let time = (end.as_millis() - start.as_millis()) as u32;

    (time, res)
}

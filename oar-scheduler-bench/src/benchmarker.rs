use crate::platform_mock;
use crate::platform_mock::PlatformBenchMock;
use crate::python_caller::schedule_cycle_on_oar_python;
use indexmap::IndexMap;
use log::info;
use oar_scheduler_core::model::job::{Job, JobBuilder, ProcSet, ProcSetCoresOp};
use oar_scheduler_core::perf::{self, PerfStats};
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
use std::future::Future;
use std::ops::RangeInclusive;
use std::time::{SystemTime, UNIX_EPOCH};
use serde::Serialize;
use num_traits::{FromPrimitive, ToPrimitive, Zero};


#[derive(Debug, Clone, Copy, Serialize)]
pub enum PrefillLevel {
    Empty,
    Light,
    Medium,
    Heavy,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub enum ResourceTopology {
    Homogeneous,
    Heterogeneous,
}

impl ResourceTopology {
    pub fn as_str(&self) -> &'static str {
        match self {
            ResourceTopology::Homogeneous => "homogeneous",
            ResourceTopology::Heterogeneous => "heterogeneous",
        }
    }
}

impl PrefillLevel {
    pub fn ratio(&self) -> u32 {
        match self {
            PrefillLevel::Empty => 0,
            PrefillLevel::Light => 20,
            PrefillLevel::Medium => 50,
            PrefillLevel::Heavy => 80,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            PrefillLevel::Empty => "empty",
            PrefillLevel::Light => "light",
            PrefillLevel::Medium => "medium",
            PrefillLevel::Heavy => "heavy",
        }
    }
}

fn get_prefilled_jobs(
    res_count: u32,
    nnodes: u32,
    waiting_jobs_count: usize,
    sample_type: WaitingJobsSampleType,
    seed: u64,
    prefill: PrefillLevel,
) -> Vec<Job> {
    if matches!(prefill, PrefillLevel::Empty) {
        return vec![];
    }

    let ratio = prefill.ratio() as usize;
    let prefill_jobs_count = (waiting_jobs_count * ratio) / 100;

    if prefill_jobs_count == 0 {
        return vec![];
    }

    let max_probe_nodes = (nnodes / 8).max(1).min(8);
    let max_long_nodes = (nnodes / 8).max(4).min(32);

    let mut jobs = match sample_type {
        WaitingJobsSampleType::HierarchyHeavy => {
            ShapedJobGenerator {
                rand: StdRng::seed_from_u64(seed + 10_000),
                count: prefill_jobs_count / 2,
                id_offset: 50_000_000,
                total_res: res_count,
                job_type: "prefill_hh_medium".to_string(),

                // medium_blocker-like
                walltime_min: 2 * 60,
                walltime_max: 8 * 60,
                walltime_step: 30,

                // 1..4 nodes, 1..4 cpus, 16..64 cores
                nodes_min: 1,
                nodes_max: 4,
                nodes_step: 1,

                cpus_min: 1,
                cpus_max: 4,
                cpus_step: 1,

                cores_min: 16,
                cores_max: 64,
                cores_step: 16,
            }
            .merge(ShapedJobGenerator {
                rand: StdRng::seed_from_u64(seed + 10_001),
                count: prefill_jobs_count - (prefill_jobs_count / 2),
                id_offset: 51_000_000,
                total_res: res_count,
                job_type: "prefill_hh_long".to_string(),

                // long_blocker-like
                walltime_min: 24 * 60,
                walltime_max: 3 * 24 * 60,
                walltime_step: 12 * 60,

                // 4..min(32, nnodes/8) nodes, 2..8 cpus, 32..64 cores
                nodes_min: 4,
                nodes_max: max_long_nodes,
                nodes_step: 1,

                cpus_min: 2,
                cpus_max: 8,
                cpus_step: 1,

                cores_min: 32,
                cores_max: 64,
                cores_step: 16,
            })
            .generate_jobs()
        }

        WaitingJobsSampleType::Fragmented => {
            // Keep prefill close to Flux mixed/planner-heavy idea:
            // medium blockers + long blockers, with a bit of small churn.
            ShapedJobGenerator {
                rand: StdRng::seed_from_u64(seed + 20_000),
                count: prefill_jobs_count / 5,
                id_offset: 60_000_000,
                total_res: res_count,
                job_type: "prefill_frag_short".to_string(),

                // short_frag
                walltime_min: 5,
                walltime_max: 30,
                walltime_step: 5,

                nodes_min: 1,
                nodes_max: 1,
                nodes_step: 1,

                cpus_min: 1,
                cpus_max: 1,
                cpus_step: 1,

                cores_min: 1,
                cores_max: 8,
                cores_step: 1,
            }
            .merge(ShapedJobGenerator {
                rand: StdRng::seed_from_u64(seed + 20_001),
                count: prefill_jobs_count * 2 / 5,
                id_offset: 61_000_000,
                total_res: res_count,
                job_type: "prefill_frag_medium".to_string(),

                // medium_blocker
                walltime_min: 2 * 60,
                walltime_max: 8 * 60,
                walltime_step: 30,

                nodes_min: 1,
                nodes_max: 4,
                nodes_step: 1,

                cpus_min: 1,
                cpus_max: 4,
                cpus_step: 1,

                cores_min: 16,
                cores_max: 64,
                cores_step: 16,
            })
            .merge(ShapedJobGenerator {
                rand: StdRng::seed_from_u64(seed + 20_002),
                count: prefill_jobs_count - (prefill_jobs_count / 5 + prefill_jobs_count * 2 / 5),
                id_offset: 62_000_000,
                total_res: res_count,
                job_type: "prefill_frag_long".to_string(),

                // long_blocker
                walltime_min: 24 * 60,
                walltime_max: 3 * 24 * 60,
                walltime_step: 12 * 60,

                nodes_min: 4,
                nodes_max: max_long_nodes,
                nodes_step: 1,

                cpus_min: 2,
                cpus_max: 8,
                cpus_step: 1,

                cores_min: 32,
                cores_max: 64,
                cores_step: 16,
            })
            .generate_jobs()
        }

        WaitingJobsSampleType::CacheFriendly => {
            // For cache-friendly prefill we still want repeated shapes,
            // but with blocker-ish durations to create background pressure.
            let templates: Vec<(i64, Vec<(Box<str>, u32)>, &'static str)> = vec![
                (2 * 60, vec![("nodes".into(), 1), ("cpus".into(), 2), ("cores".into(), 16)], "prefill_cf_a"),
                (4 * 60, vec![("nodes".into(), 2), ("cpus".into(), 2), ("cores".into(), 32)], "prefill_cf_b"),
                (8 * 60, vec![("nodes".into(), 2), ("cpus".into(), 4), ("cores".into(), 32)], "prefill_cf_c"),
                (24 * 60, vec![("nodes".into(), 4), ("cpus".into(), 4), ("cores".into(), 64)], "prefill_cf_d"),
                (
                    24 * 60,
                    vec![
                        ("nodes".into(), max_probe_nodes.max(2)),
                        ("cpus".into(), 4),
                        ("cores".into(), 64),
                    ],
                    "prefill_cf_e",
                ),
            ];

            let mut rng = StdRng::seed_from_u64(seed + 30_000);
            let mut jobs = Vec::with_capacity(prefill_jobs_count);

            for i in 0..prefill_jobs_count {
                let template_idx = rng.random_range(0..templates.len());
                let (walltime, hierarchy_req, job_type) = &templates[template_idx];
                let job_id = 70_000_000 + i as i64;

                let request = HierarchyRequest::new(
                    ProcSet::from_iter(1..=res_count),
                    hierarchy_req.clone(),
                );

                jobs.push(
                    JobBuilder::new(job_id)
                        .moldable_auto(
                            job_id,
                            *walltime,
                            HierarchyRequests::from_requests(vec![request]),
                        )
                        .add_type_key((*job_type).into())
                        .build(),
                );
            }

            jobs
        }

        _ => {
            // Reasonable default prefill:
            // mostly medium + long blockers
            ShapedJobGenerator {
                rand: StdRng::seed_from_u64(seed + 40_000),
                count: prefill_jobs_count / 3,
                id_offset: 80_000_000,
                total_res: res_count,
                job_type: "prefill_medium".to_string(),

                walltime_min: 2 * 60,
                walltime_max: 8 * 60,
                walltime_step: 30,

                nodes_min: 1,
                nodes_max: 4,
                nodes_step: 1,

                cpus_min: 1,
                cpus_max: 4,
                cpus_step: 1,

                cores_min: 16,
                cores_max: 64,
                cores_step: 16,
            }
            .merge(ShapedJobGenerator {
                rand: StdRng::seed_from_u64(seed + 40_001),
                count: prefill_jobs_count - (prefill_jobs_count / 3),
                id_offset: 81_000_000,
                total_res: res_count,
                job_type: "prefill_long".to_string(),

                walltime_min: 24 * 60,
                walltime_max: 3 * 24 * 60,
                walltime_step: 12 * 60,

                nodes_min: 4,
                nodes_max: max_long_nodes,
                nodes_step: 1,

                cpus_min: 2,
                cpus_max: 8,
                cpus_step: 1,

                cores_min: 32,
                cores_max: 64,
                cores_step: 16,
            })
            .generate_jobs()
        }
    };

    jobs.sort_by_key(|j| j.id);
    jobs
}

#[derive(Debug, Clone, Serialize)]
pub struct BenchmarkResult {
    pub perf: Option<PerfBenchmarkResult>,
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


#[derive(Debug, Clone, Default, Serialize)]
pub struct PerfBenchmarkResult {
    pub total_schedule_cycle_ns: u64,
    pub init_slot_sets_ns: u64,
    pub get_waiting_jobs_ns: u64,
    pub sort_jobs_ns: u64,
    pub schedule_jobs_ns: u64,
    pub save_assignments_ns: u64,
    pub schedule_job_ns: u64,
    pub find_slots_ns: u64,
    pub intersect_slots_ns: u64,
    pub hierarchy_request_ns: u64,
    pub quotas_ns: u64,
    pub update_slots_ns: u64,
    pub segment_tree_rebuild_ns: u64,
    pub segment_tree_query_ns: u64,
    pub anchor_cache_rebuild_ns: u64,
    pub anchor_window_min_ns: u64,
    pub jobs_seen: u64,
    pub jobs_scheduled: u64,
    pub moldables_seen: u64,
    pub slot_windows_scanned: u64,
    pub slots_split: u64,
    pub slots_intersected: u64,
    pub hierarchy_calls: u64,
    pub hierarchy_partitions_scanned: u64,
    pub quotas_checks: u64,
    pub quotas_rejects: u64,
    pub update_calls: u64,
    pub updated_slots: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub segment_tree_rebuilds: u64,
    pub segment_tree_queries: u64,
    pub segment_tree_query_slots: u64,
    pub fast_path_eligible_jobs: u64,
    pub fast_path_candidates: u64,
    pub fast_path_skipped_windows: u64,
    pub fast_path_false_positives: u64,
    pub fast_path_hits: u64,
    pub anchor_cache_rebuilds: u64,
    pub anchor_cache_slots_recomputed: u64,
    pub anchor_window_min_queries: u64,
}

impl From<PerfStats> for PerfBenchmarkResult {
    fn from(value: PerfStats) -> Self {
        PerfBenchmarkResult {
            total_schedule_cycle_ns: value.total_schedule_cycle_ns,
            init_slot_sets_ns: value.init_slot_sets_ns,
            get_waiting_jobs_ns: value.get_waiting_jobs_ns,
            sort_jobs_ns: value.sort_jobs_ns,
            schedule_jobs_ns: value.schedule_jobs_ns,
            save_assignments_ns: value.save_assignments_ns,
            schedule_job_ns: value.schedule_job_ns,
            find_slots_ns: value.find_slots_ns,
            intersect_slots_ns: value.intersect_slots_ns,
            hierarchy_request_ns: value.hierarchy_request_ns,
            quotas_ns: value.quotas_ns,
            update_slots_ns: value.update_slots_ns,
            segment_tree_rebuild_ns: value.segment_tree_rebuild_ns,
            segment_tree_query_ns: value.segment_tree_query_ns,
            anchor_cache_rebuild_ns: value.anchor_cache_rebuild_ns,
            anchor_window_min_ns: value.anchor_window_min_ns,
            jobs_seen: value.jobs_seen,
            jobs_scheduled: value.jobs_scheduled,
            moldables_seen: value.moldables_seen,
            slot_windows_scanned: value.slot_windows_scanned,
            slots_split: value.slots_split,
            slots_intersected: value.slots_intersected,
            hierarchy_calls: value.hierarchy_calls,
            hierarchy_partitions_scanned: value.hierarchy_partitions_scanned,
            quotas_checks: value.quotas_checks,
            quotas_rejects: value.quotas_rejects,
            update_calls: value.update_calls,
            updated_slots: value.updated_slots,
            cache_hits: value.cache_hits,
            cache_misses: value.cache_misses,
            segment_tree_rebuilds: value.segment_tree_rebuilds,
            segment_tree_queries: value.segment_tree_queries,
            segment_tree_query_slots: value.segment_tree_query_slots,
            fast_path_eligible_jobs: value.fast_path_eligible_jobs,
            fast_path_candidates: value.fast_path_candidates,
            fast_path_skipped_windows: value.fast_path_skipped_windows,
            fast_path_false_positives: value.fast_path_false_positives,
            fast_path_hits: value.fast_path_hits,
            anchor_cache_rebuilds: value.anchor_cache_rebuilds,
            anchor_cache_slots_recomputed: value.anchor_cache_slots_recomputed,
            anchor_window_min_queries: value.anchor_window_min_queries,
        }
    }
}


#[derive(Debug, Clone, Serialize)]
pub struct PerfBenchmarkAverageResult {
    pub total_schedule_cycle_ns: BenchmarkMeasurementStatistics<u64>,
    pub init_slot_sets_ns: BenchmarkMeasurementStatistics<u64>,
    pub get_waiting_jobs_ns: BenchmarkMeasurementStatistics<u64>,
    pub sort_jobs_ns: BenchmarkMeasurementStatistics<u64>,
    pub schedule_jobs_ns: BenchmarkMeasurementStatistics<u64>,
    pub save_assignments_ns: BenchmarkMeasurementStatistics<u64>,
    pub schedule_job_ns: BenchmarkMeasurementStatistics<u64>,
    pub find_slots_ns: BenchmarkMeasurementStatistics<u64>,
    pub intersect_slots_ns: BenchmarkMeasurementStatistics<u64>,
    pub hierarchy_request_ns: BenchmarkMeasurementStatistics<u64>,
    pub update_slots_ns: BenchmarkMeasurementStatistics<u64>,
    pub segment_tree_rebuild_ns: BenchmarkMeasurementStatistics<u64>,
    pub segment_tree_query_ns: BenchmarkMeasurementStatistics<u64>,
    pub anchor_cache_rebuild_ns: BenchmarkMeasurementStatistics<u64>,
    pub anchor_window_min_ns: BenchmarkMeasurementStatistics<u64>,
    pub slot_windows_scanned: BenchmarkMeasurementStatistics<u64>,
    pub slots_split: BenchmarkMeasurementStatistics<u64>,
    pub slots_intersected: BenchmarkMeasurementStatistics<u64>,
    pub hierarchy_partitions_scanned: BenchmarkMeasurementStatistics<u64>,
    pub quotas_checks: BenchmarkMeasurementStatistics<u64>,
    pub quotas_rejects: BenchmarkMeasurementStatistics<u64>,
    pub updated_slots: BenchmarkMeasurementStatistics<u64>,
    pub cache_hits: BenchmarkMeasurementStatistics<u64>,
    pub cache_misses: BenchmarkMeasurementStatistics<u64>,
    pub segment_tree_rebuilds: BenchmarkMeasurementStatistics<u64>,
    pub segment_tree_queries: BenchmarkMeasurementStatistics<u64>,
    pub segment_tree_query_slots: BenchmarkMeasurementStatistics<u64>,
    pub fast_path_eligible_jobs: BenchmarkMeasurementStatistics<u64>,
    pub fast_path_candidates: BenchmarkMeasurementStatistics<u64>,
    pub fast_path_skipped_windows: BenchmarkMeasurementStatistics<u64>,
    pub fast_path_false_positives: BenchmarkMeasurementStatistics<u64>,
    pub fast_path_hits: BenchmarkMeasurementStatistics<u64>,
    pub anchor_cache_rebuilds: BenchmarkMeasurementStatistics<u64>,
    pub anchor_cache_slots_recomputed: BenchmarkMeasurementStatistics<u64>,
    pub anchor_window_min_queries: BenchmarkMeasurementStatistics<u64>,
}

impl From<Vec<PerfBenchmarkResult>> for PerfBenchmarkAverageResult {
    fn from(value: Vec<PerfBenchmarkResult>) -> Self {
        let collect = |f: fn(&PerfBenchmarkResult) -> u64| value.iter().map(f).collect::<Vec<u64>>().into();
        Self {
            total_schedule_cycle_ns: collect(|r| r.total_schedule_cycle_ns),
            init_slot_sets_ns: collect(|r| r.init_slot_sets_ns),
            get_waiting_jobs_ns: collect(|r| r.get_waiting_jobs_ns),
            sort_jobs_ns: collect(|r| r.sort_jobs_ns),
            schedule_jobs_ns: collect(|r| r.schedule_jobs_ns),
            save_assignments_ns: collect(|r| r.save_assignments_ns),
            schedule_job_ns: collect(|r| r.schedule_job_ns),
            find_slots_ns: collect(|r| r.find_slots_ns),
            intersect_slots_ns: collect(|r| r.intersect_slots_ns),
            hierarchy_request_ns: collect(|r| r.hierarchy_request_ns),
            update_slots_ns: collect(|r| r.update_slots_ns),
            segment_tree_rebuild_ns: collect(|r| r.segment_tree_rebuild_ns),
            segment_tree_query_ns: collect(|r| r.segment_tree_query_ns),
            anchor_cache_rebuild_ns: collect(|r| r.anchor_cache_rebuild_ns),
            anchor_window_min_ns: collect(|r| r.anchor_window_min_ns),
            slot_windows_scanned: collect(|r| r.slot_windows_scanned),
            slots_split: collect(|r| r.slots_split),
            slots_intersected: collect(|r| r.slots_intersected),
            hierarchy_partitions_scanned: collect(|r| r.hierarchy_partitions_scanned),
            quotas_checks: collect(|r| r.quotas_checks),
            quotas_rejects: collect(|r| r.quotas_rejects),
            updated_slots: collect(|r| r.updated_slots),
            cache_hits: collect(|r| r.cache_hits),
            cache_misses: collect(|r| r.cache_misses),
            segment_tree_rebuilds: collect(|r| r.segment_tree_rebuilds),
            segment_tree_queries: collect(|r| r.segment_tree_queries),
            segment_tree_query_slots: collect(|r| r.segment_tree_query_slots),
            fast_path_eligible_jobs: collect(|r| r.fast_path_eligible_jobs),
            fast_path_candidates: collect(|r| r.fast_path_candidates),
            fast_path_skipped_windows: collect(|r| r.fast_path_skipped_windows),
            fast_path_false_positives: collect(|r| r.fast_path_false_positives),
            fast_path_hits: collect(|r| r.fast_path_hits),
            anchor_cache_rebuilds: collect(|r| r.anchor_cache_rebuilds),
            anchor_cache_slots_recomputed: collect(|r| r.anchor_cache_slots_recomputed),
            anchor_window_min_queries: collect(|r| r.anchor_window_min_queries),
        }
    }
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
            perf: None,
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

#[derive(Debug, Clone, Serialize)]
pub struct BenchmarkAverageResult {
    pub perf: PerfBenchmarkAverageResult,
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

#[derive(Debug, Clone, Serialize)]
#[allow(dead_code)]
pub struct BenchmarkMeasurementStatistics<T = u32> {
    pub min: T,
    pub max: T,
    pub mean: T,
    pub q1: T,
    pub q2: T,
    pub q3: T,
    pub std_dev: T,

    #[serde(skip_serializing)]
    pub quartiles: Quartiles,
}

impl<T> From<Vec<T>> for BenchmarkMeasurementStatistics<T>
where
    T: Copy + Ord + Zero + ToPrimitive + FromPrimitive,
{
    fn from(mut value: Vec<T>) -> Self {
        if value.is_empty() {
            return Self {
                min: T::zero(),
                max: T::zero(),
                mean: T::zero(),
                q1: T::zero(),
                q2: T::zero(),
                q3: T::zero(),
                std_dev: T::zero(),
                quartiles: Quartiles::new(&[0.0]),
            };
        }

        value.sort();

        let len = value.len();

        let values_f64: Vec<f64> = value
            .iter()
            .map(|&x| x.to_f64().expect("failed to convert value to f64"))
            .collect();

        let mean_f64 = values_f64.iter().sum::<f64>() / len as f64;

        let mean = T::from_f64(mean_f64)
            .expect("failed to convert mean from f64 to target type");

        let variance = values_f64
            .iter()
            .map(|&x| (x - mean_f64).powi(2))
            .sum::<f64>()
            / len as f64;

        let std_dev_f64 = variance.sqrt();

        let std_dev = T::from_f64(std_dev_f64)
            .expect("failed to convert std_dev from f64 to target type");

        Self {
            min: value[0],
            max: value[len - 1],
            mean,
            q1: value[len / 4],
            q2: value[len / 2],
            q3: value[len * 3 / 4],
            std_dev,
            quartiles: Quartiles::new(&values_f64),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
#[allow(dead_code)]
pub enum WaitingJobsSampleType {
    Normal,
    OldNormal,
    HighCacheHit,
    Besteffort,
    NodeOnly,
    CoreOnly,
    Fragmented,
    HierarchyHeavy,
    CacheFriendly,
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
            WaitingJobsSampleType::Fragmented => "Fragmented jobs".to_string(),
            WaitingJobsSampleType::HierarchyHeavy => "Hierarchy-heavy jobs".to_string(),
            WaitingJobsSampleType::CacheFriendly => "Cache-friendly jobs".to_string(),
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
            WaitingJobsSampleType::Fragmented => "Fragmented",
            WaitingJobsSampleType::HierarchyHeavy => "HierarchyHeavy",
            WaitingJobsSampleType::CacheFriendly => "CacheFriendly",
        }
        .to_string();
        write!(f, "{}", str)
    }
}

impl From<Vec<BenchmarkResult>> for BenchmarkAverageResult {
    fn from(value: Vec<BenchmarkResult>) -> Self {
        BenchmarkAverageResult {
            perf: value.iter().filter_map(|r| r.perf.clone()).collect::<Vec<_>>().into(),
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

#[derive(Debug, Clone, Copy, Serialize)]
pub enum BenchmarkTarget {
    #[allow(dead_code)]
    Rust,
    #[allow(dead_code)]
    Python,
    #[allow(dead_code)]
    RustFromPython,
}

#[derive(Debug, Clone, Serialize)]
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
    pub prefill: PrefillLevel,
    pub topology: ResourceTopology,
    pub nnodes: u32,
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
                "{} jobs scheduled in {} ms ({}% cache hits, {} slots, {}/{}h width ({}% usage), {}% quotas hit)",
                result.jobs_count,
                result.scheduling_time.mean,
                result.cache_hits.mean,
                result.slot_count.mean,
                result.gantt_width.mean,
                result.optimal_gantt_width.mean,
                result.resource_occupation.mean,
                result.quotas_hit.mean
            );
            info!(
                "phases: init={}ns sort={}ns schedule_jobs={}ns save={}ns | internals: find_slots={}ns intersect={}ns hierarchy={}ns update={}ns | counters: windows={} splits={} intersected_slots={} hierarchy_scans={}",
                result.perf.init_slot_sets_ns.mean,
                result.perf.sort_jobs_ns.mean,
                result.perf.schedule_jobs_ns.mean,
                result.perf.save_assignments_ns.mean,
                result.perf.find_slots_ns.mean,
                result.perf.intersect_slots_ns.mean,
                result.perf.hierarchy_request_ns.mean,
                result.perf.update_slots_ns.mean,
                result.perf.slot_windows_scanned.mean,
                result.perf.slots_split.mean,
                result.perf.slots_intersected.mean,
                result.perf.hierarchy_partitions_scanned.mean
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
            let jobs_count = sample_size;
            let target = self.target;
            let cache = self.cache;
            let sample_type = self.sample_type;
            let prefill = self.prefill;
            let nnodes = self.nnodes;
            let topology = self.topology;
            let effective_res_count = platform_mock::estimate_total_cores(topology, nnodes);
            tokio::spawn(async move {
                let waiting_jobs = get_sample_waiting_jobs(
                    effective_res_count,
                    nnodes,
                    jobs_count,
                    sample_type,
                    new_seed.wrapping_mul(1 + i as u64),
                );
                let cache_hits = count_cache_hits(&waiting_jobs);

                let platform_config = platform_mock::generate_mock_platform_config_for_topology(topology, cache, nnodes, false);

                let scheduled_jobs_prefill = if matches!(prefill, PrefillLevel::Empty) {
                    vec![]
                } else {
                    let prefill_jobs = get_prefilled_jobs(
                        effective_res_count,
                        nnodes,
                        jobs_count,
                        sample_type,
                        new_seed.wrapping_mul(10_000 + i as u64),
                        prefill,
                    );

                    let prefill_platform_config = platform_mock::generate_mock_platform_config_for_topology(topology, cache, nnodes, false);

                    let prefill_waiting_jobs = prefill_jobs
                        .into_iter()
                        .map(|j| (j.id, j))
                        .collect();

                    let mut prefill_platform =
                        PlatformBenchMock::new(prefill_platform_config, vec![], prefill_waiting_jobs);

                    let queues = vec!["default".to_string()];
                    schedule_cycle(&mut prefill_platform, &queues);
                    prefill_platform.get_scheduled_jobs()
                };

                let mut platform =
                    PlatformBenchMock::new(platform_config, scheduled_jobs_prefill, waiting_jobs);

                let queues = vec!["default".to_string()];

                let mut perf_result = None;
                let (scheduling_time, slot_count) = match target {
                    BenchmarkTarget::Rust => {
                        perf::reset();
                        let result = measure_time(|| schedule_cycle(&mut platform, &queues));
                        perf_result = perf::take().map(PerfBenchmarkResult::from);
                        result
                    }
                    BenchmarkTarget::Python => schedule_cycle_on_oar_python(&mut platform, queues, false),
                    BenchmarkTarget::RustFromPython => {
                        schedule_cycle_on_oar_python(&mut platform, queues, true)
                    }
                };

                let quotas_hits = platform
                    .get_scheduled_jobs()
                    .iter()
                    .map(|j| j.quotas_hit_count)
                    .sum::<u32>();

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
                    .map(|sd| sd.resources.core_count() as i64 * (sd.end - sd.begin + 1))
                    .sum::<i64>()
                    / effective_res_count as i64) as u32;

                let mut benchmark_result = BenchmarkResult::new(
                    jobs_count as u32,
                    platform.get_scheduled_jobs().len() as u32,
                    scheduling_time,
                    (cache_hits * 100 / jobs_count) as u32,
                    slot_count as u32,
                    quotas_hits * 100 / jobs_count as u32,
                    gantt_width as u32,
                    optimal_gantt_width,
                );
                benchmark_result.perf = perf_result;
                benchmark_result
            })
        })
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<BenchmarkResult>>()
        .into()
    }
}

pub fn get_sample_waiting_jobs(res_count: u32, nnodes: u32, jobs_count: usize, sample_type: WaitingJobsSampleType, seed: u64) -> IndexMap<i64, Job> {
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
        WaitingJobsSampleType::Fragmented => {
            let max_probe_nodes = (nnodes / 8).max(1).min(8);
            let max_long_nodes = (nnodes / 8).max(4).min(32);

            ShapedJobGenerator {
                rand: StdRng::seed_from_u64(seed),
                count: jobs_count / 2,
                id_offset: 0,
                total_res: res_count,
                job_type: "frag_short".to_string(),

                // short_frag: 5..30 min
                walltime_min: 5,
                walltime_max: 30,
                walltime_step: 5,

                // 1 node, 1 cpu, 1..8 cores
                nodes_min: 1,
                nodes_max: 1,
                nodes_step: 1,

                cpus_min: 1,
                cpus_max: 1,
                cpus_step: 1,

                cores_min: 1,
                cores_max: 8,
                cores_step: 1,
            }
            .merge(ShapedJobGenerator {
                rand: StdRng::seed_from_u64(seed + 1),
                count: jobs_count / 4,
                id_offset: 1_000_000,
                total_res: res_count,
                job_type: "frag_medium".to_string(),

                // medium_blocker: 2..8 h
                walltime_min: 2 * 60,
                walltime_max: 8 * 60,
                walltime_step: 30,

                // 1..4 nodes, 1..4 cpus, 16..64 cores
                nodes_min: 1,
                nodes_max: 4,
                nodes_step: 1,

                cpus_min: 1,
                cpus_max: 4,
                cpus_step: 1,

                cores_min: 16,
                cores_max: 64,
                cores_step: 16,
            })
            .merge(ShapedJobGenerator {
                rand: StdRng::seed_from_u64(seed + 2),
                count: jobs_count / 8,
                id_offset: 2_000_000,
                total_res: res_count,
                job_type: "frag_long".to_string(),

                // long_blocker: 1..3 days
                walltime_min: 24 * 60,
                walltime_max: 3 * 24 * 60,
                walltime_step: 12 * 60,

                // 4..min(32, nnodes/8) nodes, 2..8 cpus, 32..64 cores
                nodes_min: 4,
                nodes_max: max_long_nodes,
                nodes_step: 1,

                cpus_min: 2,
                cpus_max: 8,
                cpus_step: 1,

                cores_min: 32,
                cores_max: 64,
                cores_step: 16,
            })
            .merge(ShapedJobGenerator {
                rand: StdRng::seed_from_u64(seed + 3),
                count: jobs_count - (jobs_count / 2 + jobs_count / 4 + jobs_count / 8),
                id_offset: 3_000_000,
                total_res: res_count,
                job_type: "frag_probe".to_string(),

                // planner_probe-like: 1..2 h
                walltime_min: 60,
                walltime_max: 2 * 60,
                walltime_step: 30,

                // 1..min(8, nnodes/8) nodes, 1..4 cpus, 16..64 cores
                nodes_min: 1,
                nodes_max: max_probe_nodes,
                nodes_step: 1,

                cpus_min: 1,
                cpus_max: 4,
                cpus_step: 1,

                cores_min: 16,
                cores_max: 64,
                cores_step: 16,
            })
            .generate_jobs()
        }
        WaitingJobsSampleType::HierarchyHeavy => {
            let max_probe_nodes = (nnodes / 8).max(1).min(8);

            ShapedJobGenerator {
                rand: StdRng::seed_from_u64(seed),
                count: 2 * jobs_count / 5,
                id_offset: 0,
                total_res: res_count,
                job_type: "hh_core_in_node".to_string(),

                // medium-size
                walltime_min: 60,
                walltime_max: 4 * 60,
                walltime_step: 30,

                // 1..4 nodes, 1..4 cpus, 16..64 cores
                nodes_min: 1,
                nodes_max: 4,
                nodes_step: 1,

                cpus_min: 1,
                cpus_max: 4,
                cpus_step: 1,

                cores_min: 16,
                cores_max: 64,
                cores_step: 16,
            }
            .merge(ShapedJobGenerator {
                rand: StdRng::seed_from_u64(seed + 1),
                count: 2 * jobs_count / 5,
                id_offset: 1_000_000,
                total_res: res_count,
                job_type: "hh_node_heavy".to_string(),

                walltime_min: 2 * 60,
                walltime_max: 8 * 60,
                walltime_step: 30,

                // 2..min(8, nnodes/8) nodes, 2..8 cpus, 32..64 cores
                nodes_min: 2,
                nodes_max: max_probe_nodes.max(2),
                nodes_step: 1,

                cpus_min: 2,
                cpus_max: 8,
                cpus_step: 1,

                cores_min: 32,
                cores_max: 64,
                cores_step: 16,
            })
            .merge(ShapedJobGenerator {
                rand: StdRng::seed_from_u64(seed + 2),
                count: jobs_count / 5,
                id_offset: 2_000_000,
                total_res: res_count,
                job_type: "hh_long_shape".to_string(),

                walltime_min: 8 * 60,
                walltime_max: 24 * 60,
                walltime_step: 60,

                // bigger shape-sensitive jobs
                nodes_min: 4,
                nodes_max: max_probe_nodes.max(4),
                nodes_step: 1,

                cpus_min: 4,
                cpus_max: 8,
                cpus_step: 1,

                cores_min: 32,
                cores_max: 64,
                cores_step: 16,
            })
            .generate_jobs()
        }
        WaitingJobsSampleType::CacheFriendly => {
            generate_cache_friendly_jobs(res_count, nnodes, jobs_count, seed)
        }
    };
    jobs.into_iter()
        .map(|j| (j.id, j))
        .collect::<IndexMap<i64, Job>>()
}

fn generate_cache_friendly_jobs(
    res_count: u32,
    nnodes: u32,
    jobs_count: usize,
    seed: u64,
) -> Vec<Job> {
    let mut rng = StdRng::seed_from_u64(seed);
    let max_probe_nodes = (nnodes / 8).max(1).min(8);

    // Small dictionary of realistic repeated templates
    let templates: Vec<(i64, Vec<(Box<str>, u32)>, &'static str)> = vec![
        // easy / short
        (10, vec![("nodes".into(), 1), ("cpus".into(), 1), ("cores".into(), 4)], "cf_easy_a"),
        (20, vec![("nodes".into(), 1), ("cpus".into(), 1), ("cores".into(), 8)], "cf_easy_b"),
        (30, vec![("nodes".into(), 1), ("cpus".into(), 1), ("cores".into(), 16)], "cf_easy_c"),

        // planner-probe-like
        (60, vec![("nodes".into(), 1), ("cpus".into(), 2), ("cores".into(), 16)], "cf_probe_a"),
        (90, vec![("nodes".into(), 2), ("cpus".into(), 2), ("cores".into(), 32)], "cf_probe_b"),
        (120, vec![("nodes".into(), max_probe_nodes.min(4)), ("cpus".into(), 4), ("cores".into(), 32)], "cf_probe_c"),

        // blocker-like
        (4 * 60, vec![("nodes".into(), 2), ("cpus".into(), 4), ("cores".into(), 32)], "cf_block_a"),
        (8 * 60, vec![("nodes".into(), 4), ("cpus".into(), 4), ("cores".into(), 64)], "cf_block_b"),
    ];

    let mut jobs = Vec::with_capacity(jobs_count);

    for i in 0..jobs_count {
        let template_idx = rng.random_range(0..templates.len());
        let (walltime, hierarchy_req, job_type) = &templates[template_idx];

        let request = HierarchyRequest::new(
            ProcSet::from_iter(1..=res_count),
            hierarchy_req.clone(),
        );

        let job_id = 70_000_000 + i as i64;

        jobs.push(
            JobBuilder::new(job_id)
                .moldable_auto(
                    job_id,
                    *walltime,
                    HierarchyRequests::from_requests(vec![request]),
                )
                .add_type_key((*job_type).into())
                .build(),
        );
    }

    jobs
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

struct ShapedJobGenerator {
    rand: StdRng,
    count: usize,
    id_offset: i64,
    total_res: u32,
    job_type: String,

    walltime_min: u32,
    walltime_max: u32,
    walltime_step: u32,

    nodes_min: u32,
    nodes_max: u32,
    nodes_step: u32,

    cpus_min: u32,
    cpus_max: u32,
    cpus_step: u32,

    cores_min: u32,
    cores_max: u32,
    cores_step: u32,
}

impl ShapedJobGenerator {
    fn generate_jobs(&mut self) -> Vec<Job> {
        let mut jobs: Vec<Job> = Vec::with_capacity(self.count);

        for i in 0..self.count {
            let walltime =
                self.generate(self.walltime_min, self.walltime_max, self.walltime_step) as i64;

            let nodes = self.generate(self.nodes_min, self.nodes_max, self.nodes_step);
            let cpus = self.generate(self.cpus_min, self.cpus_max, self.cpus_step);
            let cores = self.generate(self.cores_min, self.cores_max, self.cores_step);

            let hierarchy_req = vec![
                ("nodes".into(), nodes),
                ("cpus".into(), cpus),
                ("cores".into(), cores),
            ];

            let request = HierarchyRequest::new(
                ProcSet::from_iter(1..=self.total_res),
                hierarchy_req,
            );

            let job_id = i as i64 + self.id_offset;

            jobs.push(
                JobBuilder::new(job_id)
                    .moldable_auto(
                        job_id,
                        walltime,
                        HierarchyRequests::from_requests(vec![request]),
                    )
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

    fn merge(self, other: ShapedJobGenerator) -> ShapedJobGeneratorMerged {
        ShapedJobGeneratorMerged {
            generators: vec![self, other],
        }
    }
}

struct ShapedJobGeneratorMerged {
    generators: Vec<ShapedJobGenerator>,
}

impl ShapedJobGeneratorMerged {
    fn merge(&mut self, generator: ShapedJobGenerator) -> &mut Self {
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

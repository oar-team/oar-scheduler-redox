use std::cell::RefCell;
use std::time::Instant;

#[derive(Debug, Clone, Default)]
pub struct PerfStats {
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
}

thread_local! {
    static PERF: RefCell<Option<PerfStats>> = const { RefCell::new(None) };
}

pub fn reset() {
    PERF.with(|cell| *cell.borrow_mut() = Some(PerfStats::default()));
}

pub fn clear() {
    PERF.with(|cell| *cell.borrow_mut() = None);
}

pub fn take() -> Option<PerfStats> {
    PERF.with(|cell| cell.borrow_mut().take())
}

pub fn with_stats_mut<F>(f: F)
where
    F: FnOnce(&mut PerfStats),
{
    PERF.with(|cell| {
        if let Some(stats) = cell.borrow_mut().as_mut() {
            f(stats);
        }
    });
}

pub fn add_ns(selector: fn(&mut PerfStats) -> &mut u64, delta_ns: u64) {
    with_stats_mut(|stats| *selector(stats) += delta_ns);
}

pub fn incr(selector: fn(&mut PerfStats) -> &mut u64, delta: u64) {
    with_stats_mut(|stats| *selector(stats) += delta);
}

pub fn time<T>(selector: fn(&mut PerfStats) -> &mut u64, f: impl FnOnce() -> T) -> T {
    let start = Instant::now();
    let out = f();
    add_ns(selector, start.elapsed().as_nanos().try_into().unwrap());
    out
}

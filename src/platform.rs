use crate::models::models::Job;
use crate::models::models::ProcSet;
use crate::scheduler::hierarchy::Hierarchy;
use crate::scheduler::quotas::QuotasConfig;
use std::rc::Rc;

pub trait PlatformTrait {
    fn get_now(&self) -> i64;
    fn get_max_time(&self) -> i64;

    fn get_platform_config(&self) -> &Rc<PlatformConfig>;

    fn get_scheduled_jobs(&self) -> &Vec<Job>;
    fn get_waiting_jobs(&self) -> &Vec<Job>;

    fn set_scheduled_jobs(&mut self, jobs: Vec<Job>);
}

pub struct PlatformConfig {
    /// Size of an hour in units of time (e.g., 3600 for second resolution)
    pub hour_size: i64,
    pub cache_enabled: bool, // Whether to use caching in scheduling algorithms, used in linked list algorithms only
    pub resource_set: ResourceSet,
    pub quotas_config: QuotasConfig,
}

#[derive(Debug, Clone)]
pub struct ResourceSet {
    pub default_intervals: ProcSet,
    /// For each `ProcSet`, the time until which it is available. Integrated through pseudo jobs.
    pub available_upto: Vec<(i64, ProcSet)>,
    pub hierarchy: Hierarchy,
}

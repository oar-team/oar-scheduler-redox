use crate::models::models::{Job, ProcSet};
use crate::platform::{PlatformConfig, PlatformTrait, ResourceSet};
use crate::scheduler::hierarchy::Hierarchy;
use crate::scheduler::quotas::{QuotasConfig, QuotasValue};
use std::collections::HashMap;
use std::rc::Rc;

/// In mocking, the time unit is the minute.
pub struct PlatformBenchMock {
    platform_config: Rc<PlatformConfig>,
    scheduled_jobs: Vec<Job>,
    waiting_jobs: Vec<Job>,
}
impl PlatformTrait for PlatformBenchMock {
    fn get_now(&self) -> i64 {
        0
    }
    fn get_max_time(&self) -> i64 {
        1_000_000
    }

    fn get_platform_config(&self) -> &Rc<PlatformConfig> {
        &self.platform_config
    }

    fn get_scheduled_jobs(&self) -> &Vec<Job> {
        &self.scheduled_jobs
    }
    fn get_waiting_jobs(&self) -> &Vec<Job> {
        &self.waiting_jobs
    }

    fn set_scheduled_jobs(&mut self, mut jobs: Vec<Job>) {
        self.waiting_jobs.retain(|job| !jobs.iter().any(|j| j.id == job.id));
        self.scheduled_jobs.append(&mut jobs);
    }
}
impl PlatformBenchMock {
    pub fn new(platform_config: PlatformConfig, scheduled_jobs: Vec<Job>, waiting_jobs: Vec<Job>) -> PlatformBenchMock {
        PlatformBenchMock {
            platform_config: Rc::new(platform_config),
            scheduled_jobs,
            waiting_jobs,
        }
    }
}



pub fn generate_mock_platform_config(res_count: u32, switch_size: u32, node_size: u32, cpu_size: u32) -> PlatformConfig {
    PlatformConfig {
        hour_size: 60,
        resource_set: generate_mock_resource_set(res_count, switch_size, node_size, cpu_size),
        quotas_config: Default::default(),
    }
}

/// Generates a mock resource set with a hierarchy of switches, nodes, cpus and cores as the unit.
/// `switch_size` is expressed in number of nodes, `node_size` in number of CPUs, and `cpu_size` in number of cores.
pub fn generate_mock_resource_set(res_count: u32, switch_size: u32, node_size: u32, cpu_size: u32) -> ResourceSet {

    let mut switches = Vec::new();
    let mut i = 1;
    while i <= res_count {
        let next_i = (i + (switch_size * node_size * cpu_size)).min(res_count+1);
        switches.push(ProcSet::from_iter([i..=(next_i - 1)]));
        i = next_i;
    }

    let mut nodes = Vec::new();
    i = 1;
    while i <= res_count {
        let next_i = (i + (node_size * cpu_size)).min(res_count+1);
        nodes.push(ProcSet::from_iter([i..=(next_i - 1)]));
        i = next_i;
    }

    let mut cpus = Vec::new();
    i = 1;
    while i <= res_count {
        let next_i = (i + cpu_size).min(res_count+1);
        cpus.push(ProcSet::from_iter([i..=(next_i - 1)]));
        i = next_i;
    }

    ResourceSet {
        default_intervals: ProcSet::from_iter([1..=res_count]),
        available_upto: vec![], // All resources available until max_time
        hierarchy: Hierarchy::new()
            .add_partition("switches".into(), switches.into_boxed_slice())
            .add_partition("nodes".into(), nodes.into_boxed_slice())
            .add_partition("cpus".into(), cpus.into_boxed_slice())
            .add_unit_partition("cores".into()),
    }
}
pub fn generate_mock_quotas_config(enabled: bool) -> QuotasConfig {

    let default_rules = HashMap::from([
        (("default".into(), "".into(), "".into(), "".into()), QuotasValue::new(Some(100), None, None)),
    ]);

    QuotasConfig::new(enabled, None, default_rules, Box::new(["*".into()]))
}

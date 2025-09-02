use crate::model::configuration::Configuration;
use crate::model::job::{Job, ProcSet};
use crate::platform::{PlatformConfig, PlatformTrait, ResourceSet};
use crate::scheduler::calendar::QuotasConfig;
use crate::scheduler::hierarchy::Hierarchy;
use crate::scheduler::quotas::QuotasValue;
use indexmap::IndexMap;
use std::collections::HashMap;
use std::rc::Rc;

/// In mocking, the time unit is the minute.
pub struct PlatformBenchMock {
    platform_config: Rc<PlatformConfig>,
    scheduled_jobs: Vec<Job>,
    waiting_jobs: IndexMap<i64, Job>,
}
impl PlatformTrait for PlatformBenchMock {
    fn get_now(&self) -> i64 {
        0
    }
    fn get_max_time(&self) -> i64 {
        1_000_000_000
    }

    fn get_platform_config(&self) -> &Rc<PlatformConfig> {
        &self.platform_config
    }

    fn get_scheduled_jobs(&self) -> &Vec<Job> {
        &self.scheduled_jobs
    }
    fn get_waiting_jobs(&self) -> IndexMap<i64, Job> {
        self.waiting_jobs.clone()
    }

    fn save_assignments(&mut self, assigned_jobs: IndexMap<i64, Job>) {
        // Move assigned jobs from waiting map to scheduled vec
        self.waiting_jobs.retain(|id, _job| !assigned_jobs.contains_key(id));
        self.scheduled_jobs.extend(assigned_jobs.into_values());
    }

    fn get_sum_accounting_window(&self, queues: &[String], window_start: i64, window_stop: i64) -> (f64, f64) {
        (0f64, 0f64)
    }

    fn get_sum_accounting_by_project(&self, queues: &[String], window_start: i64, window_stop: i64) -> (HashMap<String, f64>, HashMap<String, f64>) {
        (HashMap::new(), HashMap::new())
    }

    fn get_sum_accounting_by_user(&self, queues: &[String], window_start: i64, window_stop: i64) -> (HashMap<String, f64>, HashMap<String, f64>) {
        (HashMap::new(), HashMap::new())
    }
}


pub fn generate_mock_platform_config(cache_enabled: bool, res_count: u32, switch_size: u32, node_size: u32, cpu_size: u32, quotas_enable: bool) -> PlatformConfig {
    let mut config = Configuration::default();
    config.quotas = quotas_enable;
    config.cache_enabled = cache_enabled;
    config.scheduler_job_security_time = 0;
    PlatformConfig {
        resource_set: generate_mock_resource_set(res_count, switch_size, node_size, cpu_size),
        quotas_config: generate_mock_quotas_config(quotas_enable, res_count),
        config,
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

    let hierarchy = Hierarchy::new()
        .add_partition("switches".into(), switches.into_boxed_slice())
        .add_partition("nodes".into(), nodes.into_boxed_slice())
        .add_partition("cpus".into(), cpus.into_boxed_slice())
        .add_unit_partition("cores".into());

    ResourceSet {
        nb_resources_not_dead: res_count,
        nb_resources_default_not_dead: res_count,
        suspendable_resources: ProcSet::new(),
        default_resources: ProcSet::from_iter([1..=res_count]),
        available_upto: vec![], // All resources available until max_time
        hierarchy,
    }
}
pub fn generate_mock_quotas_config(enabled: bool, res_count: u32) -> QuotasConfig {

    let default_rules = HashMap::from([
        (("*".into(), "*".into(), "besteffort".into(), "*".into()), QuotasValue::new(None, None, None)),
        (("*".into(), "*".into(), "smalljobs".into(), "*".into()), QuotasValue::new(Some(res_count * 8 / 10), None, None)),
        (("*".into(), "*".into(), "midjobs".into(), "*".into()), QuotasValue::new(Some(res_count * 8 / 10), None, None)),
        (("*".into(), "*".into(), "longrun".into(), "*".into()), QuotasValue::new(None, Some(5), None)),
    ]);

    QuotasConfig::new(enabled, None, default_rules, Box::new(["*".into(), "besteffort".into(), "smalljobs".into(), "midjobs".into(), "longrun".into()]))
}

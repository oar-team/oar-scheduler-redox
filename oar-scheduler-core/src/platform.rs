use crate::model::configuration::{Configuration, QuotasAllNbResourcesMode};
pub use crate::model::job::{Job, ProcSet, ProcSetCoresOp};
#[cfg(feature = "pyo3")]
use crate::model::python::proc_set_to_python;
use crate::scheduler::calendar::QuotasConfig;
use crate::scheduler::hierarchy::Hierarchy;
use indexmap::IndexMap;
#[cfg(feature = "pyo3")]
use pyo3::prelude::{PyDictMethods, PyListMethods};
#[cfg(feature = "pyo3")]
use pyo3::types::{PyDict, PyList};
#[cfg(feature = "pyo3")]
use pyo3::{pyclass, Bound, IntoPyObject, IntoPyObjectRef, PyErr, Python};
use std::collections::HashMap;
use std::rc::Rc;

pub trait PlatformTrait {
    fn get_now(&self) -> i64;
    fn get_max_time(&self) -> i64;
    fn get_platform_config(&self) -> &Rc<PlatformConfig>;

    /// Returns already scheduled jobs (in higher priority queues), or advanced reservations.
    fn get_scheduled_jobs(&self) -> &Vec<Job>;

    /// Returns the jobs waiting to be scheduled for the provided queues.
    /// Jobs should be sorted according to the meta-scheduler sorting algorithm.
    /// Using `IndexMap` to keep jobs ordered while still allowing O(1) access by job ID.
    fn get_waiting_jobs(&self) -> IndexMap<u32, Job>;

    /// Save the scheduled jobs assignments.
    /// This function is called after scheduling jobs to remove the assigned jobs from the waiting list,
    /// to add them to the scheduled list, and to save them to the database
    fn save_assignments(&mut self, assigned_jobs: IndexMap<u32, Job>);

    // --- Accounting DB access ---
    /// Returns summed accounting for all queues in [window_start, window_stop):
    /// (ASKED, USED)
    fn get_sum_accounting_window(
        &self,
        queues: &[String],
        window_start: i64,
        window_stop: i64,
    ) -> (f64, f64);

    /// Returns (ASKED, USED) per project for the given queues and window.
    fn get_sum_accounting_by_project(
        &self,
        queues: &[String],
        window_start: i64,
        window_stop: i64,
    ) -> (HashMap<String, f64>, HashMap<String, f64>);

    /// Returns (ASKED, USED) per user for the given queues and window.
    fn get_sum_accounting_by_user(
        &self,
        queues: &[String],
        window_start: i64,
        window_stop: i64,
    ) -> (HashMap<String, f64>, HashMap<String, f64>);
}

#[cfg_attr(feature = "pyo3", derive(IntoPyObjectRef))]
pub struct PlatformConfig {
    pub resource_set: ResourceSet,
    pub quotas_config: QuotasConfig,
    pub config: Configuration
}

/// ResourceSet provide a resource description with the hierarchy of resources.
/// Resources in the ProcSet are identified by an enumerated ID according to the sorting order (0..N-1).
#[derive(Debug, Clone)]
#[cfg_attr(feature = "pyo3-abi3-py38", pyclass(module = "oar_scheduler_redox"))]
pub struct ResourceSet {
    pub nb_resources_not_dead: u32,
    pub nb_resources_default_not_dead: u32,
    pub suspendable_resources: ProcSet,
    /// Default available resources for slot initialization.
    pub default_resources: ProcSet,
    /// For each `ProcSet`, the time until which it is available. Integrated through pseudo jobs.
    pub available_upto: Vec<(i64, ProcSet)>,
    pub hierarchy: Hierarchy,
}

#[cfg(feature = "pyo3")]
impl<'a> IntoPyObject<'a> for &ResourceSet {
    type Target = PyDict;
    type Output = Bound<'a, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'a>) -> Result<Self::Output, Self::Error> {
        let dict = PyDict::new(py);

        let default_intervals = proc_set_to_python(py, &self.default_resources);
        dict.set_item("default_intervals", default_intervals)?;

        let available_upto = PyList::empty(py);
        for (time, proc_set) in &self.available_upto {
            let tuple = (time, proc_set_to_python(py, proc_set));
            available_upto.append(tuple)?;
        }
        dict.set_item("available_upto", available_upto)?;

        dict.set_item("hierarchy", &self.hierarchy)?;

        Ok(dict)
    }
}

/// Builds a QuotasConfig Rust struct from the configuration and resource set.
pub fn build_quotas_config(config: &Configuration, res_set: &ResourceSet) -> QuotasConfig {
    if config.quotas {
        if config.quotas_conf_file.is_none() {
            panic!("Quotas are enabled but no quotas configuration file is provided.");
        }
        if config.quotas_window_time_limit.is_none() {
            panic!("Quotas are enabled but no quotas window time limit is provided.");
        }
        let all_value = match &config.quotas_all_nb_resources_mode {
            QuotasAllNbResourcesMode::DefaultNotDead => res_set.nb_resources_not_dead as i64,
            QuotasAllNbResourcesMode::All => res_set.default_resources.core_count() as i64,
        };
        QuotasConfig::load_from_file(config.quotas_conf_file.clone().unwrap().as_str(), true, all_value, config.quotas_window_time_limit.unwrap())
    } else {
        QuotasConfig::new(false, None, Default::default(), Box::new([]))
    }
}

use crate::models::ProcSet;
use crate::models::{proc_set_to_python, Job};
use crate::scheduler::hierarchy::Hierarchy;
use crate::scheduler::quotas::QuotasConfig;
use indexmap::IndexMap;
#[cfg(feature = "pyo3")]
use pyo3::prelude::{PyDictMethods, PyListMethods};
#[cfg(feature = "pyo3")]
use pyo3::types::{PyDict, PyList};
#[cfg(feature = "pyo3")]
use pyo3::{pyclass, Bound, IntoPyObject, IntoPyObjectRef, PyErr, Python};
use std::rc::Rc;

pub trait PlatformTrait {
    fn get_now(&self) -> i64;
    fn get_max_time(&self) -> i64;
    fn get_platform_config(&self) -> &Rc<PlatformConfig>;
    /// Returns already scheduled jobs (in higher priority queues), or advanced reservations.
    fn get_scheduled_jobs(&self) -> &Vec<Job>;
    /// Returns the jobs waiting to be scheduled.
    /// Jobs are sorted according to the sorting algorithm.
    /// Using `IndexMap` to keep jobs ordered while still allowing O(1) access by job ID.
    fn get_waiting_jobs(&self) -> &IndexMap<u32, Job>;
    /// Save the scheduled jobs assignments.
    /// This function is called after scheduling jobs to remove the assigned jobs from the waiting list,
    /// to add them to the scheduled list, and to save them to the database
    fn save_assignments(&mut self, assigned_jobs: IndexMap<u32, Job>);
}

#[cfg_attr(feature = "pyo3", derive(IntoPyObjectRef))]
pub struct PlatformConfig {
    /// Size of an hour in units of time (e.g., 3600 for second resolution)
    pub hour_size: i64,
    pub job_security_time: i64,
    pub cache_enabled: bool,
    pub resource_set: ResourceSet,
    pub quotas_config: QuotasConfig,
}

/// ResourceSet provide a resource description with the hierarchy of resources.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "pyo3-abi3-py38", pyclass(module = "oar3_scheduler_lib"))]
pub struct ResourceSet {
    pub default_intervals: ProcSet,
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

        let default_intervals = proc_set_to_python(py, &self.default_intervals);
        dict.set_item("default_intervals", default_intervals).unwrap();

        let available_upto = PyList::empty(py);
        for (time, proc_set) in &self.available_upto {
            let tuple = (time, proc_set_to_python(py, proc_set));
            available_upto.append(tuple).unwrap();
        }
        dict.set_item("available_upto", available_upto).unwrap();

        dict.set_item("hierarchy", &self.hierarchy).unwrap();

        Ok(dict)
    }
}

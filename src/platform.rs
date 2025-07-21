use crate::models::models::{proc_set_to_python, Job, Moldable};
use crate::models::models::ProcSet;
use crate::scheduler::hierarchy::Hierarchy;
use crate::scheduler::quotas::QuotasConfig;
use std::rc::Rc;
use pyo3::{Bound, IntoPyObject, IntoPyObjectRef, PyErr, Python};
use pyo3::prelude::{PyDictMethods, PyListMethods};
use pyo3::types::{PyDict, PyList};

pub trait PlatformTrait {
    fn get_now(&self) -> i64;
    fn get_max_time(&self) -> i64;

    fn get_platform_config(&self) -> &Rc<PlatformConfig>;

    fn get_scheduled_jobs(&self) -> &Vec<Job>;
    fn get_waiting_jobs(&self) -> &Vec<Job>;

    fn set_scheduled_jobs(&mut self, jobs: Vec<Job>);
}

#[derive(IntoPyObjectRef)]
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

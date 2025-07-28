use oar3_scheduler::models::Job;
use oar3_scheduler::platform::{PlatformConfig, PlatformTrait, ResourceSet};
use oar3_scheduler::scheduler::quotas::{QuotasConfig, QuotasMap, QuotasValue};
use pyo3::prelude::{PyAnyMethods, PyDictMethods, PyListMethods};
use pyo3::types::{PyDict, PyTuple};
use pyo3::{Bound, PyAny, PyResult};
use std::rc::Rc;

struct Platform {
    now: i64,
    platform_config: Rc<PlatformConfig>,
    scheduled_jobs: Vec<Job>,
    waiting_jobs: Vec<Job>,
}

impl PlatformTrait for Platform {
    fn get_now(&self) -> i64 {
        self.now
    }
    fn get_max_time(&self) -> i64 {
        2i64.pow(31)
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
    fn set_scheduled_jobs(&mut self, jobs: Vec<Job>) {
        self.waiting_jobs.retain(|job| !jobs.iter().any(|j| j.id == job.id));
        self.scheduled_jobs.extend(jobs);
    }
}

impl Platform {
    pub fn build_platform(
        py_platform: &Bound<PyAny>,
        py_session: &Bound<PyAny>,
        py_config: &Bound<PyAny>,
        py_queues: &Bound<PyAny>,
        py_quotas: &Bound<PyAny>,
    ) -> PyResult<Self> {
        let now: i64 = py_platform.getattr("get_time")?.call0()?.extract()?;

        // Get resource set and build PlatformConfig
        let kwargs = PyDict::new(py_platform.py());
        kwargs.set_item("session", py_session)?;
        kwargs.set_item("config", py_config)?;
        let py_res_set: Bound<PyAny> = py_platform.getattr("resource_set")?.call((), Some(&kwargs))?;

        // Get waiting jobs
        let kwargs = PyDict::new(py_platform.py());
        kwargs.set_item("session", py_session.clone())?;
        let waiting_jobs: Bound<PyTuple> = py_platform.getattr("get_waiting_jobs")?.call((py_queues,), Some(&kwargs))?.extract()?;

        Ok(Platform {
            now,
            platform_config: Rc::new(build_platform_config(py_session, py_config, &py_res_set, py_quotas)?),
            scheduled_jobs: Vec::new(),
            waiting_jobs: build_waiting_jobs()?,
        })
    }
}

pub fn build_platform_config(
    py_session: &Bound<PyAny>,
    py_config: &Bound<PyAny>,
    py_res_set: &Bound<PyAny>,
    py_quotas: &Bound<PyAny>,
) -> PyResult<PlatformConfig> {
    Ok(PlatformConfig {
        hour_size: 60 * 60, // Assuming 1 second resolution
        cache_enabled: true,
        quotas_config: build_quotas_config(py_session, py_config, py_quotas)?,
        resource_set: build_resource_set(py_session, py_config, py_res_set)?,
    })
}

pub fn build_resource_set(py_session: &Bound<PyAny>, py_config: &Bound<PyAny>, py_res_set: &Bound<PyAny>) -> PyResult<ResourceSet> {
    todo!();
    // let default_intervals: ProcSet = py_res_set.getattr("default_intervals")?.extract()?;
    // let available_upto: Vec<(i64, ProcSet)> = py_res_set.getattr("available_upto")?.extract()?;
    // let hierarchy: Hierarchy = py_res_set.getattr("hierarchy")?.extract()?;
    //
    // Ok(ResourceSet {
    //     default_intervals,
    //     available_upto,
    //     hierarchy,
    // })
}

pub fn build_quotas_config(py_session: &Bound<PyAny>, py_config: &Bound<PyAny>, py_quotas: &Bound<PyAny>) -> PyResult<QuotasConfig> {
    let enabled: bool = py_quotas.getattr("enabled")?.extract()?;
    let calendar = None; // Temporal quotas not implemented yet
    let default_rules: QuotasMap = py_quotas
        .getattr("default_rules")?
        .downcast::<PyDict>()?
        .iter()
        .map(|(k, v)| {
            let k = k.extract::<(String, String, String, String)>()?;
            let key = (k.0.into_boxed_str(), k.1.into_boxed_str(), k.2.into_boxed_str(), k.3.into_boxed_str());

            let v: Vec<Option<i64>> = v.extract::<Vec<i64>>()?.iter().map(|x| if x < &0 { None } else { Some(*x) }).collect();
            let value: QuotasValue = QuotasValue::new(v[0].map(|i| i as u32), v[1].map(|i| i as u32), v[2]);

            Ok((key, value))
        })
        .collect::<PyResult<QuotasMap>>()?;

    let tracked_job_types: Box<[Box<str>]> = py_quotas
        .getattr("job_types")?
        .extract::<Vec<String>>()?
        .iter()
        .map(|s| s.clone().into_boxed_str())
        .collect();
    Ok(QuotasConfig::new(enabled, calendar, default_rules, tracked_job_types))
}

pub fn build_waiting_jobs() -> PyResult<Vec<Job>> {
    Ok(Vec::new())
}

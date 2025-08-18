use crate::converters::{build_job, build_platform_config, proc_set_to_python};
use indexmap::IndexMap;
use oar3_scheduler::models::Job;
use oar3_scheduler::platform::{PlatformConfig, PlatformTrait};
use pyo3::prelude::{PyAnyMethods, PyDictMethods, PyListMethods};
use pyo3::types::{PyDict, PyList, PyTuple};
use pyo3::{Bound, IntoPyObjectExt, PyAny, PyResult};
use std::rc::Rc;

/// Rust Platform using Python objects and functions to interact with the OAR platform.
pub struct Platform<'p> {
    now: i64,
    platform_config: Rc<PlatformConfig>,
    scheduled_jobs: Vec<Job>,
    waiting_jobs: IndexMap<u32, Job>,
    py_platform: Bound<'p, PyAny>,
    py_session: Bound<'p, PyAny>,
    py_res_set: Bound<'p, PyAny>,
    py_waiting_jobs_map: Bound<'p, PyDict>,
}

impl PlatformTrait for Platform<'_> {
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
    fn get_waiting_jobs(&self) -> &IndexMap<u32, Job> {
        &self.waiting_jobs
    }
    fn save_assignments(&mut self, assigned_jobs: IndexMap<u32, Job>) {
        // Update python scheduled jobs
        Self::save_assignments_python(self, &assigned_jobs).unwrap();
        // Save assign in the Python platform
        self.py_platform
            .getattr("save_assigns")
            .unwrap()
            .call1((&self.py_session, &self.py_waiting_jobs_map, &self.py_res_set))
            .unwrap();

        // Move assigned jobs from waiting map to scheduled vec
        self.waiting_jobs.retain(|id, _job| !assigned_jobs.contains_key(id));
        self.scheduled_jobs.extend(assigned_jobs.into_values());
    }
}

impl<'p> Platform<'p> {
    /// Updates the Python waiting jobs in `self.py_waiting_jobs_map` with the assignments from the Rust `assigned_jobs` parameter.
    fn save_assignments_python(&self, assigned_jobs: &IndexMap<u32, Job>) -> PyResult<()> {
        for (py_job_id, py_job) in &self.py_waiting_jobs_map {
            if let Some(job) = assigned_jobs.get(&py_job_id.extract::<u32>()?) {
                if let Some(sd) = &job.assignment {
                    py_job.setattr("start_time", sd.begin)?;
                    py_job.setattr("walltime", sd.end - sd.begin + 1)?;
                    py_job.setattr("end_time", sd.end)?;
                    py_job.setattr("moldable_id", job.moldables[sd.moldable_index].id)?;
                    py_job.setattr("res_set", proc_set_to_python(py_job.py(), &sd.proc_set)?)?;
                }
            }
        }
        Ok(())
    }

    /// Transforms a Python platform into a Rust Platform struct.
    /// The Rust Platform will keep a reference to Python objects to be able to transfert data back to Python after scheduling.
    pub fn from_python(
        py_platform: &Bound<'p, PyAny>,
        py_session: &Bound<'p, PyAny>,
        py_config: &Bound<'p, PyAny>,
        py_queues: &Bound<'p, PyAny>,
    ) -> PyResult<Self> {
        let py_now = py_platform.getattr("get_time")?.call0()?;
        let now: i64 = py_now.extract()?;
        let py_job_security_time_int: Bound<PyAny> = py_config
            .get_item("SCHEDULER_JOB_SECURITY_TIME")?
            .extract::<String>()?
            .parse::<i64>()?
            .into_bound_py_any(py_config.py())?;
        let job_security_time = py_job_security_time_int.extract::<i64>()?;

        // Get the resource set
        let kwargs = PyDict::new(py_platform.py());
        kwargs.set_item("session", py_session)?;
        kwargs.set_item("config", py_config)?;
        let py_res_set: Bound<PyAny> = py_platform.getattr("resource_set")?.call((), Some(&kwargs))?;

        // Get already scheduled jobs
        let py_scheduled_jobs: Bound<PyAny> = py_platform
            .getattr("get_scheduled_jobs")?
            .call((py_session, &py_res_set, &py_job_security_time_int, &py_now), None)?;
        let py_scheduled_jobs = py_scheduled_jobs.downcast::<PyList>()?;

        // Get waiting jobs
        let kwargs = PyDict::new(py_platform.py());
        kwargs.set_item("session", py_session.clone())?;
        let py_waiting_jobs_tuple = py_platform.getattr("get_waiting_jobs")?.call((py_queues,), Some(&kwargs))?;
        let py_waiting_jobs_map = py_waiting_jobs_tuple.downcast::<PyTuple>()?.get_item(0)?;
        let py_waiting_jobs_map = py_waiting_jobs_map.downcast::<PyDict>()?;
        let py_waiting_jobs_ids = py_waiting_jobs_tuple.downcast::<PyTuple>()?.get_item(1)?;
        py_platform.getattr("get_data_jobs")?.call(
            (
                py_session,
                &py_waiting_jobs_map,
                &py_waiting_jobs_ids,
                &py_res_set,
                &py_job_security_time_int,
            ),
            None,
        )?;

        // Sort waiting jobs
        let py_sorted_waiting_job_ids = py_platform.py().import("oar.kao.kamelot")?.getattr("jobs_sorting")?.call1((
            &py_session,
            &py_config,
            &py_queues,
            &py_now,
            &py_waiting_jobs_ids,
            &py_waiting_jobs_map,
            &py_platform,
        ))?;
        let waiting_jobs: IndexMap<u32, Job> = py_sorted_waiting_job_ids
            .downcast::<PyList>()?
            .iter()
            .map(|py_id| {
                let id: u32 = py_id.extract()?;
                let py_job = py_waiting_jobs_map.get_item(py_id)?.unwrap();
                Ok((id, build_job(&py_job)?))
            })
            .collect::<PyResult<IndexMap<u32, Job>>>()?;

        Ok(Platform {
            now,
            platform_config: Rc::new(build_platform_config(py_res_set.clone(), job_security_time)?),
            scheduled_jobs: py_scheduled_jobs.iter().map(|py_job| build_job(&py_job)).collect::<PyResult<Vec<Job>>>()?,
            waiting_jobs,
            py_platform: py_platform.clone(),
            py_session: py_session.clone(),
            py_res_set,
            py_waiting_jobs_map: py_waiting_jobs_map.clone(),
        })
    }
}

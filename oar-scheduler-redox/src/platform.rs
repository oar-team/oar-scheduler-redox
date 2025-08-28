use crate::converters::{build_job, build_platform_config, proc_set_to_python};
use indexmap::{indexmap, IndexMap};
use log::info;
use oar_scheduler_core::model::configuration::Configuration;
use oar_scheduler_core::models::Job;
use oar_scheduler_core::platform::{PlatformConfig, PlatformTrait};
use pyo3::prelude::{PyAnyMethods, PyDictMethods, PyListMethods};
use pyo3::types::{PyDict, PyList, PyTuple};
use pyo3::{Bound, Py, PyAny, PyResult, Python};
use std::rc::Rc;

/// Rust Platform using Python objects and functions to interact with the OAR platform.
pub struct Platform {
    now: i64,
    platform_config: Rc<PlatformConfig>,
    scheduled_jobs: Vec<Job>,

    waiting_jobs: Option<IndexMap<u32, Job>>,
    py_waiting_jobs_map: Option<Py<PyDict>>,

    py_platform: Py<PyAny>,
    py_session: Py<PyAny>,
    py_config: Py<PyAny>,
    py_res_set: Py<PyAny>,
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
    fn get_waiting_jobs(&self) -> IndexMap<u32, Job> {
        self.waiting_jobs
            .clone()
            .expect("Waiting jobs not loaded. Call `Platform::load_waiting_jobs` before starting the scheduling.")
    }

    fn save_assignments(&mut self, assigned_jobs: IndexMap<u32, Job>) {
        assigned_jobs.iter().for_each(|(_, job)| {
            if let Some(sd) = &job.assignment {
                info!("Assigned job {}: start_time={}, end_time={}, moldable_id={}, proc_set={:?}, moldable_walltime={}", job.id, sd.begin, sd.end, job.moldables[sd.moldable_index].id, sd.proc_set, job.moldables[sd.moldable_index].walltime);
            } else {
                info!("Job {} has no assignment!", job.id);
            }
        });

        Python::with_gil(|py| -> PyResult<()> {
            // Update python scheduled jobs
            let py_scheduled_jobs = Self::save_assignments_python(self, py, &assigned_jobs);

            // Save assign in the Python platform
            self.py_platform
                .getattr(py, "save_assigns")
                .unwrap()
                .call1(py, (&self.py_session, &py_scheduled_jobs, &self.py_res_set))
                .map(|_| ())
        })
        .unwrap();
        // Move assigned jobs from waiting map to scheduled vec
        if let Some(waiting_jobs) = &mut self.waiting_jobs {
            waiting_jobs.retain(|id, _job| !assigned_jobs.contains_key(id));
        } else {
            panic!("Waiting jobs not loaded. Call `Platform::load_waiting_jobs` before starting the scheduling.");
        }
        self.scheduled_jobs.extend(assigned_jobs.into_values());

        // Clear waiting jobs to avoid scheduling two times the same jobs in case of a bug.
        self.waiting_jobs = None;
        self.py_waiting_jobs_map = None;
    }
}

impl Platform {
    /// Updates the Python waiting jobs in `self.py_waiting_jobs_map` with the assignments from the Rust `assigned_jobs` parameter.
    /// Returns a dictionary containing the jobs of `self.py_waiting_jobs_map` filtered by keeping only the assigned jobs.
    fn save_assignments_python<'s>(&self, py: Python<'s>, assigned_jobs: &'s IndexMap<u32, Job>) -> Bound<'s, PyDict> {
        let py_scheduled_jobs = PyDict::new(py);
        if let Some(py_waiting_jobs_map) = &self.py_waiting_jobs_map {
            for (py_job_id, py_job) in py_waiting_jobs_map.bind(py) {
                if let Some(job) = assigned_jobs.get(&py_job_id.extract::<u32>().unwrap()) {
                    if let Some(sd) = &job.assignment {
                        py_job.setattr("start_time", sd.begin).unwrap();
                        py_job.setattr("walltime", sd.end - sd.begin + 1).unwrap();
                        py_job.setattr("end_time", sd.end).unwrap();
                        py_job.setattr("moldable_id", job.moldables[sd.moldable_index].id).unwrap();
                        py_job.setattr("res_set", proc_set_to_python(py_job.py(), &sd.proc_set)).unwrap();
                        py_scheduled_jobs.set_item(py_job_id, py_job).unwrap();
                    }
                }
            }
        } else {
            panic!("Waiting jobs not loaded. Call `Platform::load_waiting_jobs` before starting the scheduling.");
        }
        py_scheduled_jobs
    }

    /// Transforms a Python platform into a Rust Platform struct.
    /// The Rust Platform will keep a reference to Python objects to be able to transfert data back to Python after scheduling.
    pub fn from_python(py_platform: &Bound<PyAny>, py_session: &Bound<PyAny>, py_config: &Bound<PyAny>, py_now: &Bound<PyAny>, py_scheduled_jobs: Option<&Bound<PyAny>>) -> Self {

        let now: i64 = py_now.extract().unwrap();
        let config: Configuration = py_config.extract().unwrap();

        // Get the resource set
        let kwargs = PyDict::new(py_platform.py());
        kwargs.set_item("session", py_session).unwrap();
        kwargs.set_item("config", py_config).unwrap();
        let py_res_set: Bound<PyAny> = py_platform.getattr("resource_set").unwrap().call((), Some(&kwargs)).unwrap();

        // Get already scheduled jobs
        let py_scheduled_jobs = if let Some(py_scheduled_jobs) = py_scheduled_jobs {
            py_scheduled_jobs.clone()
        }else {
            py_platform
                .getattr("get_scheduled_jobs")
                .unwrap()
                .call((py_session, &py_res_set, &config.scheduler_job_security_time, &py_now), None)
                .unwrap()
        };
        let py_scheduled_jobs = py_scheduled_jobs.downcast::<PyList>().unwrap();

        Platform {
            now,
            platform_config: Rc::new(build_platform_config(py_res_set.clone(), config)),
            scheduled_jobs: py_scheduled_jobs
                .iter()
                .map(|py_job| build_job(&py_job))
                .collect::<Vec<Job>>(),
            waiting_jobs: None,
            py_waiting_jobs_map: None,
            py_platform: py_platform.clone().unbind(),
            py_session: py_session.clone().unbind(),
            py_config: py_config.clone().unbind(),
            py_res_set: py_res_set.unbind(),
        }
    }

    /// Fetches the waiting jobs for the provided queues from the Python platform,
    /// sorts them according to the meta-scheduler sorting algorithm,
    /// and stores them in this Platform instance.
    pub fn load_waiting_jobs(&mut self, py_queues: &Bound<PyAny>, reservation: Option<&String>) {
        let py = py_queues.py();

        // Get waiting jobs
        let kwargs = PyDict::new(py);
        kwargs.set_item("session", self.py_session.bind(py).clone()).unwrap();
        if let Some(reservation) = reservation {
            kwargs.set_item("reservation", reservation).unwrap();
        }
        let py_waiting_jobs_tuple = self
            .py_platform
            .bind(py)
            .getattr("get_waiting_jobs")
            .unwrap()
            .call((&py_queues,), Some(&kwargs))
            .unwrap();
        let py_waiting_jobs_map = py_waiting_jobs_tuple.downcast::<PyTuple>().unwrap().get_item(0).unwrap();
        let py_waiting_jobs_map = py_waiting_jobs_map.downcast::<PyDict>().unwrap();
        self.py_waiting_jobs_map = Some(py_waiting_jobs_map.clone().unbind());

        // Not calling `get_data_jobs` if there are no waiting jobs (otherwise, `get_data_jobs` will fail).
        if py_waiting_jobs_map.len() == 0 {
            self.waiting_jobs = Some(indexmap![]);
            return;
        }

        let py_waiting_jobs_ids = py_waiting_jobs_tuple.downcast::<PyTuple>().unwrap().get_item(1).unwrap();
        self.py_platform
            .getattr(py, "get_data_jobs")
            .unwrap()
            .call(
                py,
                (
                    &self.py_session,
                    &py_waiting_jobs_map,
                    &py_waiting_jobs_ids,
                    &self.py_res_set,
                    &self.platform_config.config.scheduler_job_security_time,
                ),
                None,
            )
            .unwrap();

        // Sort waiting jobs
        let py_sorted_waiting_job_ids = py
            .import("oar.kao.kamelot")
            .unwrap()
            .getattr("jobs_sorting")
            .unwrap()
            .call1((
                &self.py_session,
                &self.py_config,
                &py_queues,
                &self.now,
                &py_waiting_jobs_ids,
                &py_waiting_jobs_map,
                &self.py_platform,
            ))
            .unwrap();

        // Create Rust IndexMap from Python jobs
        self.waiting_jobs = Some(
            py_sorted_waiting_job_ids
                .downcast::<PyList>()
                .unwrap()
                .iter()
                .map(|py_id| {
                    let id: u32 = py_id.extract().unwrap();
                    let py_job = py_waiting_jobs_map.get_item(py_id).unwrap().unwrap();
                    Ok((id, build_job(&py_job)))
                })
                .collect::<PyResult<IndexMap<u32, Job>>>()
                .unwrap(),
        );
    }

    pub(crate) fn get_py_session(&self) -> &Py<PyAny> {
        &self.py_session
    }
    pub(crate) fn get_py_config(&self) -> &Py<PyAny> {
        &self.py_config
    }
}

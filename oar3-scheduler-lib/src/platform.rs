use log::info;
use oar3_scheduler::models::{Job, Moldable, ProcSet, ProcSetCoresOp, ScheduledJobData, TimeSharingType};
use oar3_scheduler::platform::{PlatformConfig, PlatformTrait, ResourceSet};
use oar3_scheduler::scheduler::hierarchy::{Hierarchy, HierarchyRequest, HierarchyRequests};
use oar3_scheduler::scheduler::quotas::{QuotasConfig, QuotasMap, QuotasValue};
use pyo3::ffi::c_str;
use pyo3::prelude::{PyAnyMethods, PyDictMethods, PyListMethods};
use pyo3::types::{IntoPyDict, PyDict, PyList, PyTuple};
use pyo3::{Bound, PyAny, PyResult, Python};
use std::collections::HashMap;
use std::rc::Rc;

pub struct Platform<'p> {
    now: i64,
    platform_config: Rc<PlatformConfig>,
    scheduled_jobs: Vec<Job>,
    waiting_jobs: Vec<Job>,
    py_platform: Bound<'p, PyAny>,
    py_session: Bound<'p, PyAny>,
    py_res_set: Bound<'p, PyAny>,
    py_waiting_jobs: Bound<'p, PyList>,
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
    fn get_waiting_jobs(&self) -> &Vec<Job> {
        &self.waiting_jobs
    }
    fn set_scheduled_jobs(&mut self, jobs: Vec<Job>) {
        // Update python scheduled jobs
        Self::update_py_waiting_jobs_from_rs_jobs(self, &jobs).unwrap();
        // Save assign in the Python platform
        self.py_platform
            .getattr("save_assigns")
            .unwrap()
            .call1((&self.py_session, &self.py_waiting_jobs, &self.py_res_set))
            .unwrap();

        // Save jobs in the rust platform
        self.waiting_jobs.retain(|job| !jobs.iter().any(|j| j.id == job.id));
        self.scheduled_jobs.extend(jobs);
    }
}

impl<'p> Platform<'p> {
    fn update_py_waiting_jobs_from_rs_jobs(&self, jobs: &[Job]) -> PyResult<()> {
        let jobs_map: HashMap<u32, &Job> = jobs.iter().map(|j| (j.id, j)).collect();

        for py_job in self.py_waiting_jobs.clone() {
            let id: u32 = py_job.getattr("id")?.extract()?;

            if let Some(job) = jobs_map.get(&id) {
                if let Some(sd) = &job.scheduled_data {
                    py_job.setattr("start_time", sd.begin)?;
                    py_job.setattr("end_time", sd.end)?;
                    py_job.setattr("moldable_id", job.moldables[sd.moldable_index].id)?;
                    py_job.setattr("res_set", Self::proc_set_to_python(py_job.py(), &sd.proc_set)?)?;
                }
            }
        }
        Ok(())
    }

    pub fn build_platform(
        py_platform: &Bound<'p, PyAny>,
        py_session: &Bound<'p, PyAny>,
        py_config: &Bound<'p, PyAny>,
        py_queues: &Bound<'p, PyAny>,
        py_job_security_time: &Bound<PyAny>,
    ) -> PyResult<Self> {
        let py_now = py_platform.getattr("get_time")?.call0()?;
        let now: i64 = py_now.extract()?;
        info!("Building platform with now: {}", now);

        // Get the resource set
        let kwargs = PyDict::new(py_platform.py());
        kwargs.set_item("session", py_session)?;
        kwargs.set_item("config", py_config)?;
        let py_res_set: Bound<PyAny> = py_platform.getattr("resource_set")?.call((), Some(&kwargs))?;

        // Get waiting jobs
        let kwargs = PyDict::new(py_platform.py());
        kwargs.set_item("session", py_session.clone())?;
        let py_waiting_jobs_tuple = py_platform.getattr("get_waiting_jobs")?.call((py_queues,), Some(&kwargs))?;
        let py_waiting_jobs_map = py_waiting_jobs_tuple.downcast::<PyTuple>()?.get_item(0)?;
        let py_waiting_jobs_list: Bound<PyList> = py_waiting_jobs_map.downcast::<PyDict>()?.values();
        let py_waiting_jobs_ids = py_waiting_jobs_tuple.downcast::<PyTuple>()?.get_item(1)?;
        py_platform.getattr("get_data_jobs")?.call(
            (
                py_session,
                py_waiting_jobs_map,
                py_waiting_jobs_ids,
                py_res_set.clone(),
                py_job_security_time,
            ),
            None,
        )?;

        Ok(Platform {
            now,
            platform_config: Rc::new(Self::build_platform_config(py_res_set.clone())?),
            scheduled_jobs: Vec::new(),
            waiting_jobs: Self::build_waiting_jobs(&py_waiting_jobs_list)?,
            py_platform: py_platform.clone(),
            py_session: py_session.clone(),
            py_res_set,
            py_waiting_jobs: py_waiting_jobs_list.clone(),
        })
    }
    fn build_platform_config(py_res_set: Bound<PyAny>) -> PyResult<PlatformConfig> {
        Ok(PlatformConfig {
            hour_size: 60 * 60, // Assuming 1 second resolution
            cache_enabled: true,
            quotas_config: Self::build_quotas_config(py_res_set.py())?,
            resource_set: Self::build_resource_set(&py_res_set)?,
        })
    }

    fn build_resource_set(py_res_set: &Bound<PyAny>) -> PyResult<ResourceSet> {
        let py_default_intervals = py_res_set.getattr("default_itvs")?;
        let available_upto = py_res_set
            .getattr("available_upto")?
            .downcast::<PyDict>()?
            .iter()
            .map(|(k, v)| {
                let time: i64 = k.extract()?;
                let proc_set = Self::build_proc_set(&v)?;
                Ok((time, proc_set))
            })
            .collect::<PyResult<Vec<_>>>()?;

        let mut unit_partition = None;
        let partitions = py_res_set
            .getattr("hierarchy")?
            .downcast::<PyDict>()?
            .iter()
            .map(|(k, v)| {
                let key: String = k.extract()?;
                let value: Box<[ProcSet]> = Self::build_proc_sets(&v)?;
                Ok((key.into_boxed_str(), value))
            })
            .collect::<PyResult<HashMap<_, _>>>()?
            .into_iter()
            .filter(|(name, res)| {
                // If cores count is always 1, we can consider it a unit partition
                if unit_partition.is_none() && res.into_iter().all(|proc_set| proc_set.core_count() == 1) {
                    unit_partition = Some((*name).clone());
                    return false;
                }
                true
            })
            .collect();

        Ok(ResourceSet {
            default_intervals: Self::build_proc_set(&py_default_intervals)?,
            available_upto,
            hierarchy: Hierarchy::new_defined(partitions, unit_partition),
        })
    }
    fn build_proc_set(py_proc_set: &Bound<PyAny>) -> PyResult<ProcSet> {
        Ok(py_proc_set
            .py()
            .eval(
                c_str!("[(i.inf, i.sup) for i in list(p.intervals())]"),
                Some(&[("p", py_proc_set)].into_py_dict(py_proc_set.py())?),
                None,
            )?
            .extract::<Vec<(u32, u32)>>()?
            .iter()
            .map(|(inf, sup)| ProcSet::from_iter([*inf..=*sup]))
            .fold(ProcSet::new(), |acc, x| acc | x))
    }
    fn proc_set_to_python(py: Python<'p>, proc_set: &ProcSet) -> PyResult<Bound<'p, PyAny>> {
        let intervals = proc_set
            .ranges()
            .map(|r| PyList::new(py, [*r.start(), *r.end()]))
            .collect::<PyResult<Vec<_>>>()?;
        let intervals = PyTuple::new(py, intervals)?;

        py.import("procset")?.getattr("ProcSet")?.call1(intervals)
    }
    fn build_proc_sets(py_proc_sets: &Bound<PyAny>) -> PyResult<Box<[ProcSet]>> {
        Ok(py_proc_sets
            .py()
            .eval(
                c_str!("[[(i.inf, i.sup) for i in list(p.intervals())] for p in ps]"),
                Some(&[("ps", py_proc_sets)].into_py_dict(py_proc_sets.py())?),
                None,
            )?
            .extract::<Vec<Vec<(u32, u32)>>>()?
            .iter()
            .map(|vec| {
                vec.iter()
                    .map(|(inf, sup)| ProcSet::from_iter([*inf..=*sup]))
                    .fold(ProcSet::new(), |acc, x| acc | x)
            })
            .collect::<Box<[ProcSet]>>())
    }

    fn build_quotas_config(py: Python) -> PyResult<QuotasConfig> {
        let py_quotas: Bound<PyAny> = py.import("oar.kao.quotas")?.getattr("Quotas")?;

        let enabled: bool = py_quotas.getattr("enabled")?.extract()?;
        let calendar = None; // Temporal quotas not implemented yet
        let default_rules: QuotasMap = py_quotas
            .getattr("default_rules")?
            .downcast::<PyDict>()?
            .iter()
            .map(|(k, v)| {
                // Extract the python tuple key and convert it to a tuple of boxed str.
                let k = k.extract::<(String, String, String, String)>()?;
                let key = (k.0.into_boxed_str(), k.1.into_boxed_str(), k.2.into_boxed_str(), k.3.into_boxed_str());
                // Transform the value (list) to QuotasValue, replacing -1 with None and keeping other values as u32 or i64.
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

    fn build_waiting_jobs(py_jobs: &Bound<PyList>) -> PyResult<Vec<Job>> {
        let mut jobs = Vec::new();
        for job in py_jobs.iter() {
            let id: u32 = job.getattr("id")?.extract()?;
            let name: Option<String> = job.getattr("name")?.extract()?;
            let user: Option<String> = job.getattr("user")?.extract()?;
            let project: Option<String> = job.getattr("project")?.extract()?;
            let queue: String = job.getattr("queue_name")?.extract()?;
            let types: HashMap<String, String> = job.getattr("types")?.extract()?;
            let types: Vec<Box<str>> = types.into_iter().map(|(s1, s2)| format!("{}={}", s1, s2).into_boxed_str()).collect();

            let time_sharing: bool = job.getattr_opt("ts")?.map(|o| o.extract()).unwrap_or(Ok(false))?;
            let time_sharing = if time_sharing {
                let time_sharing_user_name: String = job.getattr("ts_user")?.extract()?;
                let time_sharing_job_name: String = job.getattr("ts_name")?.extract()?;
                Some(TimeSharingType::from_str(&time_sharing_user_name, &time_sharing_job_name))
            } else {
                None
            };

            // Moldables
            let moldables: Vec<_> = job
                .getattr("mld_res_rqts")?
                .downcast::<PyList>()?
                .iter()
                .map(|moldable| {
                    let moldable = moldable.downcast::<PyTuple>()?;
                    let id: u32 = moldable.get_item(0)?.extract()?;
                    let walltime: i64 = moldable.get_item(1)?.extract()?;
                    let requests: Vec<HierarchyRequest> = moldable
                        .get_item(2)?
                        .downcast::<PyList>()?
                        .iter()
                        .map(|req| {
                            let req = req.downcast::<PyTuple>()?;
                            let level_nbs = req.get_item(0)?;
                            let level_nbs = level_nbs
                                .downcast::<PyList>()?
                                .into_iter()
                                .map(|level_nb_tuple| {
                                    let level_nb_tuple = level_nb_tuple.downcast::<PyTuple>()?;
                                    let level_name: String = level_nb_tuple.get_item(0)?.extract()?;
                                    let level_nb: u32 = level_nb_tuple.get_item(1)?.extract()?;
                                    Ok((level_name.into_boxed_str(), level_nb))
                                })
                                .collect::<PyResult<Vec<_>>>()?;
                            let filter = Self::build_proc_set(&req.get_item(1)?)?;

                            Ok(HierarchyRequest::new(filter, level_nbs))
                        })
                        .collect::<PyResult<Vec<_>>>()?;
                    Ok(Moldable::new(id, walltime, HierarchyRequests::from_requests(requests)))
                })
                .collect::<PyResult<_>>()?;

            let mut scheduled_data: Option<ScheduledJobData> = None;
            if job.hasattr("start_time")? && job.hasattr("walltime")? {
                let begin: i64 = job.getattr("start_time")?.extract()?;
                let walltime: i64 = job.getattr("walltime")?.extract()?;
                let end: i64 = begin + walltime - 1;

                let proc_set: ProcSet = Self::build_proc_set(&job.getattr("res_set")?)?;

                let moldables_id: u32 = job.getattr("moldable_id")?.extract()?;
                let moldable_index = moldables.iter().position(|m| m.id == moldables_id).unwrap_or(0);

                scheduled_data = Some(ScheduledJobData {
                    begin,
                    end,
                    proc_set,
                    moldable_index,
                });
            }

            jobs.push(Job {
                id,
                name: name.map(|n| n.into_boxed_str()),
                user: user.map(|u| u.into_boxed_str()),
                project: project.map(|p| p.into_boxed_str()),
                queue: queue.into_boxed_str(),
                types,
                moldables,
                scheduled_data,
                quotas_hit_count: 0,
                time_sharing,
            });
        }
        Ok(jobs)
    }
}

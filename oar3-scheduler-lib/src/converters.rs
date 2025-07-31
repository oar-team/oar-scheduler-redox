use oar3_scheduler::models::{Job, JobAssignment, Moldable, ProcSet, ProcSetCoresOp, TimeSharingType};
use oar3_scheduler::platform::{PlatformConfig, ResourceSet};
use oar3_scheduler::scheduler::hierarchy::{Hierarchy, HierarchyRequest, HierarchyRequests};
use oar3_scheduler::scheduler::quotas::{QuotasConfig, QuotasMap, QuotasValue};
use pyo3::ffi::c_str;
use pyo3::prelude::{PyAnyMethods, PyDictMethods, PyListMethods};
use pyo3::types::{IntoPyDict, PyDict, PyList, PyTuple};
use pyo3::{Bound, PyAny, PyResult, Python};
use std::collections::HashMap;
use indexmap::IndexMap;

/// Builds a PlatformConfig Rust struct from a Python resource set.
pub fn build_platform_config(py_res_set: Bound<PyAny>) -> PyResult<PlatformConfig> {
    Ok(PlatformConfig {
        hour_size: 60 * 60, // Assuming 1 second resolution
        cache_enabled: true,
        quotas_config: build_quotas_config(py_res_set.py())?,
        resource_set: build_resource_set(&py_res_set)?,
    })
}

/// Builds a ResourceSet Rust struct from a Python resource set.
fn build_resource_set(py_res_set: &Bound<PyAny>) -> PyResult<ResourceSet> {
    let py_default_intervals = py_res_set.getattr("roid_itvs")?;
    let available_upto = py_res_set
        .getattr("available_upto")?
        .downcast::<PyDict>()?
        .iter()
        .map(|(k, v)| {
            let time: i64 = k.extract()?;
            let proc_set = build_proc_set(&v)?;
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
            let value: Box<[ProcSet]> = build_proc_sets(&v)?;
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
        default_intervals: build_proc_set(&py_default_intervals)?,
        available_upto,
        hierarchy: Hierarchy::new_defined(partitions, unit_partition),
    })
}
/// Builds a Rust ProcSet (range-set-blaze lib) from a Python ProcSet (procset lib).
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
/// Converts a Rust ProcSet (range-set-blaze lib) to a Python ProcSet (procset lib).
pub fn proc_set_to_python<'p>(py: Python<'p>, proc_set: &ProcSet) -> PyResult<Bound<'p, PyAny>> {
    let intervals = proc_set
        .ranges()
        .map(|r| PyList::new(py, [*r.start(), *r.end()]))
        .collect::<PyResult<Vec<_>>>()?;
    let intervals = PyTuple::new(py, intervals)?;

    py.import("procset")?.getattr("ProcSet")?.call1(intervals)
}
/// Converts a Python list of ProcSets to a Rust array of ProcSets.
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
/// Builds a QuotasConfig Rust struct from the Python Quotas class' static attributes.
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
/// Transforms a Python job object into a Rust Job struct.
pub fn build_job(py_job: &Bound<PyAny>) -> PyResult<Job> {
    let name: Option<String> = py_job.getattr("name")?.extract()?;
    let user: Option<String> = py_job.getattr("user")?.extract()?;
    let project: Option<String> = py_job.getattr("project")?.extract()?;
    let queue: String = py_job.getattr("queue_name")?.extract()?;
    let types = py_job
        .getattr("types")?
        .extract::<HashMap<String, Option<String>>>()?
        .into_iter()
        .map(|(k, v)| {
            (
                k.into_boxed_str(),
                v.and_then(|s| if s.is_empty() { None } else { Some(s.clone().into_boxed_str()) }),
            )
        })
        .collect::<HashMap<_, _>>();
    let time_sharing: bool = py_job.getattr_opt("ts")?.map(|o| o.extract()).unwrap_or(Ok(false))?;
    let time_sharing = if time_sharing {
        let time_sharing_user_name: String = py_job.getattr("ts_user")?.extract()?;
        let time_sharing_job_name: String = py_job.getattr("ts_name")?.extract()?;
        Some(TimeSharingType::from_str(&time_sharing_user_name, &time_sharing_job_name))
    } else {
        None
    };

    // Moldables
    let moldables: Vec<_> = py_job
        .getattr("mld_res_rqts")?
        .downcast::<PyList>()?
        .iter()
        .map(|moldable| build_moldable(&moldable))
        .collect::<PyResult<_>>()?;

    let mut assignment: Option<JobAssignment> = None;
    if py_job.hasattr("start_time")? && py_job.hasattr("walltime")? {
        let begin: Option<i64> = py_job.getattr("start_time")?.extract()?;
        let walltime: Option<i64> = py_job.getattr("walltime")?.extract()?;
        if let (Some(begin), Some(walltime)) = (begin, walltime)
            && begin > 0
            && walltime > 0
        {
            let end: i64 = begin + walltime - 1;

            let proc_set: ProcSet = build_proc_set(&py_job.getattr("res_set")?)?;

            let moldables_id: u32 = py_job.getattr("moldable_id")?.extract()?;
            let moldable_index = moldables.iter().position(|m| m.id == moldables_id).unwrap_or(0);

            assignment = Some(JobAssignment {
                begin,
                end,
                proc_set,
                moldable_index,
            });
        }
    }

    // Dependencies
    let dependencies: Vec<(u32, Box<str>, Option<i32>)> = py_job
        .getattr("deps")?
        .downcast::<PyList>()?
        .iter()
        .map(|dep| {
            let dep = dep.downcast::<PyTuple>()?;
            let id: u32 = dep.get_item(0)?.extract()?;
            let name: String = dep.get_item(1)?.extract()?;
            let state: Option<i32> = dep.get_item(2)?.extract()?;
            Ok((id, name.into_boxed_str(), state))
        })
        .collect::<PyResult<_>>()?;

    Ok(Job {
        id: py_job.getattr("id")?.extract::<u32>()?,
        name: name.map(|n| n.into_boxed_str()),
        user: user.map(|u| u.into_boxed_str()),
        project: project.map(|p| p.into_boxed_str()),
        queue: queue.into_boxed_str(),
        types,
        moldables,
        assignment,
        quotas_hit_count: 0,
        time_sharing,
        dependencies,
    })
}
/// Builds a Moldable Rust struct from a Python moldable object.
fn build_moldable(py_moldable: &Bound<PyAny>) -> PyResult<Moldable> {
    let id: u32 = py_moldable.get_item(0)?.extract()?;
    let walltime: i64 = py_moldable.get_item(1)?.extract()?;

    let requests: Vec<HierarchyRequest> = py_moldable
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
            let filter = build_proc_set(&req.get_item(1)?)?;

            Ok(HierarchyRequest::new(filter, level_nbs))
        })
        .collect::<PyResult<Vec<_>>>()?;

    Ok(Moldable::new(id, walltime, HierarchyRequests::from_requests(requests)))
}

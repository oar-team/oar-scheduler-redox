/*
 * Copyright (c) 2025 Clément GRENNERAT
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, version 3.
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see https://www.gnu.org/licenses/.
 *
 */

use oar_scheduler_core::model::configuration::Configuration;
use oar_scheduler_core::model::job::{Job, JobAssignment, Moldable, PlaceholderType, ProcSet, ProcSetCoresOp, TimeSharingType};
use oar_scheduler_core::platform;
use oar_scheduler_core::platform::{PlatformConfig, ResourceSet};
use oar_scheduler_core::scheduler::hierarchy::{Hierarchy, HierarchyRequest, HierarchyRequests};
use pyo3::ffi::c_str;
use pyo3::prelude::{PyAnyMethods, PyDictMethods, PyListMethods};
use pyo3::types::{IntoPyDict, PyDict, PyList, PyTuple};
use pyo3::{Bound, PyAny, PyResult, Python};
use std::collections::HashMap;

/// Builds a PlatformConfig Rust struct from a Python resource set.
pub fn build_platform_config(py_res_set: Bound<PyAny>, config: Configuration) -> PlatformConfig {
    let resource_set = build_resource_set(&py_res_set);
    let quotas_config = platform::build_quotas_config(&config, &resource_set);

    PlatformConfig {
        quotas_config,
        resource_set,
        config,
    }
}

/// Builds a ResourceSet Rust struct from a Python resource set.
fn build_resource_set(py_res_set: &Bound<PyAny>) -> ResourceSet {
    let py_default_intervals = py_res_set.getattr("roid_itvs").unwrap();
    let available_upto = py_res_set
        .getattr("available_upto")
        .unwrap()
        .downcast::<PyDict>()
        .unwrap()
        .iter()
        .map(|(k, v)| {
            let time: i64 = k.extract().unwrap();
            let proc_set = build_proc_set(&v);
            Ok((time, proc_set))
        })
        .collect::<PyResult<Vec<_>>>()
        .unwrap();

    let mut unit_partitions = vec![];
    let partitions = py_res_set
        .getattr("hierarchy")
        .unwrap()
        .downcast::<PyDict>()
        .unwrap()
        .iter()
        .map(|(k, v)| {
            let key: String = k.extract().unwrap();
            let value: Box<[ProcSet]> = build_proc_sets(&v);
            Ok((key.into_boxed_str(), value))
        })
        .collect::<PyResult<HashMap<_, _>>>()
        .unwrap()
        .into_iter()
        .filter(|(name, res)| {
            // If cores count is always 1, we can consider it a unit partition
            if res.into_iter().all(|proc_set| proc_set.core_count() == 1) {
                unit_partitions.push((*name).clone());
                return false;
            }
            true
        })
        .collect();

    let default_resources = build_proc_set(&py_default_intervals);
    ResourceSet {
        nb_resources_not_dead: default_resources.core_count(),
        nb_resources_default_not_dead: default_resources.core_count(),
        suspendable_resources: ProcSet::new(),
        default_resources,
        available_upto,
        hierarchy: Hierarchy::new_defined(partitions, unit_partitions),
    }
}
/// Builds a Rust ProcSet (range-set-blaze lib) from a Python ProcSet (procset lib).
fn build_proc_set(py_proc_set: &Bound<PyAny>) -> ProcSet {
    py_proc_set
        .py()
        .eval(
            c_str!("[(i.inf, i.sup) for i in list(p.intervals())]"),
            Some(&[("p", py_proc_set)].into_py_dict(py_proc_set.py()).unwrap()),
            None,
        )
        .unwrap()
        .extract::<Vec<(u32, u32)>>()
        .unwrap()
        .iter()
        .map(|(inf, sup)| ProcSet::from_iter([*inf..=*sup]))
        .fold(ProcSet::new(), |acc, x| acc | x)
}
/// Converts a Rust ProcSet (range-set-blaze lib) to a Python ProcSet (procset lib).
pub fn proc_set_to_python<'p>(py: Python<'p>, proc_set: &ProcSet) -> Bound<'p, PyAny> {
    let intervals = proc_set
        .ranges()
        .map(|r| PyList::new(py, [*r.start(), *r.end()]))
        .collect::<PyResult<Vec<_>>>()
        .unwrap();
    let intervals = PyTuple::new(py, intervals).unwrap();

    py.import("procset").unwrap().getattr("ProcSet").unwrap().call1(intervals).unwrap()
}
/// Converts a Python list of ProcSets to a Rust array of ProcSets.
fn build_proc_sets(py_proc_sets: &Bound<PyAny>) -> Box<[ProcSet]> {
    py_proc_sets
        .py()
        .eval(
            c_str!("[[(i.inf, i.sup) for i in list(p.intervals())] for p in ps]"),
            Some(&[("ps", py_proc_sets)].into_py_dict(py_proc_sets.py()).unwrap()),
            None,
        )
        .unwrap()
        .extract::<Vec<Vec<(u32, u32)>>>()
        .unwrap()
        .iter()
        .map(|vec| {
            vec.iter()
                .map(|(inf, sup)| ProcSet::from_iter([*inf..=*sup]))
                .fold(ProcSet::new(), |acc, x| acc | x)
        })
        .collect::<Box<[ProcSet]>>()
}
/// Transforms a Python job object into a Rust Job struct.
pub fn build_job(py_job: &Bound<PyAny>) -> Job {
    let name: Option<String> = py_job.getattr("name").unwrap().extract().unwrap();
    let user: Option<String> = py_job.getattr("user").unwrap().extract().unwrap();
    let project: Option<String> = py_job.getattr("project").unwrap().extract().unwrap();
    let queue: String = py_job.getattr("queue_name").unwrap().extract().unwrap();
    let types = py_job
        .getattr("types")
        .unwrap()
        .extract::<HashMap<String, Option<String>>>()
        .unwrap()
        .into_iter()
        .map(|(k, v)| {
            (
                k.into_boxed_str(),
                v.and_then(|s| if s.is_empty() { None } else { Some(s.clone().into_boxed_str()) }),
            )
        })
        .collect::<HashMap<_, _>>();
    let time_sharing: bool = py_job.getattr_opt("ts").unwrap().map(|o| o.extract()).unwrap_or(Ok(false)).unwrap();
    let time_sharing = if time_sharing {
        let time_sharing_user_name: String = py_job.getattr("ts_user").unwrap().extract().unwrap();
        let time_sharing_job_name: String = py_job.getattr("ts_name").unwrap().extract().unwrap();
        Some(TimeSharingType::from_str(&time_sharing_user_name, &time_sharing_job_name))
    } else {
        None
    };

    // NO_PLACEHOLDER = 0 ; PLACEHOLDER = 1 ; ALLOW = 2
    let placeholder_type = py_job
        .getattr("ph")
        .map_or(Ok(0), |ph| {
            ph.extract::<i32>()
                .map_err(|_| pyo3::exceptions::PyTypeError::new_err("Invalid placeholder type"))
        })
        .unwrap();

    let placeholder = match placeholder_type {
        0 => PlaceholderType::None,
        1 => PlaceholderType::Allow(py_job.getattr("ph_name").unwrap().extract::<String>().unwrap().into_boxed_str()),
        2 => PlaceholderType::Placeholder(py_job.getattr("ph_name").unwrap().extract::<String>().unwrap().into_boxed_str()),
        _ => panic!("Invalid placeholder type value"),
    };

    // Moldables (scheduled jobs do not have mdl_res_rqts defined)
    let moldables: Vec<_> = if py_job.hasattr("mld_res_rqts").unwrap() {
        py_job
            .getattr("mld_res_rqts")
            .unwrap()
            .downcast::<PyList>()
            .unwrap()
            .iter()
            .map(|moldable| build_moldable(&moldable))
            .collect()
    } else {
        Vec::new()
    };

    // Assignment
    let mut assignment: Option<JobAssignment> = None;
    if py_job.hasattr("start_time").unwrap() && py_job.hasattr("walltime").unwrap() {
        let begin: Option<i64> = py_job.getattr("start_time").unwrap().extract().unwrap();
        let walltime: Option<i64> = py_job.getattr("walltime").unwrap().extract().unwrap();
        if let (Some(begin), Some(walltime)) = (begin, walltime) {
            if walltime > 0 {
                let end: i64 = begin + walltime - 1;

                let proc_set: ProcSet = build_proc_set(&py_job.getattr("res_set").unwrap());

                let moldables_id: i64 = py_job.getattr("moldable_id").unwrap().extract().unwrap();
                let moldable_index = moldables.iter().position(|m| m.id == moldables_id).unwrap_or(0);

                assignment = Some(JobAssignment {
                    begin,
                    end,
                    resources: proc_set,
                    moldable_index,
                });
            }
        }
    }
    // Advance reservation start time
    let mut advance_reservation_start_time = None;
    if assignment.is_none() && py_job.hasattr("start_time").unwrap() {
        let begin: Option<i64> = py_job.getattr("start_time").unwrap().extract().unwrap();
        if let Some(begin) = begin {
            if begin > 0 {
                advance_reservation_start_time = Some(begin);
            }
        }
    }

    // Dependencies (scheduled jobs do not have mdl_res_rqts defined)
    let dependencies: Vec<(i64, Box<str>, Option<i32>)> = if py_job.hasattr("deps").unwrap() {
        py_job
            .getattr("deps")
            .unwrap()
            .downcast::<PyList>()
            .unwrap()
            .iter()
            .map(|dep| {
                let dep = dep.downcast::<PyTuple>().unwrap();
                let id: i64 = dep.get_item(0).unwrap().extract().unwrap();
                let name: String = dep.get_item(1).unwrap().extract().unwrap();
                let state: Option<i32> = dep.get_item(2).unwrap().extract().unwrap();
                Ok((id, name.into_boxed_str(), state))
            })
            .collect::<PyResult<_>>()
            .unwrap()
    } else {
        Vec::new()
    };

    // no_quotas
    let no_quotas: bool = py_job.getattr_opt("no_quotas").unwrap().map(|o| o.extract()).unwrap_or(Ok(false)).unwrap();

    Job {
        id: py_job.getattr("id").unwrap().extract::<i64>().unwrap(),
        name: name.map(|n| n.into_boxed_str()),
        user: user.map(|u| u.into_boxed_str()),
        project: project.map(|p| p.into_boxed_str()),
        queue: queue.into_boxed_str(),
        types,
        moldables,
        no_quotas,
        assignment,
        quotas_hit_count: 0,
        time_sharing,
        placeholder,
        dependencies,
        advance_reservation_begin: advance_reservation_start_time,
        submission_time: py_job.getattr_opt("submission_time").unwrap().map(|v| v.extract::<i64>()).unwrap_or(Ok(0)).unwrap(),
        qos: py_job.getattr_opt("qos").unwrap().map(|v| v.extract::<f64>()).unwrap_or(Ok(0.0)).unwrap(),
        nice: py_job.getattr_opt("nice").unwrap().map(|v| v.extract::<f64>()).unwrap_or(Ok(1.0)).unwrap(),
        karma: 0.0,
        message: String::new(),
    }
}
/// Builds a Moldable Rust struct from a Python moldable object.
fn build_moldable(py_moldable: &Bound<PyAny>) -> Moldable {
    let id: i64 = py_moldable.get_item(0).unwrap().extract().unwrap();
    let walltime: i64 = py_moldable.get_item(1).unwrap().extract().unwrap();

    let requests: Vec<HierarchyRequest> = py_moldable
        .get_item(2)
        .unwrap()
        .downcast::<PyList>()
        .unwrap()
        .iter()
        .map(|req| {
            let req = req.downcast::<PyTuple>().unwrap();
            let level_nbs = req.get_item(0).unwrap();
            let level_nbs = level_nbs
                .downcast::<PyList>()
                .unwrap()
                .into_iter()
                .map(|level_nb_tuple| {
                    let level_nb_tuple = level_nb_tuple.downcast::<PyTuple>().unwrap();
                    let level_name: String = level_nb_tuple.get_item(0).unwrap().extract().unwrap();
                    let level_nb: u32 = level_nb_tuple.get_item(1).unwrap().extract().unwrap();
                    Ok((level_name.into_boxed_str(), level_nb))
                })
                .collect::<PyResult<Vec<_>>>()
                .unwrap();
            let filter = build_proc_set(&req.get_item(1).unwrap());

            Ok(HierarchyRequest::new(filter, level_nbs))
        })
        .collect::<PyResult<Vec<_>>>()
        .unwrap();

    Moldable::new(id, walltime, HierarchyRequests::from_requests(requests))
}

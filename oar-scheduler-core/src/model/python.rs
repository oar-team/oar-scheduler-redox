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

use crate::model::job::{Job, Moldable, ProcSet};
use pyo3::prelude::{PyAnyMethods, PyListMethods, PyModule};
use pyo3::types::{PyDict, PyList, PyTuple};
use pyo3::{Bound, IntoPyObject, PyAny, PyErr, Python};
use std::collections::HashMap;

pub fn proc_set_to_python<'a>(py: Python<'a>, proc_set: &ProcSet) -> Bound<'a, PyAny> {
    let procset_module = PyModule::import(py, "procset").unwrap();
    let procset_class = procset_module.getattr("ProcSet").unwrap();
    let procint_class = procset_module.getattr("ProcInt").unwrap();

    let list = PyList::empty(py);
    for range in proc_set.ranges() {
        list.append(procint_class.call1((range.start(), range.end())).unwrap()).unwrap();
    }

    let procset_instance = procset_class.call1(PyTuple::new(py, list).unwrap()).unwrap();
    procset_instance
}

impl<'a> IntoPyObject<'a> for &Job {
    type Target = PyDict;
    type Output = Bound<'a, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'a>) -> Result<Self::Output, Self::Error> {
        let dict = PyDict::new(py);
        dict.set_item("id", self.id)?;

        if let Some(name) = &self.name {
            dict.set_item("name", name.as_ref())?;
        } else {
            dict.set_item("name", py.None())?;
        }
        if let Some(user) = &self.user {
            dict.set_item("user", user.as_ref())?;
        } else {
            dict.set_item("user", py.None())?;
        }
        if let Some(project) = &self.project {
            dict.set_item("project", project.as_ref())?;
        } else {
            dict.set_item("project", py.None())?;
        }

        dict.set_item("queue", self.queue.clone().as_ref())?;
        dict.set_item(
            "types",
            self.types
                .iter()
                .map(|(k, v)| (k.as_ref(), v.clone().map(|v| v.to_string())))
                .collect::<HashMap<&str, Option<String>>>(),
        )?;
        dict.set_item("moldables", self.moldables.iter().enumerate().collect::<Vec<(usize, &Moldable)>>())?;
        if let Some(assignment) = &self.assignment {
            let assignment_dict = PyDict::new(py);
            assignment_dict.set_item("begin", assignment.begin)?;
            assignment_dict.set_item("end", assignment.end)?;
            assignment_dict.set_item("proc_set", proc_set_to_python(py, &assignment.proc_set))?;
            assignment_dict.set_item("moldable_index", assignment.moldable_index)?;
            dict.set_item("assignment", assignment_dict)?;
        } else {
            dict.set_item("assignment", py.None())?;
        }
        Ok(dict)
    }
}

impl<'a> IntoPyObject<'a> for &Moldable {
    type Target = PyDict;
    type Output = Bound<'a, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'a>) -> Result<Self::Output, Self::Error> {
        let dict = PyDict::new(py);
        dict.set_item("id", &self.id)?;
        dict.set_item("walltime", &self.walltime)?;
        dict.set_item("requests", &self.requests)?;
        dict.set_item("cache_key", &self.cache_key.to_string())?;
        Ok(dict)
    }
}

use crate::model::job::{ProcSet, ProcSetCoresOp};
#[cfg(feature = "pyo3")]
use crate::model::python::proc_set_to_python;
use auto_bench_fct::auto_bench_fct_hy;
use log::warn;
#[cfg(feature = "pyo3")]
use pyo3::prelude::{PyAnyMethods, PyDictMethods, PyListMethods};
#[cfg(feature = "pyo3")]
use pyo3::types::{PyDict, PyList, PyTuple};
#[cfg(feature = "pyo3")]
use pyo3::{Bound, IntoPyObject, PyAny, PyErr, Python};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HierarchyRequests(pub Box<[HierarchyRequest]>);
impl HierarchyRequests {
    pub fn from_requests(requests: Vec<HierarchyRequest>) -> Self {
        HierarchyRequests(requests.into_boxed_slice())
    }
    pub fn new_single(filter: ProcSet, level_nbs: Vec<(Box<str>, u32)>) -> Self {
        HierarchyRequests::from_requests(vec![HierarchyRequest::new(filter, level_nbs)])
    }
    pub fn get_cache_key(&self) -> String {
        self.0
            .iter()
            .map(|req| {
                format!(
                    "{}-{}",
                    req.filter,
                    req.level_nbs
                        .iter()
                        .map(|(name, count)| format!("{}:{}", name, count))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            })
            .collect::<Vec<_>>()
            .join(";")
    }
}
#[cfg(feature = "pyo3")]
impl<'a> IntoPyObject<'a> for &HierarchyRequests {
    type Target = PyAny;
    type Output = Bound<'a, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'a>) -> Result<Self::Output, Self::Error> {
        Ok(self.0.iter().map(|req| req).collect::<Vec<_>>().into_pyobject(py).unwrap())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HierarchyRequest {
    pub filter: ProcSet,
    pub level_nbs: Box<[(Box<str>, u32)]>, // Level name, number of resources requested at that level
}
impl HierarchyRequest {
    pub fn new(filter: ProcSet, level_nbs: Vec<(Box<str>, u32)>) -> Self {
        HierarchyRequest {
            filter,
            level_nbs: level_nbs.into_boxed_slice(),
        }
    }
}
#[cfg(feature = "pyo3")]
impl<'a> IntoPyObject<'a> for &HierarchyRequest {
    type Target = PyDict;
    type Output = Bound<'a, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'a>) -> Result<Self::Output, Self::Error> {
        let request_dict = PyDict::new(py);
        request_dict.set_item("filter", proc_set_to_python(py, &self.filter)).unwrap();
        request_dict
            .set_item(
                "level_nbs",
                self.level_nbs
                    .iter()
                    .map(|n| {
                        // Tuple like (n.0.to_string(), n.1)
                        PyTuple::new(py, [n.0.to_string()])
                            .unwrap()
                            .add(PyTuple::new(py, [n.1]).unwrap())
                            .unwrap()
                    })
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        Ok(request_dict)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hierarchy {
    partitions: HashMap<Box<str>, Box<[ProcSet]>>, // Level name, partitions of that level
    pub(crate) unit_partition: Option<Box<str>>,   // Name of a virtual unitary partition (correspond to a single u32 in ProcSet), e.g. "core"
}

impl Hierarchy {
    pub fn new() -> Self {
        Self::new_defined(HashMap::new(), None)
    }
    pub fn new_defined(partitions: HashMap<Box<str>, Box<[ProcSet]>>, unit_partition: Option<Box<str>>) -> Self {
        Hierarchy { partitions, unit_partition }
    }
    pub fn add_partition(mut self, name: Box<str>, partitions: Box<[ProcSet]>) -> Self {
        if self.has_partition(&name) {
            panic!("A partition with the name {} already exists.", name);
        }
        self.partitions.insert(name, partitions);
        self
    }
    pub fn add_unit_partition(mut self, name: Box<str>) -> Self {
        if self.has_partition(&name) {
            panic!("A partition with the name {} already exists.", name);
        }
        if self.unit_partition.is_some() {
            panic!("A unit partition is already defined.");
        }
        self.unit_partition = Some(name);
        self
    }
    pub fn has_partition(&self, name: &Box<str>) -> bool {
        self.partitions.contains_key(name.as_ref()) || Some(name) == self.unit_partition.as_ref()
    }
    #[auto_bench_fct_hy]
    pub fn request(&self, available_proc_set: &ProcSet, request: &HierarchyRequests) -> Option<ProcSet> {
        let result = request.0.iter().try_fold(ProcSet::new(), |acc, req| {
            self.find_resource_hierarchies_scattered(&(available_proc_set & &req.filter), &req.level_nbs)
                .map(|partition| partition | acc)
        });
        result
    }
    #[auto_bench_fct_hy]
    pub fn find_resource_hierarchies_scattered(&self, available_proc_set: &ProcSet, level_requests: &[(Box<str>, u32)]) -> Option<ProcSet> {
        let (name, request) = &level_requests[0];
        // Optimization for core that should correspond to a single proc.
        if Some(name) == self.unit_partition.as_ref() {
            return available_proc_set.sub_proc_set_with_cores(*request);
        }

        if let Some(partitions) = self.partitions.get(name) {
            let (proc_sets, count) = partitions
                .iter()
                .filter_map(|proc_set| {
                    if level_requests.len() > 1 {
                        // If the next level is core, do not iterate over it and do the check directly. The core level should correspond to a single proc.
                        if Some(name) == self.unit_partition.as_ref() {
                            proc_set.sub_proc_set_with_cores(level_requests[1].1)
                        } else {
                            self.find_resource_hierarchies_scattered(&(proc_set & available_proc_set), &level_requests[1..])
                        }
                    } else if proc_set.is_subset(&available_proc_set) {
                        Some(proc_set.clone())
                    } else {
                        None
                    }
                })
                .take(*request as usize)
                .fold((ProcSet::new(), 0), |(acc, count), proc_set| (acc | proc_set, count + 1));

            if count < *request {
                return None;
            }
            Some(proc_sets)
        } else {
            warn!("No such hierarchy level matching name {}", name);
            None
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'a> IntoPyObject<'a> for &Hierarchy {
    type Target = PyDict;
    type Output = Bound<'a, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'a>) -> Result<Self::Output, Self::Error> {
        let dict = PyDict::new(py);

        let partitions_dict = PyDict::new(py);
        for (name, partitions) in &self.partitions {
            let partitions_list = PyList::empty(py);
            for partition in partitions {
                partitions_list.append(proc_set_to_python(py, partition)).unwrap();
            }
            partitions_dict.set_item(name.to_string(), partitions_list).unwrap();
        }
        dict.set_item("partitions", partitions_dict).unwrap();

        if let Some(unit_partition) = &self.unit_partition {
            dict.set_item("unit_partition", unit_partition.to_string()).unwrap();
        } else {
            dict.set_item("unit_partition", py.None()).unwrap();
        }

        Ok(dict)
    }
}

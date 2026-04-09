use crate::model::job::{ProcSet, ProcSetCoresOp};
use crate::perf;
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
use std::collections::{HashMap, HashSet};

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

pub type LevelId = u16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CompiledLevelRequest {
    level_id: LevelId,
    count: u32,
    is_unit: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HierarchyBuildError {
    MissingLevel(Box<str>),
    MissingLevelId(Box<str>),
    OrphanPartition {
        level: Box<str>,
        proc_set: ProcSet,
    },
    AmbiguousParent {
        level: Box<str>,
        proc_set: ProcSet,
        parent_count: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HierarchyNode {
    pub level_id: LevelId,
    pub proc_set: ProcSet,
    pub children: Box<[HierarchyNode]>,
}

impl HierarchyNode {
    fn new(level_id: LevelId, proc_set: ProcSet, children: Box<[HierarchyNode]>) -> Self {
        Self {
            level_id,
            proc_set,
            children,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hierarchy {
    // Keep the old representation during the transition period.
    partitions: HashMap<Box<str>, Box<[ProcSet]>>,
    // Explicit level order.
    level_order: Vec<Box<str>>,
    // Level name -> compact integer id.
    level_ids: HashMap<Box<str>, LevelId>,
    // Fast checks for unit levels.
    unit_level_ids: HashSet<LevelId>,
    // New representation for fast traversal.
    roots: Box<[HierarchyNode]>,
    // Kept for compatibility/debugging.
    unit_partitions: Vec<Box<str>>,
}

use log::info;
impl Hierarchy {
    pub fn new() -> Self {
        Self {
            partitions: HashMap::new(),
            level_order: vec![],
            level_ids: HashMap::new(),
            unit_level_ids: HashSet::new(),
            roots: Vec::new().into_boxed_slice(),
            unit_partitions: vec![],
        }
    }

    pub fn new_defined_ordered(
        partitions: HashMap<Box<str>, Box<[ProcSet]>>,
        level_order: Vec<Box<str>>,
        unit_partitions: Vec<Box<str>>,
    ) -> Self {
        let mut hierarchy = Self {
            partitions,
            level_order,
            level_ids: HashMap::new(),
            unit_level_ids: HashSet::new(),
            roots: Vec::new().into_boxed_slice(),
            unit_partitions,
        };
        hierarchy.rebuild_metadata();
        hierarchy
    }
    pub fn add_partition(mut self, name: Box<str>, partitions: Box<[ProcSet]>) -> Self {
        if self.has_partition(&name) {
            panic!("A partition with the name {} already exists.", name);
        }
        self.level_order.push(name.clone());
        self.partitions.insert(name, partitions);
        self.rebuild_metadata();
        self
    }
    pub fn add_unit_partition(mut self, name: Box<str>) -> Self {
        if self.has_partition(&name) {
            panic!("A partition with the name {} already exists.", name);
        }
        self.unit_partitions.push(name);
        self.rebuild_metadata();
        self
    }
    pub fn has_partition(&self, name: &Box<str>) -> bool {
        self.partitions.contains_key(name.as_ref()) || self.unit_partitions.contains(name)
    }
    pub fn unit_partitions(&self) -> &Vec<Box<str>> {
        &self.unit_partitions
    }

    pub fn level_order(&self) -> &Vec<Box<str>> {
        &self.level_order
    }

    pub fn roots(&self) -> &[HierarchyNode] {
        &self.roots
    }

    fn build_tree(
        partitions: &HashMap<Box<str>, Box<[ProcSet]>>,
        level_order: &[Box<str>],
        level_ids: &HashMap<Box<str>, LevelId>,
    ) -> Result<Box<[HierarchyNode]>, HierarchyBuildError> {
        if level_order.is_empty() {
            return Ok(Vec::new().into_boxed_slice());
        }

        Self::build_level_from_slice(partitions, level_order, level_ids)
    }

    fn build_level_from_slice(
        partitions: &HashMap<Box<str>, Box<[ProcSet]>>,
        level_order: &[Box<str>],
        level_ids: &HashMap<Box<str>, LevelId>,
    ) -> Result<Box<[HierarchyNode]>, HierarchyBuildError> {
        let level_name = &level_order[0];
        let level_id = *level_ids
            .get(level_name)
            .ok_or_else(|| HierarchyBuildError::MissingLevelId(level_name.clone()))?;

        let current_partitions = partitions
            .get(level_name)
            .ok_or_else(|| HierarchyBuildError::MissingLevel(level_name.clone()))?;

        // Last real level: children are empty
        if level_order.len() == 1 {
            let nodes = current_partitions
                .iter()
                .cloned()
                .map(|proc_set| HierarchyNode::new(level_id, proc_set, Vec::new().into_boxed_slice()))
                .collect::<Vec<_>>()
                .into_boxed_slice();

            return Ok(nodes);
        }

        let next_level_name = &level_order[1];
        let next_partitions = partitions
            .get(next_level_name)
            .ok_or_else(|| HierarchyBuildError::MissingLevel(next_level_name.clone()))?;

        // For each parent, collect the list of children indices
        let mut children_by_parent: Vec<Vec<usize>> = vec![Vec::new(); current_partitions.len()];

        for child_idx in 0..next_partitions.len() {
            let child = &next_partitions[child_idx];

            // Ignore empty partitions.
            if child.is_empty() {
                continue;
            }

            let matching_parents: Vec<usize> = current_partitions
                .iter()
                .enumerate()
                .filter_map(|(parent_idx, parent)| child.is_subset(parent).then_some(parent_idx))
                .collect();

            match matching_parents.len() {
                0 => {
                    return Err(HierarchyBuildError::OrphanPartition {
                        level: next_level_name.clone(),
                        proc_set: child.clone(),
                    });
                }
                1 => {
                    children_by_parent[matching_parents[0]].push(child_idx);
                }
                n => {
                    return Err(HierarchyBuildError::AmbiguousParent {
                        level: next_level_name.clone(),
                        proc_set: child.clone(),
                        parent_count: n,
                    });
                }
            }
        }

        let mut nodes = Vec::with_capacity(current_partitions.len());

        for (parent_idx, parent_proc_set) in current_partitions.iter().enumerate() {
            let child_indices = &children_by_parent[parent_idx];

            let children = if child_indices.is_empty() {
                Vec::new().into_boxed_slice()
            } else {
                let subset_next_partitions: Box<[ProcSet]> = child_indices
                    .iter()
                    .map(|&idx| next_partitions[idx].clone())
                    .collect();

                let mut sub_partitions: HashMap<Box<str>, Box<[ProcSet]>> = HashMap::new();
                sub_partitions.insert(next_level_name.clone(), subset_next_partitions);

                // IMPORTANT: deeper levels must also be restricted by the current parent_proc_set,
                // otherwise foreign partitions will end up in the subtree and become orphaned.
                for deeper_level_name in level_order.iter().skip(2) {
                    let deeper = partitions
                        .get(deeper_level_name)
                        .ok_or_else(|| HierarchyBuildError::MissingLevel(deeper_level_name.clone()))?;

                    let filtered_deeper: Box<[ProcSet]> = deeper
                        .iter()
                        .filter(|proc_set| proc_set.is_subset(parent_proc_set))
                        .cloned()
                        .collect();

                    sub_partitions.insert(deeper_level_name.clone(), filtered_deeper);
                }

                Self::build_level_from_slice(&sub_partitions, &level_order[1..], level_ids)?
            };

            nodes.push(HierarchyNode::new(level_id, parent_proc_set.clone(), children));
        }

        Ok(nodes.into_boxed_slice())
    }

    #[auto_bench_fct_hy]
    pub fn request(&self, available_proc_set: &ProcSet, request: &HierarchyRequests) -> Option<ProcSet> {
        let _timer = std::time::Instant::now();
        perf::incr(|s| &mut s.hierarchy_calls, 1);
        let result = request.0.iter().try_fold(ProcSet::new(), |acc, req| {
            let filtered_available = available_proc_set & &req.filter;
            self.find_resource_hierarchies_scattered(&filtered_available, &req.level_nbs)
                .map(|partition| partition | acc)
        });
        perf::add_ns(|s| &mut s.hierarchy_request_ns, _timer.elapsed().as_nanos().try_into().unwrap());
        result
    }
    #[auto_bench_fct_hy]
    pub fn find_resource_hierarchies_scattered(
        &self,
        available_proc_set: &ProcSet,
        level_requests: &[(Box<str>, u32)],
    ) -> Option<ProcSet> {
        let compiled = self.compile_level_requests(level_requests)?;
        self.find_in_nodes(&self.roots, available_proc_set, &compiled)
            .map(|(proc_set, _count)| proc_set)
    }

    fn find_in_nodes(
        &self,
        nodes: &[HierarchyNode],
        available_proc_set: &ProcSet,
        level_requests: &[CompiledLevelRequest],
    ) -> Option<(ProcSet, u32)> {
        if available_proc_set.is_empty() || level_requests.is_empty() {
            return None;
        }

        perf::incr(|s| &mut s.hierarchy_partitions_scanned, nodes.len() as u64);

        let wanted = level_requests[0];

        if wanted.is_unit {
            return available_proc_set
                .sub_proc_set_with_cores(wanted.count)
                .map(|ps| (ps, wanted.count));
        }

        let mut acc = ProcSet::new();
        let mut count: u32 = 0;

        for node in nodes {
            let node_available = &node.proc_set & available_proc_set;
            if node_available.is_empty() {
                continue;
            }

            if node.level_id == wanted.level_id {
                let selected: Option<ProcSet> = if level_requests.len() > 1 {
                    let next = level_requests[1];

                    if next.is_unit {
                        node_available.sub_proc_set_with_cores(next.count)
                    } else {
                        self.find_in_nodes(&node.children, &node_available, &level_requests[1..])
                            .map(|(ps, _)| ps)
                    }
                } else if node.proc_set.is_subset(available_proc_set) {
                    Some(node.proc_set.clone())
                } else {
                    None
                };

                if let Some(proc_set) = selected {
                    acc = acc | proc_set;
                    count += 1;

                    if count == wanted.count {
                        return Some((acc, count));
                    }
                }
            } else {
                if let Some((proc_set, found_count)) =
                    self.find_in_nodes(&node.children, &node_available, level_requests)
                {
                    acc = acc | proc_set;
                    count += found_count;

                    if count >= wanted.count {
                        return Some((acc, count));
                    }
                }
            }
        }

        if count >= wanted.count {
            Some((acc, count))
        } else {
            None
        }
    }

    fn rebuild_metadata(&mut self) {
        self.level_ids.clear();
        self.unit_level_ids.clear();

        let mut next_id: LevelId = 0;

        for level_name in &self.level_order {
            self.level_ids.insert(level_name.clone(), next_id);
            next_id += 1;
        }

        for unit_name in &self.unit_partitions {
            self.level_ids.insert(unit_name.clone(), next_id);
            self.unit_level_ids.insert(next_id);
            next_id += 1;
        }

        self.roots = Self::build_tree(&self.partitions, &self.level_order, &self.level_ids)
            .unwrap_or_else(|err| panic!("Failed to build hierarchy tree: {:?}", err));
    }

    fn level_id(&self, level_name: &str) -> Option<LevelId> {
        self.level_ids.get(level_name).copied()
    }

    fn compile_level_requests(
        &self,
        level_requests: &[(Box<str>, u32)],
    ) -> Option<Vec<CompiledLevelRequest>> {
        let mut compiled = Vec::with_capacity(level_requests.len());

        for (level_name, count) in level_requests {
            let level_id = self.level_id(level_name)?;
            compiled.push(CompiledLevelRequest {
                level_id,
                count: *count,
                is_unit: self.unit_level_ids.contains(&level_id),
            });
        }

        Some(compiled)
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
        dict.set_item(
            "level_order",
            self.level_order.iter().map(|name| name.to_string()).collect::<Vec<String>>(),
        )
            .unwrap();
        dict.set_item(
            "unit_partitions",
            self.unit_partitions.iter().map(|name| name.to_string()).collect::<Vec<String>>(),
        )
            .unwrap();

        Ok(dict)
    }
}

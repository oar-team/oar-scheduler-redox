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
pub struct AnchorSpec {
    pub level_name: Box<str>,
    pub count: u32,
    pub filter: ProcSet,
    pub is_unit: bool,
}

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
    pub subtree_core_count: u32,
    pub subtree_level_counts: Box<[u32]>,
}

impl HierarchyNode {
    fn new(
        level_id: LevelId,
        proc_set: ProcSet,
        children: Box<[HierarchyNode]>,
        level_count_len: usize,
    ) -> Self {
        let mut subtree_level_counts = vec![0u32; level_count_len];
        subtree_level_counts[level_id as usize] = 1;

        for child in children.iter() {
            for (idx, count) in child.subtree_level_counts.iter().enumerate() {
                subtree_level_counts[idx] += *count;
            }
        }

        Self {
            level_id,
            subtree_core_count: proc_set.core_count(),
            proc_set,
            children,
            subtree_level_counts: subtree_level_counts.into_boxed_slice(),
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
        let level_order = Self::canonicalize_level_order(&partitions, &level_order);

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

        let level_count_len = level_ids.len();
        Self::build_level_from_slice(partitions, level_order, level_ids, level_count_len)
    }

    fn build_level_from_slice(
        partitions: &HashMap<Box<str>, Box<[ProcSet]>>,
        level_order: &[Box<str>],
        level_ids: &HashMap<Box<str>, LevelId>,
        level_count_len: usize,
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
                .map(|proc_set| {
                    HierarchyNode::new(level_id, proc_set, Vec::new().into_boxed_slice(), level_count_len)
                })
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

                Self::build_level_from_slice(&sub_partitions, &level_order[1..], level_ids, level_count_len)?
            };

            nodes.push(HierarchyNode::new(
                level_id,
                parent_proc_set.clone(),
                children,
                level_count_len,
            ));
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
        let wanted = compiled[0];
        self.find_in_nodes(&self.roots, available_proc_set, &compiled)
            .and_then(|(proc_set, count)| (count >= wanted.count).then_some(proc_set))
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
            let available_cores = available_proc_set.core_count();
            if available_cores == 0 {
                return None;
            }

            let take = available_cores.min(wanted.count);
            return available_proc_set
                .sub_proc_set_with_cores(take)
                .map(|ps| (ps, take));
        }

        let mut acc = ProcSet::new();
        let mut count: u32 = 0;

        for node in nodes {
            if node.subtree_level_counts[wanted.level_id as usize] == 0 {
                continue;
            }

            let node_available = &node.proc_set & available_proc_set;
            if node_available.is_empty() {
                continue;
            }

            if node.level_id == wanted.level_id {
                let selected: Option<ProcSet> = if level_requests.len() > 1 {
                    let next = level_requests[1];

                    if next.is_unit {
                        if node_available.core_count() < next.count {
                            None
                        } else {
                            node_available.sub_proc_set_with_cores(next.count)
                        }
                    } else if node.children.is_empty()
                        || node.subtree_level_counts[next.level_id as usize] == 0
                    {
                        None
                    } else {
                        self.find_in_nodes(&node.children, &node_available, &level_requests[1..])
                            .and_then(|(ps, found_count)| (found_count >= next.count).then_some(ps))
                    }
                } else if node.proc_set.is_subset(available_proc_set) {
                    Some(node.proc_set.clone())
                } else {
                    None
                };

                if let Some(proc_set) = selected {
                    acc = acc | proc_set;
                    count += 1;

                    if count >= wanted.count {
                        return Some((acc, count));
                    }
                }
            } else {
                let remaining = wanted.count.saturating_sub(count);
                if remaining == 0 {
                    return Some((acc, count));
                }

                let adjusted_requests = Self::with_first_count(level_requests, remaining);

                if let Some((proc_set, found_count)) =
                    self.find_in_nodes(&node.children, &node_available, &adjusted_requests)
                {
                    acc = acc | proc_set;
                    count += found_count.min(remaining);

                    if count >= wanted.count {
                        return Some((acc, count));
                    }
                }
            }
        }

        if count > 0 {
            Some((acc, count))
        } else {
            None
        }
    }

    fn rebuild_metadata(&mut self) {
        self.level_ids.clear();
        self.unit_level_ids.clear();

        // normalize the order of levels every time before rebuilding the tree,
        // including during incremental add_partition().
        self.level_order = Self::canonicalize_level_order(&self.partitions, &self.level_order);

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

    fn with_first_count(
        level_requests: &[CompiledLevelRequest],
        new_count: u32,
    ) -> Vec<CompiledLevelRequest> {
        let mut updated = level_requests.to_vec();
        if let Some(first) = updated.first_mut() {
            first.count = new_count;
        }
        updated
    }

    fn canonicalize_level_order(
        partitions: &HashMap<Box<str>, Box<[ProcSet]>>,
        level_order: &[Box<str>],
    ) -> Vec<Box<str>> {
        let n = level_order.len();

        let pos: HashMap<&str, usize> = level_order
            .iter()
            .enumerate()
            .map(|(i, name)| (name.as_ref(), i))
            .collect();

        let contains_level = |a: &str, b: &str| -> bool {
            let parents = partitions
                .get(a)
                .unwrap_or_else(|| panic!("Missing level {}", a));
            let children = partitions
                .get(b)
                .unwrap_or_else(|| panic!("Missing level {}", b));

            children.iter().all(|child| {
                parents.iter().any(|parent| child.is_subset(parent))
            })
        };

        let mut indegree = vec![0usize; n];
        let mut edges: Vec<Vec<usize>> = vec![Vec::new(); n];

        for i in 0..n {
            for j in 0..n {
                if i == j {
                    continue;
                }

                let a = level_order[i].as_ref();
                let b = level_order[j].as_ref();

                let a_contains_b = contains_level(a, b);
                let b_contains_a = contains_level(b, a);

                // strict ordering only
                if a_contains_b && !b_contains_a {
                    edges[i].push(j);
                }
            }
        }

        for outs in &edges {
            for &j in outs {
                indegree[j] += 1;
            }
        }

        let mut available: Vec<usize> = (0..n).filter(|&i| indegree[i] == 0).collect();

        // stable tie-break by original position
        available.sort_by_key(|&i| pos[level_order[i].as_ref()]);

        let mut result = Vec::with_capacity(n);

        while let Some(i) = {
            if available.is_empty() {
                None
            } else {
                Some(available.remove(0))
            }
        } {
            result.push(level_order[i].clone());

            for &j in &edges[i] {
                indegree[j] -= 1;
                if indegree[j] == 0 {
                    available.push(j);
                }
            }

            available.sort_by_key(|&k| pos[level_order[k].as_ref()]);
        }

        // If some equivalent/incomparable levels remain due to no strict edges,
        // topological sort still returns all of them through stable tie-break.
        if result.len() != n {
            panic!("Failed to canonicalize hierarchy level order");
        }

        result
    }

    pub fn derive_anchor_spec(&self, requests: &HierarchyRequests) -> Option<AnchorSpec> {
        let req = requests.0.first()?;
        let (level_name, _last_count) = req.level_nbs.last()?.clone();
        let level_id = self.level_id(&level_name)?;
        let is_unit = self.unit_level_ids.contains(&level_id);

        let count = req
            .level_nbs
            .iter()
            .fold(1u32, |acc, (_name, c)| acc.saturating_mul(*c));

        Some(AnchorSpec {
            level_name,
            count,
            filter: req.filter.clone(),
            is_unit,
        })
    }

    pub fn count_available_units(
        &self,
        available_proc_set: &ProcSet,
        filter: &ProcSet,
        level_name: &str,
    ) -> Option<u32> {
        let filtered = available_proc_set.clone() & filter.clone();
        if filtered.is_empty() {
            return Some(0);
        }

        let level_id = self.level_id(level_name)?;
        if self.unit_level_ids.contains(&level_id) {
            return Some(filtered.core_count());
        }

        Some(self.count_units_in_nodes(&self.roots, &filtered, level_id))
    }

    fn count_units_in_nodes(
        &self,
        nodes: &[HierarchyNode],
        filtered: &ProcSet,
        wanted_level: LevelId,
    ) -> u32 {
        let mut total = 0;

        for node in nodes {
            if node.subtree_level_counts[wanted_level as usize] == 0 {
                continue;
            }

            let intersection = node.proc_set.clone() & filtered.clone();
            if intersection.is_empty() {
                continue;
            }

            if node.level_id == wanted_level {
                if node.proc_set.is_subset(filtered) {
                    total += 1;
                }
            } else {
                total += self.count_units_in_nodes(&node.children, filtered, wanted_level);
            }
        }

        total
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

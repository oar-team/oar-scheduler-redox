use crate::models::models::{ProcSet, ProcSetCoresOp};
use log::warn;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HierarchyRequests(Box<[HierarchyRequest]>);
impl HierarchyRequests {
    pub fn from_requests(requests: Vec<HierarchyRequest>) -> Self {
        HierarchyRequests(requests.into_boxed_slice())
    }
    pub fn get_cache_key(&self) -> String {
        self.0.iter()
            .map(|req| format!("{}-{}", req.filter, req.level_nbs.iter().map(|(name, count)| format!("{}:{}", name, count)).collect::<Vec<_>>().join(",")))
            .collect::<Vec<_>>()
            .join(";")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HierarchyRequest {
    pub filter: ProcSet,
    pub level_nbs: Box<[(Box<str>, u32)]> // Level name, number of resources requested at that level
}
impl HierarchyRequest {
    pub fn new(filter: ProcSet, level_nbs: Vec<(Box<str>, u32)>) -> Self {
        HierarchyRequest {
            filter,
            level_nbs: level_nbs.into_boxed_slice(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hierarchy{
    partitions: HashMap<Box<str>, Box<[ProcSet]>>, // Level name, partitions of that level
    unit_partition: Option<Box<str>>, // Name of a virtual unitary partition (correspond to a single u32 in ProcSet), e.g. "core"
}

impl Hierarchy {
    pub fn new() -> Self {
        Hierarchy {
            partitions: HashMap::new(),
            unit_partition: None,
        }
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
}

impl Hierarchy {
    pub fn request(&self, available_proc_set: &ProcSet, request: &HierarchyRequests) -> Option<ProcSet> {
        request.0.iter().try_fold(ProcSet::new(), |acc, req| {
            self.find_resource_hierarchies_scattered(&(available_proc_set & &req.filter), &req.level_nbs)
                .map(|partition| partition | acc)
        })
    }
    pub fn find_resource_hierarchies_scattered(&self, available_proc_set: &ProcSet, level_requests: &[(Box<str>, u32)]) -> Option<ProcSet> {
        let (name, request) = &level_requests[0];
        // Optimization for core that should correspond to a single proc.
        if let Some(_name) = self.unit_partition.as_ref() {
            return available_proc_set.sub_proc_set_with_cores(*request);
        }

        if let Some(partitions) = self.partitions.get(name) {
            let (proc_sets, count) = partitions.iter()
                .filter_map(|proc_set| {
                    if level_requests.len() > 1 {
                        // If next level is core, do not iterate over it and do the check directly. The core level should correspond to a single proc.
                        if let Some(_name) = self.unit_partition.as_ref() {
                            proc_set.sub_proc_set_with_cores(level_requests[1].1)
                        } else {
                            self.find_resource_hierarchies_scattered(&(proc_set & available_proc_set), &level_requests[1..])
                        }
                    }else if proc_set.is_subset(&available_proc_set) {
                        Some(proc_set.clone())
                    }else {
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

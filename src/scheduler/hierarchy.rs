use crate::models::models::{ProcSet, ProcSetCoresOp};
use std::collections::HashMap;
use log::warn;

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
    unit_partition: Box<str>, // Name of a virtual unitary partition (correspond to a single u32 in ProcSet), e.g. "core"

}

impl Hierarchy {
    pub fn new(unit_partition: Box<str>) -> Self {
        Hierarchy {
            partitions: HashMap::new(),
            unit_partition,
        }
    }
    pub fn add_partition(mut self, level: Box<str>, partitions: Box<[ProcSet]>) -> Self {
        if level.as_ref() == self.unit_partition.as_ref() {
            panic!("Cannot add a partition with the same name as the unit partition: {}", self.unit_partition);
        }
        self.partitions.insert(level, partitions);
        self
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
        if name.as_ref() == self.unit_partition.as_ref() {
            return available_proc_set.sub_proc_set_with_cores(*request);
        }

        if let Some(partitions) = self.partitions.get(name) {
            let (proc_sets, count) = partitions.iter()
                .filter_map(|proc_set| {
                    if level_requests.len() > 1 {
                        // If next level is core, do not iterate over it and do the check directly. The core level should correspond to a single proc.
                        if level_requests[1].0.as_ref() == self.unit_partition.as_ref() {
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

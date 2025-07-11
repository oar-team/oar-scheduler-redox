use crate::models::models::ProcSet;
use std::collections::HashMap;

pub struct HierarchyRequest {
    pub filter: ProcSet,
    pub level_nbs: Box<[(Box<str>, u32)]> // Level name, number of resources requested at that level
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hierarchy(HashMap<Box<str>, Box<[ProcSet]>>); // Level name, partitions of that level

impl Hierarchy {
    pub fn new() -> Self {
        Hierarchy(HashMap::new())
    }
    pub fn add_partition(mut self, level: Box<str>, partitions: Box<[ProcSet]>) -> Self {
        self.0.insert(level, partitions);
        self
    }
}

impl Hierarchy {
    pub fn request(&self, available_proc_set: &ProcSet, request: &Box<[HierarchyRequest]>) -> ProcSet {
        request.into_iter().fold(ProcSet::new(), |acc, req| {
            if let Some(partition) = self.find_resource_hierarchies_scattered(&(available_proc_set & &req.filter), &req.level_nbs) {
                acc | partition
            } else {
                acc
            }
        })
    }
    pub fn find_resource_hierarchies_scattered(&self, available_proc_set: &ProcSet, level_requests: &[(Box<str>, u32)]) -> Option<ProcSet> {
        let (name, request) = &level_requests[0];
        if let Some(partitions) = self.0.get(name) {
            let (proc_sets, count) = partitions.iter()
                .filter_map(|proc_set| {
                    if level_requests.len() > 1 {
                        self.find_resource_hierarchies_scattered(&(proc_set & available_proc_set), &level_requests[1..])
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
            None
        }
    }
}

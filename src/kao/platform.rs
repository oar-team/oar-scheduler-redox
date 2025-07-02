use crate::kao::kamelot_basic::{Config, ResourceSet};
use crate::lib::models::Job;
use std::collections::HashMap;

pub trait PlatformTrait {
    fn get_resource_set(&self, config: &Config) -> &ResourceSet;
    fn get_waiting_jobs(&self) -> Vec<Job>;
    fn get_data_jobs(&self) -> Vec<Job>;
    fn get_scheduled_jobs(&self) -> Vec<Job>;
    fn save_assignments(&self);
    fn get_sum_accounting_window(&self) -> i64;
    fn get_sum_accounting_by_project(&self) -> HashMap<i32, i64>;
    fn get_sum_accounting_by_user(&self) -> HashMap<i32, i64>;
}

pub struct PlatformTest {
    resource_set: ResourceSet,
    jobs: HashMap<u32, Job>,

    pub running_jobs_ids: Vec<u32>,
    pub waiting_jobs_ids: Vec<u32>,
    pub completed_jobs_ids: Vec<u32>,
}
impl PlatformTest {
    pub fn new(resource_set: ResourceSet, jobs: Vec<Job>) -> PlatformTest {
        PlatformTest {
            resource_set,
            jobs: jobs.iter().map(|j| (j.id, j.clone())).collect(),
            running_jobs_ids: vec![],
            waiting_jobs_ids: vec![],
            completed_jobs_ids: vec![],
        }
    }
}

impl PlatformTrait for PlatformTest {
    fn get_resource_set(&self, config: &Config) -> &ResourceSet {
        &self.resource_set
    }

    fn get_waiting_jobs(&self) -> Vec<Job> {
        self.waiting_jobs_ids.iter().map(|id| self.jobs.get(id).unwrap().clone()).collect()
    }

    fn get_data_jobs(&self) -> Vec<Job> {
        vec![]
    }

    fn get_scheduled_jobs(&self) -> Vec<Job> {
        self.running_jobs_ids.iter().map(|id| self.jobs.get(id).unwrap().clone()).collect()
    }

    fn save_assignments(&self) {
        println!("Assignments to save:");
        println!("Waiting {:?}", self.waiting_jobs_ids);
        println!("Running {:?}", self.running_jobs_ids);
        println!("Completed {:?}", self.completed_jobs_ids);
    }

    fn get_sum_accounting_window(&self) -> i64 {
        todo!()
    }

    fn get_sum_accounting_by_project(&self) -> HashMap<i32, i64> {
        todo!()
    }

    fn get_sum_accounting_by_user(&self) -> HashMap<i32, i64> {
        todo!()
    }
}

use crate::models::models::Job;
use crate::scheduler::slot::ProcSet;

pub trait PlatformTrait {
    fn get_now(&self) -> i64;
    fn get_max_time(&self) -> i64;
    
    fn get_resource_set(&self) -> &ResourceSet;
    
    fn get_scheduled_jobs(&self) -> &Vec<Job>;
    fn get_waiting_jobs(&self) -> &Vec<Job>;
    
    fn set_scheduled_jobs(&mut self, jobs: Vec<Job>);
}

pub struct PlatformTest {
    resource_set: ResourceSet,
    scheduled_jobs: Vec<Job>,
    waiting_jobs: Vec<Job>,
}
impl PlatformTest {
    pub fn new(resource_set: ResourceSet, scheduled_jobs: Vec<Job>, waiting_jobs: Vec<Job>) -> PlatformTest {
        PlatformTest {
            resource_set,
            scheduled_jobs,
            waiting_jobs,
        }
    }
}

impl PlatformTrait for PlatformTest {
    fn get_now(&self) -> i64 {
        0
    }
    fn get_max_time(&self) -> i64 {
        10000
    }
    
    fn get_resource_set(&self) -> &ResourceSet {
        &self.resource_set
    }

    fn get_scheduled_jobs(&self) -> &Vec<Job> {
        &self.scheduled_jobs
    }
    fn get_waiting_jobs(&self) -> &Vec<Job> {
        &self.waiting_jobs
    }
    
    fn set_scheduled_jobs(&mut self, mut jobs: Vec<Job>) {
        self.waiting_jobs.retain(|job| !jobs.iter().any(|j| j.id == job.id));
        self.scheduled_jobs.append(&mut jobs);
    }
}


pub struct ResourceSet {
    pub default_intervals: ProcSet,
    pub available_upto: Vec<(i64, ProcSet)>,
}
impl Default for ResourceSet {
    fn default() -> ResourceSet {
        ResourceSet {
            default_intervals: ProcSet::from_iter([0..=99]),
            available_upto: vec![(150, ProcSet::from_iter([0..=49]))],
        }
    }
}

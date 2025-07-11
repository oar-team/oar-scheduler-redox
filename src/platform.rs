use crate::models::models::Job;
use crate::models::models::ProcSet;
use crate::scheduler::hierarchy::Hierarchy;

pub trait PlatformTrait {
    fn get_now(&self) -> i64;
    fn get_max_time(&self) -> i64;

    fn get_resource_set(&self) -> &ResourceSet;

    fn get_scheduled_jobs(&self) -> &Vec<Job>;
    fn get_waiting_jobs(&self) -> &Vec<Job>;

    fn set_scheduled_jobs(&mut self, jobs: Vec<Job>);
}

#[derive(Clone)]
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
        1_000_000
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

#[derive(Clone)]
pub struct ResourceSet {
    pub default_intervals: ProcSet,
    pub available_upto: Vec<(i64, ProcSet)>,
    pub hierarchy: Hierarchy,
}
impl Default for ResourceSet {
    fn default() -> ResourceSet {
        ResourceSet {
            default_intervals: ProcSet::from_iter([0..=99]),
            available_upto: vec![(150, ProcSet::from_iter([0..=49]))],
            hierarchy: Hierarchy::new()
                .add_partition("switch".into(), Box::new([ProcSet::from_iter([0..=49]), ProcSet::from_iter([50..=99])]))
                .add_partition(
                    "node".into(),
                    Box::new([
                        ProcSet::from_iter([0..=16]),
                        ProcSet::from_iter([17..=33]),
                        ProcSet::from_iter([34..=49]),
                        ProcSet::from_iter([50..=66]),
                        ProcSet::from_iter([67..=83]),
                        ProcSet::from_iter([84..=99]),
                    ]),
                )
                .add_partition("core".into(), (0..=99).map(|x| ProcSet::from_iter([x..=x])).collect::<Box<[ProcSet]>>()),
        }
    }
}

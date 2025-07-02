use crate::models::models::Job;
use crate::platform::{PlatformTest, ResourceSet};
use crate::scheduler::kamelot_basic::schedule_cycle;
use crate::scheduler::slot::ProcSet;

mod platform;
mod scheduler;
mod models;

fn main() {

    let resource_set = ResourceSet::default();
    let scheduled_jobs: Vec<Job> = vec![
        Job::new_scheduled_from_proc_set(2, 20, 55, ProcSet::from_iter([0..=9])),
        Job::new_scheduled_from_proc_set(3, 50, 70, ProcSet::from_iter([20..=24])),
    ];
    let waiting_jobs: Vec<Job> = vec![
        Job::new_from_proc_set(0, 20, ProcSet::from_iter([20..=24])),
        Job::new_from_proc_set(1, 50, ProcSet::from_iter([0..=9])),
    ];
    let platform = PlatformTest::new(resource_set, scheduled_jobs, waiting_jobs);

    let queues = vec!["default".to_string()];
    schedule_cycle(platform, queues);
}

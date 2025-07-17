use crate::models::models::{Job, ScheduledJobData};
use crate::platform::PlatformTrait;
use crate::scheduler::scheduling_basic::schedule_jobs;
use crate::scheduler::slot::SlotSet;
use std::collections::HashMap;
use std::rc::Rc;

pub fn schedule_cycle<T: PlatformTrait>(platform: &mut T, _queues: Vec<String>) -> usize {
    let now = platform.get_now();
    let max_time = platform.get_max_time();

    let mut waiting_jobs = platform.get_waiting_jobs().clone();

    if waiting_jobs.len() > 0 {
        let mut initial_slot_set = SlotSet::from_platform_config(Rc::clone(platform.get_platform_config()), now, max_time);

        // Resource availability (available_upto field) is integrated through pseudo jobs
        let mut pseudo_jobs = platform.get_platform_config().resource_set
            .available_upto
            .iter()
            .filter(|(time, _)| time < &max_time)
            .map(|(time, intervals)| {
                Job::new_scheduled(
                    0,
                    "pseudo_job".to_string(),
                    "pseudo_job".to_string(),
                    "pseudo_job".to_string(),
                    vec![],
                    vec![],
                    ScheduledJobData::new(*time + 1, max_time, intervals.clone(), 0)
                )
            })
            .collect::<Vec<Job>>();
        pseudo_jobs.sort_by_key(|j| j.begin().unwrap());
        initial_slot_set.split_slots_for_jobs_and_update_resources(&pseudo_jobs.iter().collect(), true, false, None);

        // Get already scheduled jobs advanced reservations and jobs from higher priority queues
        let mut scheduled_jobs = platform
            .get_scheduled_jobs()
            .iter()
            .collect::<Vec<&Job>>();
        scheduled_jobs.sort_by_key(|j| j.begin().unwrap());
        initial_slot_set.split_slots_for_jobs_and_update_resources(&scheduled_jobs, true, true, None);

        // Scheduling
        let mut slot_sets = HashMap::from([("default".to_string(), initial_slot_set)]);
        schedule_jobs(&mut slot_sets, &mut waiting_jobs);

        // Save assignments
        let scheduled_jobs = waiting_jobs.into_iter().filter(|j| j.is_scheduled()).collect::<Vec<Job>>();
        platform.set_scheduled_jobs(scheduled_jobs);

        return slot_sets.get("default").unwrap().slot_count();
    }
    0
}

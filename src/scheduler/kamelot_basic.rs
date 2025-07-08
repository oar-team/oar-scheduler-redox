use crate::models::models::{Job, ScheduledJobData};
use crate::platform::PlatformTrait;
use crate::scheduler::scheduling_basic::schedule_jobs_ct;
use crate::scheduler::slot::SlotSet;
use std::collections::HashMap;


pub fn schedule_cycle<T: PlatformTrait>(platform: &mut T, queues: Vec<String>, cache_enabled: bool) -> usize {
    let now = platform.get_now();
    let max_time = platform.get_max_time();

    let mut waiting_jobs = platform.get_waiting_jobs().clone();

    if waiting_jobs.len() > 0 {
        let resource_set = platform.get_resource_set();
        let mut initial_slot_set = SlotSet::from_intervals(resource_set.default_intervals.clone(), now, max_time);

        // Resource availability (available_upto field) is integrated through pseudo job
        let mut pseudo_jobs = resource_set
            .available_upto
            .iter()
            .filter(|(time, _)| time < &max_time)
            .map(|(time, intervals)| {
                ScheduledJobData::new(*time + 1, max_time, intervals.clone(), 0)
            })
            .collect::<Vec<ScheduledJobData>>();
        pseudo_jobs.sort_by_key(|j| j.begin);
        initial_slot_set.split_slots_for_jobs_and_update_resources(&pseudo_jobs, true, None);

        // Get already scheduled jobs advanced reservations and jobs from higher priority queues
        let mut scheduled_jobs = platform.get_scheduled_jobs().iter().map(|j| {
            j.scheduled_data.as_ref().expect("Platform scheduled job has no scheduled data").clone()
        }).collect::<Vec<ScheduledJobData>>();
        scheduled_jobs.sort_by_key(|j| j.begin);
        initial_slot_set.split_slots_for_jobs_and_update_resources(&scheduled_jobs, true, None);

        // Scheduling
        let mut slot_sets = HashMap::from([("default".to_string(), initial_slot_set)]);
        schedule_jobs_ct(&mut slot_sets, &mut waiting_jobs, cache_enabled);

        // Save assignments
        let scheduled_jobs = waiting_jobs.into_iter().filter(|j| j.is_scheduled()).collect::<Vec<Job>>();
        platform.set_scheduled_jobs(scheduled_jobs);

        return slot_sets.get("default").unwrap().slot_count();
    }
    0
}

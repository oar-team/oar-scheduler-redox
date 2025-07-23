use std::collections::HashMap;
use crate::models::models::Job;
use crate::platform::PlatformTrait;
use crate::scheduler::scheduling_tree::schedule_jobs;
use crate::scheduler::tree_slot::TreeSlotSet;
use std::rc::Rc;


// Returns the number of slots
pub fn schedule_cycle<T: PlatformTrait>(platform: &mut T, _queues: Vec<String>) -> usize {
    let now = platform.get_now();
    let max_time = platform.get_max_time();

    let mut waiting_jobs = platform.get_waiting_jobs().clone();

    if waiting_jobs.len() > 0 {
        let initial_slot_set = TreeSlotSet::from_platform_config(Rc::clone(platform.get_platform_config()), now, max_time);

        // Not supported for now
        /*// Resource availability (available_upto field) is integrated through pseudo job
        let mut pseudo_jobs = resource_set
            .available_upto
            .iter()
            .filter(|(time, _)| time < &max_time)
            .map(|(time, intervals)| Job::new_scheduled_from_proc_set(0, *time+1, max_time, intervals.clone()))
            .collect::<Vec<Job>>();
        pseudo_jobs.sort_by_key(|j| j.begin);
        initial_slot_set.split_slots_for_jobs_and_update_resources(&pseudo_jobs, true, None);

        // Get already scheduled jobs advanced reservations and jobs from higher priority queues
        let scheduled_jobs = platform.get_scheduled_jobs();
        initial_slot_set.split_slots_for_jobs_and_update_resources(&scheduled_jobs, true, None);*/

        // Scheduling
        let mut slot_sets = HashMap::from([("default".to_string(), initial_slot_set)]);
        schedule_jobs(&mut slot_sets, &mut waiting_jobs);

        // Save assignments
        let scheduled_jobs = waiting_jobs.into_iter().filter(|j| j.is_scheduled()).collect::<Vec<Job>>();
        platform.set_scheduled_jobs(scheduled_jobs);

        slot_sets.get("default").unwrap().print_tree();

        return slot_sets.get("default").unwrap().count_leaves();
    }
    0
}

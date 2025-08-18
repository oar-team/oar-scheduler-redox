use crate::models::{Job, JobAssignment, JobBuilder};
use crate::platform::PlatformTrait;
use crate::scheduler::scheduling::{get_job_slot_set_name, schedule_jobs};
use crate::scheduler::slot::SlotSet;
use indexmap::IndexMap;
use std::collections::HashMap;
use std::rc::Rc;

pub fn schedule_cycle<T: PlatformTrait>(platform: &mut T, queues: Vec<String>) -> usize {
    let now = platform.get_now();
    let max_time = platform.get_max_time();

    let platform_config = platform.get_platform_config();
    let mut waiting_jobs = platform.get_waiting_jobs().clone();

    if waiting_jobs.len() > 0 {
        let mut initial_slot_set = SlotSet::from_platform_config(Rc::clone(platform_config), now, max_time);

        // Resource availability (available_upto field) is integrated through pseudo jobs
        let mut pseudo_jobs = platform_config
            .resource_set
            .available_upto
            .iter()
            .filter(|(time, _)| time < &max_time)
            .map(|(time, intervals)| {
                JobBuilder::new(0)
                    .name("pseudo_job".into())
                    .user("pseudo_job".into())
                    .project("pseudo_job".into())
                    .queue("pseudo_job".into())
                    .assign(JobAssignment::new(*time + 1, max_time, intervals.clone(), 0))
                    .build()
            })
            .collect::<Vec<Job>>();
        pseudo_jobs.sort_by_key(|j| j.begin().unwrap());
        initial_slot_set.split_slots_for_jobs_and_update_resources(&pseudo_jobs.iter().collect(), false, true, None);

        // Initialize slot sets map
        let mut slot_sets = HashMap::from([("default".into(), initial_slot_set)]);

        // Place already scheduled jobs, advanced reservations and jobs from higher priority queues
        let mut scheduled_jobs = platform.get_scheduled_jobs().iter().collect::<Vec<&Job>>();
        scheduled_jobs.sort_by_key(|j| j.begin().unwrap());
        if queues.len() != 1 || queues[0] != "besteffort" {
            // Unless scheduling best-effort queue, not taking into account the existing best-effort jobs.
            scheduled_jobs.retain(|j| j.queue.as_ref() != "besteffort");
        }
        let mut slot_set_jobs: HashMap<Box<str>, Vec<&Job>> = HashMap::new();
        scheduled_jobs.into_iter().for_each(|job| {
            let slot_set_name = get_job_slot_set_name(job);
            slot_set_jobs
                .entry(slot_set_name)
                .and_modify(|vec| {
                    vec.push(job);
                })
                .or_insert(vec![job]);
        });
        for (slot_set_name, jobs) in slot_set_jobs {
            let slot_set = slot_sets.get_mut(&slot_set_name).expect(format!("Slot set {} does not exist", slot_set_name).as_str());
            slot_set.split_slots_for_jobs_and_update_resources(&jobs, true, true, None);
        }

        // Scheduling
        schedule_jobs(&mut slot_sets, &mut waiting_jobs);

        // Save assignments
        let assigned_jobs = waiting_jobs
            .into_iter()
            .filter(|(_id, job)| job.assignment.is_some())
            .collect::<IndexMap<u32, Job>>();
        platform.save_assignments(assigned_jobs);

        return slot_sets.get("default").unwrap().slot_count();
    }
    0
}

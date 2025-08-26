use crate::models::{Job, JobAssignment, JobBuilder, ProcSet};
use crate::platform::PlatformTrait;
use crate::scheduler::scheduling::schedule_jobs;
use crate::scheduler::slotset::SlotSet;
use indexmap::IndexMap;
use log::info;
use std::collections::HashMap;
use std::rc::Rc;

pub fn schedule_cycle<T: PlatformTrait>(platform: &mut T, queues: Vec<String>) -> usize {
    // Insert the already-scheduled besteffort jobs into the slot sets only if scheduling this queue.
    let allow_besteffort = queues.len() == 1 && queues[0] == "besteffort";
    let mut slot_sets = init_slot_sets(platform, allow_besteffort);

    internal_schedule_cycle(platform, &mut slot_sets, queues)
}

pub fn internal_schedule_cycle<T: PlatformTrait>(platform: &mut T, slot_sets: &mut HashMap<Box<str>, SlotSet>, queues: Vec<String>) -> usize {
    let platform_config = platform.get_platform_config();
    let mut waiting_jobs = platform.get_waiting_jobs();

    {
        info!(
            "Internal scheduling {} jobs ({} scheduled jobs). Queues: {:?}",
            waiting_jobs.len(),
            platform.get_scheduled_jobs().len(),
            queues
        );
        info!("ResourceSet: {:?}", platform_config.resource_set);
        info!(
            "job_security_time: {} | hour_size: {} | cache_enabled: {}",
            platform_config.job_security_time, platform_config.hour_size, platform_config.cache_enabled
        );
        // waiting_jobs.values().for_each(|j| {
        //     info!("{:?}", j);
        // });
    }

    if waiting_jobs.len() > 0 {
        // Scheduling
        schedule_jobs(slot_sets, &mut waiting_jobs);

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

/// Initialize slot sets map with the default slot set initialized from the platform configuration.
pub fn init_slot_sets<P>(platform: &P, allow_besteffort: bool) -> HashMap<Box<str>, SlotSet>
where
    P: PlatformTrait,
{
    let now = platform.get_now();
    let max_time = platform.get_max_time();
    let platform_config = platform.get_platform_config();

    let mut initial_slot_set = SlotSet::from_platform_config(Rc::clone(platform_config), now, max_time);

    // Resource availability (available_upto field) is integrated through pseudo jobs
    slot_set_integrate_resource_availability(max_time, &platform_config.resource_set.available_upto, &mut initial_slot_set);
    // Initialize slot sets map
    let mut slot_sets = HashMap::from([("default".into(), initial_slot_set)]);
    // Place already scheduled jobs, advanced reservations and jobs from higher priority queues
    add_already_scheduled_jobs_to_slot_set(&mut slot_sets, platform, allow_besteffort, true);

    slot_sets
}

/// Create pseudo jobs at the end of the slot_set
/// allowing to restrict the resource availability until times defined in `available_upto`.
fn slot_set_integrate_resource_availability(max_time: i64, available_upto: &Vec<(i64, ProcSet)>, slot_set: &mut SlotSet) {
    let mut pseudo_jobs = available_upto
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
    slot_set.split_slots_for_jobs_and_update_resources(&pseudo_jobs.iter().collect(), false, true, None);
}

/// Inserts the scheduled_jobs of the platform into the slot_sets.
/// If `allow_besteffort` is true, the besteffort jobs are inserted.
/// If `allow_other` is true, the non-besteffort jobs are inserted.
pub fn add_already_scheduled_jobs_to_slot_set<T>(slot_sets: &mut HashMap<Box<str>, SlotSet>, platform: &T, allow_besteffort: bool, allow_other: bool)
where
    T: PlatformTrait,
{
    let mut scheduled_jobs = platform.get_scheduled_jobs().iter().collect::<Vec<&Job>>();
    scheduled_jobs.sort_by_key(|j| j.begin().unwrap());
    if allow_besteffort && !allow_other {
        // Retain only besteffort jobs
        scheduled_jobs.retain(|j| j.queue.as_ref() == "besteffort");
    }else if !allow_besteffort && allow_other {
        // Retain only non-besteffort jobs
        scheduled_jobs.retain(|j| j.queue.as_ref() != "besteffort");
    }else if !allow_besteffort && !allow_other {
        return;
    }
    let mut slot_set_jobs: HashMap<Box<str>, Vec<&Job>> = HashMap::new();
    scheduled_jobs.into_iter().for_each(|job| {
        let slot_set_name = job.slot_set_name();
        slot_set_jobs
            .entry(slot_set_name)
            .and_modify(|vec| {
                vec.push(job);
            })
            .or_insert(vec![job]);
    });
    for (slot_set_name, jobs) in slot_set_jobs {
        let slot_set = slot_sets
            .get_mut(&slot_set_name)
            .expect(format!("Slot set {} does not exist", slot_set_name).as_str());
        slot_set.split_slots_for_jobs_and_update_resources(&jobs, true, true, None);
    }
}

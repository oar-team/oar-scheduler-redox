use crate::model::job::{Job, JobAssignment, JobBuilder, ProcSet};
use crate::platform::PlatformTrait;
use crate::scheduler::scheduling::schedule_jobs;
use crate::scheduler::slotset::SlotSet;
use crate::scheduler::sorting::sort_jobs;
use indexmap::IndexMap;
use log::{debug, info};
use std::collections::HashMap;
use std::rc::Rc;

pub fn schedule_cycle<T: PlatformTrait>(platform: &mut T, queues: &Vec<String>) -> usize {
    // Insert the already-scheduled besteffort jobs into the slot sets only if scheduling this queue.
    let allow_besteffort = queues.len() == 1 && queues[0] == "besteffort";
    let (mut slot_sets, _besteffort_jobs) = init_slot_sets(platform, allow_besteffort);

    internal_schedule_cycle(platform, &mut slot_sets, queues)
}

pub fn internal_schedule_cycle<T: PlatformTrait>(platform: &mut T, slot_sets: &mut HashMap<Box<str>, SlotSet>, queues: &Vec<String>) -> usize {
    let _platform_config = platform.get_platform_config();
    let mut waiting_jobs = platform.get_waiting_jobs(queues.to_vec());

    {
        // info!(
        //     "Internal scheduling {} jobs ({} scheduled jobs). Queues: {:?}",
        //     waiting_jobs.len(),
        //     platform.get_scheduled_jobs().len(),
        //     queues
        // );
        // info!("ResourceSet: {:?}", platform_config.resource_set);
        // info!(
        //     "job_security_time: {} | cache_enabled: {}",
        //     _platform_config.job_security_time, _platform_config.cache_enabled
        // );
        // waiting_jobs.values().for_each(|j| {
        //     info!("{:?}", j);
        // });
    }

    if waiting_jobs.len() > 0 {
        // Sorting
        sort_jobs(platform, queues, &mut waiting_jobs);

        // Limit the number of jobs scheduled per round for this group of queues.
        limit_jobs_per_round(platform, queues, &mut waiting_jobs);

        // Scheduling
        schedule_jobs(slot_sets, &mut waiting_jobs);

        // Save assignments
        let assigned_jobs = waiting_jobs
            .into_iter()
            .filter(|(_id, job)| job.assignment.is_some())
            .collect::<IndexMap<i64, Job>>();
        debug!(
            "Kamelot internal saving {} assigned job(s): {:?}",
            assigned_jobs.len(),
            assigned_jobs.keys().collect::<Vec<_>>()
        );
        platform.save_assignments(assigned_jobs);

        return slot_sets.get("default").unwrap().slot_count();
    }
    0
}

/// Limit the number of jobs scheduled in this round for this group of queues
/// (`MAX_JOB_PER_QUEUES_GROUP_SCHEDULING_ROUND`).
///
/// The kept jobs are the first ones of the scheduling order, i.e. the ones
/// [`sort_jobs`] put in front of `waiting_jobs`; the others are left untouched
/// in the `Waiting` state and are candidates for the next scheduling round.
/// Mirrors the truncation done in `oar.kao.kamelot.internal_schedule_cycle`.
fn limit_jobs_per_round<T: PlatformTrait>(platform: &T, queues: &[String], waiting_jobs: &mut IndexMap<i64, Job>) {
    let Some(max_jobs) = platform.get_platform_config().config.max_job_per_queues_group() else {
        return;
    };
    if waiting_jobs.len() > max_jobs {
        info!(
            "MAX_JOB_PER_QUEUES_GROUP_SCHEDULING_ROUND={} reached for queues {:?}: scheduling {} of {} waiting jobs",
            max_jobs,
            queues,
            max_jobs,
            waiting_jobs.len()
        );
        waiting_jobs.truncate(max_jobs);
    }
}

/// Initialize slot sets map with the `default` SlotSet initialized with resource availability and already scheduled jobs.
/// Returns the slot sets map and a Vec of already scheduled besteffort jobs inserted in the slotset.
pub fn init_slot_sets<P>(platform: &P, allow_besteffort: bool) -> (HashMap<Box<str>, SlotSet>, Vec<Job>)
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
    let besteffort_jobs = add_already_scheduled_jobs_to_slot_set(&mut slot_sets, platform, allow_besteffort, true);

    (slot_sets, besteffort_jobs)
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
/// Returns a Vec with all the besteffort jobs that were added to the slot sets, ordered by start time.
pub fn add_already_scheduled_jobs_to_slot_set<T>(slot_sets: &mut HashMap<Box<str>, SlotSet>, platform: &T, allow_besteffort: bool, allow_other: bool) -> Vec<Job>
where
    T: PlatformTrait,
{
    let mut scheduled_jobs = platform.get_scheduled_jobs();
    // let mut scheduled_jobs = scheduled_jobs.iter().collect::<Vec<&Job>>();
    scheduled_jobs.sort_by_key(|j| j.begin().unwrap());
    if allow_besteffort && !allow_other {
        // Retain only besteffort jobs
        scheduled_jobs.retain(|j| j.queue.as_ref() == "besteffort");
    } else if !allow_besteffort && allow_other {
        // Retain only non-besteffort jobs
        scheduled_jobs.retain(|j| j.queue.as_ref() != "besteffort");
    } else if !allow_besteffort && !allow_other {
        return vec![];
    }
    let mut slot_set_jobs: HashMap<Box<str>, Vec<&Job>> = HashMap::new();
    scheduled_jobs.iter().for_each(|job| {
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
    if !allow_besteffort {
        return vec![];
    }
    if allow_other {
        scheduled_jobs.retain(|j| j.queue.as_ref() == "besteffort");
    }
    scheduled_jobs
}

use crate::hooks::get_hooks_manager;
use crate::model::job::{Job, JobAssignment, JobBuilder, Moldable, ProcSet, ProcSetCoresOp};
use crate::scheduler::quotas;
use crate::scheduler::slot::Slot;
use crate::scheduler::slotset::SlotSet;
use auto_bench_fct::auto_bench_fct_hy;
use indexmap::IndexMap;
use log::{error, info, warn};
use std::cmp::max;
use std::collections::HashMap;

/// Schedule loop with support for jobs container - can be recursive
pub fn schedule_jobs(slot_sets: &mut HashMap<Box<str>, SlotSet>, waiting_jobs: &mut IndexMap<i64, Job>) {
    let job_ids = waiting_jobs.keys().into_iter().cloned().collect::<Box<[i64]>>();
    for job_id in job_ids {
        // Check job dependencies
        let dependencies = waiting_jobs.get(&job_id).unwrap().dependencies.clone();
        let mut min_begin: Option<i64> = None;
        if !dependencies.iter().all(|(dep_job_id, dep_state, dep_exit_code)| {
            if dep_state.as_ref() == "Error" {
                info!(
                    "Job {} has a dependency on job {} which is in error state, ignoring dependency.",
                    job_id, dep_job_id
                );
                return true;
            }
            if dep_state.as_ref() == "Waiting" {
                if let Some(dep_job) = waiting_jobs.get(dep_job_id) {
                    if let Some(dep_assignment) = &dep_job.assignment.as_ref() {
                        min_begin = Some(min_begin.map_or(dep_assignment.end + 1, |min| min.max(dep_assignment.end + 1)));
                        return true;
                    } else {
                        warn!(
                            "Job {} has a dependency on job {} which has not been scheduled. Please review the sorting algorithm and check that job {} has been scheduled correctly.",
                            job_id, dep_job_id, dep_job_id
                        );
                    }
                }
                return false;
            }
            if dep_state.as_ref() == "Terminated" && (*dep_exit_code == Some(0) || *dep_exit_code == None) {
                return true;
            }
            false
        }) {
            info!("Job {} has unsatisfied dependencies and can't be scheduled.", job_id);
            continue;
        }

        // Schedule job
        let job = waiting_jobs.get_mut(&job_id).unwrap();
        if let Some(slot_set) = get_job_slot_set(slot_sets, job) {
            if !get_hooks_manager().hook_assign(slot_set, job, min_begin) {
                schedule_job(slot_set, job, min_begin);
            }

            // Manage container jobs
            if job.types.contains_key(&Box::from("container")) {
                update_container_job_slot_set(slot_sets, job);
            }
        }
    }
}

/// According to a Job’s resources and a `SlotSet`, find the time and the resources to launch a job.
/// This function supports the moldable jobs. In case of multiple moldable jobs corresponding to the request,
/// it selects the first to finish.
///
/// This function has two side effects.
///   - Assign the results directly to the `job` (such as start_time, resources, etc.)
///   - Split the slot_set to reflect the new allocation
#[auto_bench_fct_hy]
pub fn schedule_job(slotset: &mut SlotSet, job: &mut Job, min_begin: Option<i64>) {
    let mut chosen_slot_id_left = None;
    let mut chosen_begin = None;
    let mut chosen_end = None;
    let mut chosen_moldable_index = None;
    let mut chosen_proc_set = None;

    let mut total_quotas_hit_count = 0;

    job.moldables.iter().enumerate().for_each(|(i, moldable)| {
        if let Some((slot_id_left, _slot_id_right, proc_set, quotas_hit_count)) = find_slots_for_moldable(slotset, job, moldable, min_begin) {
            total_quotas_hit_count += quotas_hit_count;
            let begin = slotset.get_slot(slot_id_left).unwrap().begin();
            let end = begin + max(0, moldable.walltime - 1);

            if chosen_end.is_none() || end < chosen_end.unwrap() {
                chosen_slot_id_left = Some(slot_id_left);
                chosen_begin = Some(begin);
                chosen_end = Some(end);
                chosen_moldable_index = Some(i);
                chosen_proc_set = Some(proc_set);
            }
        }
    });

    if let Some(chosen_moldable_index) = chosen_moldable_index {
        job.assignment = Some(JobAssignment::new(
            chosen_begin.unwrap(),
            chosen_end.unwrap(),
            chosen_proc_set.clone().unwrap(),
            chosen_moldable_index,
        ));
        job.quotas_hit_count = total_quotas_hit_count;
        slotset.split_slots_for_job_and_update_resources(&job, true, true, chosen_slot_id_left);
    } else {
        warn!("Warning: no node found for job {:?}", job);
        //slotset.to_table().printstd();
    }
}

/// Returns left slot id, right slot id, proc_set and quotas hit count.
#[auto_bench_fct_hy]
pub fn find_slots_for_moldable(slotset: &mut SlotSet, job: &Job, moldable: &Moldable, min_begin: Option<i64>) -> Option<(i32, i32, ProcSet, u32)> {
    let mut iter = slotset.iter();
    // Start at cache if available
    if job.can_use_cache() {
        if let Some(cache_first_slot) = slotset.get_cache_first_slot(moldable) {
            iter = iter.start_at(cache_first_slot);
        }
    }
    // Start at the minimum begin time if specified
    let cache_begin = iter.peek().map(|s| s.begin()).unwrap_or(slotset.begin());
    if let Some(min_begin) = min_begin {
        if min_begin > cache_begin {
            if let Some(start_slot) = slotset.slot_at(min_begin, iter.peek().map(|s| s.id())) {
                // If min_begin is not the beginning of a slot, we need to split the current slot at min_begin
                // (can occur if the job is not in the same slot set as its dependencies).
                if start_slot.begin() < min_begin {
                    let (_left_slot_id, right_slot_id) = slotset.find_and_split_at(min_begin, true);
                    iter = slotset.iter().start_at(right_slot_id);
                } else {
                    iter = iter.start_at(start_slot.id());
                }
            } else if min_begin > slotset.end() {
                return None; // No slots available after the minimum begin time
            }
        }
    }

    // A cache entry is set to the first slot available before the quotas check, so the cache key does not include the job user, project, types or queue.
    let mut cache_first_slot = None;

    let mut quotas_hit_count = 0;

    let mut count = 0;
    let res = iter.with_width(moldable.walltime).find_map(|(left_slot, right_slot)| {
        count += 1;
        let left_slot_id = left_slot.id();
        let right_slot_id = right_slot.id();
        let left_slot_begin = left_slot.begin();

        let empty: Box<str> = "".into();
        let (ts_user_name, ts_job_name) = job.time_sharing.as_ref().map_or((None, None), |_| {
            (Some(job.user.as_ref().unwrap_or(&empty)), Some(job.name.as_ref().unwrap_or(&empty)))
        });
        let available_resources = slotset.intersect_slots_intervals(left_slot_id, right_slot_id, ts_user_name, ts_job_name, &job.placeholder);

        // Finding resources according to hook or hierarchy request
        {
            if let Some(res) = get_hooks_manager().hook_find(slotset, job, moldable, min_begin, available_resources.clone()) {
                res
            } else {
                slotset
                    .get_platform_config()
                    .resource_set
                    .hierarchy
                    .request(&available_resources, &moldable.requests)
            }
        }
            .and_then(|proc_set| {
                if cache_first_slot.is_none() {
                    cache_first_slot = Some(left_slot.id());
                }

                // Checking quotas
            if slotset.get_platform_config().quotas_config.enabled && !job.no_quotas {
                if let Some(calendar) = &slotset.get_platform_config().quotas_config.calendar {
                    if left_slot_begin + moldable.walltime - 1 > slotset.begin() + calendar.quotas_window_time_limit() {
                        warn!(
                            "Job {} cannot be scheduled: no slots available within the quotas time limit ({} seconds).",
                            job.id,
                            calendar.quotas_window_time_limit()
                        );
                        return None;
                    }
                }
                let slots = slotset.iter().between(left_slot_id, right_slot_id);
                let end = left_slot_begin + moldable.walltime - 1;
                if let Some((msg, rule, limit)) = quotas::check_slots_quotas(slots, job, left_slot_begin, end, proc_set.core_count()) {
                    info!(
                        "Quotas limitation reached for job {}: {}, rule: {:?}, limit: {}",
                        job.id, msg, rule, limit
                    );
                    quotas_hit_count += 1;
                    return None; // Skip this slot if quotas check fails
                }
            }
                Some((left_slot_id, right_slot_id, proc_set, quotas_hit_count))
            })
    });

    if job.can_set_cache() && slotset.get_platform_config().config.cache_enabled {
        if let Some(cache_first_slot_id) = cache_first_slot {
            slotset.insert_cache_entry(moldable.cache_key.clone(), cache_first_slot_id);
        }
    }

    res
}

/// Returns the slot set for a job using get_job_slot_set_name.
pub fn get_job_slot_set<'s>(slotsets: &'s mut HashMap<Box<str>, SlotSet>, job: &Job) -> Option<&'s mut SlotSet> {
    let slot_set_name = job.slot_set_name();
    if !slotsets.contains_key(&slot_set_name) {
        error!(
            "Job {} can't be scheduled, slot set {} is missing. Skip it for this round.",
            job.id, slot_set_name
        );
        return None;
    }
    Some(slotsets.get_mut(&slot_set_name).unwrap())
}

/// Creates or updates the child slot set of a container job.
/// The child slot set is named after the job's "container" type, or defaults to the job ID.
/// Support having multiple container jobs with the same children slot set.
pub fn update_container_job_slot_set(slotsets: &mut HashMap<Box<str>, SlotSet>, job: &Job) {
    assert!(job.types.contains_key("container"));

    let default_slot_set = slotsets.get(&Box::from("default")).expect("Default SlotSet not found");

    let inner_slot_set_name = job
        .types
        .get(&Box::from("container"))
        .map(|name| name.clone())
        .unwrap()
        .unwrap_or(format!("{}", job.id).into_boxed_str());

    if let Some(assignment) = &job.assignment {
        let platform_config = default_slot_set.get_platform_config().clone();
        if !slotsets.contains_key(&inner_slot_set_name) {
            // Create a new slot set for the inner jobs.
            let inner_slot = Slot::new(
                platform_config.clone(),
                1,
                None,
                None,
                default_slot_set.begin(),
                default_slot_set.end(),
                ProcSet::new(),
                None,
            );
            slotsets.insert(inner_slot_set_name.clone(), SlotSet::from_slot(inner_slot));
        }
        // Increment the resources of the slot set using a pseudo job.
        let pseudo_job = JobBuilder::new(0)
            .name_opt(job.name.clone())
            .user_opt(job.user.clone())
            // .time_sharing_opt(job.time_sharing.clone()) Do not apply the time-sharing to the available slots of the children slot set
            // .placeholder(job.placeholder.clone()) Do not apply the placeholder to the available slots of the children slot set
            .assign(JobAssignment::new(
                assignment.begin,
                assignment.end - platform_config.config.scheduler_job_security_time, // Removing the security time added by get_data_jobs.
                assignment.resources.clone(),
                0,
            ))
            .build();
        slotsets
            .get_mut(&inner_slot_set_name)
            .unwrap()
            .split_slots_for_job_and_update_resources(&pseudo_job, false, false, None);
    }
}

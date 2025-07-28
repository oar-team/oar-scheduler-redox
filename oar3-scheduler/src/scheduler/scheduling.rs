use crate::models::{Job, Moldable, ProcSet, ProcSetCoresOp, ScheduledJobData};
use crate::scheduler::quotas;
use crate::scheduler::slot::SlotSet;
use auto_bench_fct::auto_bench_fct_hy;
use log::warn;
use std::cmp::max;
use std::collections::HashMap;

/// Schedule loop with support for jobs container - can be recursive
pub fn schedule_jobs(slot_sets: &mut HashMap<String, SlotSet>, waiting_jobs: &mut Vec<Job>) {
    waiting_jobs.into_iter().for_each(|job| {
        let slot_set_name = "default".to_string();

        let slot_set = slot_sets.get_mut(&slot_set_name).expect("SlotSet not found");
        schedule_job(slot_set, job);
    });
}

/// According to a Job’s resources and a `SlotSet`, find the time and the resources to launch a job.
/// This function supports the moldable jobs. In case of multiple moldable jobs corresponding to the request,
/// it selects the first to finish.
///
/// This function has two side effects.
///   - Assign the results directly to the `job` (such as start_time, resources, etc.)
///   - Split the slot_set to reflect the new allocation
#[auto_bench_fct_hy]
pub fn schedule_job(slot_set: &mut SlotSet, job: &mut Job) {
    let mut chosen_slot_id_left = None;
    let mut chosen_begin = None;
    let mut chosen_end = None;
    let mut chosen_moldable_index = None;
    let mut chosen_proc_set = None;

    let mut total_quotas_hit_count = 0;

    job.moldables.iter().enumerate().for_each(|(i, moldable)| {
        if let Some((slot_id_left, _slot_id_right, proc_set, quotas_hit_count)) = find_slots_for_moldable(slot_set, job, moldable) {
            total_quotas_hit_count += quotas_hit_count;
            let begin = slot_set.get_slot(slot_id_left).unwrap().begin();
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
        job.scheduled_data = Some(ScheduledJobData::new(
            chosen_begin.unwrap(),
            chosen_end.unwrap(),
            chosen_proc_set.unwrap(),
            chosen_moldable_index,
        ));
        job.quotas_hit_count = total_quotas_hit_count;
        slot_set.split_slots_for_job_and_update_resources(&job, true, chosen_slot_id_left);
    } else {
        warn!("Warning: no node found for job {:?}", job);
        slot_set.to_table().printstd();
    }
}

/// Returns left slot id, right slot id, proc_set and quotas hit count.
#[auto_bench_fct_hy]
pub fn find_slots_for_moldable(slot_set: &mut SlotSet, job: &Job, moldable: &Moldable) -> Option<(i32, i32, ProcSet, u32)> {
    let mut iter = slot_set.iter();
    if job.time_sharing.is_none() && let Some(cache_first_slot) = slot_set.get_cache_first_slot(moldable) {
        iter = iter.start_at(cache_first_slot);
    }

    // A cache entry is set to the first slot available before the quotas check, so the cache key does not include the job user, project, types or queue.
    let mut cache_first_slot = None;

    let mut quotas_hit_count = 0;

    let mut count = 0;
    let res = iter.with_width(moldable.walltime).find_map(|(left_slot, right_slot)| {
        count += 1;

        let available_resources = if job.time_sharing.is_some() {
            slot_set.intersect_slots_intervals_with_time_sharing(left_slot.id(), right_slot.id(), &job.user, &job.name)
        }else {
            slot_set.intersect_slots_intervals(left_slot.id(), right_slot.id())
        };

        // Finding resources according to hierarchy request
        slot_set
            .get_platform_config()
            .resource_set
            .hierarchy
            .request(&available_resources, &moldable.requests)
            .map(|proc_set| (left_slot.id(), right_slot.id(), proc_set))
            .and_then(|result| {
                if cache_first_slot.is_none() {
                    cache_first_slot = Some(left_slot.id());
                }

                // Checking quotas
                if slot_set.get_platform_config().quotas_config.enabled {
                    let slots = slot_set.iter().between(left_slot.id(), right_slot.id()).collect::<Vec<_>>();
                    if let Some((_msg, _rule, _limit)) = quotas::check_slots_quotas(slots, job, result.2.core_count()) {
                        //info!("Quotas limitation reached for job {}: {}, rule: {:?}, limit: {}", job.id, msg, rule, limit);
                        quotas_hit_count += 1;
                        return None; // Skip this slot if quotas check fails
                    }
                }
                Some((result.0, result.1, result.2, quotas_hit_count))
            })
    });

    if job.time_sharing.is_none() && slot_set.get_platform_config().cache_enabled {
        if let Some(cache_first_slot_id) = cache_first_slot {
            slot_set.insert_cache_entry(moldable.get_cache_key(), cache_first_slot_id);
        }
    }

    res
}

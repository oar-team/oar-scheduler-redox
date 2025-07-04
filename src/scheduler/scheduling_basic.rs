use crate::models::models::{Job, Moldable};
use crate::scheduler::slot::SlotSet;
use std::cmp::max;
use std::collections::HashMap;

/// Schedule loop with support for jobs container - can be recursive
pub fn schedule_jobs_ct(slot_sets: &mut HashMap<String, SlotSet>, waiting_jobs: &mut Vec<Job>) {
    waiting_jobs.into_iter().for_each(|job| {
        let slot_set_name = "default".to_string();

        let slot_set = slot_sets.get_mut(&slot_set_name).expect("SlotSet not found");
        assign_resources_mld_job_split_slots(slot_set, job);
    });
    // println!("SlotSet after scheduling jobs: ");
    // slot_sets.get("default").unwrap().to_table().printstd();
}

/// According to a Job’s resources and a `SlotSet`, find the time and the resources to launch a job.
/// This function supports the moldable jobs. In case of multiple moldable jobs corresponding to the request,
/// it selects the first to finish.
///
/// This function has two side effects.
///   - Assign the results directly to the `job` (such as start_time, resources, etc.)
///   - Split the slot_set to reflect the new allocation
pub fn assign_resources_mld_job_split_slots(slot_set: &mut SlotSet, job: &mut Job) {
    let mut chosen_slot_id_left = None;
    let mut chosen_begin = None;
    let mut chosen_end = None;
    let mut chosen_moldable_index = None;

    job.moldables.iter().enumerate().for_each(|(i, moldable)| {

        if let Some((slot_id_left, _slot_id_right)) = find_first_suitable_contiguous_slots(slot_set, moldable) {
            let begin = slot_set.get_slot(slot_id_left).unwrap().begin();
            let end = begin + max(0, moldable.walltime - 1);

            if chosen_end.is_none() || end < chosen_end.unwrap() {
                chosen_slot_id_left = Some(slot_id_left);
                chosen_begin = Some(begin);
                chosen_end = Some(end);
                chosen_moldable_index = Some(i);
            }
        }
    });

    // if let Some(chosen_moldable_index) = chosen_moldable_index {
    //     slot_set.insert_cache_entry(job.moldables.get(chosen_moldable_index).unwrap().get_cache_key(), chosen_slot_id_left.unwrap());
    // }
    job.begin = chosen_begin;
    job.end = chosen_end;
    job.chosen_moldable_index = chosen_moldable_index;
    if chosen_slot_id_left.is_some() {
        slot_set.split_slots_for_job_and_update_resources(job, true, chosen_slot_id_left);
    }
}

pub fn find_first_suitable_contiguous_slots(slot_set: &SlotSet, moldable: &Moldable) -> Option<(i32, i32)> {
    let mut iter = slot_set.iter();
    // if let Some(cache_first_slot) = slot_set.get_cache_first_slot(moldable) {
    //     iter = iter.start_at(cache_first_slot);
    // }
    let mut count = 0;
    let res =iter.with_width(moldable.walltime).find(|(left_slot, right_slot)| {
        count += 1;
        let available_resources = slot_set.intersect_slots_intervals(left_slot.id(), right_slot.id());
        moldable.proc_set.is_subset(&available_resources)
    }).map(|(left_slot, right_slot)| {
        (left_slot.id(), right_slot.id())
    });
    println!("Found slots for moldable visiting {} slots", count);
    res
}

use crate::models::models::Job;
use crate::scheduler::tree_slotset::TreeSlotSet;
use std::cmp::max;
use std::collections::HashMap;

/// Schedule loop with support for jobs container - can be recursive
pub fn schedule_jobs_ct(slot_sets: &mut HashMap<String, TreeSlotSet>, waiting_jobs: &mut Vec<Job>) {
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
pub fn assign_resources_mld_job_split_slots(slot_set: &mut TreeSlotSet, job: &mut Job) {
    let mut chosen_node_id_left = None;
    let mut chosen_begin = None;
    let mut chosen_end = None;
    let mut chosen_moldable_index = None;

    job.moldables.iter().enumerate().for_each(|(i, moldable)| {

        if let Some(tree_node) = slot_set.find_node_for_moldable(moldable) {
            let begin = tree_node.begin();
            let end = begin + max(0, moldable.walltime - 1);

            if chosen_end.is_none() || end < chosen_end.unwrap() {
                chosen_node_id_left = Some(tree_node.node_id());
                chosen_begin = Some(begin);
                chosen_end = Some(end);
                chosen_moldable_index = Some(i);
            }
        }
    });

    job.begin = chosen_begin;
    job.end = chosen_end;
    job.chosen_moldable_index = chosen_moldable_index;
    if let Some(node_id) = chosen_node_id_left {
        slot_set.claim_node_for_moldable(node_id, job.moldables.get(chosen_moldable_index.unwrap()).unwrap());
    }else {
        println!("Warning: no node found for job {:?}", job);
        slot_set.to_table(true).printstd();
    }
}

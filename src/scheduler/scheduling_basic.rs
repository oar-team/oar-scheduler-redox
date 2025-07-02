use crate::models::models::Job;
use crate::scheduler::slot::SlotSet;

pub fn schedule_jobs_ct(slot_set: &mut SlotSet, waiting_jobs: &Vec<Job>) {
    waiting_jobs.into_iter().for_each(|job| {
        assign_resources_mld_job_split_slots(slot_set, job);
    });
}

pub fn assign_resources_mld_job_split_slots(slot_set: &mut SlotSet, job: &Job) {
    
}
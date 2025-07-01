use std::collections::HashMap;
use crate::kao::slot::{ProcSet, SlotSet, MAX_TIME};
use crate::lib::models::Job;
use std::time::UNIX_EPOCH;

pub struct Config {}

pub struct Platform {}
impl Platform {
    pub fn get_waiting_jobs(&self) -> Vec<Job> {
        vec![
            Job::new(0, 1751375836 + 3600 * 24 * 7, 3600*2, ProcSet::from_iter([0..=49])),
            Job::new(1, 1751375836 + 3600 * 24 * 7, 3600*4, ProcSet::from_iter([0..=49])),
        ]
    }
    pub fn get_scheduled_jobs(&self) -> Vec<Job> {
        vec![
            Job::new(3, 1751375836 + 3600 * 24 * 6, 3600 * 6, ProcSet::from_iter([20..=24])),
            Job::new(2, 1751375836 + 3600 * 24 * 8, 3600 * 24, ProcSet::from_iter([0..=9])),
        ]
    }
    pub fn resource_set(&self, config: Config) -> ResourceSet {
        ResourceSet::default()
    }
}

pub struct ResourceSet {
    pub default_intervals: ProcSet,
    pub nb_resources_all: u32,
    pub nb_absent: u32,
    pub nb_resources_not_dead: u32,
    pub nb_resources_default_not_dead: u32,
    pub nb_resources_default: u32,
    pub available_upto: HashMap<i64, ProcSet>,
}
impl Default for ResourceSet {
    fn default() -> ResourceSet {
        ResourceSet {
            default_intervals: ProcSet::from_iter([0..=99]),
            nb_resources_all: 0,
            nb_absent: 0,
            nb_resources_not_dead: 0,
            nb_resources_default_not_dead: 0,
            nb_resources_default: 0,
            available_upto: HashMap::from([(1761588000, ProcSet::from_iter([0..=49]))]),
        }
    }
}

pub fn schedule_cycle(config: Config, platform: Platform, queues: Vec<String>) {
    let now = std::time::SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;

    // Retrieve waiting jobs
    let waiting_jobs = platform.get_waiting_jobs();

    if waiting_jobs.len() > 0 {
        // Determine Global Resource Intervals and Initial Slot
        let resource_set = platform.resource_set(config);
        let mut initial_slot_set = SlotSet::from_intervals(resource_set.default_intervals, now);

        // Resource availability (Available_upto field) is integrated through pseudo job
        resource_set.available_upto.iter().for_each(|(time, intervals)| {
            let job = Job::new(0, *time, MAX_TIME - *time + 1, intervals.clone());
            initial_slot_set.split_slots_for_job_and_update_resources(&job, true, None);
        });
        println!("Initial SlotSet:");
        initial_slot_set.to_table().printstd();


        // Get additional waiting jobs' data

        // Get already scheduled jobs advanced reservations and jobs from more higher priority queues
        let scheduled_jobs = platform.get_scheduled_jobs();
        
        // Split the slots to make them fit the jobs
        initial_slot_set.split_slots_for_jobs_and_update_resources(&scheduled_jobs, true, None);


        println!("Initial SlotSet with already scheduled jobs:");
        initial_slot_set.to_table().printstd();
        
        // Scheduled
        

        // Save assignement
    }
}

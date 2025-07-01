use crate::kao::slot::{ProcSet, SlotSet, MAX_TIME};
use crate::lib::models::Job;
use std::collections::HashMap;
use std::time::UNIX_EPOCH;
use crate::kao::platform::{PlatformTrait};
use crate::kao::scheduling_basic::{schedule_jobs_ct};

pub struct Config {}

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

pub fn schedule_cycle<T: PlatformTrait>(config: Config, platform: T, queues: Vec<String>){
    let now = std::time::SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;

    // Retrieve waiting jobs
    let waiting_jobs = platform.get_waiting_jobs();

    if waiting_jobs.len() > 0 {
        // Determine Global Resource Intervals and Initial Slot
        let resource_set = platform.get_resource_set(&config);
        let mut initial_slot_set = SlotSet::from_intervals(resource_set.default_intervals.clone(), now);

        // Resource availability (Available_upto field) is integrated through pseudo job
        let pseudo_jobs = resource_set
            .available_upto
            .iter()
            .map(|(time, intervals)| Job::new(0, *time, MAX_TIME - *time + 1, intervals.clone()))
            .collect::<Vec<Job>>();
        initial_slot_set.split_slots_for_jobs_and_update_resources(&pseudo_jobs, true, None);

        println!("Initial SlotSet:");
        initial_slot_set.to_table().printstd();

        // Get additional waiting jobs' data
        let _ = platform.get_data_jobs();

        // Get already scheduled jobs advanced reservations and jobs from higher priority queues
        let scheduled_jobs = platform.get_scheduled_jobs();
        initial_slot_set.split_slots_for_jobs_and_update_resources(&scheduled_jobs, true, None);

        println!("SlotSet with already scheduled jobs:");
        initial_slot_set.to_table().printstd();

        // Scheduled
        schedule_jobs_ct(&mut initial_slot_set, waiting_jobs);

        // Save assignement
        platform.save_assignments();
    }
}

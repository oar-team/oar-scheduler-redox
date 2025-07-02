use crate::models::models::Job;
use crate::platform::PlatformTrait;
use crate::scheduler::scheduling_basic::schedule_jobs_ct;
use crate::scheduler::slot::SlotSet;


pub fn schedule_cycle<T: PlatformTrait>(platform: T, queues: Vec<String>){
    let now = platform.get_now();
    let max_time = platform.get_max_time();

    // Retrieve waiting jobs
    let waiting_jobs = platform.get_waiting_jobs();

    if waiting_jobs.len() > 0 {
        // Determine Global Resource Intervals and Initial Slot
        let resource_set = platform.get_resource_set();
        let mut initial_slot_set = SlotSet::from_intervals(resource_set.default_intervals.clone(), now, max_time);

        // Resource availability (Available_upto field) is integrated through pseudo job
        let pseudo_jobs = resource_set
            .available_upto
            .iter()
            .filter(|(time, _)| time < &max_time)
            .map(|(time, intervals)| Job::new_scheduled_from_proc_set(0, *time+1, max_time, intervals.clone()))
            .collect::<Vec<Job>>();
        initial_slot_set.split_slots_for_jobs_and_update_resources(&pseudo_jobs, true, None);

        println!("Initial SlotSet:");
        initial_slot_set.to_table().printstd();

        // Get additional waiting jobs' data
        //let _ = platform.get_data_jobs();

        // Get already scheduled jobs advanced reservations and jobs from higher priority queues
        let scheduled_jobs = platform.get_scheduled_jobs();
        initial_slot_set.split_slots_for_jobs_and_update_resources(&scheduled_jobs, true, None);

        println!("SlotSet with already scheduled jobs:");
        initial_slot_set.to_table().printstd();

        // Scheduled
        schedule_jobs_ct(&mut initial_slot_set, waiting_jobs);

        // Save assignement
        //platform.save_assignments();
    }
}

use crate::platform::Platform;
use oar_scheduler_core::scheduler::kamelot;
use oar_scheduler_db::model::queues::Queue;

pub fn queues_schedule(platform: &mut Platform) {

    // TODO: Initialize gant visualization with the `gantt_init_with_running_jobs` behavior

    // Init slotset
    let mut slot_sets = kamelot::init_slot_sets(platform, false);

    // Schedule each queue
    let grouped_queues: Vec<Vec<Queue>> = Queue::get_all_grouped_by_priority(&platform.session()).expect("Failed to get queues from database");
    for queues in grouped_queues {
        let active_queues = queues.iter().filter(|q| q.state.to_lowercase() == "active").map(|q| q.queue_name.clone()).collect::<Vec<String>>();

        // Insert scheduled besteffort jobs if queues = ['besteffort'].
        if active_queues.len() == 1 && active_queues[0] == "besteffort" {
            kamelot::add_already_scheduled_jobs_to_slot_set(&mut slot_sets, &mut *platform, true, false);
        }

        // Schedule jobs
        kamelot::internal_schedule_cycle(&mut *platform, &mut slot_sets, &active_queues);

        for queue in active_queues {
            // TODO: (MVP REQUIRED) Manage waiting reservation jobs with the `handle_waiting_reservation_jobs` behavior

            // TODO: (MVP REQUIRED) Check reservation jobs with the `check_reservation_jobs` behavior
        }
    }
}

use crate::platform::Platform;
use crate::queues_schedule::queues_schedule;

pub fn meta_schedule(platform: &mut Platform) -> i64 {
    let mut exit_code = 0;

    // TODO: Implement `process_walltime_change_requests` with config values WALLTIME_CHANGE_ENABLED, WALLTIME_CHANGE_APPLY_TIME, WALLTIME_INCREMENT

    // Schedule queues
    queues_schedule(platform);

    // TODO: Implement `get_gantt_jobs_to_launch` with config values SCHEDULER_BESTEFFORT_KILL_DURATION_BEFORE_RESERVATION

    // TODO: Implement the besteffort kill & jobs launch logic

    // TODO: Update gantt visualization tables

    // TODO: Manage node sleep/wakeup with config values ENERGY_SAVING_MODE, SCHEDULER_TIMEOUT, ENERGY_SAVING_INTERNAL, SCHEDULER_NODE_MANAGER_SLEEP_CMD, SCHEDULER_NODE_MANAGER_WAKE_UP_CMD

    // TODO: Implement resuming jobs logic

    // TODO: Notify users about the start prediction

    // TODO: Process toAckReservation jobs

    // TODO: Process toLaunch jobs

    // Done
    exit_code
}

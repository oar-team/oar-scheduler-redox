/*
 * Copyright (c) 2025 Clément GRENNERAT
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, version 3.
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see https://www.gnu.org/licenses/.
 *
 */
use crate::platform::Platform;
use crate::queues_schedule::queues_schedule;
use oar_scheduler_core::platform::PlatformTrait;
use oar_scheduler_db::model::gantt_flush_tables;

pub fn meta_schedule(platform: &mut Platform) -> i64 {
    let mut exit_code = 0;

    // TODO: Implement `process_walltime_change_requests` with config values WALLTIME_CHANGE_ENABLED, WALLTIME_CHANGE_APPLY_TIME, WALLTIME_INCREMENT

    // Initialize gantt tables with running/already scheduled jobs so they are accessible from `platform.get_scheduled_jobs()`
    gantt_init_with_running_jobs(platform);

    // Schedule queues
    queues_schedule(platform);

    // TODO: (MVP REQUIRED) Implement `get_gantt_jobs_to_launch` with config values SCHEDULER_BESTEFFORT_KILL_DURATION_BEFORE_RESERVATION

    // TODO: Implement the besteffort kill & jobs launch logic

    // TODO: Update gantt visualization tables

    // TODO: Manage node sleep/wakeup with config values ENERGY_SAVING_MODE, SCHEDULER_TIMEOUT, ENERGY_SAVING_INTERNAL, SCHEDULER_NODE_MANAGER_SLEEP_CMD, SCHEDULER_NODE_MANAGER_WAKE_UP_CMD

    // TODO: Implement resuming jobs logic

    // TODO: Notify users about the start prediction

    // TODO: Process toAckReservation jobs

    // TODO: (MVP REQUIRED) Process toLaunch jobs

    // Done
    exit_code
}

/// Initialize gantt tables with scheduled reservation jobs, Running jobs, toLaunch jobs and Launching jobs.
fn gantt_init_with_running_jobs(platform: &mut Platform) {
    gantt_flush_tables(&platform.session());
    let current_jobs = platform.get_fully_scheduled_jobs();
    platform.save_assignments(current_jobs);
}

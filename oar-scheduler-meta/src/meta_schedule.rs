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
use log::{debug, warn};
use oar_scheduler_core::platform::{Job, PlatformTrait};
use oar_scheduler_db::model::gantt;
use oar_scheduler_db::model::jobs::JobDatabaseRequests;
use std::collections::HashSet;

pub fn meta_schedule(platform: &mut Platform) -> i64 {
    let mut exit_code = 0;
    let now = platform.get_now();

    // TODO: Implement `process_walltime_change_requests` with config values WALLTIME_CHANGE_ENABLED, WALLTIME_CHANGE_APPLY_TIME, WALLTIME_INCREMENT

    // Initialize gantt tables with running/already scheduled jobs so they are accessible from `platform.get_scheduled_jobs()`
    gantt_init_with_running_jobs(platform);

    // Schedule queues
    let besteffort_scheduled_jobs = queues_schedule(platform);

    // Getting waiting gantt jobs with a start time before now + min(security_time, kill_duration_before_reservation)
    let jobs_to_launch_with_security_time = platform.get_gantt_jobs_to_launch_with_security_time();

    let jobs_to_launch = jobs_to_launch_with_security_time
        .iter()
        .filter(|job| job.assignment.as_ref().unwrap().begin <= now)
        .collect::<Vec<_>>();

    // Killing besteffort jobs on which new jobs have been scheduled.
    let no_killed_job = check_besteffort_jobs_to_kill(platform, &besteffort_scheduled_jobs, &jobs_to_launch);
    if no_killed_job {} else {
        exit_code = 2;
    }

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
    gantt::gantt_flush_tables(&platform.session());
    let current_jobs = platform.get_fully_scheduled_jobs();
    platform.save_assignments(current_jobs);
    // In the Python code, scheduled_jobs are fetched and a SlotSet is build, but this code is kept into the
    // `kamelot::init_slot_sets` function to avoid code duplication.
}

/// Detect if there are besteffort jobs to kill
/// return true if there is no frag job (no job marked as to be killed), false otherwise
fn check_besteffort_jobs_to_kill(platform: &mut Platform, besteffort_scheduled_jobs: &Vec<Job>, jobs_to_launch: &Vec<&Job>) -> bool {
    // TODO: Review and implement the logic of that function:
    //   https://github.com/oar-team/oar3/blob/e6b6e7e59eb751cc2e7388d6c2fb7f94a3ac8c6e/oar/kao/meta_sched.py#L148-L227
    //   The algorithm might need adaptations as we don’t have the resources_id to job_id map (maybe build them here),
    //   and our jobs in `besteffort_scheduled_jobs` are only present if we scheduled the besteffort queue in this cycle.
    true
}


/*def handle_jobs_to_launch(
    session, config, jobs_to_launch_lst, current_time_sec, current_time_sql
):
    logger.debug("Begin processing jobs to launch (start time <= " + current_time_sql)

    return_code = 0

    for job in jobs_to_launch_lst:
        return_code = 1
        logger.debug(
            "Set job " + str(job.id) + " state to toLaunch at " + current_time_sql
        )

        #
        # Advance Reservation
        #
        walltime = job.walltime
        if (job.reservation == "Scheduled") and (job.start_time < current_time_sec):
            max_time = walltime - (current_time_sec - job.start_time)

            set_moldable_job_max_time(session, job.moldable_id, max_time)
            set_gantt_job_start_time(session, job.moldable_id, current_time_sec)
            logger.warning(
                "Reduce walltime of job "
                + str(job.id)
                + "to "
                + str(max_time)
                + "(was  "
                + str(walltime)
                + " )"
            )

            add_new_event(
                session,
                "REDUCE_RESERVATION_WALLTIME",
                job.id,
                "Change walltime from " + str(walltime) + " to " + str(max_time),
            )

            w_max_time = duration_to_sql(max_time)
            new_message = re.sub(r"W=\d+:\d+:\d+", "W=" + w_max_time, job.message)

            if new_message != job.message:
                set_job_message(session, job.id, new_message)

        prepare_job_to_be_launched(session, config, job, current_time_sec)

    logger.debug("End processing of jobs to launch")

    return return_code*/

fn handle_jobs_to_launch(platform: &mut Platform, jobs_to_launch: &Vec<&Job>) -> i32 {
    let now = platform.get_now();
    debug!("Begin processing jobs to launch (start time <= {})", now);
    let mut return_code = 0;
    for job in jobs_to_launch {
        return_code = 1;
        debug!("Set job {} state to toLaunch at {}", job.id, now);

        // AR jobs tightening
        if let Some(begin) = job.advance_reservation_begin {
            if begin < now {
                // The job should start now, so we update its assignment to start now
                let mut new_job = job.clone();
                if let Some(assignment) = &new_job.assignment {
                    let walltime = assignment.end - assignment.begin + 1;
                    let new_walltime = assignment.end - now + 1;
                    warn!("Reducing the walltime of the job {} from {} to {}", job.id, walltime, new_walltime);


                    // TODO: finish the implementation of the AR jobs part
                    // set_moldable_job_max_time(session, job.moldable_id, max_time)
                    // set_gantt_job_start_time(session, job.moldable_id, current_time_sec)
                    // add_new_event(platform, "REDUCE_RESERVATION_WALLTIME", job.id, format!("Change walltime from {} to {}", walltime, max_time));

                    // updating job’s message
                    let old_walltime_str = format!("{:02}:{:02}:{:02}", walltime / 3600, (walltime % 3600) / 60, walltime % 60);
                    let new_walltime_str = format!("{:02}:{:02}:{:02}", new_walltime / 3600, (new_walltime % 3600) / 60, new_walltime % 60);
                    let message = job.message.replace(&format!("W={}", old_walltime_str), &format!("W={}", new_walltime_str));
                    if message != job.message {
                        job.set_message(&platform.session(), message.as_str()).expect("Unable to set job message");
                    }
                }
            }
        }

        // set_job_start_time_assigned_moldable_id(
        //     session, job.id, current_time_sec, job.moldable_id
        // )
        //
        // add_resource_job_pairs(session, job.moldable_id)
        //
        // set_job_state(session, config, job.id, "toLaunch")
        //
        // notify_to_run_job(config, job.id)

    }
    return_code
}

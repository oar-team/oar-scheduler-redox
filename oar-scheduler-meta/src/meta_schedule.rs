use crate::platform::Platform;
use crate::queues_schedule::queues_schedule;
use log::{debug, warn};
use oar_scheduler_core::platform::{Job, PlatformTrait};
use oar_scheduler_db::model::jobs::{JobDatabaseRequests, JobState};
use oar_scheduler_db::model::moldable::MoldableDatabaseRequests;
use oar_scheduler_db::model::{events, gantt, SqlEnum};
use std::collections::HashSet;
use std::process::{exit, Command};

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
    if no_killed_job {
        if handle_jobs_to_launch(platform, &jobs_to_launch) == 1 {
            exit_code = 0; // Exit code was already at 0 anyway... Following the Python code.
        }
    } else {
        exit_code = 2;
    }

    // TODO: Update gantt visualization tables

    // TODO: Manage node sleep/wakeup with config values ENERGY_SAVING_MODE, SCHEDULER_TIMEOUT, ENERGY_SAVING_INTERNAL, SCHEDULER_NODE_MANAGER_SLEEP_CMD, SCHEDULER_NODE_MANAGER_WAKE_UP_CMD

    // TODO: Implement resuming jobs logic

    // TODO: Notify users about the start prediction

    // TODO: Process toAckReservation jobs

    let jobs_by_state = platform.get_current_non_waiting_jobs_by_state();

    if let Some(jobs) = jobs_by_state.get(&JobState::Resuming.as_str().to_string()) {
        // TODO: Implement the resuming logic:
        //  https://github.com/oar-team/oar3/blob/e6b6e7e59eb751cc2e7388d6c2fb7f94a3ac8c6e/oar/kao/meta_sched.py#L572-L651
    }
    if let Some(jobs) = jobs_by_state.get(&JobState::ToError.as_str().to_string()) {
        // TODO: Implement the toError logic:
        // https://github.com/oar-team/oar3/blob/e6b6e7e59eb751cc2e7388d6c2fb7f94a3ac8c6e/oar/kao/meta_sched.py#L686-L705
    }
    if let Some(jobs) = jobs_by_state.get(&JobState::ToAckReservation.as_str().to_string()) {
        // TODO: Implement the toAckReservation logic:
        //   https://github.com/oar-team/oar3/blob/e6b6e7e59eb751cc2e7388d6c2fb7f94a3ac8c6e/oar/kao/meta_sched.py#L709-L741
    }
    if let Some(jobs) = jobs_by_state.get(&JobState::ToLaunch.as_str().to_string()) {
        for job in jobs {
            notify_to_run_job(platform, job.id);
        }
    }

    debug!("End of Meta Scheduler");
    exit_code
}

/// Initialize gantt tables with scheduled reservation jobs, Running jobs, toLaunch jobs and Launching jobs.
fn gantt_init_with_running_jobs(platform: &mut Platform) {
    gantt::gantt_flush_tables(&platform.session());
    let current_jobs = platform.get_fully_scheduled_jobs();
    debug!("(gantt_init with running jobs: save assignement with current");
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

fn handle_jobs_to_launch(platform: &mut Platform, jobs_to_launch: &Vec<&Job>) -> i32 {
    let now = platform.get_now();
    debug!("Begin processing jobs to launch (start time <= {})", now);
    let mut return_code = 0;
    for job in jobs_to_launch {
        return_code = 1;

        if job.assignment.is_none() {
            panic!("Can’t mark job {} as toLaunch as it has no assignment", job.id);
        }
        let assignment = job.assignment.as_ref().unwrap();
        let moldable = job.moldables.get(assignment.moldable_index).unwrap();
        let mut start_time = assignment.begin;

        // AR jobs tightening
        if let Some(begin) = job.advance_reservation_begin {
            if begin < now {
                start_time = now;
                // The job should start now, so we update its assignment to start now
                let mut new_job = job.clone();
                let walltime = assignment.end - assignment.begin + 1;
                let new_walltime = assignment.end - now + 1;
                warn!("Reducing the walltime of the job {} from {} to {}", job.id, walltime, new_walltime);

                moldable
                    .set_walltime(&platform.session(), new_walltime)
                    .expect("Unable to update AR moldable walltime");
                moldable
                    .set_gantt_job_start_time(&platform.session(), now)
                    .expect("Unable to update AR moldable start time in gantt");
                events::add_new_event(
                    &platform.session(),
                    "REDUCE_RESERVATION_WALLTIME",
                    job.id,
                    format!("Change walltime from {} to {}", walltime, new_walltime).as_str(),
                );

                // updating job’s message
                let old_walltime_str = format!("{:02}:{:02}:{:02}", walltime / 3600, (walltime % 3600) / 60, walltime % 60);
                let new_walltime_str = format!("{:02}:{:02}:{:02}", new_walltime / 3600, (new_walltime % 3600) / 60, new_walltime % 60);
                let message = job
                    .message
                    .replace(&format!("W={}", old_walltime_str), &format!("W={}", new_walltime_str));
                if message != job.message {
                    job.set_message(&platform.session(), message.as_str()).expect("Unable to set job message");
                }
            }
        }

        debug!("Set job {} state to toLaunch at {}", job.id, now);
        job.assign_moldable_and_set_start_time(&platform.session(), moldable.id, start_time)
            .unwrap();
        moldable
            .save_resources_as_assigned_resources(&platform.session(), &assignment.resources)
            .expect("Unable to save assigned resources");
        job.set_state(&platform.session(), JobState::ToLaunch).expect("Unable to set job state");
        notify_to_run_job(platform, job.id)
    }
    return_code
}

fn notify_to_run_job(_platform: &Platform, job_id: i64) {
    // TODO: Tell bipbip commander to run a job. It can also notifies oar2 almighty if METASCHEDULER_OAR3_WITH_OAR2 configuration variable is set to yes.:
    //  https://github.com/oar-team/oar3/blob/e6b6e7e59eb751cc2e7388d6c2fb7f94a3ac8c6e/oar/kao/meta_sched.py#L81-L118
    debug!("Notify to run job {}", job_id);

    // Testing with a temporary script
    Command::new("oar-notify-to-run-job")
        .arg(job_id.to_string())
        .output()
        .expect("failed to run oar-notify-to-run-job");

}

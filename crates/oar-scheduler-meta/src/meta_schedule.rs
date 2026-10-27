use crate::platform::Platform;
use crate::queues_schedule::queues_schedule;
use log::{debug, error, warn};
use oar_scheduler_core::platform::{Job, PlatformTrait};
use oar_scheduler_db::model::jobs::{JobDatabaseRequests, JobReservation, JobState};
use oar_scheduler_db::model::moldable::MoldableDatabaseRequests;
use oar_scheduler_db::model::{events, gantt, meta, SqlEnum};
use std::collections::{HashMap, HashSet};

pub fn meta_schedule(platform: &mut Platform) -> i64 {
    let mut exit_code = 0;
    let now = platform.get_now();

    // Process pending walltime change requests (only if enabled in the configuration).
    if platform.get_platform_config().config.walltime_change_enabled() {
        crate::walltime_change::process_walltime_change_requests(platform);
    }

    // Initialize gantt tables with running/already scheduled jobs so they are accessible from `platform.get_scheduled_jobs()`
    gantt_init_with_running_jobs(platform);

    // Schedule queues
    let besteffort_scheduled_jobs = queues_schedule(platform);

    // Getting waiting gantt jobs with a start time before now + max(security_time, kill_duration_before_reservation)
    let jobs_to_launch_with_security_time = platform.get_gantt_jobs_to_launch_with_security_time();

    // Filter jobs that are not yet ready to be launched: keep only those whose
    // start time is reached and none of whose resources is `Absent`.
    let absent_resources = platform.get_absent_resources();
    let jobs_to_launch = jobs_to_launch_with_security_time
        .iter()
        .filter(|job| {
            let Some(assignment) = job.assignment.as_ref() else {
                return false;
            };
            assignment.begin <= now && (absent_resources.clone() & assignment.resources.clone()).is_empty()
        })
        .collect::<Vec<_>>();

    // Besteffort jobs are killed for the jobs that are about to be launched, i.e.
    // the jobs of the whole security window (as in OAR3, which passes
    // `jobs_to_launch_with_security_time` to `check_besteffort_jobs_to_kill`), and
    // not only for the jobs that are launched during this round.
    let jobs_within_security_time = jobs_to_launch_with_security_time.iter().collect::<Vec<_>>();

    // Killing besteffort jobs on which new jobs have been scheduled.
    let jobs_to_frag = check_besteffort_jobs_to_kill(platform, &besteffort_scheduled_jobs, &jobs_within_security_time);
    if !jobs_to_frag {
        if handle_jobs_to_launch(platform, &jobs_to_launch) == 1 {
            exit_code = 0; // Exit code was already at 0 anyway... Following the Python code.
        }
    } else {
        exit_code = 2;
    }

    // Update gantt visualization tables
    if let Err(err) = meta::update_gantt_visualization(&platform.session()) {
        warn!("Unable to update gantt visualization tables: {}", err);
    }

    // Manage dynamic node feature for energy saving.
    if let Some(mode) = platform.get_platform_config().config.energy_saving_mode.clone() {
        if !mode.is_empty() {
            if mode == "metascheduler_decision_making" {
                let changes = crate::energy_saving::compute_nodes_energy_saving(platform);
                crate::energy_saving::apply_nodes_energy_saving(platform, &changes);
            } else if mode == "batsim_scheduler_proxy_decision_making" {
                warn!("ENERGY_SAVING_MODE=batsim_scheduler_proxy_decision_making is not supported by the Rust meta-scheduler");
            } else {
                warn!("Error ENERGY_SAVING_MODE unknown: {}", mode);
            }
        }
    }

    let jobs_by_state = platform.get_current_non_waiting_jobs_by_state();

    // Handle jobs waiting to be resumed.
    handle_resuming_jobs(platform, &jobs_by_state);

    // Notify oarsub -I clients about their start prediction
    notify_start_predictions(platform, now);

    // Process "toError" jobs
    if let Some(jobs) = jobs_by_state.get(JobState::ToError.as_str()) {
        for job in jobs {
            if let Some(info_type) = &job.info_type {
                if job.job_type.as_ref() == "INTERACTIVE"
                    || (job.job_type.as_ref() == "PASSIVE" && job.reservation.as_ref() == JobReservation::Scheduled.as_str())
                {
                    debug!("Notify oarsub job (num:{}) in error; jobInfo={}", job.id, info_type);
                    platform.notifier().notify_tcp_socket(info_type, &job.message);
                    platform.notifier().notify_tcp_socket(info_type, "BAD JOB");
                }
            }
            debug!("Set job {} to state Error", job.id);
            job.set_state(&platform.session(), JobState::Error).expect("Unable to set job state to Error");
        }
    }

    // Process "toAckReservation" jobs
    if let Some(jobs) = jobs_by_state.get(JobState::ToAckReservation.as_str()) {
        for job in jobs {
            let Some(info_type) = &job.info_type else {
                warn!("Job {} is in toAckReservation state but has no info_type, skipping notification", job.id);
                exit_code = 2;
                continue;
            };
            debug!("Treat job {} in toAckReservation state", job.id);

            let nb_sent = platform.notifier().notify_tcp_socket(info_type, "GOOD RESERVATION");
            if nb_sent == 0 {
                warn!("Frag job {}, cannot notify oarsub for the reservation", job.id);
                events::add_new_event(
                    &platform.session(),
                    "CANNOT_NOTIFY_OARSUB",
                    job.id,
                    format!("Can not notify oarsub for the job {}", job.id).as_str(),
                );
                platform.frag_job(job.id);
                exit_code = 2;
            } else {
                debug!("Notify oarsub for a RESERVATION (idJob={}) --> OK; jobInfo={}", job.id, info_type);
                job.set_state(&platform.session(), JobState::Waiting).expect("Unable to set job state to Waiting");
                let start_time = job.advance_reservation_begin.or_else(|| job.begin()).unwrap_or(0);
                if (start_time - 1) <= now && exit_code == 0 {
                    exit_code = 1;
                }
            }
        }
    }

    // Process "toLaunch" jobs
    if let Some(jobs) = jobs_by_state.get(JobState::ToLaunch.as_str()) {
        for job in jobs {
            platform.notify_to_run_job(job.id);
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

/// Notify the `oarsub -I` clients about the start prediction of their waiting
/// interactive jobs.
fn notify_start_predictions(platform: &Platform, now: i64) {
    let predictions = match meta::get_gantt_waiting_interactive_prediction_date(&platform.session()) {
        Ok(predictions) => predictions,
        Err(err) => {
            warn!("Unable to get interactive start predictions: {}", err);
            return;
        }
    };
    for prediction in predictions {
        let start_prediction = format_epoch(prediction.start_time);
        debug!(
            "[{}] Notifying user of the start prediction: {} ({})",
            prediction.job_id, start_prediction, prediction.message
        );
        platform.notifier().notify_tcp_socket(
            &prediction.info_type,
            format!("[{}] Start prediction: {} ({})", format_epoch(now), start_prediction, prediction.message).as_str(),
        );
    }
}

/// Detect if there are besteffort jobs to kill.
/// Returns `true` if at least one besteffort job must be fragged, `false` otherwise.
fn check_besteffort_jobs_to_kill(platform: &mut Platform, besteffort_scheduled_jobs: &[Job], jobs_to_launch: &[&Job]) -> bool {
    let now = platform.get_now();
    debug!("Begin processing of besteffort jobs to kill");

    // Jobs about to be launched, indexed by job id.
    let jobs_by_id: HashMap<i64, &Job> = jobs_to_launch.iter().map(|job| (job.id, *job)).collect();

    // Resource index -> id of the job about to be launched on it.
    let mut rid_to_launch_jid: HashMap<u32, i64> = HashMap::new();
    for job in jobs_to_launch {
        if let Some(assignment) = &job.assignment {
            for rid in assignment.resources.iter() {
                rid_to_launch_jid.insert(rid, job.id);
            }
        }
    }

    // Resource index -> besteffort job currently using it.
    let mut besteffort_rid2job: HashMap<u32, &Job> = HashMap::new();
    for job in besteffort_scheduled_jobs {
        if let Some(assignment) = &job.assignment {
            for rid in assignment.resources.iter() {
                besteffort_rid2job.insert(rid, job);
            }
        }
    }

    let mut fragged_jobs: HashSet<i64> = HashSet::new();
    let mut return_code = false;

    for (rid, job_id) in rid_to_launch_jid.iter() {
        let Some(be_job) = besteffort_rid2job.get(rid) else {
            continue;
        };
        let Some(job_to_launch) = jobs_by_id.get(job_id) else {
            continue;
        };

        if is_timesharing_for_two_jobs(be_job, job_to_launch) {
            debug!(
                "Resource {} is needed for job {}, but besteffort job {} can live, because timesharing compatible",
                rid, job_id, be_job.id
            );
            continue;
        }

        if fragged_jobs.contains(&be_job.id) {
            continue;
        }

        let mut skip_kill = false;
        // Check if we must checkpoint the besteffort job before killing it.
        if be_job.checkpoint > 0 {
            let checkpoint_first_date = meta::get_job_events(&platform.session(), be_job.id)
                .unwrap_or_default()
                .iter()
                .filter(|event| event.ev_type == "CHECKPOINT")
                .map(|event| event.date)
                .min();
            if checkpoint_first_date.is_none() || now <= checkpoint_first_date.unwrap() + be_job.checkpoint {
                skip_kill = true;
                platform.notifier().send_checkpoint_signal(be_job.id);
                debug!("Send checkpoint signal to the job {}", be_job.id);
            }
        }

        if !skip_kill {
            debug!(
                "Resource {} need(s) to be freed for job {}: killing besteffort job {}",
                rid, job_to_launch.id, be_job.id
            );
            events::add_new_event(
                &platform.session(),
                "BESTEFFORT_KILL",
                be_job.id,
                format!("kill the besteffort job {}", be_job.id).as_str(),
            );
            platform.frag_job(be_job.id);
        }

        fragged_jobs.insert(be_job.id);
        return_code = true;
    }

    debug!("End processing of besteffort jobs to kill\n");
    return_code
}

/// Returns `true` if the two jobs are allowed to timeshare the same resources.
/// Mirrors `oar.lib.job_handling.is_timesharing_for_two_jobs`.
fn is_timesharing_for_two_jobs(j1: &Job, j2: &Job) -> bool {
    let t1 = j1.types.get(&Box::from("timesharing")).and_then(|v| v.as_deref());
    let t2 = j2.types.get(&Box::from("timesharing")).and_then(|v| v.as_deref());
    let (Some(t1), Some(t2)) = (t1, t2) else {
        return false;
    };

    if t1 == "*,*" && t2 == "*,*" {
        return true;
    }
    if j1.user == j2.user && j1.name == j2.name {
        return true;
    }
    if j1.user == j2.user && t1 == "user,*" && t2 == "user,*" {
        return true;
    }
    if j1.name == j2.name && t1 == "*,name" && t2 == "*,name" {
        return true;
    }
    false
}

fn handle_jobs_to_launch(platform: &mut Platform, jobs_to_launch: &[&Job]) -> i32 {
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

        // AR jobs tightening: if the reservation start time is in the past, start
        // the job now and reduce its walltime accordingly.
        if job.reservation.as_ref() == JobReservation::Scheduled.as_str() && assignment.begin < now {
            start_time = now;
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
            let old_walltime_str = format_duration(walltime);
            let new_walltime_str = format_duration(new_walltime);
            let message = job
                .message
                .replace(&format!("W={}", old_walltime_str), &format!("W={}", new_walltime_str));
            if message != job.message {
                job.set_message(&platform.session(), message.as_str()).expect("Unable to set job message");
            }
        }

        debug!("Set job {} state to toLaunch at {}", job.id, now);
        job.assign_moldable_and_set_start_time(&platform.session(), moldable.id, start_time)
            .unwrap();
        moldable
            .save_resources_as_assigned_resources(&platform.session(), &assignment.resources)
            .expect("Unable to save assigned resources");
        job.set_state(&platform.session(), JobState::ToLaunch).expect("Unable to set job state");
        platform.notify_to_run_job(job.id);
    }
    return_code
}

/// Formats a duration in seconds as `HH:MM:SS`, matching `duration_to_sql`.
fn format_duration(seconds: i64) -> String {
    format!("{:02}:{:02}:{:02}", seconds / 3600, (seconds % 3600) / 60, seconds % 60)
}

/// Formats an epoch timestamp as `YYYY-MM-DD HH:MM:SS` in UTC.
fn format_epoch(epoch: i64) -> String {
    let days = epoch.div_euclid(86400);
    let secs = epoch.rem_euclid(86400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        year,
        month,
        day,
        secs / 3600,
        (secs % 3600) / 60,
        secs % 60
    )
}

/// Converts a number of days since the Unix epoch to a `(year, month, day)`
/// civil date. Algorithm from Howard Hinnant's `civil_from_days`.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Handle the jobs that are waiting to be resumed.
///
/// Mirrors the resuming part of `oar.kao.meta_sched.meta_schedule`. The Python
/// implementation is explicitly marked as *NOT ENTIRELY IMPLEMENTED* (the
/// cpuset/taktuk handling is left as a TODO): only the well-defined parts are
/// implemented here.
fn handle_resuming_jobs(platform: &Platform, jobs_by_state: &HashMap<String, Vec<Job>>) {
    let Some(jobs) = jobs_by_state.get(JobState::Resuming.as_str()) else {
        return;
    };
    let session = platform.session();

    for job in jobs {
        // The job can be resumed only if no other active job uses its resources.
        let other_jobs = match job.assignment.as_ref().and_then(|assignment| job.moldables.get(assignment.moldable_index)) {
            Some(moldable) => meta::get_jobs_on_resuming_job_resources(session, job.id, moldable.id).unwrap_or_default(),
            None => Vec::new(),
        };
        if !other_jobs.is_empty() {
            debug!(
                "[{}] Resuming job is delayed: other jobs ({:?}) are using its resources",
                job.id, other_jobs
            );
            continue;
        }

        debug!("[{}] Resuming job", job.id);
        if job.types.contains_key::<Box<str>>(&"noop".into()) {
            resume_job(platform, job);
            debug!("[{}] Resume NOOP job OK", job.id);
            continue;
        }

        let (script, timeout) = {
            let config = &platform.get_platform_config().config;
            (
                config.just_before_resume_exec_file.clone(),
                config.suspend_resume_script_timeout.unwrap_or(30).max(0) as u64,
            )
        };
        let Some(script) = script else {
            debug!("[{}] No JUST_BEFORE_RESUME_EXEC_FILE configured, resuming the job directly", job.id);
            resume_job(platform, job);
            continue;
        };

        debug!("[{}] Running post suspend script: `{} {}'", job.id, script, job.id);
        let (timed_out, return_code) = crate::energy_saving::run_command_with_args(&script, &[job.id.to_string()], timeout);
        if timed_out {
            error!("[{}] Suspend script timeouted", job.id);
            events::add_new_event(session, "RESUME_SCRIPT_ERROR", job.id, "Suspend script timeouted");
        }
        if return_code != 0 {
            let message = format!("[{}] Suspend script error, return code = {}", job.id, return_code);
            error!("{}", message);
            events::add_new_event(session, "RESUME_SCRIPT_ERROR", job.id, &message);
            platform.frag_job(job.id);
        } else {
            resume_job(platform, job);
        }
    }
}

/// Set the job back to `Running` and reset the `suspended_jobs` flag of the
/// resources that no longer host a suspended job.
/// Mirrors `oar.lib.job_handling.resume_job_action`.
fn resume_job(platform: &Platform, job: &Job) {
    let session = platform.session();
    if let Err(err) = job.set_state(session, JobState::Running) {
        warn!("Unable to resume job {}: {}", job.id, err);
        return;
    }
    if let Err(err) = meta::clear_suspended_jobs(session) {
        warn!("Unable to reset the suspended job resources after resuming job {}: {}", job.id, err);
    }
}

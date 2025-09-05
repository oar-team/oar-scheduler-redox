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
use indexmap::IndexMap;
use log::{info, warn};
use oar_scheduler_core::model::job::JobAssignment;
use oar_scheduler_core::platform::{Job, PlatformTrait, ProcSetCoresOp};
use oar_scheduler_core::scheduler::slotset::SlotSet;
use oar_scheduler_core::scheduler::{kamelot, quotas};
use oar_scheduler_db::model::jobs::{JobDatabaseRequests, JobState};
use oar_scheduler_db::model::queues::Queue;
use std::collections::HashMap;

/// Returns the list of already-scheduled besteffort jobs inserted in the SlotSet.
pub fn queues_schedule(platform: &mut Platform) -> Vec<Job> {
    // Init slotset
    let (mut slot_sets, besteffort_scheduled_jobs) = kamelot::init_slot_sets(platform, false);
    info!("Slotset map: {:?}", slot_sets.keys().collect::<Vec<&Box<str>>>());

    // Schedule each queue
    let grouped_queues: Vec<Vec<Queue>> = Queue::get_all_grouped_by_priority(&platform.session()).expect("Failed to get queues from database");
    for queues in grouped_queues {
        let active_queues = queues
            .iter()
            .filter(|q| q.state.to_lowercase() == "active")
            .map(|q| q.queue_name.clone())
            .collect::<Vec<String>>();

        info!("Scheduling queue(s): {:?}", active_queues);
        info!("Slotset map: {:?}", slot_sets.keys().collect::<Vec<&Box<str>>>());


        // Insert scheduled besteffort jobs if queues = ['besteffort'].
        if active_queues.len() == 1 && active_queues[0] == "besteffort" {
            kamelot::add_already_scheduled_jobs_to_slot_set(&mut slot_sets, &mut *platform, true, false);
        }

        // Schedule jobs
        kamelot::internal_schedule_cycle(&mut *platform, &mut slot_sets, &active_queues);

        for queue in active_queues {
            // TODO: Manage waiting reservation jobs with the `handle_waiting_reservation_jobs` behavior:
            //   https://github.com/oar-team/oar3/blob/e6b6e7e59eb751cc2e7388d6c2fb7f94a3ac8c6e/oar/kao/queues_sched.py#L421-L512

            // Check new AR jobs
            check_reservation_jobs(platform, &mut slot_sets, &queue)
        }
    }
    besteffort_scheduled_jobs
}

fn check_reservation_jobs(platform: &mut Platform, slot_sets: &mut HashMap<Box<str>, SlotSet>, queue: &String) {
    let platform_config = platform.get_platform_config();
    let job_security_time = platform_config.config.scheduler_job_security_time;
    let now = platform.get_now();

    let jobs: IndexMap<i64, Job> = platform.get_waiting_to_schedule_ar_jobs(queue.clone());
    if jobs.is_empty() {
        return;
    }

    // Process each job for reservation
    let mut assigned_jobs = IndexMap::new();
    for mut job in jobs.into_values() {
        // Only process the first moldable for AR jobs
        let moldable = job.moldables.get(0).expect("No moldable found for job");

        // Check if reservation is too old
        let mut start_time = job.advance_reservation_begin.unwrap();
        let end_time = start_time + moldable.walltime - 1;
        if now > start_time + moldable.walltime {
            set_job_resa_not_scheduled(&platform, &job, "Reservation expired and couldn't be started.");
            continue;
        } else if start_time < now {
            start_time = now;
        }

        let ss_name = job.slot_set_name();
        let slot_set = slot_sets.get_mut(&*ss_name).expect("SlotSet not found");

        let effective_end = end_time - job_security_time;
        let (left_slot_id, right_slot_id) = match slot_set.get_encompassing_range(start_time, effective_end, None) {
            Some((s1, s2)) => (s1.id(), s2.id()),
            None => {
                // Skipping, reservation might be after max_time.
                warn!("Job {} cannot be scheduled: no slots available for the requested time range.", job.id);
                continue;
            }
        };

        // Time-sharing and placeholder
        let empty: Box<str> = "".into();
        let (ts_user_name, ts_job_name) = job.time_sharing.as_ref().map_or((None, None), |_| {
            (Some(job.user.as_ref().unwrap_or(&empty)), Some(job.name.as_ref().unwrap_or(&empty)))
        });
        let available_resources = slot_set.intersect_slots_intervals(left_slot_id, right_slot_id, ts_user_name, ts_job_name, &job.placeholder);

        let res = slot_set
            .get_platform_config()
            .resource_set
            .hierarchy
            .request(&available_resources, &moldable.requests);

        if let Some(proc_set) = res {
            if slot_set.get_platform_config().quotas_config.enabled && !job.no_quotas {
                let slots = slot_set.iter().between(left_slot_id, right_slot_id);
                if let Some((_msg, _rule, _limit)) = quotas::check_slots_quotas(slots, &job, start_time, end_time, proc_set.core_count()) {
                    set_job_resa_scheduled(&platform, &job, Some("This AR cannot run: quotas exceeded"));
                    continue;
                }
            }

            job.assignment = Some(JobAssignment::new(start_time, end_time, proc_set, 0));
            slot_set.split_slots_for_job_and_update_resources(&job, true, true, None);
            set_job_resa_scheduled(&platform, &job, None);
            assigned_jobs.insert(job.id, job);
        } else {
            set_job_resa_scheduled(&platform, &job, Some("This AR cannot run: not enough resources"));
            continue;
        }
    }
    if !assigned_jobs.is_empty() {
        platform.save_assignments(assigned_jobs);
    }
}

fn set_job_resa_state(platform: &Platform, job: &Job, state: JobState, message: Option<&str>, scheduled: bool) {
    job.set_state(&platform.session(), state).expect("Unable to set job state");
    if let Some(message) = message {
        job.set_message(&platform.session(), message).expect("Unable to set job message");
    }
    if scheduled {
        job.set_resa_state(&platform.session(), "Scheduled")
            .expect("Unable to set job reservation state");
    }
}
fn set_job_resa_scheduled(platform: &Platform, job: &Job, error: Option<&str>) {
    if let Some(error) = error {
        set_job_resa_state(platform, job, JobState::ToError, Some(error), true);
    } else {
        set_job_resa_state(platform, job, JobState::ToAckReservation, None, true);
    }
}
fn set_job_resa_not_scheduled(platform: &Platform, job: &Job, error: &str) {
    set_job_resa_state(platform, job, JobState::Error, Some(error), false);
}

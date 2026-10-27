use crate::platform::Platform;
use indexmap::IndexMap;
use log::{debug, info, warn};
use oar_scheduler_core::model::job::{JobAssignment, ProcSet};
use oar_scheduler_core::platform::{Job, PlatformTrait, ProcSetCoresOp};
use oar_scheduler_core::scheduler::slotset::SlotSet;
use oar_scheduler_core::scheduler::{kamelot, quotas};
use oar_scheduler_db::model::jobs::{JobDatabaseRequests, JobState};
use oar_scheduler_db::model::queues::Queue;
use oar_scheduler_db::model::{events, meta};
use std::collections::HashMap;

///
pub fn queues_schedule(platform: &mut Platform) -> Vec<Job> {
    // Init slotset
    let (mut slot_sets, _) = kamelot::init_slot_sets(platform, false);
    info!("Slotset map: {:?}", slot_sets.keys().collect::<Vec<&Box<str>>>());

    // Besteffort jobs currently occupying resources, used later to detect which
    // besteffort jobs must be killed when higher priority jobs get scheduled.
    let besteffort_scheduled_jobs = platform
        .get_scheduled_jobs()
        .into_iter()
        .filter(|job| job.types.contains_key::<Box<str>>(&"besteffort".into()))
        .collect::<Vec<Job>>();

    // Schedule each queue
    let grouped_queues: Vec<Vec<Queue>> = Queue::get_all_grouped_by_priority(&platform.session()).expect("Failed to get queues from database");
    for queues in grouped_queues {
        let active_queues = queues
            .iter()
            .filter(|q| q.state.to_lowercase() == "active")
            .map(|q| q.queue_name.clone())
            .collect::<Vec<String>>();
        if active_queues.is_empty() {
            continue;
        }

        info!("Scheduling queue(s): {:?}", active_queues);
        info!("Slotset map: {:?}", slot_sets.keys().collect::<Vec<&Box<str>>>());


        // Insert scheduled besteffort jobs if queues = ['besteffort'].
        if active_queues.len() == 1 && active_queues[0] == "besteffort" {
            kamelot::add_already_scheduled_jobs_to_slot_set(&mut slot_sets, &mut *platform, true, false);
        }

        // Schedule jobs
        kamelot::internal_schedule_cycle(&mut *platform, &mut slot_sets, &active_queues);

        // Resources that are currently usable (Alive or Absent), used to detect
        // advance reservations waiting for their resources.
        let alive_resources = get_alive_resources(platform);

        for queue in active_queues {
            // Process accepted advance reservations waiting for their resources
            handle_waiting_reservation_jobs(platform, &queue, &alive_resources);

            // Check new AR jobs
            check_reservation_jobs(platform, &mut slot_sets, &queue)
        }
    }
    besteffort_scheduled_jobs
}

/// The set of resources that are currently `Alive` or `Absent` (mirrors
/// `resource_set.roid_itvs`).
fn get_alive_resources(platform: &Platform) -> ProcSet {
    let session = platform.session();
    meta::get_alive_resource_ids(session)
        .unwrap_or_default()
        .iter()
        .filter_map(|resource_id| session.resource_id_to_resource_index(*resource_id))
        .collect()
}

/// Replace every `R=<digits>` occurrence of a job message by `R=<nb>`.
/// Returns `None` when the message does not change.
fn replace_resource_count(message: &str, nb_resources: usize) -> Option<String> {
    let mut new_message = String::with_capacity(message.len());
    let bytes = message.as_bytes();
    let mut i = 0;
    let mut replaced = false;
    while i < bytes.len() {
        if bytes[i] == b'R' && i + 1 < bytes.len() && bytes[i + 1] == b'=' {
            let mut j = i + 2;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            if j > i + 2 {
                new_message.push_str(&format!("R={}", nb_resources));
                replaced = true;
                i = j;
                continue;
            }
        }
        new_message.push(bytes[i] as char);
        i += 1;
    }
    if replaced { Some(new_message) } else { None }
}

/// Handle the accepted advance reservations that are waiting for their
/// resources. Mirrors `oar.kao.meta_sched.handle_waiting_reservation_jobs`.
fn handle_waiting_reservation_jobs(platform: &Platform, queue_name: &str, alive_resources: &ProcSet) {
    let session = platform.session();
    let now = platform.get_now();
    let reservation_waiting_timeout = platform.get_platform_config().config.reservation_waiting_resources_timeout;

    debug!("Queue {}: begin processing accepted Advance Reservations", queue_name);

    let ar_jobs = platform.get_gantt_waiting_scheduled_ar_jobs(queue_name.to_string());

    for job in ar_jobs {
        let Some(assignment) = job.assignment.as_ref() else {
            continue;
        };
        let Some(moldable) = job.moldables.get(assignment.moldable_index) else {
            continue;
        };
        let moldable_id = moldable.id;
        let walltime = moldable.walltime;
        let res_set = assignment.resources.clone();
        let start_time = assignment.begin;

        // Test if the AR job is expired and handle it.
        if now > start_time + walltime {
            warn!(
                "[{}] set job state to Error: advance reservation expired and couldn't be started",
                job.id
            );
            if let Err(err) = job.set_state(session, JobState::Error) {
                warn!("Unable to set job {} state to Error: {}", job.id, err);
            }
            if let Err(err) = job.set_message(session, "Reservation expired and couldn't be started.") {
                warn!("Unable to set job {} message: {}", job.id, err);
            }
        } else {
            // Determine the currently available resources of this reservation.
            let avail_res = alive_resources.clone() & res_set.clone();

            if avail_res.is_empty() && start_time < now {
                warn!("[{}] advance reservation is waiting because no resource is present", job.id);
                // Delay the launching time.
                if let Err(err) = meta::set_gantt_job_start_time(session, moldable_id, now + 1) {
                    warn!("Unable to delay reservation {}: {}", job.id, err);
                }
            } else if start_time < now {
                if (start_time + reservation_waiting_timeout) > now {
                    if avail_res != res_set {
                        // The expected resources are not all available, wait the timeout.
                        warn!(
                            "[{}] advance reservation is waiting because not all resources are available yet (timeout in {} seconds)",
                            job.id,
                            start_time + reservation_waiting_timeout - now
                        );
                        if let Err(err) = meta::set_gantt_job_start_time(session, moldable_id, now + 1) {
                            warn!("Unable to delay reservation {}: {}", job.id, err);
                        }
                    }
                } else {
                    // It's time to launch the AR job: remove the missing resources.
                    let missing_resources = res_set.clone() - avail_res.clone();
                    let missing_ids = missing_resources
                        .iter()
                        .filter_map(|resource_index| session.resource_index_to_resource_id(resource_index))
                        .collect::<Vec<i32>>();
                    if let Err(err) = meta::remove_gantt_resources(session, moldable_id, &missing_ids) {
                        warn!("Unable to remove missing resources of reservation {}: {}", job.id, err);
                    }
                    warn!(
                        "[{}] remove some resources assigned to this advance reservation, because they are not Alive",
                        job.id
                    );
                    events::add_new_event(
                        session,
                        "SCHEDULER_REDUCE_NB_RESSOURCES_FOR_RESERVATION",
                        job.id,
                        format!("[MetaSched] Reduce the number of resources for the job {}", job.id).as_str(),
                    );

                    let nb_res = (res_set.len() - missing_resources.len()) as usize;
                    if let Some(new_message) = replace_resource_count(job.message.as_str(), nb_res) {
                        if let Err(err) = job.set_message(session, &new_message) {
                            warn!("Unable to set job {} message: {}", job.id, err);
                        }
                    }
                }
            }
        }
    }

    debug!(
        "Queue {}: end processing of reservations with missing resources",
        queue_name
    );
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

        // The security window ends at `start_time + walltime - job_security_time`
        // (Python's `t_e`), i.e. `end_time + 1 - job_security_time` with an
        // inclusive end.
        let effective_end = end_time + 1 - job_security_time;
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
        debug!("Check reservations: save assignments");
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

#[cfg(test)]
mod tests {
    use super::replace_resource_count;

    #[test]
    fn test_replace_resource_count() {
        assert_eq!(replace_resource_count("R=4 W=1:00:00", 2).as_deref(), Some("R=2 W=1:00:00"));
        assert_eq!(
            replace_resource_count("R=4/R=7 W=1", 3).as_deref(),
            Some("R=3/R=3 W=1")
        );
        // No `R=<digits>` in the message: it must not change.
        assert_eq!(replace_resource_count("Reservation expired", 2), None);
        assert_eq!(replace_resource_count("R=", 2), None);
    }
}

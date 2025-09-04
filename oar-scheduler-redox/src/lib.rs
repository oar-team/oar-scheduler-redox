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

mod converters;
mod platform;
#[cfg(test)]
mod test;

use crate::platform::Platform;
use indexmap::IndexMap;
use log::{warn, LevelFilter};
use oar_scheduler_core::model::job::{Job, JobAssignment, ProcSetCoresOp};
use oar_scheduler_core::platform::PlatformTrait;
use oar_scheduler_core::scheduler::slotset::SlotSet;
use oar_scheduler_core::scheduler::{kamelot, quotas};
use pyo3::prelude::*;
use std::cell::RefCell;
use std::collections::HashMap;

/// Python module declaration
#[pymodule]
fn oar_scheduler_redox(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(schedule_cycle_external, m)?)?;
    m.add_function(wrap_pyfunction!(build_redox_platform, m)?)?;
    m.add_function(wrap_pyfunction!(build_redox_slot_sets, m)?)?;
    m.add_function(wrap_pyfunction!(schedule_cycle_internal, m)?)?;
    m.add_function(wrap_pyfunction!(check_reservation_jobs, m)?)?;

    env_logger::Builder::new().filter(None, LevelFilter::Info).init();

    // Register plugin hooks from the oar-scheduler-hooks crate into the oar-scheduler-core crate
    if let Some(hooks) = oar_scheduler_hooks::Hooks::new() {
        oar_scheduler_core::hooks::set_hooks_handler(hooks);
    }

    Ok(())
}

/// Schedules the jobs from the platform and saves the assignments back to the platform.
/// Should be called in external scheduler mode.
#[pyfunction]
fn schedule_cycle_external(py_session: Bound<PyAny>, py_config: Bound<PyAny>, py_platform: Bound<PyAny>, py_now: Bound<PyAny>, py_queues: Bound<PyAny>) -> PyResult<()> {
    // Extracting the platform (including the resource set, quotas config, and waiting jobs)
    let mut platform = Platform::from_python(&py_platform, &py_session, &py_config, &py_now, None);

    // Loading the waiting jobs from the python platform for this specific queues
    platform.load_waiting_jobs(&py_queues, None);

    // Scheduling (Platform automatically calls py_platform.save_assigns upon saving scheduled jobs.)
    let queues: Vec<String> = py_queues.extract().unwrap();
    kamelot::schedule_cycle(&mut platform, &queues);

    Ok(())
}

/// PlatformHandle is not thread-safe and cannot be sent across threads.
/// All functions taking a Bound<PlatformHandle> parameter should never release the GIL.
#[pyclass(unsendable)]
struct PlatformHandle {
    inner: RefCell<Platform>,
}
/// SlotSetsHandle is not thread-safe and cannot be sent across threads.
/// All functions taking a Bound<SlotSetsHandle> parameter should never release the GIL.
#[pyclass(unsendable)]
struct SlotSetsHandle {
    inner: RefCell<HashMap<Box<str>, SlotSet>>,
}

#[pyfunction]
fn build_redox_platform(
    py: Python,
    py_session: Bound<PyAny>,
    py_config: Bound<PyAny>,
    py_platform: Bound<PyAny>,
    py_now: Bound<PyAny>,
    py_scheduled_jobs: Bound<PyAny>,
) -> PyResult<Py<PlatformHandle>> {
    let platform = Platform::from_python(&py_platform, &py_session, &py_config, &py_now, Some(&py_scheduled_jobs));
    Py::new(
        py,
        PlatformHandle {
            inner: RefCell::new(platform),
        },
    )
}

#[pyfunction]
fn build_redox_slot_sets(platform: Bound<PlatformHandle>) -> PyResult<Py<SlotSetsHandle>> {
    let py = platform.py();
    let platform_handle_ref = platform.borrow();
    let platform = platform_handle_ref.inner.borrow();

    let slot_sets = kamelot::init_slot_sets(&*platform, false);

    Py::new(
        py,
        SlotSetsHandle {
            inner: RefCell::new(slot_sets),
        },
    )
}

#[pyfunction]
fn schedule_cycle_internal(platform: Bound<PlatformHandle>, slot_sets: Bound<SlotSetsHandle>, py_queues: Bound<PyAny>) -> PyResult<()> {
    let platform_handle_ref = platform.borrow_mut();
    let mut platform = platform_handle_ref.inner.borrow_mut();
    let slot_sets_handle_ref = slot_sets.borrow();
    let mut slot_sets = slot_sets_handle_ref.inner.borrow_mut();
    let queues: Vec<String> = py_queues.extract()?;

    // Loading the waiting jobs from the python platform into the rust platform for these specific queues
    platform.load_waiting_jobs(&py_queues, None);

    // Insert scheduled besteffort jobs if py_queues = ['besteffort'].
    if queues.len() == 1 && queues[0] == "besteffort" {
        kamelot::add_already_scheduled_jobs_to_slot_set(&mut *slot_sets, &mut *platform, true, false);
    }

    kamelot::internal_schedule_cycle(&mut *platform, &mut *slot_sets, &queues);
    Ok(())
}

#[pyfunction]
fn check_reservation_jobs(platform: Bound<PlatformHandle>, slot_sets: Bound<SlotSetsHandle>, py_queue: Bound<PyAny>) {
    let py = platform.py();
    let platform_handle_ref = platform.borrow_mut();
    let mut platform = platform_handle_ref.inner.borrow_mut();

    let platform_config = platform.get_platform_config();
    let job_security_time = platform_config.config.scheduler_job_security_time;
    let now = platform.get_now();
    let job_handling = PyModule::import(py, "oar.lib.job_handling").expect("Could not import job_handling");
    let slot_sets_handle_ref = slot_sets.borrow();
    let mut slot_sets = slot_sets_handle_ref.inner.borrow_mut();

    // Load jobs to schedule for the queue
    platform.load_waiting_jobs(&py_queue, Some(&"toSchedule".to_string()));

    let jobs: IndexMap<i64, Job> = platform.get_waiting_jobs();
    if jobs.is_empty() {
        return;
    }

    // Process each job for reservation
    let mut assigned_jobs = IndexMap::new();
    for (_id, mut job) in jobs.into_iter() {
        // Only process the first moldable for AR jobs
        let moldable = job.moldables.get(0).expect("No moldable found for job");

        // Check if reservation is too old
        let mut start_time = job.advance_reservation_start_time.unwrap();
        let end_time = start_time + moldable.walltime - 1;
        if now > start_time + moldable.walltime {
            set_job_resa_not_scheduled(&job_handling, &platform, job.id, "Reservation expired and couldn't be started.");
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
                    set_job_resa_scheduled(&job_handling, &platform, job.id, Some("This AR cannot run: quotas exceeded"));
                    continue;
                }
            }

            job.assignment = Some(JobAssignment::new(start_time, end_time, proc_set, 0));
            slot_set.split_slots_for_job_and_update_resources(&job, true, true, None);
            set_job_resa_scheduled(&job_handling, &platform, job.id, None);
            assigned_jobs.insert(job.id, job);
        } else {
            set_job_resa_scheduled(&job_handling, &platform, job.id, Some("This AR cannot run: not enough resources"));
            continue;
        }
    }
    if !assigned_jobs.is_empty() {
        platform.save_assignments(assigned_jobs);
    }
}

fn set_job_resa_state(job_handling: &Bound<PyModule>, platform: &Platform, job_id: i64, state: &str, message: Option<&str>, scheduled: bool) {
    job_handling
        .getattr("set_job_state")
        .unwrap()
        .call1((platform.get_py_session(), platform.get_py_config(), job_id, state))
        .unwrap();
    if let Some(message) = message {
        job_handling
            .getattr("set_job_message")
            .unwrap()
            .call1((platform.get_py_session(), job_id, message))
            .unwrap();
    }
    if scheduled {
        job_handling
            .getattr("set_job_resa_state")
            .unwrap()
            .call1((platform.get_py_session(), job_id, "Scheduled"))
            .unwrap();
    }
}
fn set_job_resa_scheduled(job_handling: &Bound<PyModule>, platform: &Platform, job_id: i64, error: Option<&str>) {
    if let Some(error) = error {
        set_job_resa_state(job_handling, platform, job_id, "toError", Some(error), true);
    } else {
        set_job_resa_state(job_handling, platform, job_id, "toAckReservation", None, true);
    }
}
fn set_job_resa_not_scheduled(job_handling: &Bound<PyModule>, platform: &Platform, job_id: i64, error: &str) {
    set_job_resa_state(job_handling, platform, job_id, "Error", Some(error), false);
}

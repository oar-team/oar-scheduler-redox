mod converters;
mod platform;

use crate::platform::Platform;
use log::LevelFilter;
use oar_scheduler_core::scheduler::kamelot;
use oar_scheduler_core::scheduler::slot::SlotSet;
use pyo3::prelude::*;
use std::cell::RefCell;
use std::collections::HashMap;

/// Python module declaration
#[pymodule]
fn oar_scheduler_redox(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(schedule_cycle_external, m).unwrap()).unwrap();
    m.add_function(wrap_pyfunction!(build_redox_platform, m).unwrap()).unwrap();
    m.add_function(wrap_pyfunction!(build_redox_slot_sets, m).unwrap()).unwrap();
    m.add_function(wrap_pyfunction!(schedule_cycle_internal, m).unwrap()).unwrap();

    env_logger::Builder::new().filter(None, LevelFilter::Info).init();

    Ok(())
}

/// Schedules the jobs from the platform and saves the assignments back to the platform.
/// Should be called in external scheduler mode.
#[pyfunction]
fn schedule_cycle_external(py_session: Bound<PyAny>, py_config: Bound<PyAny>, py_platform: Bound<PyAny>, py_queues: Bound<PyAny>) -> PyResult<()> {
    // Extracting the platform (including the resource set, quotas config, and waiting jobs)
    let mut platform = Platform::from_python(&py_platform, &py_session, &py_config, None).unwrap();

    // Loading the waiting jobs from the python platform for this specific queues
    platform.load_waiting_jobs(&py_queues);

    // Scheduling (Platform automatically calls py_platform.save_assigns upon saving scheduled jobs.)
    let queues: Vec<String> = py_queues.extract().unwrap();
    kamelot::schedule_cycle(&mut platform, queues);

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
    let mut platform = Platform::from_python(&py_platform, &py_session, &py_config, Some(&py_scheduled_jobs)).unwrap();
    let now: i64 = py_now.extract().unwrap();
    platform.set_now(now);
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
    platform.load_waiting_jobs(&py_queues);

    // Insert scheduled besteffort jobs if py_queues = ['besteffort'].
    if queues.len() == 1 && queues[0] == "besteffort" {
        kamelot::add_already_scheduled_jobs_to_slot_set(&mut *slot_sets, &mut *platform, true, false);
    }

    kamelot::internal_schedule_cycle(&mut *platform, &mut *slot_sets, queues);
    Ok(())
}

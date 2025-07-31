mod converters;
mod platform;
use pyo3::prelude::*;
use oar3_scheduler::scheduler::kamelot;
use crate::platform::Platform;

/// Python module declaration
#[pymodule]
fn oar3_scheduler_lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(schedule_cycle, m)?)?;
    Ok(())
}

/// Schedules the jobs from the platform and saves the assignments back to the platform.
/// Should be called in external scheduler mode.
#[pyfunction]
fn schedule_cycle(
    py_session: Bound<PyAny>,
    py_config: Bound<PyAny>,
    py_platform: Bound<PyAny>,
    py_queues: Bound<PyAny>,
) -> PyResult<()> {
    // Extracting the platform (including the resource set, quotas config, and waiting jobs)
    let mut platform = Platform::from_python(&py_platform, &py_session, &py_config, &py_queues)?;

    // Scheduling (Platform automatically calls py_platform.save_assigns upon saving scheduled jobs.)
    let queues: Vec<String> = py_queues.extract()?;
    kamelot::schedule_cycle(&mut platform, queues);

    Ok(())
}




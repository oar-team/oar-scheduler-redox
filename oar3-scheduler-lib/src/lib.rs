mod model_py;
mod platform;

use oar3_scheduler::platform::PlatformTrait;
use pyo3::prelude::*;
use pyo3::types::PyInt;

/// Python module declaration
#[pymodule]
fn oar3_scheduler_lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(schedule_cycle, m)?)?;
    Ok(())
}

/// Formats the sum of two numbers as string.
#[pyfunction]
fn schedule_cycle(
    session: Py<PyAny>,
    config: Py<PyAny>,
    platform: Py<PyAny>,
    slot_sets: Py<PyAny>,
    job_security_time: Py<PyInt>,
    queues: Py<PyAny>,
    quotas: Py<PyAny>, // Quotas class (not an instance, but a class reference to access global attributes)
) -> PyResult<String> {
    todo!();
}




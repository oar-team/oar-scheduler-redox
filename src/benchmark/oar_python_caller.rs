use crate::benchmark::benchmarker::measure_time;
use crate::models::models::{Job, ProcSet, ScheduledJobData};
use crate::platform::PlatformTrait;
use pyo3::ffi::c_str;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use std::collections::HashMap;
use std::ffi::CStr;

const PYTHON_MODULE_NAME: &str = "oar.kao.kamelot_basic";
const PYTHON_MODULE_DIR: &str = "/Users/clement/CodeIF/oar3/";
const PYTHON_SITE_PACKAGES_DIR: &str = "/Users/clement/CodeIF/oar3/.venv/lib/python3.10/site-packages";

const ADAPTER_MODULE: &CStr = c_str!("adapter");
const ADAPTER_FILE: &CStr = c_str!("adapter.py");
const ADAPTER_CODE: &CStr = c_str!(include_str!("adapter.py"));

// Returns (elapsed ms, slot count)
pub fn schedule_cycle_on_oar_python<T: PlatformTrait>(platform: &mut T, _queues: Vec<String>) -> (u32, usize) {
    let time = Python::with_gil(|py| {
        let sys = py.import("sys").unwrap();
        sys.getattr("path").unwrap().call_method1("append", (PYTHON_MODULE_DIR,)).unwrap();
        sys.getattr("path").unwrap().call_method1("append", (PYTHON_SITE_PACKAGES_DIR,)).unwrap();

        PyModule::from_code(py, ADAPTER_CODE, ADAPTER_FILE, ADAPTER_MODULE).unwrap();

        //info!("Python {:?}, path = {:?}", sys.getattr("version"), sys.getattr("path"));

        let platform_py = create_platform(py, platform);

        // Run scheduling using basic scheduler (no quotas)
        let schedule_cycle = py.import(PYTHON_MODULE_NAME).unwrap().getattr("schedule_cycle").unwrap();
        let time = measure_time(|| {
            schedule_cycle.call1((py.None(), create_config(py), platform_py.clone_ref(py))).unwrap();
        }).0;

        let waiting_jobs = platform.get_waiting_jobs();
        let waiting_jobs_map = waiting_jobs.iter().map(|job| (job.id, job.clone())).collect::<HashMap<u32, Job>>();
        let mut assigned_jobs = Vec::with_capacity(waiting_jobs.len());

        // Gather scheduled jobs scheduling data to update rust objects
        let scheduled_jobs_py: Vec<Bound<PyDict>> = platform_py
            .getattr(py, "scheduled_jobs_benchmark_report")
            .unwrap()
            .call0(py)
            .unwrap()
            .extract(py)
            .unwrap();
        for job_py in scheduled_jobs_py {
            // print job_py keys and values for debugging
            //info!("Job Py: {:?}", job_py);

            let id: u32 = job_py.get_item("id").unwrap().unwrap().extract::<u32>().unwrap();
            let quotas_hit_count: u32 = job_py.get_item("quotas_hit_count").unwrap().unwrap().extract().unwrap();
            let begin: i64 = job_py.get_item("begin").unwrap().unwrap().extract().unwrap();
            let end: i64 = job_py.get_item("end").unwrap().unwrap().extract().unwrap();
            let moldable_index: usize = job_py.get_item("moldable_index").unwrap().unwrap().extract().unwrap();
            let mut proc_set = ProcSet::new();
            let proc_set_list: Vec<Bound<PyTuple>> = job_py.get_item("proc_set").unwrap().unwrap().extract().unwrap();
            for range in proc_set_list {
                let start: u32 = range.get_item(0).unwrap().extract().unwrap();
                let end: u32 = range.get_item(1).unwrap().extract().unwrap();
                proc_set = proc_set | ProcSet::from_iter([start..=end]);
            }

            //info!("Extracted begin-end is {}-{}, proc_set = {:?}", begin, end, proc_set);
            let mut job = waiting_jobs_map.get(&id).cloned().unwrap();
            job.quotas_hit_count = quotas_hit_count;
            job.scheduled_data = Some(ScheduledJobData {
                begin,
                end,
                proc_set,
                moldable_index,
            });
            assigned_jobs.push(job.clone());
        }

        platform.set_scheduled_jobs(assigned_jobs);

        Ok::<u32, PyErr>(time)
    })
    .unwrap();

    (time, 0)
}

fn create_config(py: Python) -> Bound<PyAny> {
    // Create a default Configuration instance
    let config_module = PyModule::import(py, "oar.lib.configuration").unwrap();
    let config_class = config_module.getattr("Configuration").unwrap();

    config_class.call0().unwrap()
}

fn create_platform<T: PlatformTrait>(py: Python, platform: &T) -> Py<PyAny> {
    let platform_module = PyModule::import(py, "adapter").unwrap();
    let platform_class = platform_module.getattr("PlatformAdapter").unwrap();

    let platform = platform_to_dict(py, platform);

    let platform_instance = platform_class.call1((platform,)).unwrap();

    platform_instance.into()
}

pub fn platform_to_dict<'a, P: PlatformTrait>(py: Python<'a>, platform: &P) -> Bound<'a, PyDict> {
    let dict = PyDict::new(py);

    // Convert platform config
    dict.set_item("platform_config", platform.get_platform_config().as_ref()).unwrap();

    // Convert scheduled jobs
    let scheduled_jobs = PyList::empty(py);
    for job in platform.get_scheduled_jobs() {
        scheduled_jobs.append(job).unwrap();
    }
    dict.set_item("scheduled_jobs", scheduled_jobs).unwrap();

    // Convert waiting jobs
    let waiting_jobs = PyList::empty(py);
    for job in platform.get_waiting_jobs() {
        waiting_jobs.append(job).unwrap();
    }
    dict.set_item("waiting_jobs", waiting_jobs).unwrap();

    dict
}

use std::ffi::CStr;
use log::info;
use pyo3::ffi::c_str;
use crate::benchmark::platform_mock::PlatformBenchMock;
use crate::models::models::{Job, ProcSet};
use crate::platform::{PlatformConfig, PlatformTrait};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

const python_module_name: &str = "oar.kao.kamelot_basic";
const python_module_dir: &str = "/Users/clement/CodeIF/oar3/";
const python_site_packages_dir: &str = "/Users/clement/CodeIF/oar3/.venv/lib/python3.10/site-packages";

const adapter_module: &CStr = c_str!("adapter");
const adapter_file: &CStr = c_str!("adapter.py");
const adapter_code: &CStr = c_str!(include_str!("adapter.py"));

// Returns (elapsed ms, slot count)
pub fn schedule_cycle_on_oar_python<T: PlatformTrait>(platform: &mut T, _queues: Vec<String>) -> (u32, usize) {

    Python::with_gil(|py| {
        let sys = py.import("sys").unwrap();
        sys.getattr("path").unwrap().call_method1("append", (python_module_dir,)).unwrap();
        sys.getattr("path").unwrap().call_method1("append", (python_site_packages_dir,)).unwrap();

        PyModule::from_code(py, adapter_code, adapter_file, adapter_module).unwrap();

        log::info!("Python {:?}, path = {:?}", sys.getattr("version"), sys.getattr("path"));

        // Importer le module Python
        let kamelot_basic = py.import(python_module_name).unwrap();

        kamelot_basic
            .getattr("schedule_cycle").unwrap()
            .call1((py.None(), create_config(py), create_platform(py, platform))).unwrap();

        Ok::<(), PyErr>(())
    })
    .unwrap();

    (0, 0)
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
    let config_dict = platform_config_to_dict(py, &platform.get_platform_config());
    dict.set_item("platform_config", config_dict).unwrap();

    // Convert scheduled jobs
    let scheduled_jobs = PyList::empty(py);
    for job in platform.get_scheduled_jobs() {
        scheduled_jobs.append(job_to_dict(py, job)).unwrap();
    }
    dict.set_item("scheduled_jobs", scheduled_jobs).unwrap();

    // Convert waiting jobs
    let waiting_jobs = PyList::empty(py);
    for job in platform.get_waiting_jobs() {
        waiting_jobs.append(job_to_dict(py, job)).unwrap();
    }
    dict.set_item("waiting_jobs", waiting_jobs).unwrap();

    dict
}
fn platform_config_to_dict<'a>(py: Python<'a>, config: &PlatformConfig) -> Bound<'a, PyDict> {
    let dict = PyDict::new(py);

    dict.set_item("hour_size", config.hour_size).unwrap();
    dict.set_item("cache_enabled", config.cache_enabled).unwrap();

    // Convert ResourceSet
    let resource_set_dict = PyDict::new(py);
    // Convert default_intervals (assuming ProcSet can be converted to a list)
    let default_intervals = proc_set_to_python(py, &config.resource_set.default_intervals);
    resource_set_dict.set_item("default_intervals", default_intervals).unwrap();

    // Convert available_upto
    let available_upto = PyList::empty(py);
    for (time, proc_set) in &config.resource_set.available_upto {
        let tuple = (time, proc_set_to_python(py, proc_set));
        available_upto.append(tuple).unwrap();
    }
    resource_set_dict.set_item("available_upto", available_upto).unwrap();

    // Convert hierarchy (simplified)
    let hierarchy_dict = PyDict::new(py);
    // You'll need to implement proper conversion for Hierarchy
    hierarchy_dict.set_item(
        "unit_partition",
        config.resource_set.hierarchy.unit_partition.as_ref().map(|s| s.to_string()),
    ).unwrap();
    dict.set_item("resource_set", resource_set_dict).unwrap();
    dict.set_item("hierarchy", hierarchy_dict).unwrap();

    // Convert QuotasConfig
    let quotas_dict = PyDict::new(py);
    quotas_dict.set_item("enabled", config.quotas_config.enabled).unwrap();
    // Add other quotas config fields as needed
    dict.set_item("quotas_config", quotas_dict).unwrap();

    dict
}

fn proc_set_to_python<'a>(py: Python<'a>, proc_set: &ProcSet) -> Bound<'a, PyAny> {
    let procset_module = PyModule::import(py, "procset").unwrap();
    let procset_class = procset_module.getattr("ProcSet").unwrap();
    let procint_class = procset_module.getattr("ProcInt").unwrap();

    let list = PyList::empty(py);
    for range in proc_set.ranges() {
        list.append(procint_class.call1((range.start(), range.end())).unwrap()).unwrap();
    }

    info!("ProcSet {:?}", list);
    let procset_instance = procset_class.call1(PyTuple::new(py, list).unwrap()).unwrap();
    procset_instance
}

fn job_to_dict<'a>(py: Python<'a>, job: &Job) -> Bound<'a, PyDict> {
    let dict = PyDict::new(py);
    dict.set_item("id", job.id).unwrap();
    dict.set_item("user", job.user.as_str()).unwrap();
    dict.set_item("project", job.project.as_str()).unwrap();
    dict.set_item("types", job.types.iter().map(|s| s.as_str()).collect::<Vec<_>>()).unwrap();
    dict.set_item("queue", job.queue.as_str()).unwrap();
    dict.set_item(
        "moldables",
        job.moldables
            .iter()
            .enumerate()
            .map(|(i, m)| {
                let moldable_dict = PyDict::new(py);
                moldable_dict.set_item("index", i).unwrap();
                moldable_dict.set_item("walltime", m.walltime).unwrap();
                // Requests:
                moldable_dict.set_item(
                    "requests",
                    m.requests
                        .0
                        .iter()
                        .map(|r| {
                            let request_dict = PyDict::new(py);
                            request_dict.set_item("filter", proc_set_to_python(py, &r.filter)).unwrap();
                            request_dict.set_item(
                                "level_nbs",
                                r.level_nbs.iter().map(|n| {
                                    // Tuple like (n.0.to_string(), n.1)
                                    PyTuple::new(py, [n.0.to_string()])
                                        .unwrap()
                                        .add(PyTuple::new(py, [n.1]).unwrap())
                                        .unwrap()
                                }).collect::<Vec<_>>()
                            ).unwrap();
                            Ok(request_dict)
                        })
                        .collect::<PyResult<Vec<_>>>().unwrap(),
                ).unwrap();
                Ok(moldable_dict)
            })
            .collect::<PyResult<Vec<_>>>().unwrap(),
    ).unwrap();
    dict
}

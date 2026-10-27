//! Port of the energy saving tests of `oar3/tests/kao/test_db_all_in_one.py`
//! (`test_db_all_in_one_wakeup_node_*` and `test_db_all_in_one_sleep_node_*`).
//!
//! The Python tests replace `oar.lib.tools.fork_and_feed_stdin` by a fake that
//! records the nodes given to the node manager commands, and `zmq` by a fake
//! (`fakezmq`) that records the messages sent to Greta. Here:
//!
//! - the external node manager commands write their standard input to a file
//!   (`cat > file`), which is read by the test;
//! - the internal mode talks to a fake Greta pull socket bound on a local port.

use crate::energy_saving::NodeChanges;
use crate::meta_schedule::meta_schedule;
use crate::platform::Platform;
use crate::test::meta_schedule_test::{get_job, insert_job, insert_queue, setup_with_addresses_and_config};
use oar_scheduler_core::platform::PlatformTrait;
use oar_scheduler_db::model::jobs::JobState;
use oar_scheduler_db::model::resources::Resource;
use oar_scheduler_db::model::SqlEnum;
use std::path::PathBuf;
use std::time::Duration;

/// The 2-2-1 platform of the `test_db_all_in_one.py` fixture, with the energy
/// saving settings of the `active_energy_saving` fixture.
struct EnergySavingSetup {
    platform: Platform,
    sleep_output: PathBuf,
    wakeup_output: PathBuf,
}

/// Extracts `(host, port)` from a `tcp://host:port` endpoint.
fn split_endpoint(endpoint: &str) -> (String, String) {
    let address = endpoint.strip_prefix("tcp://").expect("the endpoint should be a tcp:// URI");
    let (host, port) = address.rsplit_once(':').expect("the endpoint should contain a port");
    (host.to_string(), port.to_string())
}

fn setup_energy_saving(internal: bool, greta_endpoint: Option<(String, String)>) -> EnergySavingSetup {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let sleep_output = directory.path().join("sleep_nodes");
    let wakeup_output = directory.path().join("wakeup_nodes");
    // The commands are run during the test: the directory must outlive the platform.
    std::mem::forget(directory);

    let sleep_cmd = format!("cat > {}", sleep_output.display());
    let wakeup_cmd = format!("cat > {}", wakeup_output.display());
    let internal_flag = if internal { "yes" } else { "no" };

    let (mut platform, _notifications) = setup_with_addresses_and_config(
        &["localhost0", "localhost0", "localhost1", "localhost1", "localhost2"],
        |config| {
            config.energy_saving_mode = Some("metascheduler_decision_making".to_string());
            config.energy_saving_internal = Some(internal_flag.to_string());
            config.scheduler_node_manager_sleep_cmd = Some(sleep_cmd.clone());
            config.scheduler_node_manager_wake_up_cmd = Some(wakeup_cmd.clone());
            config.scheduler_node_manager_sleep_time = Some(15);
            config.scheduler_node_manager_idle_time = Some(30);
            config.scheduler_node_manager_wakeup_time = Some(30);
            if let Some((host, port)) = greta_endpoint {
                config.greta_server = Some(host);
                config.greta_port = Some(port);
            }
        },
    );
    insert_queue(&platform, "default", 0);
    insert_queue(&platform, "admin", 100);
    EnergySavingSetup {
        platform,
        sleep_output,
        wakeup_output,
    }
}

/// Reads the node names written by a node manager command (sorted).
fn read_nodes(path: &PathBuf) -> Vec<String> {
    let content = std::fs::read_to_string(path).unwrap_or_default();
    let mut nodes = content.lines().map(|line| line.trim().to_string()).filter(|line| !line.is_empty()).collect::<Vec<_>>();
    nodes.sort();
    nodes
}

/// Sets the state and/or the `available_upto` date of every resource.
fn set_resources(platform: &Platform, state: Option<&str>, available_upto: Option<i64>) {
    for resource in Resource::get_all_sorted(&platform.session(), "resource_id", &vec![]).unwrap() {
        if let Some(state) = state {
            resource.set_state(&platform.session(), state).unwrap();
        }
        if let Some(available_upto) = available_upto {
            resource.set_available_upto(&platform.session(), available_upto).unwrap();
        }
    }
}

/// `oar3/tests/kao/test_db_all_in_one.py::test_db_all_in_one_wakeup_node_1`
///
/// The platform is asleep (`Absent`): the nodes needed by the job are woken up and
/// the job waits for them.
#[test]
fn test_db_all_in_one_wakeup_node_1() {
    let EnergySavingSetup {
        mut platform,
        wakeup_output,
        ..
    } = setup_energy_saving(false, None);
    let job_id = insert_job(&platform, "default", "resource_id=4", 60, vec![]);
    let now = platform.get_now();
    set_resources(&platform, Some("Absent"), Some(now + 1000));

    meta_schedule(&mut platform);

    assert_eq!(get_job(&platform, job_id).state, JobState::Waiting.as_str());
    assert_eq!(read_nodes(&wakeup_output), vec!["localhost0", "localhost1"]);
}

/// `oar3/tests/kao/test_db_all_in_one.py::test_db_all_in_one_sleep_node_1`
///
/// The nodes that are not used by the scheduled job are halted.
#[test]
fn test_db_all_in_one_sleep_node_1() {
    let EnergySavingSetup {
        mut platform,
        sleep_output,
        ..
    } = setup_energy_saving(false, None);
    let now = platform.get_now();
    let job_id = insert_job(&platform, "default", "resource_id=1", 60, vec![]);
    set_resources(&platform, None, Some(now + 50000));

    meta_schedule(&mut platform);

    assert_eq!(get_job(&platform, job_id).state, JobState::ToLaunch.as_str());
    assert_eq!(read_nodes(&sleep_output), vec!["localhost1", "localhost2"]);
}

/// `oar3/tests/kao/test_db_all_in_one.py::test_db_all_in_one_wakeup_node_energy_saving_internal_1`
///
/// With `ENERGY_SAVING_INTERNAL=yes`, the wake up command is sent to Greta.
#[test]
fn test_db_all_in_one_wakeup_node_energy_saving_internal_1() {
    let context = omq::Context::new();
    let greta = context.blocking_socket(omq::SocketType::Pull, omq::Options::default());
    let endpoint = split_endpoint(&greta.bind("tcp://127.0.0.1:0").expect("bind the fake Greta").to_string());

    let EnergySavingSetup { mut platform, .. } = setup_energy_saving(true, Some(endpoint));
    let job_id = insert_job(&platform, "default", "resource_id=4", 60, vec![]);
    let now = platform.get_now();
    set_resources(&platform, Some("Absent"), Some(now + 1000));

    meta_schedule(&mut platform);

    assert_eq!(get_job(&platform, job_id).state, JobState::Waiting.as_str());
    let message = greta
        .recv_timeout(Duration::from_secs(10))
        .expect("Greta should receive the wake up command");
    assert_eq!(
        String::from_utf8(message[0].to_vec()).unwrap(),
        r#"{"cmd":"WAKEUP","nodes":["localhost0","localhost1"]}"#
    );
}

/// `oar3/tests/kao/test_db_all_in_one.py::test_db_all_in_one_sleep_node_energy_saving_internal_1`
///
/// With `ENERGY_SAVING_INTERNAL=yes`, the halt command is sent to Greta.
#[test]
fn test_db_all_in_one_sleep_node_energy_saving_internal_1() {
    let context = omq::Context::new();
    let greta = context.blocking_socket(omq::SocketType::Pull, omq::Options::default());
    let endpoint = split_endpoint(&greta.bind("tcp://127.0.0.1:0").expect("bind the fake Greta").to_string());

    let EnergySavingSetup { mut platform, .. } = setup_energy_saving(true, Some(endpoint));
    let now = platform.get_now();
    let job_id = insert_job(&platform, "default", "resource_id=1", 60, vec![]);
    set_resources(&platform, None, Some(now + 50000));

    meta_schedule(&mut platform);

    assert_eq!(get_job(&platform, job_id).state, JobState::ToLaunch.as_str());
    let message = greta
        .recv_timeout(Duration::from_secs(10))
        .expect("Greta should receive the halt command");
    let message = String::from_utf8(message[0].to_vec()).unwrap();
    assert!(message.starts_with(r#"{"cmd":"HALT","nodes":["#), "unexpected message: {}", message);
    assert!(message.contains("localhost1"), "unexpected message: {}", message);
    assert!(message.contains("localhost2"), "unexpected message: {}", message);
}

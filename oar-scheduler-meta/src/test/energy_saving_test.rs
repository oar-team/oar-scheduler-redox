use crate::energy_saving::{NodeChanges, apply_nodes_energy_saving};
use crate::test::meta_schedule_test::setup_with_config;
use std::time::Duration;

/// Extracts `(host, port)` from a `tcp://host:port` endpoint.
fn split_endpoint(endpoint: &str) -> (String, String) {
    let address = endpoint.strip_prefix("tcp://").expect("the endpoint should be a tcp:// URI");
    let (host, port) = address.rsplit_once(':').expect("the endpoint should contain a port");
    (host.to_string(), port.to_string())
}

/// With `ENERGY_SAVING_INTERNAL=yes`, the node changes are pushed to Greta over
/// ZMQ (`HALT`, `WAKEUP`, and a `CHECK` when there is nothing to do) and the
/// external node manager commands are *not* run.
#[test]
fn test_internal_energy_saving_sends_commands_to_greta() {
    let context = omq::Context::new();
    let greta = context.blocking_socket(omq::SocketType::Pull, omq::Options::default());
    let (host, port) = split_endpoint(&greta.bind("tcp://127.0.0.1:0").expect("bind the fake Greta").to_string());

    // The external commands would create this file: they must not be run.
    let directory = tempfile::tempdir().expect("a temporary directory");
    let marker = directory.path().join("external_node_manager_command_ran");
    let marker_command = marker.clone();

    let platform = setup_with_config(1, move |config| {
        config.energy_saving_internal = Some("yes".to_string());
        config.greta_server = Some(host);
        config.greta_port = Some(port);
        config.scheduler_node_manager_sleep_cmd = Some(format!("touch {}", marker_command.display()));
        config.scheduler_node_manager_wake_up_cmd = Some(format!("touch {}", marker_command.display()));
    });

    let recv = || {
        let message = greta
            .recv_timeout(Duration::from_secs(10))
            .expect("Greta should receive the energy saving command");
        String::from_utf8(message[0].to_vec()).expect("the command should be valid UTF-8")
    };

    apply_nodes_energy_saving(
        &platform,
        &NodeChanges {
            halt: vec!["node1".to_string()],
            wakeup: vec![],
        },
    );
    assert_eq!(recv(), r#"{"cmd":"HALT","nodes":["node1"]}"#);

    apply_nodes_energy_saving(
        &platform,
        &NodeChanges {
            halt: vec![],
            wakeup: vec!["node2".to_string()],
        },
    );
    assert_eq!(recv(), r#"{"cmd":"WAKEUP","nodes":["node2"]}"#);

    // Nothing to halt or to wake up: Greta is asked to check its nodes.
    apply_nodes_energy_saving(&platform, &NodeChanges::default());
    assert_eq!(recv(), r#"{"cmd":"CHECK"}"#);

    assert!(
        !marker.exists(),
        "the external node manager commands must not be run when the internal module is enabled"
    );
}

/// Without `ENERGY_SAVING_INTERNAL`, the external node manager commands are run
/// and nothing is sent to Greta.
#[test]
fn test_external_energy_saving_does_not_use_greta() {
    let context = omq::Context::new();
    let greta = context.blocking_socket(omq::SocketType::Pull, omq::Options::default());
    let (host, port) = split_endpoint(&greta.bind("tcp://127.0.0.1:0").expect("bind the fake Greta").to_string());

    let directory = tempfile::tempdir().expect("a temporary directory");
    let marker = directory.path().join("external_node_manager_command_ran");
    let marker_command = marker.clone();

    let platform = setup_with_config(1, move |config| {
        config.energy_saving_internal = Some("no".to_string());
        config.greta_server = Some(host);
        config.greta_port = Some(port);
        config.scheduler_node_manager_sleep_cmd = Some(format!("touch {}", marker_command.display()));
    });

    apply_nodes_energy_saving(
        &platform,
        &NodeChanges {
            halt: vec!["node1".to_string()],
            wakeup: vec![],
        },
    );

    assert!(marker.exists(), "the external node manager command should have been run");
    assert!(
        greta.recv_timeout(Duration::from_millis(500)).is_err(),
        "nothing should be sent to Greta when the internal module is disabled"
    );
}

//! Port of `oar3/tests/kao/test_db_walltime_change.py`.
//!
//! The tests of `oarwalltime` request validation are not ported: the Rust port
//! has no user-facing walltime change request API (no `walltime.request`), only
//! the database primitives (`insert_walltime_change_request`,
//! `process_walltime_change_requests`). See `docs/tests/oar3-port.md`.

use crate::platform::Platform;
use crate::test::meta_schedule_test::{insert_job, setup};
use crate::test::oar3_metasched_test::{insert_queues, insert_running_job};
use crate::walltime_change::{get_conf, process_walltime_change_requests};
use oar_scheduler_core::platform::PlatformTrait;
use oar_scheduler_db::model::meta;

/// `oar3/tests/kao/test_db_walltime_change.py::test_process_walltime_change_requests_void`
#[test]
fn test_process_walltime_change_requests_void() {
    let (mut platform, _notifications) = setup(5);
    insert_queues(&platform);

    process_walltime_change_requests(&platform);

    assert!(meta::get_jobs_with_walltime_change(&platform.session()).unwrap().is_empty());
}

/// `oar3/tests/kao/test_db_walltime_change.py::test_process_walltime_change_requests_job_not_running`
#[test]
fn test_process_walltime_change_requests_job_not_running() {
    let (mut platform, _notifications) = setup(5);
    insert_queues(&platform);
    let job_id = insert_job(&platform, "default", "resource_id=4", 60, vec![]);
    meta::insert_walltime_change_request(&platform.session(), job_id, 3663, "NO", "NO").unwrap();

    process_walltime_change_requests(&platform);

    let (pending, granted) = meta::get_walltime_change(&platform.session(), job_id)
        .unwrap()
        .expect("the request should still exist");
    assert_eq!(granted, 0);
    assert_eq!(pending, 3663);
}

/// Returns the description of the `WALLTIME` event of a job (the Python tests
/// query the single event logged for the job).
fn walltime_change_event(platform: &Platform, job_id: i64) -> String {
    let events = meta::get_job_events(&platform.session(), job_id).unwrap();
    assert_eq!(events.len(), 1, "exactly one event should have been logged: {:?}", events);
    events.into_iter().next().unwrap().description
}

/// `oar3/tests/kao/test_db_walltime_change.py::test_process_walltime_change_requests`
///
/// The Python test is skipped on SQLite ("bug raises with sqlite database"); the
/// Rust port works on both backends.
#[test]
fn test_process_walltime_change_requests() {
    let (mut platform, _notifications) = setup(5);
    insert_queues(&platform);
    let now = platform.get_now();
    let job_id = insert_running_job(&platform, "default", 2, 60, now, vec![]);
    meta::insert_walltime_change_request(&platform.session(), job_id, 3663, "NO", "NO").unwrap();

    process_walltime_change_requests(&platform);

    let (pending, granted) = meta::get_walltime_change(&platform.session(), job_id).unwrap().unwrap();
    assert_eq!(granted, 3663);
    assert_eq!(pending, 0);
    assert_eq!(
        walltime_change_event(&platform, job_id),
        "walltime changed: 1:2:3 (granted: +1:1:3/pending: 0:0:0)"
    );
}

/// `oar3/tests/kao/test_db_walltime_change.py::test_process_walltime_change_requests_inner`
///
/// The walltime change of an `inner` job cannot exceed the end of its container.
#[test]
fn test_process_walltime_change_requests_inner() {
    let (mut platform, _notifications) = setup(5);
    insert_queues(&platform);
    let now = platform.get_now();

    let container_id = insert_running_job(&platform, "default", 2, 60, now, vec!["container".to_string()]);
    let inner_id = insert_running_job(&platform, "default", 2, 25, now, vec![format!("inner={}", container_id)]);
    meta::insert_walltime_change_request(&platform.session(), inner_id, 3663, "NO", "NO").unwrap();

    process_walltime_change_requests(&platform);

    let (pending, granted) = meta::get_walltime_change(&platform.session(), inner_id).unwrap().unwrap();
    assert_eq!(granted, 35);
    assert_eq!(pending, 3628);
    assert_eq!(
        walltime_change_event(&platform, inner_id),
        "walltime changed: 0:1:0 (granted: +0:0:35/pending: +1:0:28)"
    );
}

/// `oar3/tests/kao/test_db_walltime_change.py::test_walltime_get_conf`
///
/// Divergence: `oar.lib.walltime.get_conf` returns the raw configuration value
/// when it is not numeric (`"*"`, `"alice,bob"`, `""`), whereas the Rust
/// implementation is typed (`i64`) and falls back to `default` (0) in that case.
#[test]
fn test_walltime_get_conf() {
    // (configuration value, queue, expected)
    let cases: &[(&str, &str, i64)] = &[
        ("*", "default", 0), // Python: "*"
        ("alice,bob", "default", 0), // Python: "alice,bob"
        ("", "default", 0),  // Python: ""
        ("7200", "default", 7200),
        ("{'default': 7200}", "default", 7200),
        ("{'default': 7200}", "besteffort", 0),
        ("{'_': '*', 'besteffort': ''}", "default", 0), // Python: "*"
        ("{'_': '*', 'besteffort': ''}", "besteffort", 0), // Python: ""
        ("{default => 7200}", "default", 7200),
        ("{_ => '*', besteffort => ''}", "default", 0), // Python: "*"
        ("{_ => '*', besteffort => ''}", "besteffort", 0), // Python: ""
        ("{default => }", "default", 0),
        ("{1, 2}", "default", 0),
    ];
    for (value, queue, expected) in cases {
        assert_eq!(
            get_conf(Some(value), queue, 0, 0),
            *expected,
            "get_conf({:?}, {:?})",
            value,
            queue
        );
    }
    // No configuration value at all: the default is returned.
    assert_eq!(get_conf(None, "default", 0, 42), 42);
    // Values between 0 and 1 are a ratio of the walltime.
    assert_eq!(get_conf(Some("0.5"), "default", 3600, 0), 1800);
}

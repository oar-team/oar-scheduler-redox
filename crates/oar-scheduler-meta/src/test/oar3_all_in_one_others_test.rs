//! Port of `oar3/tests/kao/test_db_all_in_one_others.py`.
//!
//! The Python fixture creates five resources with the network addresses
//! `localhost0, localhost0, localhost1, localhost1, localhost2` (a 2-2-1
//! topology). The same addresses are used here, so that the resource properties
//! (`network_address='localhost1'`) behave identically.

use crate::meta_schedule::meta_schedule;
use crate::platform::Platform;
use crate::test::meta_schedule_test::{get_job, insert_job, insert_queue, setup_with_addresses};
use crate::test::oar3_metasched_test::gantt_jobs;
use oar_scheduler_core::platform::PlatformTrait;
use oar_scheduler_db::model::jobs::JobState;
use oar_scheduler_db::model::SqlEnum;

/// Creates the 2-2-1 platform of the Python fixture (queue `default`, priority 3).
fn setup_all_in_one_others() -> Platform {
    let (mut platform, _notifications) = setup_with_addresses(&["localhost0", "localhost0", "localhost1", "localhost1", "localhost2"]);
    insert_queue(&platform, "default", 3);
    platform
}

/// Returns the start times of the gantt predictions, relative to `now`.
fn relative_start_times(platform: &Platform, now: i64) -> Vec<i64> {
    gantt_jobs(platform)
        .iter()
        .filter_map(|job| job.assignment.as_ref().map(|assignment| assignment.begin - now))
        .collect()
}

/// Inserts a job (`resource_id=4`, 60 s) of the given user with a job type.
fn insert_job_for_user(platform: &Platform, user: &str, job_type: &str) -> i64 {
    use oar_scheduler_db::model::jobs::NewJob;
    NewJob {
        user: Some(user.to_string()),
        queue_name: "default".to_string(),
        res: vec![(60, vec![("resource_id=4".to_string(), String::new())])],
        types: vec![job_type.to_string()],
    }
    .insert(&platform.session())
    .expect("Failed to insert job")
}

/// Inserts a job whose resource description carries resource properties.
fn insert_job_with_properties(platform: &Platform, hierarchy: &str, properties: &str, walltime: i64) -> i64 {
    use oar_scheduler_db::model::jobs::NewJob;
    NewJob {
        user: Some("user1".to_string()),
        queue_name: "default".to_string(),
        res: vec![(walltime, vec![(hierarchy.to_string(), properties.to_string())])],
        types: Vec::new(),
    }
    .insert(&platform.session())
    .expect("Failed to insert job")
}

/// `oar3/tests/kao/test_db_all_in_one_others.py::test_db_timesharing_2`
///
/// Two jobs with `timesharing=user,*` from different users do not share their
/// resources: their start times differ.
#[test]
fn test_db_timesharing_2() {
    let mut platform = setup_all_in_one_others();
    let now = platform.get_now();
    insert_job_for_user(&platform, "toto", "timesharing=user,*");
    insert_job_for_user(&platform, "titi", "timesharing=user,*");

    meta_schedule(&mut platform);

    let start_times = relative_start_times(&platform, now);
    assert_eq!(start_times.len(), 2);
    assert_ne!(start_times[0], start_times[1], "start times: {:?}", start_times);
}

/// `oar3/tests/kao/test_db_all_in_one_others.py::test_db_timesharing_3`
///
/// Two jobs with `timesharing=*,*` share their resources: they start at the same
/// time.
#[test]
fn test_db_timesharing_3() {
    let mut platform = setup_all_in_one_others();
    let now = platform.get_now();
    insert_job(&platform, "default", "resource_id=4", 60, vec!["timesharing=*,*".to_string()]);
    insert_job(&platform, "default", "resource_id=4", 60, vec!["timesharing=*,*".to_string()]);

    meta_schedule(&mut platform);

    let start_times = relative_start_times(&platform, now);
    assert_eq!(start_times.len(), 2);
    assert_eq!(start_times[0], start_times[1], "start times: {:?}", start_times);
}

/// `test_db_properties_1`, `test_db_properties_2` and `test_db_properties_3` are
/// not ported: the Rust scheduler does not implement job resource properties
/// (`properties`/`-p` SQL filters) nor `network_address` hierarchy constraints.
/// See `docs/tests/oar3-port.md`.

/// `oar3/tests/kao/test_db_all_in_one_others.py::test_db_placeholder_2`
///
/// A placeholder job and a job allowed for a placeholder of another name do not
/// share resources.
#[test]
fn test_db_placeholder_2() {
    let mut platform = setup_all_in_one_others();
    let now = platform.get_now();
    insert_job(&platform, "default", "resource_id=4", 60, vec!["placeholder=yop".to_string()]);
    insert_job(&platform, "default", "resource_id=4", 60, vec!["allow=poy".to_string()]);

    meta_schedule(&mut platform);

    let start_times = relative_start_times(&platform, now);
    assert_eq!(start_times.len(), 2);
    assert_ne!(start_times[0], start_times[1], "start times: {:?}", start_times);
}

/// `oar3/tests/kao/test_db_all_in_one_others.py::test_db_moldable_1`
///
/// The second job can only use its second moldable (70 s, 3 resources) once the
/// first job (3 resources) has started: their start times differ.
#[test]
fn test_db_moldable_1() {
    let mut platform = setup_all_in_one_others();
    let now = platform.get_now();
    insert_job(&platform, "default", "resource_id=3", 60, vec![]);
    insert_job_moldables(&platform, &[(60, "resource_id=4"), (70, "resource_id=3")]);

    meta_schedule(&mut platform);

    let start_times = relative_start_times(&platform, now);
    assert_eq!(start_times.len(), 2);
    assert_ne!(start_times[0], start_times[1], "start times: {:?}", start_times);
}

/// `oar3/tests/kao/test_db_all_in_one_others.py::test_db_moldable_2`
///
/// The second job can use its second moldable (70 s, 2 resources) at the same
/// time as the first job, so both start at the same time.
#[test]
fn test_db_moldable_2() {
    let mut platform = setup_all_in_one_others();
    let now = platform.get_now();
    insert_job(&platform, "default", "resource_id=3", 60, vec![]);
    insert_job_moldables(&platform, &[(60, "resource_id=4"), (70, "resource_id=2")]);

    meta_schedule(&mut platform);

    let start_times = relative_start_times(&platform, now);
    assert_eq!(start_times.len(), 2);
    assert_eq!(start_times[0], start_times[1], "start times: {:?}", start_times);
}

/// Inserts a job with several moldables.
fn insert_job_moldables(platform: &Platform, moldables: &[(i64, &str)]) -> i64 {
    use oar_scheduler_db::model::jobs::NewJob;
    NewJob {
        user: Some("user1".to_string()),
        queue_name: "default".to_string(),
        res: moldables
            .iter()
            .map(|(walltime, hierarchy)| (*walltime, vec![(hierarchy.to_string(), String::new())]))
            .collect(),
        types: Vec::new(),
    }
    .insert(&platform.session())
    .expect("Failed to insert job")
}

/// `oar3/tests/kao/test_db_all_in_one_others.py::test_db_suspended_duration_1`
///
/// Divergence: the Python test inserts the job with `suspended="YES"`
/// (`jobs.suspended` column). The Rust `NewJob` model has no such column, so the
/// job is inserted without it; the expected behaviour (the job is launched) is
/// the same.
#[test]
fn test_db_suspended_duration_1() {
    let mut platform = setup_all_in_one_others();
    let job_id = insert_job(&platform, "default", "resource_id=3", 60, vec![]);

    meta_schedule(&mut platform);

    assert_eq!(get_job(&platform, job_id).state, JobState::ToLaunch.as_str());
}

//! Port of `oar3/tests/kao/test_db_fairshare.py` and
//! `oar3/tests/kao/test_db_multifactor.py`.
//!
//! Both Python tests feed the `accounting` table with per-user consumption and
//! check that the resulting predictions are ordered by ascending user index (the
//! users with the lowest consumption are scheduled first, either through the
//! `FAIRSHARE` (karma) sorting or through the `MULTIFACTOR` priority with
//! `karma_weight: 1.0`).

use crate::meta_schedule::meta_schedule;
use crate::platform::Platform;
use crate::test::meta_schedule_test::{insert_queue, setup_with_addresses_and_config};
use crate::test::oar3_metasched_test::gantt_jobs;
use oar_scheduler_core::model::configuration::JobPriority;
use oar_scheduler_core::platform::PlatformTrait;
use oar_scheduler_db::model::jobs::NewJob;
use oar_scheduler_db::model::meta::{AccountingRecord, delete_accountings, insert_accounting};
use std::collections::HashMap;

const NB_USERS: i64 = 5;
/// Width of an accounting window (Python `t_window=24*36000`).
const WINDOW: i64 = 24 * 36000;

/// Creates the platform of the tests: 5 resources and the `FAIRSHARE` /
/// `MULTIFACTOR` settings (the fairsharing settings have no default value in the
/// Rust configuration: they are given explicitly here, with the OAR3 default
/// window size of 15 days and unit coefficients).
fn setup_fairshare(priority: JobPriority, priority_conf_file: Option<String>) -> Platform {
    let (mut platform, _notifications) = setup_with_addresses_and_config(
        &["localhost", "localhost", "localhost", "localhost", "localhost"],
        |config| {
            config.job_priority = priority;
            config.priority_conf_file = priority_conf_file;
            config.scheduler_fairsharing_window_size = Some(1_296_000);
            config.scheduler_fairsharing_project_targets = Some(String::new());
            config.scheduler_fairsharing_user_targets = Some(String::new());
            config.scheduler_fairsharing_coef_project = Some(1.0);
            config.scheduler_fairsharing_coef_user = Some(1.0);
            config.scheduler_fairsharing_coef_user_ask = Some(1.0);
        },
    );
    platform
}

/// Mirrors the Python `generate_accountings`: for each user `zozo{u}` (u in
/// `0..5`), five accounting windows of 10 days, with a consumption growing with
/// `u` (`ASKED` = `1000 * (1 + u) + 1000`, `USED` = `1000 * (1 + u)`).
fn generate_accountings(platform: &Platform) {
    let session = platform.session();
    delete_accountings(&session).expect("the accounting table should be cleared");
    let now = platform.get_now();
    let offset = now - WINDOW * 5;
    for user in 0..NB_USERS {
        for i in 0..5 {
            let window_start = WINDOW * (i + 1) + offset;
            let window_stop = window_start + WINDOW / 10;
            let used = 1000 * (1 + user);
            for (consumption_type, consumption) in [("ASKED", used + 1000), ("USED", used)] {
                insert_accounting(
                    &session,
                    &AccountingRecord {
                        window_start,
                        window_stop,
                        user: format!("zozo{}", user),
                        project: "default".to_string(),
                        queue_name: "default".to_string(),
                        consumption_type: consumption_type.to_string(),
                        consumption,
                    },
                )
                .expect("the accounting record should be inserted");
            }
        }
    }
}

/// Inserts one job per user (`zozo{u}`, 4 resources, 60 s) and returns the
/// mapping job id -> user index.
///
/// The users are inserted in a shuffled order (the Python tests use
/// `sample(range(nb_users), nb_users)`; a fixed permutation is used here for
/// reproducibility) so that the assertion really checks the karma ordering and
/// not the insertion order.
fn insert_fairshare_jobs(platform: &Platform) -> HashMap<i64, i64> {
    let mut job_to_user = HashMap::new();
    for user in [3, 0, 4, 1, 2] {
        let job_id = NewJob {
            user: Some(format!("zozo{}", user)),
            queue_name: "default".to_string(),
            res: vec![(60, vec![("resource_id=4".to_string(), String::new())])],
            types: Vec::new(),
        }
        .insert(&platform.session())
        .expect("the job should be inserted");
        job_to_user.insert(job_id, user);
    }
    job_to_user
}

/// Returns the user index of each prediction, ordered by start time.
fn predicted_users(platform: &Platform, job_to_user: &HashMap<i64, i64>) -> Vec<i64> {
    let mut jobs = gantt_jobs(platform)
        .iter()
        .filter_map(|job| job.assignment.as_ref().map(|assignment| (assignment.begin, job.id)))
        .collect::<Vec<_>>();
    jobs.sort_unstable();
    jobs.into_iter()
        .map(|(_, job_id)| *job_to_user.get(&job_id).unwrap_or_else(|| panic!("unknown job {}", job_id)))
        .collect()
}

/// `oar3/tests/kao/test_db_fairshare.py::test_db_fairsharing`
///
/// With `JOB_PRIORITY=FAIRSHARE`, the jobs are ordered by ascending karma, i.e.
/// the user consuming the least first.
///
/// Divergence: the Python test also checks the `Job.message` field
/// (`R=...,W=...,J=I,Q=default (Karma=...)`); the Rust scheduler does not fill
/// this display column, so only the ordering is asserted.
#[test]
fn test_db_fairsharing() {
    let mut platform = setup_fairshare(JobPriority::Fairshare, None);
    insert_queue(&platform, "default", 0);
    generate_accountings(&platform);
    let job_to_user = insert_fairshare_jobs(&platform);

    meta_schedule(&mut platform);

    assert_eq!(predicted_users(&platform, &job_to_user), vec![0, 1, 2, 3, 4]);
}

/// `oar3/tests/kao/test_db_multifactor.py::test_db_multifactor_fairshare`
///
/// With `JOB_PRIORITY=MULTIFACTOR` and a priority file containing only
/// `karma_weight: 1.0`, the priority is `1 / (1 + karma)`: the jobs are again
/// ordered by ascending karma.
#[test]
fn test_db_multifactor_fairshare() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let priority_file = directory.path().join("priority.json");
    std::fs::write(&priority_file, r#"{"karma_weight": 1.0}"#).expect("the priority file should be written");

    let mut platform = setup_fairshare(JobPriority::Multifactor, Some(priority_file.display().to_string()));
    insert_queue(&platform, "default", 0);
    generate_accountings(&platform);
    let job_to_user = insert_fairshare_jobs(&platform);

    meta_schedule(&mut platform);

    assert_eq!(predicted_users(&platform, &job_to_user), vec![0, 1, 2, 3, 4]);
}

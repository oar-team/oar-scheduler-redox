//! Port of the OAR3 meta-scheduler database tests.
//!
//! Each test is a port of a test from `oar3/tests/kao/test_db_metasched.py` or
//! `oar3/tests/kao/test_db_walltime_change.py`. The Python tests monkeypatch
//! `oar.lib.tools.get_date` to control the current time; the Rust tests use
//! [`Platform::set_now`].

use crate::meta_schedule::meta_schedule;
use crate::platform::Platform;
use crate::test::meta_schedule_test::{get_job, insert_job, insert_queue, setup, setup_with_config};
use oar_scheduler_core::model::job::ProcSet;
use oar_scheduler_core::platform::{Job, PlatformTrait};
use oar_scheduler_db::model::jobs::{JobDatabaseRequests, JobReservation, JobState};
use oar_scheduler_db::model::meta;
use oar_scheduler_db::model::moldable::MoldableDatabaseRequests;
use oar_scheduler_db::model::resources::NewResource;
use oar_scheduler_db::model::SqlEnum;

/// Creates the two queues used by `test_db_metasched.py`.
pub(crate) fn insert_queues(platform: &Platform) {
    insert_queue(platform, "default", 3);
    insert_queue(platform, "besteffort", 0);
}

/// Inserts an advance reservation job, awaiting to be scheduled: state `Waiting`,
/// reservation `toSchedule`, a start time and the `oarsub` client address.
fn insert_ar_job(platform: &Platform, hierarchy: &str, walltime: i64, start_time: i64, info_type: &str) -> i64 {
    let job_id = insert_job(platform, "default", hierarchy, walltime, vec![]);
    let job = get_job(platform, job_id);
    job.set_resa_state(&platform.session(), JobReservation::ToSchedule.as_str()).unwrap();
    job.set_start_time(&platform.session(), start_time).unwrap();
    job.set_info_type(&platform.session(), Some(info_type)).unwrap();
    job_id
}

/// Inserts a running job with `res_count` assigned resources (mirrors the Python
/// `insert_running_jobs` helper and `assign_resources`).
pub(crate) fn insert_running_job(platform: &Platform, queue: &str, res_count: u32, walltime: i64, start_time: i64, types: Vec<String>) -> i64 {
    let job_id = insert_job(platform, queue, format!("resource_id={}", res_count).as_str(), walltime, types);
    let job = get_job(platform, job_id);
    let moldable = job.moldables.first().expect("the job has a moldable");
    job.assign_moldable_and_set_start_time(&platform.session(), moldable.id, start_time)
        .unwrap();
    job.set_state(&platform.session(), JobState::Running).unwrap();
    moldable
        .save_resources_as_assigned_resources(&platform.session(), &ProcSet::from_iter([0..=(res_count - 1)]))
        .unwrap();
    job_id
}

pub(crate) fn gantt_jobs(platform: &Platform) -> Vec<Job> {
    Job::get_gantt_jobs(&platform.session(), None, None, None, None).expect("Failed to fetch the gantt jobs")
}

/// `oar3/tests/kao/test_db_metasched.py::test_db_metasched_simple_1`
#[test]
fn test_db_metasched_simple_1() {
    let (mut platform, _notifications) = setup(5);
    insert_queues(&platform);
    let job_id = insert_job(&platform, "default", "resource_id=4", 60, vec![]);

    assert_eq!(meta_schedule(&mut platform), 0);

    assert_eq!(get_job(&platform, job_id).state, JobState::ToLaunch.as_str());
}

/// `oar3/tests/kao/test_db_metasched.py::test_db_metasched_ar_1`
///
/// An advance reservation is scheduled (`reservation` becomes `Scheduled`) but
/// stays in the `Waiting` state until its start time is reached.
#[test]
fn test_db_metasched_ar_1() {
    let (mut platform, _notifications) = setup(5);
    insert_queues(&platform);
    let now = platform.get_now();
    let job_id = insert_ar_job(&platform, "resource_id=4", 60, now + 10, "localhost:4242");

    meta_schedule(&mut platform);

    let job = get_job(&platform, job_id);
    assert_eq!(job.state, "Waiting");
    assert_eq!(job.reservation.as_ref(), JobReservation::Scheduled.as_str());
}

/// `oar3/tests/kao/test_db_metasched.py::test_db_metasched_ar_check_kill_be`
///
/// A besteffort job occupying the resources of a reservation that is about to
/// start must be flagged for deletion.
#[test]
fn test_db_metasched_ar_check_kill_be() {
    let (mut platform, _notifications) = setup(5);
    insert_queues(&platform);

    let now = platform.get_now();
    // The reservation is created "in the past" (now = 100) but starts at `now`.
    platform.set_now(100);
    insert_ar_job(&platform, "resource_id=4", 60, now, "localhost:4242");
    meta_schedule(&mut platform);
    platform.set_now(now);

    let be_job_id = insert_running_job(&platform, "besteffort", 5, 500, now, vec!["besteffort".to_string()]);

    meta_schedule(&mut platform);

    let fragged = meta::get_fragged_jobs(&platform.session()).unwrap();
    assert_eq!(fragged, vec![be_job_id]);
}

/// `oar3/tests/kao/test_db_metasched.py::test_db_metasched_ar_check_kill_be_security_time`
///
/// With a besteffort kill duration of 120 s, a besteffort job must be killed
/// even though the reservation starts 60 s in the future.
#[test]
fn test_db_metasched_ar_check_kill_be_security_time() {
    let mut platform = setup_with_config(5, |config| {
        config.scheduler_besteffort_kill_duration_before_reservation = 120;
    });
    insert_queues(&platform);

    let now = platform.get_now();
    platform.set_now(100);
    insert_ar_job(&platform, "resource_id=4", 60, now + 60, "localhost:4242");
    meta_schedule(&mut platform);
    platform.set_now(now);

    let be_job_id = insert_running_job(&platform, "besteffort", 5, 500, now - 250, vec!["besteffort".to_string()]);

    meta_schedule(&mut platform);

    let fragged = meta::get_fragged_jobs(&platform.session()).unwrap();
    assert_eq!(fragged, vec![be_job_id]);
}

/// `oar3/tests/kao/test_db_metasched.py::test_db_metasched_ar_check_no_be_security_time`
///
/// With a besteffort kill duration of 60 s, a reservation starting 120 s in the
/// future is too far away: the besteffort job must not be killed.
#[test]
fn test_db_metasched_ar_check_no_be_security_time() {
    let mut platform = setup_with_config(5, |config| {
        config.scheduler_besteffort_kill_duration_before_reservation = 60;
        config.scheduler_job_security_time = 0;
    });
    insert_queues(&platform);

    let now = platform.get_now();
    platform.set_now(100);
    insert_ar_job(&platform, "resource_id=4", 60, now + 120, "localhost:4242");
    meta_schedule(&mut platform);
    platform.set_now(now);

    insert_running_job(&platform, "besteffort", 5, 500, now - 250, vec!["besteffort".to_string()]);

    meta_schedule(&mut platform);

    let fragged = meta::get_fragged_jobs(&platform.session()).unwrap();
    assert!(fragged.is_empty(), "no besteffort job should be killed, fragged: {:?}", fragged);
}

/// `oar3/tests/kao/test_db_metasched.py::test_db_metasched_be_released_for_job`
///
/// A besteffort job is released when a higher priority job is scheduled on its
/// resources.
#[test]
fn test_db_metasched_be_released_for_job() {
    let (mut platform, _notifications) = setup(5);
    insert_queues(&platform);

    let now = platform.get_now();
    platform.set_now(100);
    let be_job_id = insert_running_job(&platform, "besteffort", 5, 500, now - 250, vec!["besteffort".to_string()]);
    meta_schedule(&mut platform);
    platform.set_now(now);

    insert_job(&platform, "default", "resource_id=4", 500, vec![]);

    meta_schedule(&mut platform);

    let fragged = meta::get_fragged_jobs(&platform.session()).unwrap();
    assert_eq!(fragged, vec![be_job_id]);
}

/// `oar3/tests/kao/test_db_metasched.py::test_db_metasched_ar_2`
///
/// Three reservations are submitted for the same start time on a five resources
/// platform: the first and the third fit (4 + 1 resources), the second (2
/// resources) does not, so only two reservations are scheduled.
#[test]
fn test_db_metasched_ar_2() {
    let (mut platform, _notifications) = setup(5);
    insert_queues(&platform);

    let in_the_future = platform.get_now();
    platform.set_now(100);
    insert_ar_job(&platform, "resource_id=4", 60, in_the_future, "localhost:4242");
    insert_ar_job(&platform, "resource_id=2", 60, in_the_future, "localhost:4243");
    insert_ar_job(&platform, "resource_id=1", 60, in_the_future, "localhost:4243");

    meta_schedule(&mut platform);

    assert_eq!(gantt_jobs(&platform).len(), 2);
}

/// `oar3/tests/kao/test_db_metasched.py::test_db_metasched_bug_toLaunch_absent`
///
/// A job that requests more resources than there are alive must not be launched:
/// the sixth resource is `Absent`.
#[test]
fn test_db_metasched_bug_toLaunch_absent() {
    let (mut platform, _notifications) = setup(5);
    insert_queues(&platform);
    NewResource {
        network_address: "10.0.0.42".to_string(),
        r#type: "default".to_string(),
        state: "Absent".to_string(),
        labels: indexmap::IndexMap::new(),
    }
    .insert(&platform.session())
    .unwrap();
    let job_id = insert_job(&platform, "default", "resource_id=6", 60, vec![]);

    meta_schedule(&mut platform);

    assert_eq!(get_job(&platform, job_id).state, "Waiting");
}

/// `oar3/tests/kao/test_db_metasched.py::test_db_metasched_walltime_change`
///
/// A pending walltime change request of a running job is granted by the
/// meta-scheduler when `WALLTIME_CHANGE_ENABLED` is set.
#[test]
fn test_db_metasched_walltime_change() {
    let mut platform = setup_with_config(5, |config| {
        config.walltime_change_enabled = Some("yes".to_string());
    });
    insert_queues(&platform);

    let now = platform.get_now();
    let job_id = insert_running_job(&platform, "default", 2, 60, now, vec![]);
    meta::insert_walltime_change_request(&platform.session(), job_id, 600, "NO", "NO").unwrap();

    meta_schedule(&mut platform);

    let (pending, granted) = meta::get_walltime_change(&platform.session(), job_id)
        .unwrap()
        .expect("the walltime change request should still exist");
    assert_eq!(granted, 600);
    assert_eq!(pending, 0);
}

//! Port of `oar3/tests/kao/test_db_all_in_one.py` (the tests that do not
//! require quotas, energy saving or user privileges).

use crate::meta_schedule::meta_schedule;
use crate::platform::Platform;
use crate::test::meta_schedule_test::{get_job, insert_job, insert_queue, setup};
use crate::test::oar3_metasched_test::{gantt_jobs, insert_queues};
use oar_scheduler_core::platform::{Job, PlatformTrait};
use oar_scheduler_db::model::jobs::{JobDatabaseRequests, JobReservation, JobState, NewJob};
use oar_scheduler_db::model::moldable::MoldableDatabaseRequests;
use oar_scheduler_db::model::resources::NewResource;
use oar_scheduler_db::model::SqlEnum;

/// Creates the two queues of the `test_db_all_in_one.py` fixture.
fn insert_all_in_one_queues(platform: &Platform) {
    insert_queue(platform, "default", 0);
    insert_queue(platform, "admin", 100);
}

/// Inserts an advance reservation (`resource_id=4`, waiting to be scheduled) and
/// runs the meta-scheduler once, like the Python `insert_and_sched_ar` helper.
fn insert_and_sched_ar(platform: &mut Platform, start_time: i64, walltime: i64, types: Vec<String>) -> i64 {
    let job_id = insert_job(platform, "default", "resource_id=4", walltime, types);
    let job = get_job(platform, job_id);
    job.set_resa_state(&platform.session(), JobReservation::ToSchedule.as_str()).unwrap();
    job.set_start_time(&platform.session(), start_time).unwrap();
    job.set_info_type(&platform.session(), Some("localhost:4242")).unwrap();

    meta_schedule(platform);

    job_id
}

/// Moves the start time of every gantt prediction (Python tests update the
/// `gantt_jobs_predictions` table directly).
fn set_gantt_start_time(platform: &Platform, start_time: i64) {
    for job in gantt_jobs(platform) {
        let moldable = job.moldables.first().expect("the job has a moldable");
        moldable.set_gantt_job_start_time(&platform.session(), start_time).unwrap();
    }
}

/// `oar3/tests/kao/test_db_all_in_one.py::test_db_all_in_one_simple_1`
#[test]
fn test_db_all_in_one_simple_1() {
    let (mut platform, _notifications) = setup(5);
    insert_all_in_one_queues(&platform);
    let job_id = insert_job(&platform, "default", "resource_id=4", 60, vec![]);
    assert_eq!(get_job(&platform, job_id).state, JobState::Waiting.as_str());

    meta_schedule(&mut platform);

    assert_eq!(get_job(&platform, job_id).state, JobState::ToLaunch.as_str());
}

/// `oar3/tests/kao/test_db_all_in_one.py::test_db_all_in_one_ar_different_moldable_id`
///
/// The job has two moldables, so that its moldable ids differ from its job id:
/// the meta-scheduler must use the moldable id stored in the gantt prediction.
#[test]
fn test_db_all_in_one_ar_different_moldable_id() {
    let (mut platform, _notifications) = setup(5);
    insert_queues(&platform);

    // A dummy job with two moldables (walltime 60 and 100).
    let dummy_id = NewJob {
        user: Some("zozo".to_string()),
        queue_name: "default".to_string(),
        res: vec![
            (60, vec![("resource_id=6".to_string(), String::new())]),
            (100, vec![("resource_id=6".to_string(), String::new())]),
        ],
        types: Vec::new(),
    }
    .insert(&platform.session())
    .unwrap();
    assert_eq!(get_job(&platform, dummy_id).moldables.len(), 2);

    let now = platform.get_now();
    let job_id = insert_and_sched_ar(&mut platform, now + 10, 60, vec![]);

    // Put the reservation in the past so that it is ready to be launched.
    let new_start_time = now - 20;
    set_gantt_start_time(&platform, new_start_time);

    meta_schedule(&mut platform);

    assert_eq!(get_job(&platform, job_id).state, JobState::ToLaunch.as_str());
}

/// `oar3/tests/kao/test_db_all_in_one.py::test_db_all_in_one_ar_1`
#[test]
fn test_db_all_in_one_ar_1() {
    let (mut platform, _notifications) = setup(5);
    insert_queues(&platform);
    let now = platform.get_now();

    let job_id = insert_and_sched_ar(&mut platform, now + 10, 60, vec![]);

    let job = get_job(&platform, job_id);
    assert_eq!(job.state, JobState::Waiting.as_str());
    assert_eq!(job.reservation.as_ref(), JobReservation::Scheduled.as_str());
}

/// `oar3/tests/kao/test_db_all_in_one.py::test_db_all_in_one_AR_2`
///
/// A reservation that is already over (`start_time + walltime < now`) is
/// cancelled.
#[test]
fn test_db_all_in_one_AR_2() {
    let (mut platform, _notifications) = setup(5);
    insert_queues(&platform);
    let now = platform.get_now();

    let job_id = insert_and_sched_ar(&mut platform, now - 1000, 60, vec![]);

    assert_eq!(get_job(&platform, job_id).state, JobState::Error.as_str());
}

/// `oar3/tests/kao/test_db_all_in_one.py::test_db_all_in_one_AR_3`
#[test]
fn test_db_all_in_one_AR_3() {
    let (mut platform, _notifications) = setup(5);
    insert_queues(&platform);
    let now = platform.get_now();

    let job_id = insert_and_sched_ar(&mut platform, now + 1000, 60, vec![]);

    // The reservation and its gantt prediction are moved to the past.
    let new_start_time = now - 2000;
    get_job(&platform, job_id).set_start_time(&platform.session(), new_start_time).unwrap();
    set_gantt_start_time(&platform, new_start_time);

    meta_schedule(&mut platform);

    assert_eq!(get_job(&platform, job_id).state, JobState::Error.as_str());
}

/// `oar3/tests/kao/test_db_all_in_one.py::test_db_all_in_one_AR_4`
///
/// The gantt prediction is in the past but the reservation start time is not: the
/// reservation is launched.
#[test]
fn test_db_all_in_one_AR_4() {
    let (mut platform, _notifications) = setup(5);
    insert_queues(&platform);
    let now = platform.get_now();

    let job_id = insert_and_sched_ar(&mut platform, now + 10, 60, vec![]);

    set_gantt_start_time(&platform, now - 20);

    meta_schedule(&mut platform);

    assert_eq!(get_job(&platform, job_id).state, JobState::ToLaunch.as_str());
}

/// `oar3/tests/kao/test_db_all_in_one.py::test_db_all_in_one_AR_5`
///
/// All the resources are `Suspected`: the reservation cannot be launched yet.
#[test]
fn test_db_all_in_one_AR_5() {
    let (mut platform, _notifications) = setup(5);
    insert_queues(&platform);
    let now = platform.get_now();

    let job_id = insert_and_sched_ar(&mut platform, now + 10, 60, vec![]);

    let new_start_time = now - 20;
    get_job(&platform, job_id).set_start_time(&platform.session(), new_start_time).unwrap();
    set_gantt_start_time(&platform, new_start_time);
    set_resources_state(&platform, "Suspected");

    meta_schedule(&mut platform);

    assert_eq!(get_job(&platform, job_id).state, JobState::Waiting.as_str());
}

/// Sets the state of every resource of the platform.
fn set_resources_state(platform: &Platform, state: &str) {
    let resources =
        oar_scheduler_db::model::resources::Resource::get_all_sorted(&platform.session(), "resource_id", &vec![]).unwrap();
    for resource in resources {
        resource.set_state(&platform.session(), state).unwrap();
    }
}

/// `oar3/tests/kao/test_db_all_in_one.py::test_db_all_in_one_AR_6`
#[test]
fn test_db_all_in_one_AR_6() {
    let (mut platform, _notifications) = setup(5);
    insert_queues(&platform);
    let now = platform.get_now();

    let job_id = insert_and_sched_ar(&mut platform, now + 10, 600, vec![]);

    let new_start_time = now - 350;
    get_job(&platform, job_id).set_start_time(&platform.session(), new_start_time).unwrap();
    set_gantt_start_time(&platform, new_start_time);

    meta_schedule(&mut platform);

    assert_eq!(get_job(&platform, job_id).state, JobState::ToLaunch.as_str());
}

/// `oar3/tests/kao/test_db_all_in_one.py::test_db_all_in_one_AR_7`
#[test]
fn test_db_all_in_one_AR_7() {
    let (mut platform, _notifications) = setup(5);
    insert_queues(&platform);
    let now = platform.get_now();

    let job_id = insert_and_sched_ar(&mut platform, now + 10, 60, vec!["timesharing=*,*".to_string()]);

    let job = get_job(&platform, job_id);
    assert_eq!(job.state, JobState::Waiting.as_str());
    assert_eq!(job.reservation.as_ref(), JobReservation::Scheduled.as_str());
}

/// `oar3/tests/kao/test_db_all_in_one.py::test_db_all_in_one_BE`
#[test]
fn test_db_all_in_one_BE() {
    let (mut platform, _notifications) = setup(5);
    insert_queue(&platform, "default", 0);
    insert_queue(&platform, "besteffort", 0);

    let job_id = insert_job(&platform, "besteffort", "resource_id=1", 100, vec!["besteffort".to_string()]);

    meta_schedule(&mut platform);

    assert_eq!(get_job(&platform, job_id).state, JobState::ToLaunch.as_str());
}

/// `oar3/tests/kao/test_db_all_in_one.py::test_db_all_in_one_simple_2`
#[test]
fn test_db_all_in_one_simple_2() {
    let (mut platform, _notifications) = setup(5);
    insert_all_in_one_queues(&platform);
    let job_id = insert_job(&platform, "default", "resource_id=4", 60, vec![]);

    meta_schedule(&mut platform);

    assert_eq!(get_job(&platform, job_id).state, JobState::ToLaunch.as_str());
}

/// `oar3/tests/kao/test_db_all_in_one.py::test_db_all_in_one_simple_interactive_waiting_1`
///
/// Two interactive jobs requesting 4 resources each on a 5 resources platform: the
/// first one is launched, the second one waits.
#[test]
fn test_db_all_in_one_simple_interactive_waiting_1() {
    let (mut platform, _notifications) = setup(5);
    insert_all_in_one_queues(&platform);
    let first_id = insert_job(&platform, "default", "resource_id=4", 60, vec![]);
    let second_id = insert_job(&platform, "default", "resource_id=4", 60, vec![]);
    get_job(&platform, second_id).set_job_type(&platform.session(), "INTERACTIVE").unwrap();
    get_job(&platform, second_id)
        .set_info_type(&platform.session(), Some("0.0.0.0:1234"))
        .unwrap();

    meta_schedule(&mut platform);

    let jobs = Job::get_jobs(&platform.session(), None, None, None).unwrap();
    assert_eq!(jobs.get(&first_id).unwrap().state, JobState::ToLaunch.as_str());
    assert_eq!(jobs.get(&second_id).unwrap().state, JobState::Waiting.as_str());
}

/// `oar3/tests/kao/test_db_all_in_one.py::test_db_all_in_one_BE_to_kill`,
/// `test_db_all_in_one_BE_to_checkpoint` and `test_db_all_in_one_BE_2` are
/// skipped in OAR3 (`@pytest.mark.skip(reason="Bug occurs only in travis-CI")`):
/// they are not ported.
#[allow(dead_code)]
fn unported_skipped_tests() {}

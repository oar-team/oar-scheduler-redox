//! Port of the OAR3 Python database/meta-scheduler tests.
//!
//! Each test is a port of a test from `oar3/tests/kao/test_db_*.py`. The Python
//! tests drive the kamelot/kao entry points (`main()`), which read the queue and
//! the current time from the command line. The Rust meta-scheduler
//! ([`meta_schedule`]) has no command line interface: it schedules the waiting
//! jobs of all the active queues of the database, so the Python tests that only
//! differ by their CLI arguments are merged into a single Rust test (this is
//! noted on each concerned test).

use crate::meta_schedule::meta_schedule;
use crate::platform::Platform;
use crate::test::meta_schedule_test::{get_job, insert_default_resource, insert_job, insert_queue, setup};
use oar_scheduler_core::platform::Job;
use oar_scheduler_db::model::jobs::{JobDatabaseRequests, JobState, NewJob};
use oar_scheduler_db::model::resources::NewResource;
use oar_scheduler_db::model::SqlEnum;

fn gantt_jobs(platform: &Platform) -> Vec<Job> {
    Job::get_gantt_jobs(&platform.session(), None, None, None, None).expect("Failed to fetch the gantt jobs")
}

/// Inserts a job with several moldables (Python's `JobPseudo(mld_res_rqts=[...])`).
fn insert_moldable_job(platform: &Platform, queue: &str, moldables: Vec<(i64, &str)>) -> i64 {
    NewJob {
        user: Some("user1".to_string()),
        queue_name: queue.to_string(),
        res: moldables
            .into_iter()
            .map(|(walltime, hierarchy)| (walltime, vec![(hierarchy.to_string(), "".to_string())]))
            .collect(),
        types: vec![],
    }
    .insert(platform.session())
    .expect("Failed to insert job")
}

/// `oar3/tests/kao/test_db_kamelot_basic.py::test_db_kamelot_basic_1`
///
/// Five resources and five jobs that all request the same resource: the five
/// jobs must be scheduled (one after the other) and appear in the gantt tables.
#[test]
fn test_db_kamelot_basic_1() {
    let (mut platform, _notifications) = setup(5);
    insert_queue(&platform, "default", 3);
    for _ in 0..5 {
        insert_job(&platform, "default", "resource_id=2", 60, vec![]);
    }

    assert_eq!(meta_schedule(&mut platform), 0);

    assert_eq!(gantt_jobs(&platform).len(), 5);
}

/// `oar3/tests/kao/test_db_kamelot.py::test_db_kamelot_1`, `_2` and `_3`
///
/// The three Python tests only differ by the arguments given to the `kamelot`
/// command line (the queue name and the current time). The Rust meta-scheduler
/// does not take a queue on the command line, it schedules the waiting jobs of
/// every active queue, so the three tests are merged into this one.
#[test]
fn test_db_kamelot_1_2_3() {
    let (mut platform, _notifications) = setup(5);
    insert_queue(&platform, "default", 3);
    for _ in 0..5 {
        insert_job(&platform, "default", "resource_id=2", 60, vec![]);
    }

    assert_eq!(meta_schedule(&mut platform), 0);

    assert_eq!(gantt_jobs(&platform).len(), 5);
}

/// `oar3/tests/kao/test_db_kamelot.py::test_db_kamelot_4`
///
/// Resources of type `token` must never be allocated to a job.
#[test]
fn test_db_kamelot_4_tokens_are_never_allocated() {
    let (mut platform, _notifications) = setup(5);
    insert_queue(&platform, "default", 3);
    for _ in 0..3 {
        insert_job(&platform, "default", "resource_id=2", 60, vec![]);
    }
    for i in 0..4 {
        insert_default_resource(&platform.session(), format!("10.0.1.{}", i).as_str());
    }
    let tokens: Vec<i64> = (0..2)
        .map(|i| {
            NewResource {
                network_address: format!("10.0.2.{}", i),
                r#type: "token".to_string(),
                state: "Alive".to_string(),
                labels: indexmap::IndexMap::new(),
            }
            .insert(&platform.session())
            .expect("Failed to insert a token resource")
        })
        .collect();

    assert_eq!(meta_schedule(&mut platform), 0);

    for job in gantt_jobs(&platform) {
        let resources = job.assignment.expect("gantt jobs have an assignment").resources;
        for token in &tokens {
            assert!(
                !resources.contains(*token as u32),
                "The token resource {} must not be allocated to job {}",
                token,
                job.id
            );
        }
    }
}

/// `oar3/tests/kao/test_db_kao.py::test_db_kao_simple_1`
///
/// A job requesting an existing resource is scheduled and moved to `toLaunch`.
#[test]
fn test_db_kao_simple_1() {
    let (mut platform, _notifications) = setup(5);
    insert_queue(&platform, "default", 3);
    let job_id = insert_job(&platform, "default", "resource_id=4", 60, vec![]);

    assert_eq!(meta_schedule(&mut platform), 0);

    assert_eq!(get_job(&platform, job_id).state, JobState::ToLaunch.as_str());
}

/// `oar3/tests/kao/test_db_kao.py::test_db_kao_moldable`
///
/// The first moldable of the job requests a resource that does not exist
/// (`resource_id=6` on a five resources platform): the scheduler must fall back
/// on the second moldable and still launch the job.
#[test]
fn test_db_kao_moldable() {
    let (mut platform, _notifications) = setup(5);
    insert_queue(&platform, "default", 3);
    let job_id = insert_moldable_job(&platform, "default", vec![(5, "resource_id=6"), (2, "resource_id=2")]);

    assert_eq!(meta_schedule(&mut platform), 0);

    let job = get_job(&platform, job_id);
    println!("moldables: {:?}", job.moldables.iter().map(|m| (m.id, m.walltime)).collect::<Vec<_>>());
    println!("assignment: {:?}", job.assignment);
    assert_eq!(job.state, JobState::ToLaunch.as_str());
}

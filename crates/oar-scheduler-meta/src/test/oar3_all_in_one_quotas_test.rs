//! Port of the quotas tests of `oar3/tests/kao/test_db_all_in_one.py`
//! (`test_db_all_in_one_quotas_*` and `test_db_all_in_one_temporal_quotas_*`).

use crate::meta_schedule::meta_schedule;
use crate::platform::Platform;
use crate::test::meta_schedule_test::{get_job, insert_queue, setup_with_config};
use crate::test::oar3_metasched_test::gantt_jobs;
use oar_scheduler_core::platform::PlatformTrait;
use oar_scheduler_db::model::jobs::{JobDatabaseRequests, JobReservation, JobState, NewJob};
use oar_scheduler_db::model::SqlEnum;

/// Creates a platform with quotas enabled and the given quotas rules (mirrors
/// the `active_quotas` fixture and `create_quotas_rules_file`).
fn setup_with_quotas(rules: &str) -> Platform {
    let quotas_conf_file = tempfile::NamedTempFile::new().expect("Failed to create the quotas config file");
    std::fs::write(quotas_conf_file.path(), rules).expect("Failed to write the quotas config file");
    let path = quotas_conf_file.path().to_str().unwrap().to_string();
    // The platform reads the file lazily: the file must outlive the platform.
    std::mem::forget(quotas_conf_file);

    setup_with_config(5, |config| {
        config.quotas = true;
        config.quotas_conf_file = Some(path);
    })
}

/// Creates the queues of the `test_db_all_in_one.py` fixture.
fn insert_all_in_one_queues(platform: &Platform) {
    insert_queue(platform, "default", 0);
    insert_queue(platform, "admin", 100);
}

/// Inserts a job of the given user.
fn insert_job_for_user(platform: &Platform, user: &str, walltime: i64, res_count: u32, types: Vec<String>) -> i64 {
    NewJob {
        user: Some(user.to_string()),
        queue_name: "default".to_string(),
        res: vec![(walltime, vec![(format!("resource_id={}", res_count), String::new())])],
        types,
    }
    .insert(&platform.session())
    .expect("Failed to insert job")
}

/// Runs the meta-scheduler and returns the start times of the gantt predictions,
/// ordered by moldable id.
fn prediction_start_times(platform: &Platform) -> Vec<i64> {
    gantt_jobs(platform)
        .iter()
        .filter_map(|job| job.assignment.as_ref().map(|assignment| assignment.begin))
        .collect()
}

/// Inserts an advance reservation (`resource_id=4`) of the given user and runs
/// the meta-scheduler once.
fn insert_and_sched_ar(platform: &mut Platform, start_time: i64, user: &str) -> i64 {
    let job_id = insert_job_for_user(platform, user, 60, 4, vec![]);
    let job = get_job(platform, job_id);
    job.set_resa_state(&platform.session(), JobReservation::ToSchedule.as_str()).unwrap();
    job.set_start_time(&platform.session(), start_time).unwrap();
    job.set_info_type(&platform.session(), Some("localhost:4242")).unwrap();

    meta_schedule(platform);

    job_id
}

/// `oar3/tests/kao/test_db_all_in_one.py::test_db_all_in_one_quotas_1`
///
/// `quotas[queue, project, job_type, user] = [nb_resources, nb_jobs, hours]`:
/// the rule limiting the number of running jobs (1) and the total hours (5)
/// applies, so the three jobs are serialized.
///
/// Divergence: OAR3 packs the jobs with a `SCHEDULER_JOB_SECURITY_TIME` gap
/// (`get_data_jobs` schedules each job for `walltime + job_security_time`, so the
/// Python test expects `[0, 160, 420]`); the Rust port schedules the jobs for
/// their raw walltime and packs them back to back (`[0, 100, 300]`). See
/// `docs/tests/oar3-port.md`.
#[test]
fn test_db_all_in_one_quotas_1() {
    let mut platform = setup_with_quotas(
        r#"{
            "quotas": {
                "*,*,*,/": [-1, -1, -1],
                "/,*,*,*": [-1, 1, 5]
                }
            }"#,
    );
    insert_all_in_one_queues(&platform);
    insert_job_for_user(&platform, "toto", 100, 1, vec![]);
    insert_job_for_user(&platform, "toto", 200, 1, vec![]);
    insert_job_for_user(&platform, "toto", 200, 1, vec![]);
    let now = platform.get_now();

    meta_schedule(&mut platform);

    let start_times = prediction_start_times(&platform).into_iter().map(|start| start - now).collect::<Vec<_>>();
    assert_eq!(start_times, vec![0, 100, 300]);
}

/// `oar3/tests/kao/test_db_all_in_one.py::test_db_all_in_one_quotas_2`
///
/// The advance reservation occupies the single job slot of the quota: the two
/// other jobs are scheduled after it.
///
/// Divergence: the Python test expects `[100, 220, 380]` (the reservation is
/// packed with a `SCHEDULER_JOB_SECURITY_TIME` gap and the other jobs are placed
/// after it); the Rust port launches the first job before the reservation
/// (it ends exactly when the reservation starts) and packs the last one right
/// after the reservation: `[0, 100, 160]`.
#[test]
fn test_db_all_in_one_quotas_2() {
    let mut platform = setup_with_quotas(r#"{"quotas": {"*,*,*,/": [-1, 1, -1]}}"#);
    insert_all_in_one_queues(&platform);
    let t0 = platform.get_now();
    insert_and_sched_ar(&mut platform, t0 + 100, "toto");
    insert_job_for_user(&platform, "toto", 100, 1, vec![]);
    insert_job_for_user(&platform, "toto", 200, 1, vec![]);

    let t1 = platform.get_now();
    meta_schedule(&mut platform);

    let start_times = prediction_start_times(&platform).into_iter().map(|start| start - t1).collect::<Vec<_>>();
    assert_eq!(start_times, vec![0, 100, 160]);
}

/// `oar3/tests/kao/test_db_all_in_one.py::test_db_all_in_one_quotas_AR`
///
/// The quota allows a single resource: the advance reservation (4 resources)
/// cannot be validated.
#[test]
fn test_db_all_in_one_quotas_AR() {
    let mut platform = setup_with_quotas(r#"{"quotas": {"*,*,*,*": [1, -1, -1]}}"#);
    insert_all_in_one_queues(&platform);
    let now = platform.get_now();

    let job_id = insert_and_sched_ar(&mut platform, now + 10, "");

    assert_eq!(get_job(&platform, job_id).state, JobState::Error.as_str());
}

/// Formats an epoch as `"%Y-%m-%d %H:%M"` in local time (mirrors Python's
/// `local_to_sql(t)[:-3]`).
fn local_to_sql_minutes(epoch: i64) -> String {
    use chrono::TimeZone;
    let dt = match chrono::Local.timestamp_opt(epoch, 0) {
        chrono::LocalResult::Single(dt) => dt,
        _ => panic!("invalid time"),
    };
    dt.format("%Y-%m-%d %H:%M").to_string()
}

/// The quotas rules of the Python `quotas_simple_temporal_rules` fixture, plus an
/// optional unlimited `oneshot` window (`[start, stop]` in local time).
fn simple_temporal_rules(oneshot: Option<(String, String)>) -> String {
    let oneshot = oneshot
        .map(|(start, stop)| format!(r#""oneshot": [["{}", "{}", "quotas_2", ""]],"#, start, stop))
        .unwrap_or_default();
    format!(
        r#"{{{}
            "periodical": [["* * * *", "quotas_1", "default"]],
            "quotas_1": {{"*,*,*,/": [1, -1, -1]}},
            "quotas_2": {{"*,*,*,/": [-1, -1, -1]}}
        }}"#,
        oneshot
    )
}

/// Returns the gantt prediction start times, ordered by job id.
fn start_times_by_job_id(platform: &Platform) -> Vec<i64> {
    let mut jobs = gantt_jobs(platform)
        .iter()
        .filter_map(|job| job.assignment.as_ref().map(|assignment| (job.id, assignment.begin)))
        .collect::<Vec<_>>();
    jobs.sort_unstable_by_key(|(job_id, _)| *job_id);
    jobs.into_iter().map(|(_, begin)| begin).collect()
}

/// `oar3/tests/kao/test_db_all_in_one.py::test_db_all_in_one_temporal_quotas_1`
///
/// The periodical rule allows a single resource; the job requesting 5 resources is
/// deferred to the unlimited `oneshot` window (2 days later), while the job
/// requesting 1 resource runs now.
#[test]
fn test_db_all_in_one_temporal_quotas_1() {
    let t1 = setup_with_quotas(&simple_temporal_rules(None)).get_now() + 2 * 86400;
    let t2 = t1 + 86400;
    let rules = simple_temporal_rules(Some((local_to_sql_minutes(t1), local_to_sql_minutes(t2))));
    let mut platform = setup_with_quotas(&rules);
    insert_all_in_one_queues(&platform);
    let now = platform.get_now();
    let t1 = now + 2 * 86400;

    insert_job_for_user(&platform, "toto", 100, 5, vec![]);
    insert_job_for_user(&platform, "toto", 200, 1, vec![]);

    meta_schedule(&mut platform);

    let start_times = start_times_by_job_id(&platform);
    assert_eq!(start_times, vec![t1 - (t1 % 60), now]);
}

/// `oar3/tests/kao/test_db_all_in_one.py::test_db_all_in_one_temporal_quotas_2`
///
/// Same as [`test_db_all_in_one_temporal_quotas_1`] but the second job has the
/// `no_quotas` type and requests 5 resources.
#[test]
fn test_db_all_in_one_temporal_quotas_2() {
    let t1 = setup_with_quotas(&simple_temporal_rules(None)).get_now() + 2 * 86400;
    let t2 = t1 + 86400;
    let rules = simple_temporal_rules(Some((local_to_sql_minutes(t1), local_to_sql_minutes(t2))));
    let mut platform = setup_with_quotas(&rules);
    insert_all_in_one_queues(&platform);
    let now = platform.get_now();
    let t1 = now + 2 * 86400;

    insert_job_for_user(&platform, "toto", 100, 5, vec![]);
    insert_job_for_user(&platform, "toto", 200, 5, vec!["no_quotas".to_string()]);

    meta_schedule(&mut platform);

    let start_times = start_times_by_job_id(&platform);
    assert_eq!(start_times, vec![t1 - (t1 % 60), now]);
}

/// `oar3/tests/kao/test_db_all_in_one.py::test_db_all_in_one_temporal_quotas_AR_1`
///
/// The periodical rule allows a single resource: the advance reservation (4
/// resources) cannot be validated.
#[test]
fn test_db_all_in_one_temporal_quotas_AR_1() {
    let mut platform = setup_with_quotas(&simple_temporal_rules(None));
    insert_all_in_one_queues(&platform);
    let now = platform.get_now();

    let job_id = insert_and_sched_ar(&mut platform, now + 10, "");

    assert_eq!(get_job(&platform, job_id).state, JobState::Error.as_str());
}

/// `oar3/tests/kao/test_db_all_in_one.py::test_db_all_in_one_temporal_quotas_AR_2`
///
/// The reservation starts inside the unlimited `oneshot` window: it is scheduled.
#[test]
fn test_db_all_in_one_temporal_quotas_AR_2() {
    let t1 = setup_with_quotas(&simple_temporal_rules(None)).get_now() + 2 * 86400;
    let t2 = t1 + 86400;
    let rules = simple_temporal_rules(Some((local_to_sql_minutes(t1), local_to_sql_minutes(t2))));
    let mut platform = setup_with_quotas(&rules);
    insert_all_in_one_queues(&platform);
    let now = platform.get_now();
    let t1 = now + 2 * 86400;

    let job_id = insert_and_sched_ar(&mut platform, t1 + 3600, "");

    assert_eq!(get_job(&platform, job_id).state, JobState::Waiting.as_str());
}

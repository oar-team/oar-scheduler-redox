use crate::meta_schedule::meta_schedule;
use crate::notification::JobNotifier;
use crate::platform::Platform;
use crate::test::setup_for_tests;
use oar_scheduler_core::model::configuration::Configuration;
use oar_scheduler_core::model::job::ProcSet;
use oar_scheduler_core::platform::{Job, PlatformTrait};
use oar_scheduler_db::model::jobs::{JobDatabaseRequests, JobState, NewJob};
use oar_scheduler_db::model::meta;
use oar_scheduler_db::model::moldable::MoldableDatabaseRequests;
use oar_scheduler_db::model::queues::Queue;
use oar_scheduler_db::model::resources::NewResource;
use oar_scheduler_db::model::SqlEnum;
use oar_scheduler_db::Session;
use crate::notification::ZmqBipbipNotifier;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

/// Records the notifications emitted by the meta-scheduler.
#[derive(Default)]
pub struct Notifications {
    pub launched: Vec<i64>,
    pub tcp: Vec<(String, String)>,
    pub checkpoints: Vec<i64>,
    pub tcp_succeeds: bool,
}

/// A [`JobNotifier`] that records notifications instead of emitting them.
#[derive(Clone)]
pub struct RecordingNotifier(Rc<RefCell<Notifications>>);

impl RecordingNotifier {
    pub fn new(tcp_succeeds: bool) -> (Self, Rc<RefCell<Notifications>>) {
        let notifications = Rc::new(RefCell::new(Notifications {
            tcp_succeeds,
            ..Default::default()
        }));
        (RecordingNotifier(Rc::clone(&notifications)), notifications)
    }
}

impl JobNotifier for RecordingNotifier {
    fn notify_to_run_job(&self, job_id: i64) -> bool {
        self.0.borrow_mut().launched.push(job_id);
        true
    }
    fn notify_tcp_socket(&self, info_type: &str, message: &str) -> usize {
        self.0.borrow_mut().tcp.push((info_type.to_string(), message.to_string()));
        if self.0.borrow().tcp_succeeds {
            42
        } else {
            0
        }
    }
    fn send_checkpoint_signal(&self, job_id: i64) -> bool {
        self.0.borrow_mut().checkpoints.push(job_id);
        true
    }
}

fn insert_default_resource(session: &Session, address: &str) {
    NewResource {
        network_address: address.to_string(),
        r#type: "default".to_string(),
        state: "Alive".to_string(),
        labels: indexmap::IndexMap::new(),
    }
    .insert(session)
    .expect("Failed to insert test resource");
}

fn insert_queue(platform: &Platform, name: &str, priority: i32) {
    Queue {
        queue_name: name.to_string(),
        priority,
        scheduler_policy: "kamelot".to_string(),
        state: "Active".to_string(),
    }
    .insert(&platform.session())
    .unwrap();
}

/// Creates a platform with `nb_resources` default resources and a recording notifier.
fn setup(nb_resources: usize) -> (Platform, Rc<RefCell<Notifications>>) {
    let (session, config) = setup_for_tests(true); // Sqlite in-memory
    session.reset();
    for i in 1..=nb_resources {
        insert_default_resource(&session, format!("10.0.0.{}", i).as_str());
    }
    let mut platform = Platform::from_database(session, config);
    let (notifier, notifications) = RecordingNotifier::new(true);
    platform.set_notifier(Box::new(notifier));
    (platform, notifications)
}

/// Creates a platform like [`setup`] but lets the caller customize the configuration.
fn setup_with_config(nb_resources: usize, customize: impl FnOnce(&mut Configuration)) -> Platform {
    let (session, mut config) = setup_for_tests(true);
    session.reset();
    for i in 1..=nb_resources {
        insert_default_resource(&session, format!("10.0.0.{}", i).as_str());
    }
    customize(&mut config);
    let mut platform = Platform::from_database(session, config);
    let (notifier, _notifications) = RecordingNotifier::new(true);
    platform.set_notifier(Box::new(notifier));
    platform
}

fn insert_job(platform: &Platform, queue: &str, hierarchy: &str, walltime: i64, types: Vec<String>) -> i64 {
    NewJob {
        user: Some("user1".to_string()),
        queue_name: queue.to_string(),
        res: vec![(walltime, vec![(hierarchy.to_string(), "".to_string())])],
        types,
    }
    .insert(platform.session())
    .expect("Failed to insert job")
}

fn get_job(platform: &Platform, job_id: i64) -> Job {
    Job::get_jobs(&platform.session(), None, None, None)
        .unwrap()
        .get(&job_id)
        .cloned()
        .expect("Job not found")
}

/// A simple job is scheduled and marked as `toLaunch`, its resources are
/// assigned and the bipbip commander is notified exactly once.
#[test]
fn test_meta_schedule_launches_job() {
    let (mut platform, notifications) = setup(2);
    insert_queue(&platform, "default", 2);
    let job_id = insert_job(&platform, "default", "resource_id=1", 60, vec![]);

    let exit_code = meta_schedule(&mut platform);
    assert_eq!(exit_code, 0);

    let job = get_job(&platform, job_id);
    assert_eq!(job.state, JobState::ToLaunch.as_str());
    let assignment = job.assignment.expect("Job should have an assignment after launching");
    assert_eq!(assignment.resources.len(), 1);

    // The job was notified exactly once.
    assert_eq!(notifications.borrow().launched, vec![job_id]);

    // The gantt tables contain the scheduling decision.
    let gantt_jobs = Job::get_gantt_jobs(&platform.session(), None, None, None, None).unwrap();
    assert!(gantt_jobs.iter().any(|job| job.id == job_id));

    // The job is no longer waiting.
    let waiting = platform.get_waiting_jobs(vec!["default".to_string()]);
    assert!(!waiting.contains_key(&job_id));
}

/// The meta-scheduler notifies the bipbip commander natively through ZMQ: a job
/// that gets launched produces exactly one `OARRUN` JSON message on the
/// commander's `PULL` socket, in the format expected by `recv_json()`.
#[test]
fn test_meta_schedule_notifies_bipbip_over_zmq() {
    let context = omq::Context::new();
    let commander = context.blocking_socket(omq::SocketType::Pull, omq::Options::default());
    let endpoint = commander.bind("tcp://127.0.0.1:0").expect("bind the fake bipbip commander");

    let (mut platform, _notifications) = setup(1);
    platform.set_notifier(Box::new(ZmqBipbipNotifier::new(endpoint.to_string())));
    insert_queue(&platform, "default", 2);
    let job_id = insert_job(&platform, "default", "resource_id=1", 60, vec![]);

    assert_eq!(meta_schedule(&mut platform), 0);

    let message = commander
        .recv_timeout(Duration::from_secs(10))
        .expect("the bipbip commander should receive the OARRUN notification");
    assert_eq!(message.len(), 1, "the commander reads a single JSON frame");
    assert_eq!(
        std::str::from_utf8(&message[0]).unwrap(),
        format!(r#"{{"job_id":{}, "cmd":"OARRUN", "args":[]}}"#, job_id)
    );
}

/// A job that cannot be scheduled (it requests no available resource) must not
/// make the meta-scheduler panic: no assignment is saved, the job stays
/// `Waiting` and the cycle still succeeds.
#[test]
fn test_meta_schedule_unschedulable_job_does_not_panic() {
    let (mut platform, notifications) = setup(1);
    insert_queue(&platform, "default", 2);
    let job_id = insert_job(&platform, "default", "resource_id=42", 60, vec![]);

    let exit_code = meta_schedule(&mut platform);
    assert_eq!(exit_code, 0);

    let job = get_job(&platform, job_id);
    assert_eq!(job.state, JobState::Waiting.as_str());
    assert!(job.assignment.is_none(), "no resource matches the request");
    assert!(notifications.borrow().launched.is_empty());

    let gantt_jobs = Job::get_gantt_jobs(&platform.session(), None, None, None, None).unwrap();
    assert!(!gantt_jobs.iter().any(|job| job.id == job_id));
}

/// A besteffort job occupying a resource is flagged for deletion when a higher
/// priority job is scheduled on the same resource, and the higher priority job
/// is NOT launched during this cycle (exit code 2).
#[test]
fn test_meta_schedule_kills_besteffort_job() {
    // First cycle: schedule the besteffort job alone.
    let (mut platform, _notifications) = setup(1);
    insert_queue(&platform, "besteffort", 0);
    let besteffort_job_id = insert_job(&platform, "besteffort", "resource_id=1", 3600, vec!["besteffort".to_string()]);
    meta_schedule(&mut platform);
    assert_eq!(get_job(&platform, besteffort_job_id).state, JobState::ToLaunch.as_str());

    // Second cycle: add a higher priority queue and a job on the same resource.
    let (notifier, notifications) = RecordingNotifier::new(true);
    platform.set_notifier(Box::new(notifier));
    insert_queue(&platform, "default", 2);
    let default_job_id = insert_job(&platform, "default", "resource_id=1", 60, vec![]);

    let exit_code = meta_schedule(&mut platform);
    assert_eq!(exit_code, 2, "A besteffort job must be killed, so the exit code must be 2");

    let fragged = meta::get_fragged_jobs(&platform.session()).unwrap();
    assert!(
        fragged.contains(&besteffort_job_id),
        "The besteffort job should be flagged for deletion, fragged jobs: {:?}",
        fragged
    );
    assert!(
        !notifications.borrow().launched.contains(&default_job_id),
        "The higher priority job must not be launched while a besteffort job is being killed"
    );
}

/// A `toError` interactive job notifies its `oarsub` client and is moved to the
/// `Error` state.
#[test]
fn test_meta_schedule_processes_to_error_jobs() {
    let (mut platform, notifications) = setup(1);
    let job_id = insert_job(&platform, "default", "resource_id=1", 60, vec![]);
    let job = get_job(&platform, job_id);
    job.set_info_type(&platform.session(), Some("127.0.0.1:4242")).unwrap();
    job.set_job_type(&platform.session(), "INTERACTIVE").unwrap();
    job.set_message(&platform.session(), "Not enough resources").unwrap();
    job.set_state(&platform.session(), JobState::ToError).unwrap();

    meta_schedule(&mut platform);

    assert_eq!(get_job(&platform, job_id).state, "Error");
    let tcp = &notifications.borrow().tcp;
    assert!(tcp.iter().any(|(addr, msg)| addr == "127.0.0.1:4242" && msg == "Not enough resources"));
    assert!(tcp.iter().any(|(addr, msg)| addr == "127.0.0.1:4242" && msg == "BAD JOB"));
}

/// A `toAckReservation` job notifies its `oarsub` client and goes back to the
/// `Waiting` state. If the reservation starts now, the exit code is 1.
#[test]
fn test_meta_schedule_acks_reservation() {
    let (mut platform, notifications) = setup(1);
    let now = platform.get_now();
    let job_id = insert_job(&platform, "default", "resource_id=1", 300, vec![]);
    let job = get_job(&platform, job_id);
    job.set_info_type(&platform.session(), Some("127.0.0.1:4242")).unwrap();
    job.set_resa_state(&platform.session(), "Scheduled").unwrap();
    job.assign_moldable_and_set_start_time(&platform.session(), 0, now).unwrap();
    job.set_state(&platform.session(), JobState::ToAckReservation).unwrap();

    let exit_code = meta_schedule(&mut platform);

    assert_eq!(exit_code, 1, "A reservation starting now must make the meta-scheduler return 1");
    assert_eq!(get_job(&platform, job_id).state, JobState::Waiting.as_str());
    assert!(
        notifications
            .borrow()
            .tcp
            .iter()
            .any(|(addr, msg)| addr == "127.0.0.1:4242" && msg == "GOOD RESERVATION")
    );
}

/// A `toSchedule` advance reservation is scheduled by `check_reservation_jobs`
/// and moved to `toAckReservation`, then acknowledged.
#[test]
fn test_meta_schedule_schedules_advance_reservation() {
    let (mut platform, notifications) = setup(2);
    insert_queue(&platform, "default", 2);
    let now = platform.get_now();
    let job_id = insert_job(&platform, "default", "resource_id=1", 600, vec![]);
    let job = get_job(&platform, job_id);
    job.set_info_type(&platform.session(), Some("127.0.0.1:4242")).unwrap();
    job.set_resa_state(&platform.session(), "toSchedule").unwrap();
    job.assign_moldable_and_set_start_time(&platform.session(), 0, now + 100).unwrap();

    meta_schedule(&mut platform);

    let job = get_job(&platform, job_id);
    // The reservation has been accepted (acknowledged) and is now waiting for its start time.
    assert_eq!(job.state, JobState::Waiting.as_str());
    assert_eq!(job.reservation.as_ref(), "Scheduled");
    // The reservation has a gantt assignment (it is not launched yet, so it has
    // no assigned_resources entry).
    let gantt_job = Job::get_gantt_jobs(&platform.session(), None, None, None, None)
        .unwrap()
        .into_iter()
        .find(|job| job.id == job_id)
        .expect("The reservation should be scheduled in the gantt");
    let assignment = gantt_job.assignment.expect("The reservation should have a gantt assignment");
    assert_eq!(assignment.begin, now + 100);
    assert!(notifications
        .borrow()
        .tcp
        .iter()
        .any(|(_, msg)| msg == "GOOD RESERVATION"));
}

/// The gantt visualization tables are filled with the current scheduling
/// decisions.
#[test]
fn test_update_gantt_visualization() {
    let (mut platform, _notifications) = setup(2);
    insert_queue(&platform, "default", 2);
    let job_id = insert_job(&platform, "default", "resource_id=1", 60, vec![]);
    meta_schedule(&mut platform);

    // The job is `toLaunch` so its assignment comes from assigned_resources; the
    // visualization tables should mirror the gantt tables.
    let gantt_jobs = Job::get_gantt_jobs(&platform.session(), None, None, None, None).unwrap();
    assert!(gantt_jobs.iter().any(|job| job.id == job_id));

    // Re-running the visualization update must not fail.
    meta::update_gantt_visualization(&platform.session()).expect("Failed to update gantt visualization");
}

/// A running job with a `force` walltime change request gets its walltime
/// extended and a `WALLTIME` event.
#[test]
fn test_process_walltime_change_requests() {
    let (platform, _notifications) = setup(2);
    insert_queue(&platform, "default", 2);
    let job_id = insert_job(&platform, "default", "resource_id=1", 3600, vec![]);

    let now = platform.session().get_now();
    let job = get_job(&platform, job_id);
    let moldable_id = job.moldables[0].id;
    job.assign_moldable_and_set_start_time(&platform.session(), moldable_id, now - 100).unwrap();
    job.set_state(&platform.session(), JobState::Running).unwrap();
    let resources: ProcSet = ProcSet::from_iter([0u32]);
    job.moldables[0]
        .save_resources_as_assigned_resources(&platform.session(), &resources)
        .unwrap();

    meta::insert_walltime_change_request(&platform.session(), job_id, 600, "YES", "NO").unwrap();

    crate::walltime_change::process_walltime_change_requests(&platform);

    let job = get_job(&platform, job_id);
    assert_eq!(
        job.moldables[0].walltime,
        3600 + 600,
        "The walltime should have been extended by the whole pending time"
    );
    let events = meta::get_job_events(&platform.session(), job_id).unwrap();
    assert!(
        events.iter().any(|event| event.ev_type == "WALLTIME"),
        "A WALLTIME event should have been added"
    );
}

/// An accepted advance reservation whose start time plus walltime is in the
/// past is put in `Error` with an explanatory message.
#[test]
fn test_meta_schedule_expires_waiting_reservation() {
    let (mut platform, notifications) = setup(2);
    insert_queue(&platform, "default", 2);
    let now = platform.get_now();
    let job_id = insert_job(&platform, "default", "resource_id=1", 600, vec![]);
    let job = get_job(&platform, job_id);
    job.set_info_type(&platform.session(), Some("127.0.0.1:4242")).unwrap();
    job.set_resa_state(&platform.session(), "toSchedule").unwrap();
    job.assign_moldable_and_set_start_time(&platform.session(), 0, now + 100).unwrap();

    // First cycle: the reservation is scheduled and acknowledged.
    meta_schedule(&mut platform);
    let job = get_job(&platform, job_id);
    assert_eq!(job.reservation.as_ref(), "Scheduled");

    // Make the reservation expire by moving its predicted start time to the past.
    let moldable_id = job.moldables[0].id;
    meta::set_gantt_job_start_time(&platform.session(), moldable_id, now - 1000).unwrap();

    // Second cycle: the expired reservation is detected.
    meta_schedule(&mut platform);

    let job = get_job(&platform, job_id);
    assert_eq!(job.state, "Error");
    assert_eq!(job.message, "Reservation expired and couldn't be started.");
    let _ = notifications;
}

/// An idle alive node is selected to be halted.
#[test]
fn test_energy_saving_halts_idle_node() {
    let platform = setup_with_config(1, |config| {
        config.energy_saving_internal = Some("no".to_string());
        config.scheduler_node_manager_sleep_cmd = Some("true".to_string());
        config.scheduler_node_manager_idle_time = Some(10);
        config.scheduler_node_manager_sleep_time = Some(10);
    });
    // The node is alive with a finite availability and an old last job date.
    meta::set_resource_energy_fields(&platform.session(), 1, None, Some(100), Some(0)).unwrap();

    let changes = crate::energy_saving::compute_nodes_energy_saving(&platform);
    assert_eq!(changes.halt, vec!["10.0.0.1".to_string()]);
    assert!(changes.wakeup.is_empty(), "No wake up command is configured");
}

/// An absent node hosting a future job is selected to be woken up.
#[test]
fn test_energy_saving_wakes_absent_node() {
    let mut platform = setup_with_config(1, |config| {
        config.energy_saving_internal = Some("no".to_string());
        config.scheduler_node_manager_wake_up_cmd = Some("true".to_string());
        config.scheduler_node_manager_wakeup_time = Some(100_000);
    });
    insert_queue(&platform, "default", 2);
    let now = platform.get_now();
    let job_id = insert_job(&platform, "default", "resource_id=1", 600, vec![]);
    let job = get_job(&platform, job_id);
    job.set_info_type(&platform.session(), Some("127.0.0.1:4242")).unwrap();
    job.set_resa_state(&platform.session(), "toSchedule").unwrap();
    job.assign_moldable_and_set_start_time(&platform.session(), 0, now + 100).unwrap();

    // The reservation is scheduled and acknowledged, leaving a gantt prediction.
    meta_schedule(&mut platform);

    // Mark the node as absent: it must be woken up.
    meta::set_resource_energy_fields(&platform.session(), 1, Some("Absent"), None, None).unwrap();

    let changes = crate::energy_saving::compute_nodes_energy_saving(&platform);
    assert!(
        changes.wakeup.contains(&"10.0.0.1".to_string()),
        "The absent node should be woken up, got: {:?}",
        changes.wakeup
    );
    assert!(changes.halt.is_empty(), "No sleep command is configured");
}

/// A state change appends an entry to `job_state_logs` and closes the previous one.
#[test]
fn test_set_state_updates_job_state_logs() {
    let (platform, _notifications) = setup(1);
    let job_id = insert_job(&platform, "default", "resource_id=1", 60, vec![]);
    let job = get_job(&platform, job_id);

    job.set_state(&platform.session(), JobState::Running).unwrap();
    let job = get_job(&platform, job_id);
    job.set_state(&platform.session(), JobState::Suspended).unwrap();
    let job = get_job(&platform, job_id);
    job.set_state(&platform.session(), JobState::Running).unwrap();

    let logs = meta::get_job_state_logs(&platform.session(), job_id).unwrap();
    assert_eq!(logs.len(), 3, "Each state change must be logged: {:?}", logs);
    assert_eq!(logs[0].0, "Running");
    assert_eq!(logs[1].0, "Suspended");
    assert_eq!(logs[2].0, "Running");
    // Previous entries are closed, the last one is still open.
    assert_ne!(logs[0].2, 0);
    assert_ne!(logs[1].2, 0);
    assert_eq!(logs[2].2, 0);
}

/// Setting a job in `Error` updates the `last_job_date` of its resources.
#[test]
fn test_set_state_error_updates_resource_last_job_date() {
    let (platform, _notifications) = setup(1);
    let job_id = insert_job(&platform, "default", "resource_id=1", 60, vec![]);
    let job = get_job(&platform, job_id);
    job.assign_moldable_and_set_start_time(&platform.session(), job.moldables[0].id, platform.get_now())
        .unwrap();
    let resources: ProcSet = ProcSet::from_iter([0u32]);
    job.moldables[0]
        .save_resources_as_assigned_resources(&platform.session(), &resources)
        .unwrap();

    let job = get_job(&platform, job_id);
    job.set_state(&platform.session(), JobState::Running).unwrap();
    let running_date = meta::get_resource_last_job_date(&platform.session(), 1).unwrap().unwrap();
    assert_eq!(running_date, 0, "A running job does not update last_job_date");

    job.set_state(&platform.session(), JobState::Error).unwrap();
    let error_date = meta::get_resource_last_job_date(&platform.session(), 1).unwrap().unwrap();
    assert_ne!(error_date, 0, "An error job must update the last_job_date of its resources");
}

/// A `noop` job in the `Resuming` state is set back to `Running` when no other
/// job uses its resources.
#[test]
fn test_resuming_noop_job() {
    let (mut platform, _notifications) = setup(1);
    insert_queue(&platform, "default", 2);
    let job_id = insert_job(&platform, "default", "resource_id=1", 60, vec!["noop".to_string()]);
    let job = get_job(&platform, job_id);
    job.set_state(&platform.session(), JobState::Resuming).unwrap();

    meta_schedule(&mut platform);

    assert_eq!(get_job(&platform, job_id).state, JobState::Running.as_str());
}

/// When the just-before-resume script fails, the job is not resumed and is
/// flagged for deletion with a `RESUME_SCRIPT_ERROR` event.
#[test]
fn test_resuming_job_script_failure_frags_job() {
    let mut platform = setup_with_config(1, |config| {
        config.just_before_resume_exec_file = Some("false".to_string());
        config.suspend_resume_script_timeout = Some(5);
    });
    insert_queue(&platform, "default", 2);
    let job_id = insert_job(&platform, "default", "resource_id=1", 60, vec![]);
    let job = get_job(&platform, job_id);
    job.set_state(&platform.session(), JobState::Resuming).unwrap();

    meta_schedule(&mut platform);

    assert_eq!(
        get_job(&platform, job_id).state,
        JobState::Resuming.as_str(),
        "The job must not be resumed when the script fails"
    );
    assert!(
        meta::get_fragged_jobs(&platform.session()).unwrap().contains(&job_id),
        "The job should be flagged for deletion"
    );
    assert!(
        meta::get_job_events(&platform.session(), job_id)
            .unwrap()
            .iter()
            .any(|event| event.ev_type == "RESUME_SCRIPT_ERROR"),
        "A RESUME_SCRIPT_ERROR event should have been added"
    );
}

/// A job whose resources are `Absent` must not be launched: it stays in the
/// gantt (`Waiting`) until its resources become available.
#[test]
fn test_meta_schedule_does_not_launch_on_absent_resource() {
    let (mut platform, notifications) = setup(1);
    insert_queue(&platform, "default", 2);
    // The resource is known to the resource set but is currently absent.
    meta::set_resource_energy_fields(&platform.session(), 1, Some("Absent"), None, None).unwrap();
    let job_id = insert_job(&platform, "default", "resource_id=1", 60, vec![]);

    meta_schedule(&mut platform);

    assert_eq!(
        get_job(&platform, job_id).state,
        JobState::Waiting.as_str(),
        "A job on an absent resource must not be launched"
    );
    assert!(
        notifications.borrow().launched.is_empty(),
        "No launch notification must be sent for an absent resource"
    );
}

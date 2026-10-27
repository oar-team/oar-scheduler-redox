use crate::notification::{CommandNotifier, JobNotifier, ZmqBipbipNotifier};
use indexmap::IndexMap;
use oar_scheduler_core::model::configuration::Configuration;
use oar_scheduler_core::model::job::Job;
use oar_scheduler_core::platform::{PlatformConfig, PlatformTrait};
use oar_scheduler_db::model::gantt;
use oar_scheduler_db::model::jobs::{JobDatabaseRequests, JobReservation, JobState};
use oar_scheduler_db::model::meta;
use oar_scheduler_db::Session;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

pub struct Platform {
    now: i64,
    session: Session,
    platform_config: Rc<PlatformConfig>,
    /// Sends notifications (bipbip commander, oarsub clients).
    notifier: Box<dyn JobNotifier>,
    /// Jobs already sent to the bipbip commander, to avoid notifying twice.
    notified_jobs: HashSet<i64>,
}

impl Platform {
    pub fn from_database(mut session: Session, config: Configuration) -> Self {
        let now = session.get_now();
        let resource_set = session.get_resource_set(&config);
        let quotas_config = oar_scheduler_core::platform::build_quotas_config(&config, &resource_set);

        // Notify the bipbip commander through ZMQ by default. Deployments that
        // delegate the launch to an external command keep that behaviour by
        // setting `OAR_SCHEDULER_NOTIFY_TO_RUN_JOB_CMD`.
        let notifier: Box<dyn JobNotifier> = match std::env::var("OAR_SCHEDULER_NOTIFY_TO_RUN_JOB_CMD") {
            Ok(command) if !command.trim().is_empty() => Box::new(CommandNotifier),
            _ => Box::new(ZmqBipbipNotifier::new(config.bipbip_commander_endpoint())),
        };

        let platform_config = Rc::new(PlatformConfig {
            resource_set,
            quotas_config,
            config,
        });

        Platform {
            now,
            session,
            platform_config,
            notifier,
            notified_jobs: HashSet::new(),
        }
    }
    pub fn session(&self) -> &Session {
        &self.session
    }

    /// Overrides the current time of the platform.
    ///
    /// The platform takes the current time from the database when it is created
    /// ([`Session::get_now`]). Simulations and tests use this method to run the
    /// meta-scheduler at a controlled date.
    pub fn set_now(&mut self, now: i64) {
        self.now = now;
    }

    /// Returns the notifier used to send notifications.
    pub fn notifier(&self) -> &dyn JobNotifier {
        self.notifier.as_ref()
    }

    /// Replaces the notifier (used by tests to record notifications).
    /// Inject a custom notifier (used by tests, and available for embedders).
    #[allow(dead_code)]
    pub fn set_notifier(&mut self, notifier: Box<dyn JobNotifier>) {
        self.notifier = notifier;
    }

    /// Notifies the bipbip commander to run the job, only once per job.
    pub fn notify_to_run_job(&mut self, job_id: i64) -> bool {
        if self.notified_jobs.insert(job_id) {
            self.notifier.notify_to_run_job(job_id)
        } else {
            true
        }
    }

    /// Flags a job for deletion (best effort kill). Also notifies the almighty
    /// through the `Qdel` command in the Python implementation; the frag
    /// handling process is responsible for the actual deletion.
    pub fn frag_job(&self, job_id: i64) {
        meta::frag_job(&self.session, job_id).unwrap_or_else(|err| {
            log::error!("Unable to frag job {}: {}", job_id, err);
            false
        });
    }

    // Waiting jobs in the Gantt that should be launched before now + max(security_time, kill_duration_before_reservation)
    pub fn get_gantt_jobs_to_launch_with_security_time(&self) -> Vec<Job> {
        let mut interval = self.platform_config.config.scheduler_besteffort_kill_duration_before_reservation;
        if interval < self.platform_config.config.scheduler_job_security_time {
            interval = self.platform_config.config.scheduler_job_security_time;
        }
        let max_start_time = self.now + interval;
        Job::get_gantt_jobs(&self.session, None, None, Some(vec![JobState::Waiting]), Some(max_start_time)).unwrap()
    }

    /// The set of resources that are currently `Absent` (mirrors
    /// `resource_set.absent_roid_itvs`).
    pub fn get_absent_resources(&self) -> oar_scheduler_core::model::job::ProcSet {
        meta::get_absent_resource_ids(&self.session)
            .unwrap_or_default()
            .iter()
            .filter_map(|resource_id| self.session.resource_id_to_resource_index(*resource_id))
            .collect()
    }
    // AR jobs that are scheduled still on waiting state in the Gantt
    pub fn get_gantt_waiting_scheduled_ar_jobs(&self, queue_name: String) -> Vec<Job> {
        Job::get_gantt_jobs(
            &self.session,
            Some(vec![queue_name]),
            Some(JobReservation::Scheduled),
            Some(vec![JobState::Waiting]),
            None,
        )
            .unwrap()
    }
    // AR jobs that are not yet scheduled
    pub fn get_waiting_to_schedule_ar_jobs(&self, queue_name: String) -> IndexMap<i64, Job> {
        Job::get_jobs(
            &self.session,
            Some(vec![queue_name]),
            Some(JobReservation::ToSchedule),
            Some(vec![JobState::Waiting]),
        )
            .unwrap()
    }
    // Scheduled and at least toLaunch state jobs
    pub fn get_fully_scheduled_jobs(&self) -> IndexMap<i64, Job> {
        Job::get_jobs(
            &self.session,
            None,
            None,
            Some(vec![
                JobState::Running,
                JobState::ToLaunch,
                JobState::Launching,
                JobState::Finishing,
                JobState::Suspended,
                JobState::Resuming,
            ]),
        )
            .unwrap()
    }
    pub fn get_current_non_waiting_jobs_by_state(&self) -> HashMap<String, Vec<Job>> {
        let jobs = Job::get_jobs(
            &self.session,
            None,
            None,
            Some(vec![
                JobState::ToLaunch,
                JobState::ToError,
                JobState::ToAckReservation,
                JobState::Launching,
                JobState::Running,
                JobState::Finishing,
                JobState::Waiting,
                JobState::Hold,
                JobState::Suspended,
                JobState::Resuming,
            ]),
        )
            .unwrap();
        jobs.values().fold(HashMap::new(), |mut map, job| {
            map.entry(job.state.to_string()).or_insert_with(Vec::new).push(job.clone());
            map
        })
    }
}

impl PlatformTrait for Platform {
    fn get_now(&self) -> i64 {
        self.now
    }
    fn get_max_time(&self) -> i64 {
        2i64.pow(31)
    }
    fn get_platform_config(&self) -> &Rc<PlatformConfig> {
        &self.platform_config
    }

    fn get_scheduled_jobs(&self) -> Vec<Job> {
        Job::get_gantt_jobs(&self.session, None, None, None, None).unwrap()
    }
    fn get_waiting_jobs(&self, queues: Vec<String>) -> IndexMap<i64, Job> {
        Job::get_jobs(&self.session, Some(queues), Some(JobReservation::None), Some(vec![JobState::Waiting])).unwrap()
    }

    fn save_assignments(&mut self, assigned_jobs: IndexMap<i64, Job>) {
        // Jobs without assignment (e.g. a job being resumed but not launched yet)
        // cannot be saved in the gantt tables.
        let assigned_jobs = assigned_jobs
            .into_iter()
            .filter(|(_, job)| {
                if job.assignment.is_none() {
                    log::debug!("Skipping gantt save for job {} without assignment", job.id);
                }
                job.assignment.is_some()
            })
            .collect();
        gantt::save_jobs_assignments_in_gantt(&mut self.session, assigned_jobs).unwrap()
    }

    fn get_sum_accounting_window(&self, queues: &[String], window_start: i64, window_stop: i64) -> (f64, f64) {
        meta::get_sum_accounting_window(&self.session, queues, window_start, window_stop).expect("Failed to get accounting sums")
    }
    fn get_sum_accounting_by_project(&self, queues: &[String], window_start: i64, window_stop: i64) -> (HashMap<String, f64>, HashMap<String, f64>) {
        meta::get_sum_accounting_by_project(&self.session, queues, window_start, window_stop).expect("Failed to get accounting sums by project")
    }
    fn get_sum_accounting_by_user(&self, queues: &[String], window_start: i64, window_stop: i64) -> (HashMap<String, f64>, HashMap<String, f64>) {
        meta::get_sum_accounting_by_user(&self.session, queues, window_start, window_stop).expect("Failed to get accounting sums by user")
    }
}

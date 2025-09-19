use indexmap::IndexMap;
use oar_scheduler_core::model::configuration::Configuration;
use oar_scheduler_core::model::job::Job;
use oar_scheduler_core::platform::{PlatformConfig, PlatformTrait};
use oar_scheduler_db::model::gantt;
use oar_scheduler_db::model::jobs::{JobDatabaseRequests, JobReservation, JobState};
use oar_scheduler_db::Session;
use std::collections::HashMap;
use std::hash::Hash;
use std::rc::Rc;

pub struct Platform {
    now: i64,
    session: Session,
    platform_config: Rc<PlatformConfig>,
}

impl Platform {
    pub fn from_database(mut session: Session, config: Configuration) -> Self {
        let now = session.get_now();
        let resource_set = session.get_resource_set(&config);
        let quotas_config = oar_scheduler_core::platform::build_quotas_config(&config, &resource_set);

        let platform_config = Rc::new(PlatformConfig {
            resource_set,
            quotas_config,
            config,
        });

        Platform {
            now,
            session,
            platform_config,
        }
    }
    pub fn session(&self) -> &Session {
        &self.session
    }

    // Waiting jobs in the Gantt that should be launched before now + min(security_time, kill_duration_before_reservation)
    pub fn get_gantt_jobs_to_launch_with_security_time(&self) -> Vec<Job> {
        let mut interval = self.platform_config.config.scheduler_besteffort_kill_duration_before_reservation;
        if interval < self.platform_config.config.scheduler_job_security_time {
            interval = self.platform_config.config.scheduler_job_security_time;
        }
        let max_start_time = self.now + interval;
        Job::get_gantt_jobs(&self.session, None, None, Some(vec![JobState::Waiting]), Some(max_start_time)).unwrap()
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
        gantt::save_jobs_assignments_in_gantt(&mut self.session, assigned_jobs).unwrap()
    }

    fn get_sum_accounting_window(&self, queues: &[String], window_start: i64, window_stop: i64) -> (f64, f64) {
        todo!()
    }
    fn get_sum_accounting_by_project(&self, queues: &[String], window_start: i64, window_stop: i64) -> (HashMap<String, f64>, HashMap<String, f64>) {
        todo!()
    }
    fn get_sum_accounting_by_user(&self, queues: &[String], window_start: i64, window_stop: i64) -> (HashMap<String, f64>, HashMap<String, f64>) {
        todo!()
    }
}

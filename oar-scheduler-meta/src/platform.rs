use indexmap::IndexMap;
use oar_scheduler_core::model::configuration::Configuration;
use oar_scheduler_core::model::job::Job;
use oar_scheduler_core::platform::{PlatformConfig, PlatformTrait};
use oar_scheduler_db::Session;
use std::collections::HashMap;
use std::rc::Rc;

pub struct Platform {
    now: i64,
    session: Session,
    platform_config: Rc<PlatformConfig>,
}

impl Platform {
    pub async fn from_database(session: Session, config: Configuration) -> Self {
        let now = session.get_now().await;
        let resource_set = session.get_resource_set().await;
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

    fn get_scheduled_jobs(&self) -> &Vec<Job> {
        todo!()
    }
    fn get_waiting_jobs(&self) -> IndexMap<u32, Job> {
        todo!()
    }
    fn save_assignments(&mut self, assigned_jobs: IndexMap<u32, Job>) {
        todo!()
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

/*
 * Copyright (c) 2025 Clément GRENNERAT
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, version 3.
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see https://www.gnu.org/licenses/.
 *
 */

use indexmap::IndexMap;
use oar_scheduler_core::model::configuration::Configuration;
use oar_scheduler_core::model::job::Job;
use oar_scheduler_core::platform::{PlatformConfig, PlatformTrait};
use oar_scheduler_db::model::JobDatabaseRequests;
use oar_scheduler_db::Session;
use std::collections::HashMap;
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

    pub fn get_waiting_scheduled_ar_jobs(&self, queue_name: String) -> Vec<Job> {
        Job::get_gantt_scheduled_jobs(&self.session, Some(vec![queue_name]), Some("Scheduled".to_string()), Some(vec!["Waiting".to_string()])).unwrap()
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
        Job::get_gantt_scheduled_jobs(&self.session, Some(vec!["scheduled".to_string()]), None, None).unwrap()
    }
    fn get_waiting_jobs(&self) -> IndexMap<i64, Job> {
        Job::get_jobs(&self.session, Some(vec!["scheduled".to_string()]), Some("None".to_string()), Some(vec!["Waiting".to_string()])).unwrap()
    }
    fn save_assignments(&mut self, assigned_jobs: IndexMap<i64, Job>) {
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

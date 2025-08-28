use serde::{Deserialize, Serialize};

const DEFAULT_CONFIG_FILE: &str = "/etc/oar/oar.conf";

pub struct Configuration {
    pub job_priority: JobPriority,
    pub scheduler_job_security_time: i64,
    pub quotas: bool,
    pub quotas_conf_file: Option<String>,
    pub quotas_window_time_limit: i64,
    pub quotas_all_nb_resources_mode: QuotasAllNbResourcesMode,
    pub cache_enabled: bool,
}

impl Default for Configuration {
    fn default() -> Self {
        Configuration {
            job_priority: JobPriority::Fairshare,
            scheduler_job_security_time: 60, // 1 minute
            quotas: false,
            quotas_conf_file: None,
            quotas_window_time_limit: 60 * 24 * 3600, // 60 days
            quotas_all_nb_resources_mode: QuotasAllNbResourcesMode::DefaultNotDead,
            cache_enabled: true,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobPriority {
    Fifo,
    Fairshare,
    Multifactor,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotasAllNbResourcesMode {
    All,
    DefaultNotDead,
}

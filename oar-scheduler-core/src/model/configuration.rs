use serde::{Deserialize, Serialize};

const DEFAULT_CONFIG_FILE: &str = "/etc/oar/oar.conf";

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub struct Configuration {
    pub scheduler_job_security_time: i64,
    pub cache_enabled: bool,
    // --- Quotas configuration ---
    pub quotas: bool,
    pub quotas_conf_file: Option<String>,
    pub quotas_window_time_limit: Option<i64>,
    pub quotas_all_nb_resources_mode: QuotasAllNbResourcesMode,
    // -- Job sorting configuration ---
    pub job_priority: JobPriority,
    pub priority_conf_file: Option<String>,
    // --- Job sorting: Fairshare configuration ---
    pub scheduler_fairsharing_window_size: Option<i64>,
    pub scheduler_fairsharing_project_targets: Option<String>,
    pub scheduler_fairsharing_user_targets: Option<String>,
    pub scheduler_fairsharing_coef_project: Option<f64>,
    pub scheduler_fairsharing_coef_user: Option<f64>,
    pub scheduler_fairsharing_coef_user_ask: Option<f64>,
}

impl Default for Configuration {
    fn default() -> Self {
        Configuration {
            scheduler_job_security_time: 60, // 1 minute
            cache_enabled: true,
            quotas: false,
            quotas_conf_file: None,
            quotas_window_time_limit: Some(60 * 24 * 3600), // 60 days
            quotas_all_nb_resources_mode: QuotasAllNbResourcesMode::DefaultNotDead,
            job_priority: JobPriority::Fifo,
            priority_conf_file: None,
            scheduler_fairsharing_window_size: None,
            scheduler_fairsharing_project_targets: None,
            scheduler_fairsharing_user_targets: None,
            scheduler_fairsharing_coef_project: None,
            scheduler_fairsharing_coef_user: None,
            scheduler_fairsharing_coef_user_ask: None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobPriority {
    Fifo,
    Fairshare,
    Multifactor,
}
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum QuotasAllNbResourcesMode {
    All,
    DefaultNotDead,
}

use serde::{Deserialize, Serialize};

pub const DEFAULT_CONFIG_FILE: &str = "/etc/oar/oar.conf";

/// Default value of `RESERVATION_WAITING_RESOURCES_TIMEOUT` (from `oar.conf.in`).
pub fn default_reservation_waiting_resources_timeout() -> i64 {
    300
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Configuration {
    // --- Global configuration ---
    pub scheduler_job_security_time: i64,
    pub cache_enabled: bool,
    pub scheduler_besteffort_kill_duration_before_reservation: i64,
    /// Time (in seconds) an advance reservation waits for all its resources to
    /// become available before being launched with a reduced resource set.
    #[serde(default = "default_reservation_waiting_resources_timeout")]
    pub reservation_waiting_resources_timeout: i64,
    // --- Database configuration ---
    pub db_type: String,
    pub db_hostname: String,
    pub db_port: String,
    pub db_base_name: String,
    pub db_base_login: String,
    pub db_base_passwd: String,
    pub db_base_login_ro: String,
    pub db_base_passwd_ro: String,
    // --- Resources configuration ---
    pub scheduler_resource_order: Option<String>,
    pub scheduler_available_suspended_resource_type: Option<String>,
    pub hierarchy_labels: Option<String>,
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
    // --- Walltime change requests configuration ---
    #[serde(default)]
    pub walltime_change_enabled: Option<String>,
    #[serde(default)]
    pub walltime_change_apply_time: Option<String>,
    #[serde(default)]
    pub walltime_increment: Option<String>,
    // --- Energy saving (node sleep/wake up) configuration ---
    #[serde(default)]
    pub energy_saving_mode: Option<String>,
    #[serde(default)]
    pub energy_saving_internal: Option<String>,
    #[serde(default)]
    pub scheduler_node_manager_sleep_cmd: Option<String>,
    #[serde(default)]
    pub scheduler_node_manager_wake_up_cmd: Option<String>,
    #[serde(default)]
    pub energy_saving_node_manager_sleep_cmd: Option<String>,
    #[serde(default)]
    pub energy_saving_node_manager_wake_up_cmd: Option<String>,
    #[serde(default)]
    pub scheduler_node_manager_idle_time: Option<i64>,
    #[serde(default)]
    pub scheduler_node_manager_sleep_time: Option<i64>,
    #[serde(default)]
    pub scheduler_node_manager_wakeup_time: Option<i64>,
    #[serde(default)]
    pub scheduler_timeout: Option<i64>,
    // --- Suspend/resume configuration ---
    #[serde(default)]
    pub just_before_resume_exec_file: Option<String>,
    #[serde(default)]
    pub suspend_resume_script_timeout: Option<i64>,
}

impl Configuration {
    /// Parses a configuration from a `.conf` (key=value) string.
    pub fn load_from_str(contents: &str) -> Option<Self> {
        serde_envfile::from_str(contents).ok()
    }

    /// Load configuration from a file, in a .conf format (key=value).
    pub fn load() -> Self {
        let path = if let Ok(path) = std::env::var("OARCONFFILE") {
            path
        } else {
            DEFAULT_CONFIG_FILE.to_string()
        };

        let contents = std::fs::read_to_string(&path).ok();
        if let Some(contents) = contents {
            Self::load_from_str(&contents).unwrap_or_else(|| {
                eprintln!(
                    "Warning: could not parse configuration file '{}', using default configuration.",
                    path
                );
                Configuration::default()
            })
        } else {
            Configuration::default()
        }
    }
}

impl Default for Configuration {
    fn default() -> Self {
        Configuration {
            // --- Global configuration ---
            scheduler_job_security_time: 60, // 1 minute
            cache_enabled: true,
            scheduler_besteffort_kill_duration_before_reservation: 60, // 1 minute
            reservation_waiting_resources_timeout: default_reservation_waiting_resources_timeout(),
            // --- Database configuration ---
            db_type: "Pg".to_string(),
            db_hostname: "localhost".to_string(),
            db_port: "4321".to_string(),
            db_base_name: "oar".to_string(),
            db_base_login: "oar".to_string(),
            db_base_passwd: "oar".to_string(),
            db_base_login_ro: "oar_ro".to_string(),
            db_base_passwd_ro: "oar_ro".to_string(),
            // --- Resources configuration ---
            scheduler_resource_order: None,
            scheduler_available_suspended_resource_type: None,
            hierarchy_labels: None,
            // --- Quotas configuration ---
            quotas: false,
            quotas_conf_file: None,
            quotas_window_time_limit: Some(60 * 24 * 3600), // 60 days
            quotas_all_nb_resources_mode: QuotasAllNbResourcesMode::DefaultNotDead,
            // -- Job sorting configuration ---
            job_priority: JobPriority::Fifo,
            priority_conf_file: None,
            // --- Job sorting: Fairshare configuration ---
            scheduler_fairsharing_window_size: None,
            scheduler_fairsharing_project_targets: None,
            scheduler_fairsharing_user_targets: None,
            scheduler_fairsharing_coef_project: None,
            scheduler_fairsharing_coef_user: None,
            scheduler_fairsharing_coef_user_ask: None,
            walltime_change_enabled: None,
            walltime_change_apply_time: None,
            walltime_increment: None,
            energy_saving_mode: None,
            energy_saving_internal: None,
            scheduler_node_manager_sleep_cmd: None,
            scheduler_node_manager_wake_up_cmd: None,
            energy_saving_node_manager_sleep_cmd: None,
            energy_saving_node_manager_wake_up_cmd: None,
            scheduler_node_manager_idle_time: None,
            scheduler_node_manager_sleep_time: None,
            scheduler_node_manager_wakeup_time: None,
            scheduler_timeout: None,
            just_before_resume_exec_file: None,
            suspend_resume_script_timeout: None,
        }
    }
}

impl Configuration {
    /// In OAR configuration files, booleans are usually written as `yes`/`no`.
    /// This returns `true` only for `yes` (case-insensitive) or `true`.
    pub fn walltime_change_enabled(&self) -> bool {
        matches!(
            self.walltime_change_enabled.as_deref().map(str::trim).map(str::to_ascii_uppercase).as_deref(),
            Some("YES") | Some("TRUE")
        )
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

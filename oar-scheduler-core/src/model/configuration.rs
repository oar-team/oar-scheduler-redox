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

use serde::{Deserialize, Serialize};

pub const DEFAULT_CONFIG_FILE: &str = "/etc/oar/oar.conf";

#[derive(Serialize, Deserialize, Debug)]
pub struct Configuration {
    pub scheduler_job_security_time: i64,
    pub cache_enabled: bool,
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
}

impl Configuration {
    /// Load configuration from a file, in a .conf format (key=value).
    pub fn load() -> Self {
        let path = if let Ok(path) = std::env::var("OARCONFFILE") {
            path
        } else {
            DEFAULT_CONFIG_FILE.to_string()
        };

        let contents = std::fs::read_to_string(&path).ok();
        if let Some(contents) = contents {
            serde_envfile::from_str(&contents).unwrap_or_else(|e| {
                eprintln!(
                    "Warning: could not parse configuration file '{}': {}, using default configuration.",
                    path, e
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
            scheduler_job_security_time: 60, // 1 minute
            cache_enabled: true,
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

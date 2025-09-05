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

use crate::hooks::get_hooks_manager;
use crate::model::configuration::JobPriority;
use crate::model::job::{Job, ProcSetCoresOp};
use crate::platform::PlatformTrait;
use indexmap::IndexMap;
use log::{info, warn};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;

// Parse strings like "{default => 21.0, user1=>30}" into a map
fn parse_perl_hash_to_map_f64(s: &str) -> HashMap<String, f64> {
    let mut map = HashMap::new();
    let trimmed = s.trim();
    let inner = trimmed.trim_start_matches('{').trim_end_matches('}');
    for pair in inner.split(',') {
        let p = pair.trim();
        if p.is_empty() { continue; }
        if let Some((k, v)) = p.split_once("=>") {
            let key = k.trim().to_string();
            if let Ok(val) = v.trim().parse::<f64>() { map.insert(key, val); }
        }
    }
    map
}

#[derive(Debug, Deserialize)]
struct PriorityYaml {
    #[serde(default)]
    age_weight: f64,
    #[serde(default = "default_age_coef")]
    age_coef: f64,
    #[serde(default)]
    queue_weight: f64,
    #[serde(default)]
    queue_coefs: HashMap<String, f64>,
    #[serde(default)]
    work_weight: f64,
    #[serde(default)]
    work_mode: f64,
    #[serde(default)]
    size_weight: f64,
    #[serde(default)]
    size_mode: f64,
    #[serde(default)]
    karma_weight: f64,
    #[serde(default)]
    qos_weight: f64,
    #[serde(default)]
    nice_weight: f64,
}

fn default_age_coef() -> f64 { 1.65e-06 }

fn load_priority_yaml(path_opt: &Option<String>) -> PriorityYaml {
    if let Some(path) = path_opt {
        if let Ok(content) = fs::read_to_string(path) {
            info!("Parsing multifactor priority configuration from {}: {}", path, content);
            info!("{:?}", serde_yaml::from_str::<PriorityYaml>(&content));
            if let Ok(yaml) = serde_yaml::from_str::<PriorityYaml>(&content) {
                info!("Parsed multifactor priority configuration from {}: {}", path, content);
                return yaml;
            }
        }
    }
    info!("Using default multifactor priority configuration");
    PriorityYaml {
        age_weight: 0.0,
        age_coef: default_age_coef(),
        queue_weight: 0.0,
        queue_coefs: HashMap::new(),
        work_weight: 0.0,
        work_mode: 0.0,
        size_weight: 0.0,
        size_mode: 0.0,
        karma_weight: 0.0,
        qos_weight: 0.0,
        nice_weight: 0.0,
    }
}

/// Computes the karma for each job in waiting_jobs and saves it in the `job.karma` attribute.
fn evaluate_jobs_karma<P: PlatformTrait>(
    platform: &P,
    queues: &Vec<String>,
    waiting_jobs: &mut IndexMap<i64, Job>,
) {
    let cfg = &platform.get_platform_config().config;
    assert!(cfg.scheduler_fairsharing_window_size.is_some(), "SCHEDULER_FAIRSHARING_WINDOW_SIZE must be set");
    assert!(cfg.scheduler_fairsharing_project_targets.is_some(), "SCHEDULER_FAIRSHARING_PROJECT_TARGETS must be set");
    assert!(cfg.scheduler_fairsharing_user_targets.is_some(), "SCHEDULER_FAIRSHARING_USER_TARGETS must be set");
    assert!(cfg.scheduler_fairsharing_coef_project.is_some(), "SCHEDULER_FAIRSHARING_COEF_PROJECT must be set");
    assert!(cfg.scheduler_fairsharing_coef_user.is_some(), "SCHEDULER_FAIRSHARING_COEF_USER must be set");
    assert!(cfg.scheduler_fairsharing_coef_user_ask.is_some(), "SCHEDULER_FAIRSHARING_COEF_USER_ASK must be set");

    let window_size = cfg.scheduler_fairsharing_window_size.unwrap();
    let project_targets_pct = parse_perl_hash_to_map_f64(cfg.scheduler_fairsharing_project_targets.as_ref().unwrap());
    let user_targets_pct = parse_perl_hash_to_map_f64(cfg.scheduler_fairsharing_user_targets.as_ref().unwrap());
    let coef_project = cfg.scheduler_fairsharing_coef_project.unwrap();
    let coef_user = cfg.scheduler_fairsharing_coef_user.unwrap();
    let coef_user_ask = cfg.scheduler_fairsharing_coef_user_ask.unwrap();

    let now = platform.get_now();
    let window_start = now - window_size;
    let window_stop = now;

    let (sum_asked, sum_used) = platform.get_sum_accounting_window(&queues, window_start, window_stop);
    let (_proj_asked, proj_used) = platform.get_sum_accounting_by_project(&queues, window_start, window_stop);
    let (user_asked, user_used) = platform.get_sum_accounting_by_user(&queues, window_start, window_stop);

    for (_job_id, job) in waiting_jobs.iter_mut() {
        let project = job.project.as_deref().unwrap_or("");
        let user = job.user.as_deref().unwrap_or("");

        let proj_used_j = *proj_used.get(project).unwrap_or(&0.0);
        let user_used_j = *user_used.get(user).unwrap_or(&0.0);
        let user_asked_j = *user_asked.get(user).unwrap_or(&0.0);

        let proj_target_pct = *project_targets_pct.get(project).unwrap_or(&0.0);
        let user_target_pct = *user_targets_pct.get(user).unwrap_or(&0.0);

        // Follow Python logic: targets are in percent, divide by 100
        let projet = coef_project * ((proj_used_j / sum_used) - (proj_target_pct / 100.0));
        let user_v = coef_user * ((user_used_j / sum_used) - (user_target_pct / 100.0));
        let user_ask_v = coef_user_ask * ((user_asked_j / sum_asked) - (user_target_pct / 100.0));

        job.karma = projet + user_v + user_ask_v;
    }
}

/// Compute multifactor priority for each job from YAML config and sort waiting_jobs by priority desc.
fn multifactor_sort<P: PlatformTrait>(platform: &P, queues: &Vec<String>, waiting_jobs: &mut IndexMap<i64, Job>) {
    // Load YAML config
    let cfg = &platform.get_platform_config().config;
    let pyaml = load_priority_yaml(&cfg.priority_conf_file);

    // Compute karma if needed
    if pyaml.karma_weight > 0.0 {
        evaluate_jobs_karma(platform, queues, waiting_jobs);
    }

    let now = platform.get_now();
    let resource_set = &platform.get_platform_config().resource_set;
    let cluster_size = resource_set.default_resources.core_count() as f64;
    let unit_names = resource_set.hierarchy.unit_partitions().clone();

    let max_time = platform.get_max_time() as f64;

    // Precompute priorities
    let mut prio: HashMap<i64, f64> = HashMap::with_capacity(waiting_jobs.len());
    for (jid, job) in waiting_jobs.iter() {
        let mut p: f64 = 0.0;

        // age
        if pyaml.age_weight > 0.0 {
            let age = (now - job.submission_time).max(0) as f64;
            let age_factor = (pyaml.age_coef * age).max(1.0);
            p += pyaml.age_weight * age_factor;
        }

        // queue
        if pyaml.queue_weight > 0.0 {
            if let Some(coef) = pyaml.queue_coefs.get(job.queue.as_ref()) {
                p += pyaml.queue_weight * (*coef);
            } else if !pyaml.queue_coefs.is_empty() {
                warn!("queue {} is not defined in queue_coefs but queue_weight > 0", job.queue);
            }
        }

        // work = nb_resources * walltime normalized to [0,1] by (cluster_size * max_time)
        if pyaml.work_weight > 0.0 && cluster_size > 0.0 && max_time > 0.0 {
            let mut work_norm: f64 = 0.0;
            let factor = if pyaml.work_mode != 0.0 {
                // big jobs prioritized
                1.0 - 1.0 / work_norm.max(1e-12).min(1.0)
            } else {
                // small jobs prioritized
                1.0 / work_norm.max(1e-12).min(1.0)
            };
            p += pyaml.work_weight * factor;
        }

        // size
        if pyaml.size_weight > 0.0 && cluster_size > 0.0 {
            let mut size_frac: f64 = 0.0;
            let factor = if pyaml.size_mode != 0.0 {
                // big jobs prioritized
                size_frac
            } else {
                // small jobs prioritized
                1.0 - size_frac
            };
            p += pyaml.size_weight * factor;
        }

        // karma
        if pyaml.karma_weight > 0.0 {
            let karma_factor = 1.0 / (1.0 + job.karma);
            p += pyaml.karma_weight * karma_factor;
        }

        // qos
        if pyaml.qos_weight > 0.0 {
            p += pyaml.qos_weight * job.qos;
        }

        // nice
        if pyaml.nice_weight > 0.0 {
            p += pyaml.nice_weight * job.nice.max(1.0);
        }

        prio.insert(*jid, p);
    }

    waiting_jobs.sort_by(|id1, _j1, id2, _j2| {
        let p1 = prio.get(id1).copied().unwrap_or(0.0);
        let p2 = prio.get(id2).copied().unwrap_or(0.0);
        p1.partial_cmp(&p2).unwrap_or(std::cmp::Ordering::Equal)
    });
    waiting_jobs.reverse(); // descending
}

pub fn sort_jobs<P>(platform: &P, queues: &Vec<String>, waiting_jobs: &mut IndexMap<i64, Job>)
where
    P: PlatformTrait,
{
    if get_hooks_manager().hook_sort(platform.get_platform_config(), queues, waiting_jobs) {
        return;
    }

    match &platform.get_platform_config().config.job_priority {
        JobPriority::Fifo => {
            // No sorting required.
        },
        JobPriority::Fairshare => {
            evaluate_jobs_karma(platform, queues, waiting_jobs);
            waiting_jobs.sort_by(|_id1, job1, _id2, job2| {
                job1.karma.partial_cmp(&job2.karma).unwrap_or(std::cmp::Ordering::Equal)
            });
        },
        JobPriority::Multifactor => {
            multifactor_sort(platform, queues, waiting_jobs);
        },
    }
}

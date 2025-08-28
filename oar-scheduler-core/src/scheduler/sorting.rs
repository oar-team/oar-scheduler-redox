use crate::hooks::get_hooks_manager;
use crate::models::Job;
use crate::platform::PlatformTrait;
use indexmap::IndexMap;
use oar_scheduler_dao::model::configuration::JobPriority;
use std::collections::HashMap;

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


/// Computes the karma for each job in waiting_jobs and saves it in the `job.karma` attribute.
fn evaluate_jobs_karma<P: PlatformTrait>(
    platform: &P,
    queues: &Vec<String>,
    waiting_jobs: &mut IndexMap<u32, Job>,
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

pub fn sort_jobs<P>(platform: &P, queues: &Vec<String>, waiting_jobs: &mut IndexMap<u32, Job>)
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
        JobPriority::Multifactor => {},
    }
}


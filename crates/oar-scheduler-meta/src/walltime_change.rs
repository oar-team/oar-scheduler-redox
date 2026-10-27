//! Processing of walltime change requests.
//!
//! Rust counterpart of `oar/kao/walltime_change.py` (which itself relies on
//! `oar/lib/walltime.py` and `oar/lib/job_handling.py`).

use crate::platform::Platform;
use log::{debug, warn};
use oar_scheduler_core::platform::PlatformTrait;
use oar_scheduler_db::model::meta;

/// A configuration value that may either be a plain value or a Perl-like hash
/// keyed by queue name (with `_` as the default key).
///
/// Values that are floats lower than or equal to `1.0` are interpreted as a
/// ratio of the reference walltime.
fn parse_config_number(token: &str) -> Option<(f64, bool)> {
    let token = token.trim();
    let is_float = token.contains('.') || token.contains('e') || token.contains('E');
    token.parse::<f64>().ok().map(|value| (value, is_float))
}

/// Parse a Perl-like hash such as `{default => 3600, targui => 7200}`.
fn parse_perl_hash(value: &str) -> Option<Vec<(String, (f64, bool))>> {
    let value = value.trim();
    let inner = value.strip_prefix('{')?.strip_suffix('}')?;
    let mut entries = Vec::new();
    for entry in inner.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        // Both `{key => value}` (Perl) and `{'key': value}` (Python dict, also
        // accepted by `oar.lib.walltime.get_conf`) are supported.
        let (key, val) = match entry.split_once("=>") {
            Some(kv) => kv,
            None => entry.split_once(':')?,
        };
        let key = key.trim().trim_matches(|c| c == '\'' || c == '"').to_string();
        let (val, is_float) = parse_config_number(val)?;
        entries.push((key, (val, is_float)));
    }
    Some(entries)
}

/// Return the value of a walltime-change configuration setting for a given
/// queue. Mirrors `oar.lib.walltime.get_conf`.
pub fn get_conf(config_value: Option<&str>, queue: &str, walltime: i64, default: i64) -> i64 {
    let value = match config_value {
        Some(value) if value.trim().starts_with('{') => match parse_perl_hash(value) {
            Some(entries) => entries
                .iter()
                .find(|(key, _)| key == queue)
                .or_else(|| entries.iter().find(|(key, _)| key == "_"))
                .map(|(_, value)| *value)
                .unwrap_or((default as f64, false)),
            None => {
                warn!("cannot parse walltime setting: {}", value);
                (default as f64, false)
            }
        },
        Some(value) => parse_config_number(value).unwrap_or((default as f64, false)),
        None => (default as f64, false),
    };

    let (value, is_float) = value;
    if walltime != 0 && is_float && value <= 1.0 {
        return (walltime as f64 * value) as i64;
    }
    value as i64
}

/// Format a duration in seconds as `H:M:S` (matching `tools.duration_to_sql`).
fn duration_to_sql(seconds: i64) -> String {
    let hour = seconds / 3600;
    let min = (seconds - hour * 3600) / 60;
    let sec = seconds % 60;
    format!("{}:{}:{}", hour, min, sec)
}

/// As [`duration_to_sql`] but with an explicit sign.
fn duration_to_sql_signed(seconds: i64) -> String {
    let sign = if seconds > 0 {
        "+"
    } else if seconds < 0 {
        "-"
    } else {
        ""
    };
    format!("{}{}", sign, duration_to_sql(seconds.abs()))
}

/// Process all the pending walltime change requests.
/// Mirrors `oar.kao.walltime_change.process_walltime_change_requests`.
pub fn process_walltime_change_requests(platform: &Platform) {
    let session = platform.session();
    let now = platform.get_now();
    let platform_config = platform.get_platform_config();
    let security_time = platform_config.config.scheduler_job_security_time;
    let apply_time_conf = platform_config.config.walltime_change_apply_time.clone();
    let increment_conf = platform_config.config.walltime_increment.clone();

    let jobs = match meta::get_jobs_with_walltime_change(session) {
        Ok(jobs) => jobs,
        Err(err) => {
            warn!("Unable to get jobs with walltime change: {}", err);
            return;
        }
    };

    for job in jobs {
        let suspended = match meta::get_job_suspended_sum_duration(session, job.job_id, now) {
            Ok(suspended) => suspended,
            Err(err) => {
                warn!("Unable to get suspended duration of job {}: {}", job.job_id, err);
                0
            }
        };
        let mut fit = job.pending;

        if fit > 0 {
            // `job.walltime` is the currently assigned walltime (including the
            // already granted extra time), so the original request is the
            // difference with the already granted time.
            let orig_walltime = job.walltime - job.granted;
            let apply_time = get_conf(apply_time_conf.as_deref(), &job.queue_name, orig_walltime, 0);
            let increment = get_conf(increment_conf.as_deref(), &job.queue_name, orig_walltime, 0);

            debug!(
                "[{}] walltime change: pending={}s delay_next_jobs={} force={} apply_time={}s increment={}s",
                job.job_id, fit, job.delay_next_jobs, job.force, apply_time, increment
            );

            if job.force != "YES" {
                let delay = job.start_time + job.walltime + suspended - apply_time - now;
                if apply_time > 0 && delay > 0 {
                    debug!("[job_id:{}] walltime change could apply in {}s", job.job_id, delay);
                    continue;
                }
                if increment > 0 && increment < fit {
                    fit = increment;
                }
            }

            debug!("[{}] walltime change try: {}/{}s", job.job_id, fit, job.pending);
            let from_ = job.start_time + job.walltime + suspended;
            let mut to = from_ + fit;

            if job.has_type("inner") {
                match job.type_value("inner").and_then(|value| value.parse::<i64>().ok()) {
                    Some(container_id) => match meta::get_running_job(session, container_id) {
                        Ok(Some((container_start, container_walltime))) => {
                            if container_start + container_walltime < to {
                                to = container_start + container_walltime;
                                debug!(
                                    "[{}] walltime change for inner job limited to the container's boundaries: {}s",
                                    job.job_id,
                                    to - from_
                                );
                            }
                        }
                        Ok(None) => debug!("[{}] walltime change for inner job but container is not found ?", job.job_id),
                        Err(err) => warn!("Unable to get running job {}: {}", container_id, err),
                    },
                    None => debug!("[{}] walltime change for inner job with an invalid container id", job.job_id),
                }
            }

            let possible = meta::get_possible_job_end_time_in_interval(
                session,
                from_,
                to,
                &job.rids,
                security_time,
                &job.delay_next_jobs,
                &job.types,
                &job.user,
                &job.name,
            )
            .unwrap_or_else(|err| {
                warn!("Unable to get possible end time for job {}: {}", job.job_id, err);
                to
            });
            fit = possible - from_;
            if fit <= 0 {
                debug!(
                    "[{}] walltime cannot be changed for now (pending: {})",
                    job.job_id,
                    duration_to_sql_signed(job.pending)
                );
                continue;
            }
        } else if fit < 0 {
            let job_remaining_time = job.start_time + job.walltime + suspended - now;
            if job_remaining_time < -fit {
                fit = -job_remaining_time;
            }
        }

        let new_walltime = job.walltime + fit;
        let new_pending = job.pending - fit;

        let message = format!(
            "walltime changed: {} (granted: {}/pending: {}{}{})",
            duration_to_sql(new_walltime),
            duration_to_sql_signed(fit),
            duration_to_sql_signed(new_pending),
            if job.force == "YES" { "/force" } else { "" },
            if job.delay_next_jobs == "YES" { "/delay next jobs" } else { "" }
        );

        debug!("[{}] {}", job.job_id, message);

        // `force` and `delay_next_jobs` are reset to "NO" once the request has
        // been processed (Python passes `None` which is stored as "NO").
        if let Err(err) = meta::update_walltime_change_request(
            session,
            job.job_id,
            new_pending,
            "NO",
            "NO",
            job.granted + fit,
            if job.force == "YES" && fit > 0 {
                job.granted_with_force + fit
            } else {
                0
            },
            if job.delay_next_jobs == "YES" && fit > 0 {
                job.granted_with_delay_next_jobs + fit
            } else {
                0
            },
        ) {
            warn!("Unable to update walltime change request of job {}: {}", job.job_id, err);
        }

        if let Err(err) = meta::change_walltime(session, job.job_id, new_walltime, &message) {
            warn!("Unable to change walltime of job {}: {}", job.job_id, err);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_conf_scalar() {
        assert_eq!(get_conf(Some("3600"), "default", 0, 0), 3600);
        assert_eq!(get_conf(Some("3600"), "default", 1000, 0), 3600);
        assert_eq!(get_conf(None, "default", 1000, 42), 42);
        assert_eq!(get_conf(Some("garbage"), "default", 1000, 42), 42);
    }

    #[test]
    fn test_get_conf_ratio() {
        // A float <= 1.0 is a ratio of the reference walltime.
        assert_eq!(get_conf(Some("0.5"), "default", 3600, 0), 1800);
        // An integer 1 is not a ratio.
        assert_eq!(get_conf(Some("1"), "default", 3600, 0), 1);
    }

    #[test]
    fn test_get_conf_hash() {
        let conf = "{default => 3600, targui => 7200}";
        assert_eq!(get_conf(Some(conf), "targui", 0, 0), 7200);
        assert_eq!(get_conf(Some(conf), "default", 0, 0), 3600);
        // Unknown queue falls back to `_` if present, otherwise to the default.
        assert_eq!(get_conf(Some(conf), "other", 0, 7), 7);
        let with_default = "{_ => 100, targui => 200}";
        assert_eq!(get_conf(Some(with_default), "other", 0, 7), 100);
    }

    #[test]
    fn test_duration_formatting() {
        assert_eq!(duration_to_sql(3661), "1:1:1");
        assert_eq!(duration_to_sql_signed(3661), "+1:1:1");
        assert_eq!(duration_to_sql_signed(-61), "-0:1:1");
        assert_eq!(duration_to_sql_signed(0), "0:0:0");
    }
}

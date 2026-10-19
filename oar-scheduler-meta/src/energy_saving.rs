//! Energy saving: decide which nodes to halt and which to wake up.
//!
//! Rust counterpart of `oar.kao.meta_sched.nodes_energy_saving` and the
//! `oar/lib/node.py` helpers it relies on.

use crate::platform::Platform;
use log::{debug, error, info, warn};
use oar_scheduler_core::platform::PlatformTrait;
use oar_scheduler_db::model::meta;

/// The nodes to halt and to wake up, as computed by [`compute_nodes_energy_saving`].
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct NodeChanges {
    pub halt: Vec<String>,
    pub wakeup: Vec<String>,
}

fn is_yes(value: Option<&str>) -> bool {
    value.map(|value| value.trim().eq_ignore_ascii_case("yes")).unwrap_or(false)
}

/// Compute the nodes to halt and to wake up.
/// Mirrors the decision-making part of `oar.kao.meta_sched.nodes_energy_saving`.
pub fn compute_nodes_energy_saving(platform: &Platform) -> NodeChanges {
    let session = platform.session();
    let now = platform.get_now();
    let config = &platform.get_platform_config().config;
    let internal = is_yes(config.energy_saving_internal.as_deref());

    let has_sleep_cmd =
        config.scheduler_node_manager_sleep_cmd.is_some() || (internal && config.energy_saving_node_manager_sleep_cmd.is_some());
    let has_wakeup_cmd =
        config.scheduler_node_manager_wake_up_cmd.is_some() || (internal && config.energy_saving_node_manager_wake_up_cmd.is_some());

    let mut changes = NodeChanges::default();

    if has_sleep_cmd {
        if let (Some(idle_duration), Some(sleep_duration)) = (
            config.scheduler_node_manager_idle_time,
            config.scheduler_node_manager_sleep_time,
        ) {
            info!("Energy saving function: looking for node to shutdown");
            let idle_nodes = meta::search_idle_nodes(session, now).unwrap_or_default();
            debug!("Idle nodes found: {:?}", idle_nodes);

            let tmp_time = now - idle_duration;
            for (node, node_idle_duration) in idle_nodes {
                if node_idle_duration < tmp_time {
                    // Search whether the node has enough time to sleep.
                    let next_job_date = meta::get_next_job_date_on_node(session, &node).unwrap_or(None);
                    if next_job_date.is_none() || (next_job_date.unwrap() - sleep_duration > now) {
                        // Search whether the node has not been woken up recently.
                        let wakeup_date = meta::get_last_wake_up_date_of_node(session, &node).unwrap_or(None);
                        if wakeup_date.is_none() || wakeup_date.unwrap() < tmp_time {
                            changes.halt.push(node);
                        }
                    }
                }
            }
        }
    }

    if has_wakeup_cmd {
        info!("Energy saving function: looking for node to wakeup");
        let wakeup_time = config.scheduler_node_manager_wakeup_time.unwrap_or(0);
        changes.wakeup = meta::get_gantt_hostname_to_wake_up(session, now, wakeup_time).unwrap_or_default();
    }

    changes
}

/// Run a command feeding the given lines on its standard input.
/// Returns `true` when the command timed out (like `tools.fork_and_feed_stdin`).
fn run_command_with_stdin(command: &str, timeout_secs: u64, lines: &[String]) -> bool {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let mut child = match Command::new("sh").arg("-c").arg(command).stdin(Stdio::piped()).spawn() {
        Ok(child) => child,
        Err(err) => {
            error!("Unable to run command `{}`: {}", command, err);
            return false;
        }
    };
    if let Some(mut stdin) = child.stdin.take() {
        for line in lines {
            if writeln!(stdin, "{}", line).is_err() {
                break;
            }
        }
    }
    let start = std::time::Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return false,
            Ok(None) => {
                if start.elapsed().as_secs() >= timeout_secs {
                    let _ = child.kill();
                    return true;
                }
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            Err(err) => {
                error!("Unable to wait for command `{}`: {}", command, err);
                return false;
            }
        }
    }
}

/// Run a command with arguments, returning `(timed_out, return_code)`.
/// The return code is `-1` when the command could not be run or was killed.
/// Mirrors `tools.call([command, args...], shell=True, timeout=...)`.
pub fn run_command_with_args(command: &str, args: &[String], timeout_secs: u64) -> (bool, i32) {
    use std::process::{Command, Stdio};

    let full_command = if args.is_empty() {
        command.to_string()
    } else {
        format!("{} {}", command, args.join(" "))
    };
    let mut child = match Command::new("sh")
        .arg("-c")
        .arg(&full_command)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(err) => {
            error!("Unable to run command `{}`: {}", full_command, err);
            return (false, -1);
        }
    };
    let start = std::time::Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return (false, status.code().unwrap_or(-1)),
            Ok(None) => {
                if start.elapsed().as_secs() >= timeout_secs {
                    let _ = child.kill();
                    return (true, -1);
                }
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            Err(err) => {
                error!("Unable to wait for command `{}`: {}", full_command, err);
                return (false, -1);
            }
        }
    }
}

/// Apply the computed node changes by running the configured node manager
/// commands. Mirrors the command execution part of `nodes_energy_saving` and
/// the `meta_schedule` call site.
///
/// The internal energy saving module (Greta, based on ZMQ) is not available in
/// the Rust meta-scheduler: when `ENERGY_SAVING_INTERNAL` is enabled a warning
/// is emitted and the external commands are used instead.
pub fn apply_nodes_energy_saving(platform: &Platform, changes: &NodeChanges) {
    let config = &platform.get_platform_config().config;
    let internal = is_yes(config.energy_saving_internal.as_deref());
    let timeout = config.scheduler_timeout.unwrap_or(30).max(0) as u64;

    if internal {
        warn!(
            "ENERGY_SAVING_INTERNAL is enabled but the internal energy saving module (Greta/ZMQ) is not available in the Rust meta-scheduler"
        );
    }

    if !changes.halt.is_empty() {
        debug!("Powering off some nodes (energy saving): {:?}", changes.halt);
        if let Some(cmd) = config
            .scheduler_node_manager_sleep_cmd
            .as_deref()
            .or(config.energy_saving_node_manager_sleep_cmd.as_deref())
        {
            if run_command_with_stdin(cmd, timeout, &changes.halt) {
                error!("Command {} timeouted ({}s) while trying to poweroff some nodes", cmd, timeout);
            }
        } else {
            warn!("No node manager sleep command configured: nodes will not be halted");
        }
    }

    if !changes.wakeup.is_empty() {
        debug!("Waking up some nodes (energy saving): {:?}", changes.wakeup);
        if let Some(cmd) = config
            .scheduler_node_manager_wake_up_cmd
            .as_deref()
            .or(config.energy_saving_node_manager_wake_up_cmd.as_deref())
        {
            if run_command_with_stdin(cmd, timeout, &changes.wakeup) {
                error!("Command {} timeouted ({}s) while trying to wake-up some nodes", cmd, timeout);
            }
        } else {
            warn!("No node manager wake up command configured: nodes will not be woken up");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_yes() {
        assert!(is_yes(Some("yes")));
        assert!(is_yes(Some("YES")));
        assert!(is_yes(Some(" yes ")));
        assert!(!is_yes(Some("no")));
        assert!(!is_yes(None));
    }
}

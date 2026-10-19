//! Notification helpers used by the meta-scheduler.
//!
//! The Python meta-scheduler notifies the bipbip commander (to actually launch a
//! job) through a ZMQ `PUSH` socket, and notifies `oarsub` clients (interactive
//! jobs and reservation acks) through a plain TCP socket.
//!
//! To keep the Rust meta-scheduler free of a ZMQ dependency, notifying the
//! bipbip commander is delegated to an external command (configurable through
//! the `OAR_SCHEDULER_NOTIFY_TO_RUN_JOB_CMD` environment variable). The TCP
//! notifications are implemented directly with `std::net`.
//!
//! Everything goes through the [`JobNotifier`] trait so that tests can record
//! notifications instead of emitting them.

use log::{debug, warn};
use std::io::Write;
use std::net::TcpStream;
use std::process::Command;
use std::time::Duration;

/// Sends the meta-scheduler notifications.
pub trait JobNotifier {
    /// Tells the bipbip commander to run a job (`OARRUN`). Returns `true` on
    /// success.
    fn notify_to_run_job(&self, job_id: i64) -> bool;

    /// Sends `message` to the `oarsub` client listening at `info_type`
    /// (formatted as `addr:port`). Returns the number of bytes sent, `0` on
    /// failure.
    fn notify_tcp_socket(&self, info_type: &str, message: &str) -> usize;

    /// Sends a checkpoint signal to a running job. Returns `true` on success.
    fn send_checkpoint_signal(&self, job_id: i64) -> bool;
}

/// Default production notifier.
pub struct CommandNotifier;

impl CommandNotifier {
    fn notify_command() -> String {
        std::env::var("OAR_SCHEDULER_NOTIFY_TO_RUN_JOB_CMD").unwrap_or_else(|_| "oar-notify-to-run-job".to_string())
    }
}

impl JobNotifier for CommandNotifier {
    fn notify_to_run_job(&self, job_id: i64) -> bool {
        let command = Self::notify_command();
        if command.is_empty() || command == "noop" {
            debug!("Skipping bipbip notification for job {} (notifier is a no-op)", job_id);
            return true;
        }
        match Command::new(&command).arg(job_id.to_string()).output() {
            Ok(output) => {
                if output.status.success() {
                    debug!("Notified bipbip commander to launch job {}", job_id);
                    true
                } else {
                    warn!(
                        "Command '{}' failed (status {}) while notifying to launch job {}: {}",
                        command,
                        output.status,
                        job_id,
                        String::from_utf8_lossy(&output.stderr)
                    );
                    false
                }
            }
            Err(err) => {
                warn!("Unable to run '{}' to notify to launch job {}: {}", command, job_id, err);
                false
            }
        }
    }

    fn notify_tcp_socket(&self, info_type: &str, message: &str) -> usize {
        let Some((addr, port)) = info_type.rsplit_once(':') else {
            warn!("Invalid info_type '{}': expected addr:port", info_type);
            return 0;
        };
        let Ok(port) = port.parse::<u16>() else {
            warn!("Invalid port in info_type '{}'", info_type);
            return 0;
        };
        let address = format!("{}:{}", addr, port);
        let Ok(mut stream) = TcpStream::connect(&address) else {
            warn!("Unable to connect to oarsub client at {}", address);
            return 0;
        };
        let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
        let payload = format!("{}\n", message);
        match stream.write_all(payload.as_bytes()) {
            Ok(()) => payload.len(),
            Err(err) => {
                warn!("Unable to notify oarsub client at {}: {}", address, err);
                0
            }
        }
    }

    fn send_checkpoint_signal(&self, job_id: i64) -> bool {
        // Mirrors the Python implementation, which is not implemented yet.
        warn!("Checkpoint signal is not implemented yet (job {})", job_id);
        false
    }
}

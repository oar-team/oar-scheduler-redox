//! Notification helpers used by the meta-scheduler.
//!
//! The Python meta-scheduler notifies the bipbip commander (to actually launch a
//! job) through a ZMQ `PUSH` socket, and notifies `oarsub` clients (interactive
//! jobs and reservation acks) through a plain TCP socket.
//!
//! [`ZmqBipbipNotifier`] implements the bipbip notification natively, with the
//! [`omq`] crate (a pure Rust ZeroMQ implementation), and is the default
//! notifier. [`CommandNotifier`] remains available for deployments that prefer
//! delegating the launch to an external command: set
//! `OAR_SCHEDULER_NOTIFY_TO_RUN_JOB_CMD` to use it.
//!
//! The TCP notifications are implemented with `std::net`.
//!
//! Everything goes through the [`JobNotifier`] trait so that tests can record
//! notifications instead of emitting them.

use log::{debug, warn};
use omq::{Message, Options, SocketType, blocking};
use std::io::Write;
use std::net::TcpStream;
use std::process::Command;
use std::sync::Mutex;
use std::time::Duration;

/// ZMQ endpoint used when none is configured (mirrors `oar.lib.configuration`).
pub const DEFAULT_BIPBIP_ENDPOINT: &str = "tcp://localhost:6671";

/// How long a queued notification may take to reach the bipbip commander when
/// the socket is closed (the process is short-lived: one meta-scheduling cycle).
const SEND_LINGER: Duration = Duration::from_secs(2);

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

/// Builds the JSON payload the bipbip commander expects for launching a job.
///
/// Mirrors `oar.lib.tools.notify_bipbip_commander({"job_id": ..., "cmd":
/// "OARRUN", "args": []})`, which the commander reads with `recv_json()`.
pub fn bipbip_run_job_payload(job_id: i64) -> String {
    format!(r#"{{"job_id":{}, "cmd":"OARRUN", "args":[]}}"#, job_id)
}

/// Notifies the bipbip commander through a ZMQ `PUSH` socket.
///
/// The socket is created on the first notification and kept for the lifetime of
/// the notifier (like the Python `tools.bipbip_commander_socket` global), so a
/// cycle that launches several jobs only pays for one connection.
pub struct ZmqBipbipNotifier {
    endpoint: String,
    socket: Mutex<Option<blocking::Socket>>,
}

impl ZmqBipbipNotifier {
    /// Creates a notifier for the given `tcp://host:port` endpoint.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            socket: Mutex::new(None),
        }
    }

    /// Returns the endpoint this notifier pushes to.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Sends one message, connecting on first use. Returns `true` when the
    /// message was handed to the socket.
    fn send(&self, payload: String) -> bool {
        let mut guard = self.socket.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

        if guard.is_none() {
            // `omq::Context` owns its IO thread(s); the blocking socket keeps a
            // handle on it, so the context can be dropped here.
            let context = omq::Context::new();
            let mut options = Options::default();
            // The meta-scheduler is a short-lived process: keep queued messages
            // alive long enough for the handshake with the commander to finish.
            options.linger = Some(SEND_LINGER);
            let socket = context.blocking_socket(SocketType::Push, options);
            if let Err(err) = socket.connect(self.endpoint.as_str()) {
                warn!("Unable to connect to the bipbip commander at '{}': {}", self.endpoint, err);
                return false;
            }
            debug!("Connected to the bipbip commander at '{}'", self.endpoint);
            *guard = Some(socket);
        }

        let Some(socket) = guard.as_ref() else {
            return false;
        };
        match socket.send(Message::from(payload)) {
            Ok(()) => true,
            Err(err) => {
                warn!("Unable to notify the bipbip commander at '{}': {}", self.endpoint, err);
                false
            }
        }
    }
}

impl Drop for ZmqBipbipNotifier {
    /// Closes the socket with its linger.
    ///
    /// The meta-scheduler is a short-lived process: without an explicit,
    /// blocking close, queued messages are discarded when the process exits
    /// right after `send` (the ZMTP handshake may not have completed yet).
    fn drop(&mut self) {
        let socket = match self.socket.get_mut() {
            Ok(socket) => socket.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(socket) = socket {
            if let Err(err) = socket.close() {
                warn!("Unable to close the bipbip commander socket at '{}': {}", self.endpoint, err);
            }
        }
    }
}

impl JobNotifier for ZmqBipbipNotifier {
    fn notify_to_run_job(&self, job_id: i64) -> bool {
        let sent = self.send(bipbip_run_job_payload(job_id));
        if sent {
            debug!("Notified bipbip commander to launch job {}", job_id);
        }
        sent
    }

    fn notify_tcp_socket(&self, info_type: &str, message: &str) -> usize {
        notify_tcp_socket(info_type, message)
    }

    fn send_checkpoint_signal(&self, job_id: i64) -> bool {
        send_checkpoint_signal(job_id)
    }
}

/// Notifier delegating the launch to the `oar-notify-to-run-job` command.
///
/// Kept for backwards compatibility: it is selected when
/// `OAR_SCHEDULER_NOTIFY_TO_RUN_JOB_CMD` is set.
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
        notify_tcp_socket(info_type, message)
    }

    fn send_checkpoint_signal(&self, job_id: i64) -> bool {
        send_checkpoint_signal(job_id)
    }
}

/// Sends `message` to the `oarsub` client listening at `addr:port`, followed by
/// a newline (mirrors `oar.lib.tools.notify_tcp_socket`).
fn notify_tcp_socket(info_type: &str, message: &str) -> usize {
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

/// Checkpoint signals are not implemented yet (mirrors the Python version).
fn send_checkpoint_signal(job_id: i64) -> bool {
    warn!("Checkpoint signal is not implemented yet (job {})", job_id);
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bipbip_payload_matches_python_format() {
        assert_eq!(bipbip_run_job_payload(42), r#"{"job_id":42, "cmd":"OARRUN", "args":[]}"#);
    }

    #[test]
    fn test_default_endpoint_mirrors_python_defaults() {
        let config = oar_scheduler_core::model::configuration::Configuration::default();
        assert_eq!(config.bipbip_commander_endpoint(), DEFAULT_BIPBIP_ENDPOINT);
    }

    /// End-to-end check of the ZMQ notification: a `PULL` socket bound on a
    /// loopback port must receive the `OARRUN` JSON payload sent by the
    /// notifier, exactly as the bipbip commander would.
    #[test]
    fn test_zmq_notifier_sends_oarrun_to_bipbip_commander() {
        let context = omq::Context::new();
        let commander = context.blocking_socket(SocketType::Pull, Options::default());
        let endpoint = commander.bind("tcp://127.0.0.1:0").expect("bind the fake bipbip commander");

        let notifier = ZmqBipbipNotifier::new(endpoint.to_string());
        assert!(notifier.notify_to_run_job(4321), "notification should be handed to the socket");

        let message = commander
            .recv_timeout(Duration::from_secs(10))
            .expect("the bipbip commander should receive the notification");
        let payload = std::str::from_utf8(&message[0]).expect("payload should be valid UTF-8");

        assert_eq!(payload, r#"{"job_id":4321, "cmd":"OARRUN", "args":[]}"#);
        assert_eq!(message.len(), 1, "the commander reads the JSON from a single frame");
        assert!(notifier.notify_to_run_job(4322), "the socket is reused for the next notification");
        let message = commander.recv_timeout(Duration::from_secs(10)).expect("second notification");
        assert_eq!(std::str::from_utf8(&message[0]).unwrap(), r#"{"job_id":4322, "cmd":"OARRUN", "args":[]}"#);
    }
}

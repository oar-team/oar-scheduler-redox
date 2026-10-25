//! Shared ZMQ plumbing for the meta-scheduler.
//!
//! The Python meta-scheduler pushes JSON commands to two ZeroMQ `PULL` sockets:
//! the bipbip commander (job launches) and Greta (energy saving). Both use the
//! same pattern: a `PUSH` socket connected to a `tcp://host:port` endpoint,
//! created once and closed with a linger when the process exits.
//!
//! [`PushSocket`] implements that pattern on top of the [`omq`] crate (a pure
//! Rust ZeroMQ implementation, wire compatible with libzmq).

use log::{debug, warn};
use omq::{Message, Options, SocketType, blocking};
use std::sync::Mutex;
use std::time::Duration;

/// How long a queued message may take to reach its peer when the socket is
/// closed.
///
/// The meta-scheduler is a short-lived process (one scheduling cycle per run):
/// without an explicit linger, messages still queued in the socket are dropped
/// when the process exits, long before the ZMTP handshake completes.
pub const DEFAULT_LINGER: Duration = Duration::from_secs(2);

/// A lazily connected ZMQ `PUSH` socket.
///
/// The socket is created on the first [`send`](PushSocket::send) and kept until
/// the value is dropped, so a cycle that emits several commands only pays for
/// one connection. No ZMQ thread is spawned when nothing is sent.
pub struct PushSocket {
    endpoint: String,
    linger: Duration,
    socket: Mutex<Option<blocking::Socket>>,
}

impl PushSocket {
    /// Creates a socket that will connect to `tcp://host:port` on first use.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self::with_linger(endpoint, DEFAULT_LINGER)
    }

    /// Creates a socket with a custom linger (see [`DEFAULT_LINGER`]).
    pub fn with_linger(endpoint: impl Into<String>, linger: Duration) -> Self {
        Self {
            endpoint: endpoint.into(),
            linger,
            socket: Mutex::new(None),
        }
    }

    /// Returns the endpoint this socket pushes to.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Sends one message, connecting on first use.
    ///
    /// Returns `true` when the message was handed to the socket (a `PUSH`
    /// socket queues messages until the peer is reachable, like libzmq).
    pub fn send(&self, payload: impl Into<Message>) -> bool {
        let mut guard = self.socket.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

        if guard.is_none() {
            // `omq::Context` owns its IO thread(s); the blocking socket keeps a
            // handle on it, so the context can be dropped here.
            let context = omq::Context::new();
            let mut options = Options::default();
            options.linger = Some(self.linger);
            let socket = context.blocking_socket(SocketType::Push, options);
            if let Err(err) = socket.connect(self.endpoint.as_str()) {
                warn!("Unable to connect to the ZMQ endpoint '{}': {}", self.endpoint, err);
                return false;
            }
            debug!("Connected to the ZMQ endpoint '{}'", self.endpoint);
            *guard = Some(socket);
        }

        let Some(socket) = guard.as_ref() else {
            return false;
        };
        match socket.send(payload.into()) {
            Ok(()) => true,
            Err(err) => {
                warn!("Unable to send to the ZMQ endpoint '{}': {}", self.endpoint, err);
                false
            }
        }
    }
}

impl Drop for PushSocket {
    /// Closes the socket with its linger, blocking until the queued messages are
    /// flushed (or the linger expires).
    ///
    /// This is required: dropping the socket without a blocking close discards
    /// queued messages as soon as the process exits.
    fn drop(&mut self) {
        let socket = match self.socket.get_mut() {
            Ok(socket) => socket.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(socket) = socket {
            if let Err(err) = socket.close() {
                warn!("Unable to close the ZMQ endpoint '{}': {}", self.endpoint, err);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_socket_delivers_to_bound_pull_socket() {
        let context = omq::Context::new();
        let pull = context.blocking_socket(SocketType::Pull, Options::default());
        let endpoint = pull.bind("tcp://127.0.0.1:0").expect("bind a PULL socket");

        let push = PushSocket::new(endpoint.to_string());
        assert_eq!(push.endpoint(), endpoint.to_string());
        assert!(push.send("{\"cmd\":\"CHECK\"}"));

        let message = pull.recv_timeout(Duration::from_secs(10)).expect("the PULL socket should receive the message");
        assert_eq!(std::str::from_utf8(&message[0]).unwrap(), "{\"cmd\":\"CHECK\"}");
    }
}

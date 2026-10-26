//! Greta client: the internal energy saving module, driven over ZMQ.
//!
//! Rust counterpart of `oar.modules.greta.GretaClient` (see
//! `oar3/oar/modules/greta.py`), used by the meta-scheduler when
//! `ENERGY_SAVING_INTERNAL=yes`.
//!
//! Greta runs as an almighty daemon and binds a ZMQ `PULL` socket on
//! `GRETA_SERVER:GRETA_PORT`. The meta-scheduler only *pushes* JSON commands to
//! it: `CHECK`, `HALT` and `WAKEUP`. Greta then takes care of the actual node
//! management (database updates, wake up/shutdown commands, timeouts), so this
//! client is fire-and-forget, exactly like the Python one.
//!
//! When `ENERGY_SAVING_INTERNAL` is not enabled, the meta-scheduler keeps using
//! the external node manager commands instead (see
//! [`apply_nodes_energy_saving`](crate::energy_saving::apply_nodes_energy_saving)).

use crate::zmq::PushSocket;
use log::debug;
use oar_scheduler_core::model::configuration::Configuration;
use std::time::Duration;

/// ZMQ endpoint used when none is configured (mirrors `GRETA_SERVER` /
/// `GRETA_PORT` defaults of `oar.lib.configuration`).
pub const DEFAULT_GRETA_ENDPOINT: &str = "tcp://localhost:6672";

/// Linger used when closing the Greta socket (mirrors `zmq.LINGER = 5000` of the
/// Python client: Greta may still be starting up, so give it some time).
const GRETA_LINGER: Duration = Duration::from_secs(5);

/// Builds the JSON payload of the `CHECK` command.
///
/// Greta then checks booting nodes, timeouts and its memorized node lists.
pub fn check_nodes_payload() -> String {
    serde_json::json!({"cmd": "CHECK"}).to_string()
}

/// Builds the JSON payload asking Greta to halt the given nodes.
pub fn halt_nodes_payload(nodes: &[String]) -> String {
    serde_json::json!({"cmd": "HALT", "nodes": nodes}).to_string()
}

/// Builds the JSON payload asking Greta to wake up the given nodes.
pub fn wake_up_nodes_payload(nodes: &[String]) -> String {
    serde_json::json!({"cmd": "WAKEUP", "nodes": nodes}).to_string()
}

/// Client pushing energy saving commands to Greta.
///
/// The socket connects on the first command and is reused for the following
/// ones, like the single `GretaClient` instance the Python `meta_schedule`
/// creates per scheduling cycle.
pub struct GretaClient {
    socket: PushSocket,
}

impl GretaClient {
    /// Creates a client for the given `tcp://host:port` endpoint.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            socket: PushSocket::with_linger(endpoint, GRETA_LINGER),
        }
    }

    /// Creates a client from the `GRETA_SERVER` / `GRETA_PORT` configuration.
    pub fn from_config(config: &Configuration) -> Self {
        Self::new(config.greta_endpoint())
    }

    /// Returns the endpoint this client pushes to.
    pub fn endpoint(&self) -> &str {
        self.socket.endpoint()
    }

    /// Asks Greta to check booting nodes and timeouts (`CHECK`).
    pub fn check_nodes(&self) -> bool {
        let sent = self.socket.send(check_nodes_payload());
        if sent {
            debug!("Sent CHECK to the energy saving module (Greta) at '{}'", self.endpoint());
        }
        sent
    }

    /// Asks Greta to halt (power off) the given nodes (`HALT`).
    pub fn halt_nodes(&self, nodes: &[String]) -> bool {
        if nodes.is_empty() {
            return true;
        }
        let sent = self.socket.send(halt_nodes_payload(nodes));
        if sent {
            debug!("Sent HALT of nodes {:?} to the energy saving module (Greta)", nodes);
        }
        sent
    }

    /// Asks Greta to wake up the given nodes (`WAKEUP`).
    pub fn wake_up_nodes(&self, nodes: &[String]) -> bool {
        if nodes.is_empty() {
            return true;
        }
        let sent = self.socket.send(wake_up_nodes_payload(nodes));
        if sent {
            debug!("Sent WAKEUP of nodes {:?} to the energy saving module (Greta)", nodes);
        }
        sent
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nodes(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    #[test]
    fn test_payloads_match_python_client_messages() {
        assert_eq!(check_nodes_payload(), r#"{"cmd":"CHECK"}"#);
        assert_eq!(halt_nodes_payload(&nodes(&["node1", "node2"])), r#"{"cmd":"HALT","nodes":["node1","node2"]}"#);
        assert_eq!(wake_up_nodes_payload(&nodes(&["node1"])), r#"{"cmd":"WAKEUP","nodes":["node1"]}"#);
    }

    #[test]
    fn test_payloads_escape_node_names() {
        assert_eq!(wake_up_nodes_payload(&nodes(&["we\"ird\\node"])), r#"{"cmd":"WAKEUP","nodes":["we\"ird\\node"]}"#);
    }

    #[test]
    fn test_default_endpoint_mirrors_python_defaults() {
        let config = Configuration::default();
        assert_eq!(config.greta_endpoint(), DEFAULT_GRETA_ENDPOINT);
        let client = GretaClient::from_config(&config);
        assert_eq!(client.endpoint(), DEFAULT_GRETA_ENDPOINT);
    }

    #[test]
    fn test_greta_client_sends_commands_over_zmq() {
        let context = omq::Context::new();
        let greta = context.blocking_socket(omq::SocketType::Pull, omq::Options::default());
        let endpoint = greta.bind("tcp://127.0.0.1:0").expect("bind the fake Greta");

        let client = GretaClient::new(endpoint.to_string());
        assert!(client.halt_nodes(&nodes(&["node1", "node2"])));
        assert!(client.wake_up_nodes(&nodes(&["node3"])));
        assert!(client.check_nodes());

        let received: Vec<String> = (0..3)
            .map(|_| {
                let message = greta.recv_timeout(Duration::from_secs(10)).expect("Greta receives the command");
                std::str::from_utf8(&message[0]).expect("valid UTF-8").to_string()
            })
            .collect();

        assert_eq!(
            received,
            vec![
                r#"{"cmd":"HALT","nodes":["node1","node2"]}"#.to_string(),
                r#"{"cmd":"WAKEUP","nodes":["node3"]}"#.to_string(),
                r#"{"cmd":"CHECK"}"#.to_string(),
            ]
        );
    }
}

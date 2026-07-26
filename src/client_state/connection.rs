//! Per-remote client connection state machine.
//!
//! Plain data with pure transitions, following the shape of
//! [`crate::fleet::connection::ConnectionMachine`]: the pure client's run
//! loop drives it from real socket IO, tests drive it directly without
//! sockets, PTYs, or SSH.

use crate::protocol::framed::{HelloRemedy, NegotiatedSession};

/// Connection lifecycle of one remote as seen by the pure client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ClientConnectionState {
    /// No connection and none in flight (initial state, or after detach).
    Disconnected,
    /// A connect-plus-handshake attempt is in flight.
    Connecting { attempt: u32 },
    /// Live negotiated framed session.
    Connected { negotiated: NegotiatedSession },
    /// Not connected; retrying. `attempt` counts consecutive failures.
    Offline { attempt: u32, last_error: String },
    /// Reachable, herdr installed, but no server running there. Terminal
    /// until the user says to start one: retrying cannot spawn a daemon, and
    /// doing it unasked would write to someone else's machine.
    Stopped { message: String },
    /// The protocol version windows do not overlap. Terminal for this
    /// configuration: no retries until a side is upgraded; the remedy names
    /// which one.
    Incompatible {
        remedy: HelloRemedy,
        message: String,
    },
}

impl ClientConnectionState {
    pub(crate) fn new() -> Self {
        Self::Disconnected
    }

    pub(crate) fn is_connected(&self) -> bool {
        matches!(self, Self::Connected { .. })
    }

    /// Whether the run loop may start another connection attempt. `Stopped`
    /// and `Incompatible` are terminal: retrying cannot change either.
    pub(crate) fn may_retry(&self) -> bool {
        matches!(self, Self::Disconnected | Self::Offline { .. })
    }

    /// Whether this remote is waiting on an explicit start.
    pub(crate) fn is_stopped(&self) -> bool {
        matches!(self, Self::Stopped { .. })
    }

    /// A connect attempt started.
    pub(crate) fn connect_started(&mut self) {
        let attempt = match self {
            Self::Offline { attempt, .. } | Self::Connecting { attempt } => {
                attempt.saturating_add(1)
            }
            Self::Disconnected
            | Self::Connected { .. }
            | Self::Stopped { .. }
            | Self::Incompatible { .. } => 1,
        };
        *self = Self::Connecting { attempt };
    }

    /// The handshake completed; the session is live.
    pub(crate) fn connected(&mut self, negotiated: NegotiatedSession) {
        *self = Self::Connected { negotiated };
    }

    /// A connect attempt or a live session failed.
    pub(crate) fn connection_failed(&mut self, error: impl Into<String>) {
        let attempt = match self {
            Self::Connecting { attempt } => *attempt,
            Self::Offline { attempt, .. } => *attempt,
            _ => 0,
        };
        *self = Self::Offline {
            attempt,
            last_error: error.into(),
        };
    }

    /// The far side has herdr but no running server.
    pub(crate) fn stopped(&mut self, message: impl Into<String>) {
        *self = Self::Stopped {
            message: message.into(),
        };
    }

    /// The handshake failed because the version windows do not overlap.
    pub(crate) fn incompatible(&mut self, remedy: HelloRemedy, message: impl Into<String>) {
        *self = Self::Incompatible {
            remedy,
            message: message.into(),
        };
    }

    /// The negotiated session for the current connection, when live.
    pub(crate) fn negotiated(&self) -> Option<&NegotiatedSession> {
        match self {
            Self::Connected { negotiated } => Some(negotiated),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn negotiated() -> NegotiatedSession {
        NegotiatedSession {
            protocol: 1,
            capabilities: vec!["pane-stream".into(), "catalog".into()],
        }
    }

    #[test]
    fn connect_cycle_reaches_connected() {
        let mut state = ClientConnectionState::new();
        assert!(state.may_retry());
        state.connect_started();
        assert_eq!(state, ClientConnectionState::Connecting { attempt: 1 });
        state.connected(negotiated());
        assert!(state.is_connected());
        assert!(state.negotiated().is_some());
        assert!(!state.may_retry());
    }

    #[test]
    fn failures_escalate_the_attempt_counter() {
        let mut state = ClientConnectionState::new();
        state.connect_started();
        state.connection_failed("refused");
        assert_eq!(
            state,
            ClientConnectionState::Offline {
                attempt: 1,
                last_error: "refused".into()
            }
        );
        state.connect_started();
        assert_eq!(state, ClientConnectionState::Connecting { attempt: 2 });
        state.connection_failed("refused again");
        assert_eq!(
            state,
            ClientConnectionState::Offline {
                attempt: 2,
                last_error: "refused again".into()
            }
        );
    }

    #[test]
    fn session_drop_after_connected_restarts_the_ladder() {
        let mut state = ClientConnectionState::new();
        state.connect_started();
        state.connected(negotiated());
        state.connection_failed("connection closed");
        assert_eq!(
            state,
            ClientConnectionState::Offline {
                attempt: 0,
                last_error: "connection closed".into()
            }
        );
        state.connect_started();
        assert_eq!(state, ClientConnectionState::Connecting { attempt: 1 });
    }

    #[test]
    fn incompatible_is_terminal_for_this_config() {
        let mut state = ClientConnectionState::new();
        state.connect_started();
        state.incompatible(HelloRemedy::UpgradeClient, "upgrade the herdr client");
        assert!(!state.may_retry());
        assert!(!state.is_connected());
        match &state {
            ClientConnectionState::Incompatible { remedy, message } => {
                assert_eq!(*remedy, HelloRemedy::UpgradeClient);
                assert!(message.contains("upgrade"));
            }
            other => panic!("expected incompatible, got {other:?}"),
        }
    }
}

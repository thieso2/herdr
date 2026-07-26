use serde::{Deserialize, Serialize};

/// Connection state of one fleet remote as reported by `remote.list`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RemoteConnectionStateInfo {
    /// A local runtime: reached over this machine's API socket, so there is
    /// no bridge to connect and nothing to retry.
    Local,
    /// A connect-plus-handshake attempt is in flight.
    Connecting,
    /// Live negotiated framed session.
    Connected,
    /// Not connected; automatic reconnects continue until the remote is
    /// removed or disabled.
    Offline,
    /// Disabled in the fleet config; no connection is attempted.
    Disabled,
    /// Reachable, with herdr installed, but no server running there. Starting
    /// one writes to the host, so it never happens behind the user's back: the
    /// remote parks here until an explicit `remote start`.
    Stopped,
    /// The protocol version windows do not overlap; no automatic reconnects
    /// until one side is upgraded.
    Incompatible,
    /// The fleet holds no live state for this remote (for example when the
    /// config is read without a running fleet).
    Unknown,
}

/// One fleet remote with its live connection state, in fleet-config order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct RemoteInfo {
    pub index: usize,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<String>,
    pub enabled: bool,
    pub state: RemoteConnectionStateInfo,
    /// Reconnect attempt counter while offline or connecting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<u32>,
    /// Milliseconds until the next automatic reconnect while offline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_in_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// Names one fleet remote (for example for `remote.reset`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct RemoteTargetParams {
    pub name: String,
}

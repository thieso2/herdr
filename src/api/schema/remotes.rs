use serde::{Deserialize, Serialize};

/// Connection state of one fleet remote as reported by `remote.list`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RemoteConnectionStateInfo {
    /// The implicit local runtime (remote `#0`).
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
    /// The fleet holds no live state for this remote (for example when the
    /// config is read without a running fleet).
    Unknown,
}

/// One fleet remote with its live connection state. The local runtime is the
/// implicit entry at index 0.
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

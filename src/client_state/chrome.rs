//! Client-owned global chrome state.
//!
//! Everything here is presentation state that belongs to this client alone
//! and survives catalog resyncs and reconnects: it is never sent to the
//! server and never rebuilt from server facts. The rendered view is a pure
//! composition of the per-remote mirror (server facts) plus this chrome.

/// Presentation state owned by the pure client.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct GlobalChrome {
    /// Sidebar collapse toggle.
    pub(crate) sidebar_collapsed: bool,
    /// Sidebar workspace list scroll offset.
    pub(crate) workspace_scroll: usize,
    /// Sidebar agent panel scroll offset.
    pub(crate) agent_panel_scroll: usize,
    /// Tab bar scroll offset.
    pub(crate) tab_scroll: usize,
    /// One-line connection status surfaced while the remote is not
    /// connected (connecting, offline with retry, or incompatible).
    pub(crate) connection_status: Option<String>,
}

impl GlobalChrome {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

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
    /// Which remotes are in view and which remote holds the focus. Pure
    /// presentation: never affects connections.
    pub(crate) selection: super::fleet_view::FleetSelection,
    /// Last window title pushed by each remote; the focused remote's title
    /// wins the host terminal.
    pub(crate) window_titles: std::collections::BTreeMap<usize, String>,
    /// The add/edit-remote dialog, when open.
    pub(crate) remote_edit: Option<super::remote_edit::RemoteEditState>,
    /// The fleet as a list. The only path to editing a remote now that chip
    /// right-click is gone, and the only surface that shows disabled ones.
    pub(crate) remote_list: Option<super::remote_list::RemoteListState>,
    /// The "start this stopped remote?" confirmation, when open.
    pub(crate) remote_start: Option<super::remote_start::RemoteStartPrompt>,
    /// Double-click candidate for pane word selection, client-side mouse
    /// state exactly like the legacy `App::last_pane_click`.
    pub(crate) last_pane_click: Option<crate::app::PaneClickState>,
}

impl GlobalChrome {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

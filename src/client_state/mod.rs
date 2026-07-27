//! Pure-client state: per-remote mirrors plus global chrome.
//!
//! Behind the `[experimental] pure_client` flag the TUI runs as a pure
//! client of the local server — local is remote #0. The client holds one
//! [`RemoteMirror`] per remote: a connection state machine, the
//! event-driven session catalog (full resync on reconnect), and replicated
//! pane screens keyed by server stream id. The rendered view is a pure
//! composition over mirror plus [`chrome::GlobalChrome`].
//!
//! Everything in this module is plain data testable without PTYs, sockets,
//! or SSH, following the `AppState::test_new()` test-constructor pattern.

pub(crate) mod catalog;
pub(crate) mod chrome;
pub(crate) mod compose;
pub(crate) mod connection;
pub(crate) mod fleet_view;
#[cfg(unix)]
pub(crate) mod intent;
pub(crate) mod remote_edit;
pub(crate) mod remote_list;
pub(crate) mod remote_start;
#[cfg(unix)]
pub(crate) mod run;

use std::cell::RefCell;
use std::collections::BTreeMap;

use crate::terminal::replica::PaneReplica;

pub(crate) use catalog::SessionCatalog;
pub(crate) use connection::ClientConnectionState;

/// Index of the fleet's first remote. It used to be the implicit local
/// server; it is now simply whatever `remotes.toml` lists first, and the
/// only index guaranteed to exist in a non-empty fleet.
pub(crate) const LOCAL_REMOTE_INDEX: usize = 0;

/// Display name of the index-0 mirror in test fixtures.
#[cfg(test)]
pub(crate) const LOCAL_REMOTE_NAME: &str = "local";

/// One remote's mirrored state: connection, catalog, and pane replicas.
pub(crate) struct RemoteMirror {
    /// Stable remote index; 0 is the local server.
    pub(crate) remote_index: usize,
    /// Remote display name.
    pub(crate) name: String,
    /// Connection lifecycle of this remote.
    pub(crate) connection: ClientConnectionState,
    /// Event-driven session catalog, resynced in full on reconnect.
    pub(crate) catalog: SessionCatalog,
    /// Replicated pane screens keyed by server stream id. Each replica sits
    /// in a `RefCell` so the pane-content seam can scroll it through the
    /// shared references the UI layer works with; the pure client is
    /// single-threaded per mirror.
    pub(crate) replicas: BTreeMap<u32, RefCell<PaneReplica>>,
    /// Open stream id per public pane id.
    pub(crate) pane_streams: BTreeMap<String, u32>,
}

impl RemoteMirror {
    pub(crate) fn new(remote_index: usize, name: impl Into<String>) -> Self {
        Self {
            remote_index,
            name: name.into(),
            connection: ClientConnectionState::new(),
            catalog: SessionCatalog::new(),
            replicas: BTreeMap::new(),
            pane_streams: BTreeMap::new(),
        }
    }

    /// A mirror at index 0 named `local`. Test-only: production names every
    /// mirror after its config entry.
    #[cfg(test)]
    pub(crate) fn local() -> Self {
        Self::new(LOCAL_REMOTE_INDEX, LOCAL_REMOTE_NAME)
    }

    /// Discards all replicated state ahead of a full resync: called on every
    /// transition into `Connected` so the fresh `session.snapshot` plus
    /// re-opened streams are the only source of truth. This is what makes
    /// ghost or duplicate panes impossible across reconnects.
    pub(crate) fn begin_resync(&mut self) {
        self.clear_mirrored_state();
    }

    /// Drops everything mirrored from a session that is gone for good.
    ///
    /// Distinct from [`Self::connection_lost`], which keeps the catalog: an
    /// offline remote is expected back and resyncs in full, so holding its
    /// spaces avoids flicker across a blip. A remote that announced its stop
    /// is not coming back on its own, and keeping its catalog left its spaces
    /// and panes composed into the view — a machine on screen that is gone.
    ///
    /// Leaves the connection state alone: the caller decides what parked it.
    pub(crate) fn session_ended(&mut self) {
        self.clear_mirrored_state();
    }

    /// Everything this client mirrors from the far session: the catalog, the
    /// pane replicas, and the stream ids that address them. Shared by the two
    /// callers that drop the lot — a resync about to rebuild it, and a session
    /// that has ended.
    fn clear_mirrored_state(&mut self) {
        self.catalog = SessionCatalog::new();
        self.replicas.clear();
        self.pane_streams.clear();
    }

    /// Drops connection-scoped state when the session ends. Stream ids and
    /// replicas are meaningless across connections.
    pub(crate) fn connection_lost(&mut self, error: impl Into<String>) {
        debug_assert!(
            self.connection.is_connected() || self.replicas.is_empty(),
            "streams must not outlive their connection"
        );
        self.connection.connection_failed(error);
        self.replicas.clear();
        self.pane_streams.clear();
    }

    /// The stream id serving `pane_id`, when one is open.
    pub(crate) fn stream_for_pane(&self, pane_id: &str) -> Option<u32> {
        self.pane_streams.get(pane_id).copied()
    }

    /// The public pane id served by `stream_id`, when one is open.
    pub(crate) fn pane_for_stream(&self, stream_id: u32) -> Option<&str> {
        self.pane_streams
            .iter()
            .find_map(|(pane_id, id)| (*id == stream_id).then_some(pane_id.as_str()))
    }

    /// Records an opened pane stream and its seeded replica.
    pub(crate) fn stream_opened(
        &mut self,
        pane_id: impl Into<String>,
        stream_id: u32,
        replica: PaneReplica,
    ) {
        let pane_id = pane_id.into();
        if let Some(previous) = self.pane_streams.insert(pane_id, stream_id) {
            self.replicas.remove(&previous);
        }
        self.replicas.insert(stream_id, RefCell::new(replica));
    }

    /// Mutable access to the replica behind a stream id.
    pub(crate) fn replica_mut(&mut self, stream_id: u32) -> Option<&mut PaneReplica> {
        self.replicas.get_mut(&stream_id).map(RefCell::get_mut)
    }

    /// Removes a closed or revoked stream and its replica.
    pub(crate) fn stream_closed(&mut self, stream_id: u32) {
        self.replicas.remove(&stream_id);
        self.pane_streams.retain(|_, id| *id != stream_id);
    }

    /// Test constructor: connected local mirror with an empty catalog.
    #[cfg(test)]
    pub(crate) fn test_new() -> Self {
        let mut mirror = Self::local();
        mirror.connection.connect_started();
        mirror
            .connection
            .connected(crate::protocol::framed::NegotiatedSession {
                protocol: crate::protocol::framed::FRAMED_PROTOCOL_VERSION,
                capabilities: vec![crate::protocol::framed::CAPABILITY_PANE_STREAM.to_owned()],
            });
        mirror
    }

    /// Test constructor: a valid but adversarial catalog — duplicate labels,
    /// lookalike ids, unordered numbers, focus spread across entries — for
    /// identity-sensitive refactor tests.
    #[cfg(test)]
    pub(crate) fn test_with_adversarial_catalog() -> Self {
        let snapshot: crate::api::schema::session::SessionSnapshot =
            serde_json::from_value(serde_json::json!({
                "version": "test",
                "protocol": 3,
                "focused_workspace_id": "ws_2",
                "focused_tab_id": "t_2_1",
                "focused_pane_id": "p_2_1",
                "workspaces": [
                    {
                        "workspace_id": "ws_2",
                        "number": 2,
                        "label": "repo",
                        "focused": true,
                        "pane_count": 2,
                        "tab_count": 1,
                        "active_tab_id": "t_2_1",
                        "agent_status": "working"
                    },
                    {
                        "workspace_id": "ws_10",
                        "number": 1,
                        "label": "repo",
                        "focused": false,
                        "pane_count": 1,
                        "tab_count": 1,
                        "active_tab_id": "t_10_1",
                        "agent_status": "idle"
                    }
                ],
                "tabs": [
                    {
                        "tab_id": "t_2_1",
                        "workspace_id": "ws_2",
                        "number": 1,
                        "label": "tab",
                        "focused": true,
                        "pane_count": 2,
                        "agent_status": "working"
                    },
                    {
                        "tab_id": "t_10_1",
                        "workspace_id": "ws_10",
                        "number": 1,
                        "label": "tab",
                        "focused": false,
                        "pane_count": 1,
                        "agent_status": "idle"
                    }
                ],
                "panes": [
                    {
                        "pane_id": "p_2_1",
                        "terminal_id": "term_21",
                        "workspace_id": "ws_2",
                        "tab_id": "t_2_1",
                        "focused": true,
                        "agent_status": "working",
                        "revision": 7
                    },
                    {
                        "pane_id": "p_2_10",
                        "terminal_id": "term_210",
                        "workspace_id": "ws_2",
                        "tab_id": "t_2_1",
                        "focused": false,
                        "agent_status": "unknown",
                        "revision": 1
                    },
                    {
                        "pane_id": "p_10_1",
                        "terminal_id": "term_101",
                        "workspace_id": "ws_10",
                        "tab_id": "t_10_1",
                        "focused": false,
                        "agent_status": "idle",
                        "revision": 3
                    }
                ],
                "layouts": [],
                "agents": []
            }))
            .expect("adversarial snapshot deserializes");
        let mut mirror = Self::test_new();
        mirror.catalog.resync(&snapshot, 41);
        mirror
    }

    /// Mirror-level invariants, run from identity-sensitive tests.
    #[cfg(test)]
    pub(crate) fn assert_invariants_for_test(&self) {
        self.catalog.assert_invariants_for_test();

        for (pane_id, stream_id) in &self.pane_streams {
            assert!(
                self.replicas.contains_key(stream_id),
                "pane {pane_id} maps to stream {stream_id} without a replica"
            );
            assert!(
                self.catalog.pane(pane_id).is_some(),
                "stream {stream_id} serves pane {pane_id} missing from the catalog"
            );
        }
        assert_eq!(
            self.pane_streams.len(),
            self.replicas.len(),
            "replicas without a pane mapping"
        );
        if !self.connection.is_connected() {
            assert!(
                self.replicas.is_empty() && self.pane_streams.is_empty(),
                "streams and replicas must not outlive the connection"
            );
        }
    }
}

/// Keyed collection of remote mirrors: the local server at #0 plus one
/// mirror per configured fleet remote. Every configured remote keeps its
/// mirror (and its connection) regardless of the client's view selection.
pub(crate) struct RemoteMirrors {
    mirrors: BTreeMap<usize, RemoteMirror>,
}

impl RemoteMirrors {
    /// An empty collection. Callers insert one mirror per fleet descriptor;
    /// no runtime is implicit, so none is seeded here.
    pub(crate) fn new() -> Self {
        Self {
            mirrors: BTreeMap::new(),
        }
    }

    /// A collection seeded with one mirror at index 0 named `local`.
    ///
    /// Test-only: it is the fixture shape for "a fleet whose head is a local
    /// runtime", which production now expresses as an ordinary target-less
    /// config entry rather than an implicit remote #0.
    #[cfg(test)]
    pub(crate) fn with_local() -> Self {
        let mut mirrors = BTreeMap::new();
        mirrors.insert(LOCAL_REMOTE_INDEX, RemoteMirror::local());
        Self { mirrors }
    }

    /// The mirror at index 0. Test-only companion to [`Self::with_local`].
    #[cfg(test)]
    pub(crate) fn local(&self) -> &RemoteMirror {
        self.mirrors
            .get(&LOCAL_REMOTE_INDEX)
            .expect("index 0 mirror always exists in these fixtures")
    }

    /// The mirror at index 0. Test-only companion to [`Self::with_local`].
    #[cfg(test)]
    pub(crate) fn local_mut(&mut self) -> &mut RemoteMirror {
        self.mirrors
            .get_mut(&LOCAL_REMOTE_INDEX)
            .expect("index 0 mirror always exists in these fixtures")
    }

    pub(crate) fn get(&self, remote_index: usize) -> Option<&RemoteMirror> {
        self.mirrors.get(&remote_index)
    }

    pub(crate) fn get_mut(&mut self, remote_index: usize) -> Option<&mut RemoteMirror> {
        self.mirrors.get_mut(&remote_index)
    }

    /// Registers a remote's mirror, replacing any previous mirror at the
    /// same index (a config identity change is a different remote runtime).
    pub(crate) fn insert(&mut self, mirror: RemoteMirror) {
        self.mirrors.insert(mirror.remote_index, mirror);
    }

    /// Drops a remote's mirror. Every mirror is a config entry now, so any
    /// of them can go when the config drops it - index 0 included.
    pub(crate) fn remove(&mut self, remote_index: usize) {
        self.mirrors.remove(&remote_index);
    }

    // Whole-collection iteration currently only backs tests; composition
    // walks descriptors instead so view order follows the fleet config.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn iter(&self) -> impl Iterator<Item = &RemoteMirror> {
        self.mirrors.values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::schema::events::EventEnvelope;
    use crate::api::schema::session::SessionSnapshot;

    fn canned_snapshot(pane_id: &str) -> SessionSnapshot {
        serde_json::from_value(serde_json::json!({
            "version": "test",
            "protocol": 3,
            "focused_workspace_id": "ws_1",
            "focused_tab_id": "t_1_1",
            "focused_pane_id": pane_id,
            "workspaces": [{
                "workspace_id": "ws_1",
                "number": 1,
                "label": "repo",
                "focused": true,
                "pane_count": 1,
                "tab_count": 1,
                "active_tab_id": "t_1_1",
                "agent_status": "idle"
            }],
            "tabs": [{
                "tab_id": "t_1_1",
                "workspace_id": "ws_1",
                "number": 1,
                "label": "shell",
                "focused": true,
                "pane_count": 1,
                "agent_status": "idle"
            }],
            "panes": [{
                "pane_id": pane_id,
                "terminal_id": "term_1",
                "workspace_id": "ws_1",
                "tab_id": "t_1_1",
                "focused": true,
                "agent_status": "idle",
                "revision": 1
            }],
            "layouts": [],
            "agents": []
        }))
        .expect("canned snapshot deserializes")
    }

    fn envelope(json: serde_json::Value) -> EventEnvelope {
        serde_json::from_value(json).expect("canned event deserializes")
    }

    #[test]
    fn resync_from_canned_snapshot_populates_the_catalog() {
        let mut mirror = RemoteMirror::test_new();
        mirror.catalog.resync(&canned_snapshot("p_1_1"), 10);

        assert_eq!(mirror.catalog.sequence, 10);
        assert_eq!(mirror.catalog.workspaces.len(), 1);
        assert_eq!(mirror.catalog.focused_pane_id.as_deref(), Some("p_1_1"));
        mirror.assert_invariants_for_test();
    }

    #[test]
    fn events_after_the_snapshot_anchor_apply_in_order() {
        let mut mirror = RemoteMirror::test_new();
        mirror.catalog.resync(&canned_snapshot("p_1_1"), 10);

        // Stale event at the anchor is dropped.
        let stale = envelope(serde_json::json!({
            "event": "pane_closed",
            "data": { "type": "pane_closed", "pane_id": "p_1_1", "workspace_id": "ws_1" }
        }));
        assert!(!mirror.catalog.apply(10, &stale));
        assert!(mirror.catalog.pane("p_1_1").is_some());

        let created = envelope(serde_json::json!({
            "event": "pane_created",
            "data": { "type": "pane_created", "pane": {
                "pane_id": "p_1_2",
                "terminal_id": "term_2",
                "workspace_id": "ws_1",
                "tab_id": "t_1_1",
                "focused": false,
                "agent_status": "unknown",
                "revision": 0
            }}
        }));
        assert!(mirror.catalog.apply(11, &created));
        let focused = envelope(serde_json::json!({
            "event": "pane_focused",
            "data": { "type": "pane_focused", "pane_id": "p_1_2", "workspace_id": "ws_1" }
        }));
        assert!(mirror.catalog.apply(12, &focused));

        assert_eq!(mirror.catalog.panes.len(), 2);
        assert_eq!(mirror.catalog.focused_pane_id.as_deref(), Some("p_1_2"));
        assert!(mirror
            .catalog
            .pane("p_1_1")
            .is_some_and(|pane| !pane.focused));
        mirror.assert_invariants_for_test();
    }

    #[test]
    fn workspace_close_removes_its_tabs_and_panes() {
        let mut mirror = RemoteMirror::test_with_adversarial_catalog();
        let closed = envelope(serde_json::json!({
            "event": "workspace_closed",
            "data": { "type": "workspace_closed", "workspace_id": "ws_2" }
        }));
        assert!(mirror.catalog.apply(42, &closed));
        assert!(mirror.catalog.workspace("ws_2").is_none());
        assert!(mirror.catalog.pane("p_2_1").is_none());
        assert!(mirror.catalog.pane("p_2_10").is_none());
        assert!(mirror.catalog.pane("p_10_1").is_some());
        assert_eq!(mirror.catalog.focused_pane_id, None);
        mirror.assert_invariants_for_test();
    }

    #[test]
    fn reconnect_full_resync_leaves_no_ghost_or_duplicate_panes() {
        let mut mirror = RemoteMirror::test_new();
        mirror.catalog.resync(&canned_snapshot("p_1_1"), 10);
        let replica =
            crate::terminal::replica::PaneReplica::open("hello", 5, None, 80, 24, 64 * 1024)
                .expect("replica opens");
        mirror.stream_opened("p_1_1", 3, replica);
        mirror.assert_invariants_for_test();

        // Connection drops: stream ids and replicas die with it.
        mirror.connection_lost("connection closed");
        assert!(mirror.replicas.is_empty());
        mirror.assert_invariants_for_test();

        // Reconnect: full resync from a snapshot where the old pane is gone
        // and a new one exists. Events from before the new anchor must not
        // resurrect the old pane.
        mirror.connection.connect_started();
        mirror
            .connection
            .connected(crate::protocol::framed::NegotiatedSession {
                protocol: crate::protocol::framed::FRAMED_PROTOCOL_VERSION,
                capabilities: vec![crate::protocol::framed::CAPABILITY_PANE_STREAM.to_owned()],
            });
        mirror.begin_resync();
        mirror.catalog.resync(&canned_snapshot("p_1_9"), 50);

        let stale_recreate = envelope(serde_json::json!({
            "event": "pane_created",
            "data": { "type": "pane_created", "pane": {
                "pane_id": "p_1_1",
                "terminal_id": "term_1",
                "workspace_id": "ws_1",
                "tab_id": "t_1_1",
                "focused": false,
                "agent_status": "idle",
                "revision": 1
            }}
        }));
        assert!(!mirror.catalog.apply(49, &stale_recreate));

        assert!(
            mirror.catalog.pane("p_1_1").is_none(),
            "ghost pane survived resync"
        );
        assert_eq!(mirror.catalog.panes.len(), 1);
        let replica =
            crate::terminal::replica::PaneReplica::open("fresh", 9, None, 80, 24, 64 * 1024)
                .expect("replica opens");
        mirror.stream_opened("p_1_9", 7, replica);
        mirror.assert_invariants_for_test();
    }

    #[test]
    fn reopening_a_pane_stream_replaces_the_previous_replica() {
        let mut mirror = RemoteMirror::test_new();
        mirror.catalog.resync(&canned_snapshot("p_1_1"), 10);
        let first = crate::terminal::replica::PaneReplica::open("one", 1, None, 80, 24, 64 * 1024)
            .expect("replica opens");
        mirror.stream_opened("p_1_1", 3, first);
        let second = crate::terminal::replica::PaneReplica::open("two", 2, None, 80, 24, 64 * 1024)
            .expect("replica opens");
        mirror.stream_opened("p_1_1", 4, second);

        assert_eq!(mirror.stream_for_pane("p_1_1"), Some(4));
        assert_eq!(mirror.pane_for_stream(4), Some("p_1_1"));
        assert!(mirror.pane_for_stream(3).is_none());
        assert_eq!(mirror.replicas.len(), 1);
        mirror.assert_invariants_for_test();
    }

    #[test]
    fn mirror_collection_is_keyed_with_local_as_remote_zero() {
        let mut mirrors = RemoteMirrors::with_local();
        assert_eq!(mirrors.local().remote_index, LOCAL_REMOTE_INDEX);
        assert_eq!(mirrors.local().name, LOCAL_REMOTE_NAME);
        assert_eq!(mirrors.iter().count(), 1);

        let mirror = mirrors.local_mut();
        mirror.connection.connect_started();
        mirror
            .connection
            .connected(crate::protocol::framed::NegotiatedSession {
                protocol: crate::protocol::framed::FRAMED_PROTOCOL_VERSION,
                capabilities: Vec::new(),
            });
        mirror.catalog.resync(&canned_snapshot("p_1_1"), 1);
        let replica =
            crate::terminal::replica::PaneReplica::open("one", 1, None, 80, 24, 64 * 1024)
                .expect("replica opens");
        mirror.stream_opened("p_1_1", 1, replica);
        mirror.stream_closed(1);
        assert!(mirror.replicas.is_empty());
        assert!(mirror.stream_for_pane("p_1_1").is_none());
        mirror.assert_invariants_for_test();
    }

    #[test]
    fn global_chrome_is_plain_client_data() {
        let mut chrome = super::chrome::GlobalChrome::new();
        assert!(!chrome.sidebar_collapsed);
        assert_eq!(chrome.workspace_scroll, 0);
        assert_eq!(chrome.agent_panel_scroll, 0);
        assert_eq!(chrome.tab_scroll, 0);
        assert_eq!(chrome.connection_status, None);
        chrome.sidebar_collapsed = true;
        chrome.connection_status = Some("connecting (attempt 2)".into());
        assert_eq!(chrome, chrome.clone());
    }

    #[test]
    fn adversarial_catalog_passes_invariants() {
        let mirror = RemoteMirror::test_with_adversarial_catalog();
        mirror.assert_invariants_for_test();
        assert_eq!(mirror.catalog.workspaces.len(), 2);
        assert_eq!(mirror.catalog.panes.len(), 3);
    }
}

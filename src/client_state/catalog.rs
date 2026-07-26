//! Event-driven session catalog mirror.
//!
//! Plain data: the pure client's picture of the server session — workspaces,
//! tabs, panes, layouts, and focus — built from one full
//! [`SessionSnapshot`] resync and kept current by applying the server's
//! catalog events in sequence order. On reconnect the catalog is rebuilt
//! from a fresh snapshot; events older than the snapshot's sequence anchor
//! are dropped so no ghost or duplicate entries survive.

use crate::api::schema::events::{EventData, EventEnvelope};
use crate::api::schema::panes::{PaneInfo, PaneLayoutSnapshot};
use crate::api::schema::session::SessionSnapshot;
use crate::api::schema::tabs::TabInfo;
use crate::api::schema::workspaces::WorkspaceInfo;

/// The mirrored session catalog of one remote.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct SessionCatalog {
    /// Event sequence the catalog is current through. Events at or below
    /// this sequence are stale and must not be applied.
    pub(crate) sequence: u64,
    pub(crate) focused_workspace_id: Option<String>,
    pub(crate) focused_tab_id: Option<String>,
    pub(crate) focused_pane_id: Option<String>,
    pub(crate) workspaces: Vec<WorkspaceInfo>,
    pub(crate) tabs: Vec<TabInfo>,
    pub(crate) panes: Vec<PaneInfo>,
    pub(crate) layouts: Vec<PaneLayoutSnapshot>,
}

impl SessionCatalog {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Replaces the whole catalog from a full snapshot anchored at
    /// `sequence` in the server's event stream.
    pub(crate) fn resync(&mut self, snapshot: &SessionSnapshot, sequence: u64) {
        self.sequence = sequence;
        self.focused_workspace_id = snapshot.focused_workspace_id.clone();
        self.focused_tab_id = snapshot.focused_tab_id.clone();
        self.focused_pane_id = snapshot.focused_pane_id.clone();
        self.workspaces = snapshot.workspaces.clone();
        self.tabs = snapshot.tabs.clone();
        self.panes = snapshot.panes.clone();
        self.layouts = snapshot.layouts.clone();
    }

    /// Applies one catalog event at `sequence`. Returns false when the event
    /// is stale (at or below the catalog's current sequence) and was dropped.
    pub(crate) fn apply(&mut self, sequence: u64, envelope: &EventEnvelope) -> bool {
        if sequence <= self.sequence {
            return false;
        }
        self.sequence = sequence;
        match &envelope.data {
            EventData::WorkspaceCreated { workspace }
            | EventData::WorkspaceUpdated { workspace }
            | EventData::WorkspaceMetadataUpdated { workspace } => {
                self.upsert_workspace(workspace.clone());
            }
            EventData::WorkspaceClosed { workspace_id, .. } => {
                self.remove_workspace(workspace_id);
            }
            EventData::WorkspaceRenamed {
                workspace_id,
                label,
            } => {
                if let Some(workspace) = self.workspace_mut(workspace_id) {
                    workspace.label = label.clone();
                }
            }
            EventData::WorkspaceMoved { workspaces, .. } => {
                self.workspaces = workspaces.clone();
            }
            EventData::WorkspaceFocused { workspace_id } => {
                self.focused_workspace_id = Some(workspace_id.clone());
                for workspace in &mut self.workspaces {
                    workspace.focused = workspace.workspace_id == *workspace_id;
                }
            }
            EventData::WorktreeCreated { workspace, .. }
            | EventData::WorktreeOpened { workspace, .. } => {
                self.upsert_workspace(workspace.clone());
            }
            EventData::WorktreeRemoved {
                workspace_id,
                workspace,
                ..
            } => match workspace {
                Some(workspace) => self.upsert_workspace(workspace.clone()),
                None => self.remove_workspace(workspace_id),
            },
            EventData::TabCreated { tab } => {
                self.upsert_tab(tab.clone());
            }
            EventData::TabClosed { tab_id, .. } => {
                self.remove_tab(tab_id);
            }
            EventData::TabRenamed { tab_id, label, .. } => {
                if let Some(tab) = self.tab_mut(tab_id) {
                    tab.label = label.clone();
                }
            }
            EventData::TabMoved {
                workspace_id, tabs, ..
            } => {
                self.tabs.retain(|tab| tab.workspace_id != *workspace_id);
                self.tabs.extend(tabs.iter().cloned());
            }
            EventData::TabFocused {
                tab_id,
                workspace_id,
            } => {
                self.focused_tab_id = Some(tab_id.clone());
                for tab in &mut self.tabs {
                    if tab.workspace_id == *workspace_id {
                        tab.focused = tab.tab_id == *tab_id;
                    }
                }
                if let Some(workspace) = self.workspace_mut(workspace_id) {
                    workspace.active_tab_id = tab_id.clone();
                }
            }
            EventData::PaneCreated { pane } | EventData::PaneUpdated { pane } => {
                self.upsert_pane(pane.clone());
            }
            EventData::PaneClosed { pane_id, .. } | EventData::PaneExited { pane_id, .. } => {
                self.remove_pane(pane_id);
            }
            EventData::PaneFocused {
                pane_id,
                workspace_id,
            } => {
                self.focused_pane_id = Some(pane_id.clone());
                for pane in &mut self.panes {
                    if pane.workspace_id == *workspace_id {
                        pane.focused = pane.pane_id == *pane_id;
                    }
                }
            }
            EventData::PaneMoved {
                previous_pane_id,
                pane,
                created_workspace,
                created_tab,
                closed_workspace_id,
                closed_tab_id,
                ..
            } => {
                self.remove_pane(previous_pane_id);
                if let Some(workspace) = created_workspace {
                    self.upsert_workspace(workspace.clone());
                }
                if let Some(tab) = created_tab {
                    self.upsert_tab(tab.clone());
                }
                self.upsert_pane((**pane).clone());
                if let Some(tab_id) = closed_tab_id {
                    self.remove_tab(tab_id);
                }
                if let Some(workspace_id) = closed_workspace_id {
                    self.remove_workspace(workspace_id);
                }
            }
            EventData::PaneOutputChanged {
                pane_id, revision, ..
            } => {
                if let Some(pane) = self.pane_mut(pane_id) {
                    pane.revision = *revision;
                }
            }
            EventData::PaneAgentDetected { pane_id, agent, .. } => {
                if let Some(pane) = self.pane_mut(pane_id) {
                    pane.agent = agent.clone();
                }
            }
            EventData::PaneAgentStatusChanged {
                pane_id,
                agent_status,
                agent,
                title,
                display_agent,
                state_labels,
                ..
            } => {
                if let Some(pane) = self.pane_mut(pane_id) {
                    pane.agent_status = *agent_status;
                    if agent.is_some() {
                        pane.agent = agent.clone();
                    }
                    if title.is_some() {
                        pane.title = title.clone();
                    }
                    if display_agent.is_some() {
                        pane.display_agent = display_agent.clone();
                    }
                    pane.state_labels = state_labels.clone();
                }
            }
            EventData::LayoutUpdated { layout } => {
                self.upsert_layout(layout.clone());
            }
            // Not catalog facts: surfaced to the user through other paths.
            EventData::NotificationPosted { .. } | EventData::WindowTitleChanged { .. } => {}
        }
        true
    }

    pub(crate) fn workspace(&self, workspace_id: &str) -> Option<&WorkspaceInfo> {
        self.workspaces
            .iter()
            .find(|workspace| workspace.workspace_id == workspace_id)
    }

    fn workspace_mut(&mut self, workspace_id: &str) -> Option<&mut WorkspaceInfo> {
        self.workspaces
            .iter_mut()
            .find(|workspace| workspace.workspace_id == workspace_id)
    }

    fn tab_mut(&mut self, tab_id: &str) -> Option<&mut TabInfo> {
        self.tabs.iter_mut().find(|tab| tab.tab_id == tab_id)
    }

    pub(crate) fn pane(&self, pane_id: &str) -> Option<&PaneInfo> {
        self.panes.iter().find(|pane| pane.pane_id == pane_id)
    }

    fn pane_mut(&mut self, pane_id: &str) -> Option<&mut PaneInfo> {
        self.panes.iter_mut().find(|pane| pane.pane_id == pane_id)
    }

    fn upsert_workspace(&mut self, workspace: WorkspaceInfo) {
        match self.workspace_mut(&workspace.workspace_id) {
            Some(existing) => *existing = workspace,
            None => self.workspaces.push(workspace),
        }
    }

    fn remove_workspace(&mut self, workspace_id: &str) {
        self.workspaces
            .retain(|workspace| workspace.workspace_id != workspace_id);
        self.tabs.retain(|tab| tab.workspace_id != workspace_id);
        self.panes.retain(|pane| pane.workspace_id != workspace_id);
        self.layouts
            .retain(|layout| layout.workspace_id != workspace_id);
        if self.focused_workspace_id.as_deref() == Some(workspace_id) {
            self.focused_workspace_id = None;
        }
        self.clear_dangling_focus();
    }

    fn upsert_tab(&mut self, tab: TabInfo) {
        match self.tab_mut(&tab.tab_id) {
            Some(existing) => *existing = tab,
            None => self.tabs.push(tab),
        }
    }

    fn remove_tab(&mut self, tab_id: &str) {
        self.tabs.retain(|tab| tab.tab_id != tab_id);
        self.panes.retain(|pane| pane.tab_id != tab_id);
        self.layouts.retain(|layout| layout.tab_id != tab_id);
        if self.focused_tab_id.as_deref() == Some(tab_id) {
            self.focused_tab_id = None;
        }
        self.clear_dangling_focus();
    }

    fn upsert_pane(&mut self, pane: PaneInfo) {
        match self.pane_mut(&pane.pane_id) {
            Some(existing) => *existing = pane,
            None => self.panes.push(pane),
        }
    }

    fn remove_pane(&mut self, pane_id: &str) {
        self.panes.retain(|pane| pane.pane_id != pane_id);
        if self.focused_pane_id.as_deref() == Some(pane_id) {
            self.focused_pane_id = None;
        }
    }

    fn upsert_layout(&mut self, layout: PaneLayoutSnapshot) {
        match self.layouts.iter_mut().find(|existing| {
            existing.workspace_id == layout.workspace_id && existing.tab_id == layout.tab_id
        }) {
            Some(existing) => *existing = layout,
            None => self.layouts.push(layout),
        }
    }

    fn clear_dangling_focus(&mut self) {
        if let Some(pane_id) = self.focused_pane_id.clone() {
            if self.pane(&pane_id).is_none() {
                self.focused_pane_id = None;
            }
        }
        if let Some(tab_id) = self.focused_tab_id.clone() {
            if !self.tabs.iter().any(|tab| tab.tab_id == tab_id) {
                self.focused_tab_id = None;
            }
        }
    }

    /// Catalog referential integrity, asserted from mirror tests.
    #[cfg(test)]
    pub(crate) fn assert_invariants_for_test(&self) {
        let mut workspace_ids: Vec<&str> = self
            .workspaces
            .iter()
            .map(|workspace| workspace.workspace_id.as_str())
            .collect();
        workspace_ids.sort_unstable();
        let unique_workspaces = workspace_ids.len();
        workspace_ids.dedup();
        assert_eq!(
            unique_workspaces,
            workspace_ids.len(),
            "duplicate workspace ids"
        );

        let mut tab_ids: Vec<&str> = self.tabs.iter().map(|tab| tab.tab_id.as_str()).collect();
        tab_ids.sort_unstable();
        let unique_tabs = tab_ids.len();
        tab_ids.dedup();
        assert_eq!(unique_tabs, tab_ids.len(), "duplicate tab ids");

        let mut pane_ids: Vec<&str> = self
            .panes
            .iter()
            .map(|pane| pane.pane_id.as_str())
            .collect();
        pane_ids.sort_unstable();
        let unique_panes = pane_ids.len();
        pane_ids.dedup();
        assert_eq!(unique_panes, pane_ids.len(), "duplicate pane ids");

        for tab in &self.tabs {
            assert!(
                self.workspace(&tab.workspace_id).is_some(),
                "tab {} references missing workspace {}",
                tab.tab_id,
                tab.workspace_id
            );
        }
        for pane in &self.panes {
            assert!(
                self.workspace(&pane.workspace_id).is_some(),
                "pane {} references missing workspace {}",
                pane.pane_id,
                pane.workspace_id
            );
            assert!(
                self.tabs.iter().any(|tab| tab.tab_id == pane.tab_id),
                "pane {} references missing tab {}",
                pane.pane_id,
                pane.tab_id
            );
        }
        if let Some(pane_id) = &self.focused_pane_id {
            assert!(
                self.pane(pane_id).is_some(),
                "focused pane {pane_id} missing"
            );
        }
        if let Some(tab_id) = &self.focused_tab_id {
            assert!(
                self.tabs.iter().any(|tab| tab.tab_id == *tab_id),
                "focused tab {tab_id} missing"
            );
        }
        if let Some(workspace_id) = &self.focused_workspace_id {
            assert!(
                self.workspace(workspace_id).is_some(),
                "focused workspace {workspace_id} missing"
            );
        }
    }
}

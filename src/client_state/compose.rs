//! Pure composition of the rendered view from mirrors plus chrome.
//!
//! `compose_into` projects one remote's [`SessionCatalog`] into the
//! `AppState` shape the existing `compute_view` + `render` pair consumes;
//! `compose_fleet_into` composes N in-view mirrors into the same shape,
//! flat, with per-space remote attribution when two or more remotes are in
//! view. Workspaces, tabs, pane layouts, and terminal metadata become plain
//! state, while client-owned chrome (mode, scroll, sidebar, view
//! membership) is applied from [`GlobalChrome`]. No IO, no runtimes: pane
//! screens arrive separately through [`MirrorPaneSource`], which serves
//! replicas through the same [`PaneContentSource`] seam the server render
//! path uses.

use std::collections::HashMap;

use crate::api::schema::common::{AgentStatus, SplitDirection};
use crate::api::schema::panes::{PaneInfo, PaneLayoutSnapshot};
use crate::app::state::RemoteTag;
use crate::app::AppState;
use crate::layout::{Node, PaneId, TileLayout};
use crate::pane::PaneState;
use crate::terminal::{PaneContent, PaneContentSource, TerminalId, TerminalState};
use crate::workspace::{Tab, Workspace};

use super::chrome::GlobalChrome;
use super::fleet_view::RemoteDescriptor;
use super::{RemoteMirror, RemoteMirrors, LOCAL_REMOTE_INDEX};

/// Namespaces a server-scoped id with its remote so ids from different
/// remotes can never collide in the composed view. Local ids stay raw, which
/// keeps the single-remote view byte-identical to today's.
fn scoped_id(remote_index: usize, id: &str) -> String {
    if remote_index == LOCAL_REMOTE_INDEX {
        id.to_owned()
    } else {
        format!("r{remote_index}:{id}")
    }
}

/// The composed-view terminal id of a remote pane's terminal.
pub(crate) fn composed_terminal_id(remote_index: usize, terminal_id: &str) -> TerminalId {
    TerminalId::from_server(&scoped_id(remote_index, terminal_id))
}

/// Stable id allocations across compositions, so pane identity (selection,
/// copy mode, focus memory) survives catalog updates. Keys are
/// (remote index, public id): panes, spaces, and tabs belong to exactly one
/// remote, and equal public ids on different remotes are different panes.
#[derive(Default)]
pub(crate) struct ComposeIds {
    pane_ids: HashMap<(usize, String), PaneId>,
    /// Owner (remote index, public workspace id) per composed workspace,
    /// parallel to `AppState::workspaces`. Rebuilt by every compose pass;
    /// creation targeting and control-plane dispatch read it.
    workspace_owners: Vec<(usize, String)>,
}

impl ComposeIds {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// The local pane id for a remote's public pane id, allocated on first
    /// sight.
    pub(crate) fn pane_id(&mut self, remote_index: usize, public_pane_id: &str) -> PaneId {
        *self
            .pane_ids
            .entry((remote_index, public_pane_id.to_owned()))
            .or_insert_with(PaneId::alloc)
    }

    /// The owning remote and public pane id behind a local pane id, for
    /// control-plane calls.
    pub(crate) fn public_pane_id(&self, pane_id: PaneId) -> Option<(usize, &str)> {
        self.pane_ids.iter().find_map(|((remote, public), local)| {
            (*local == pane_id).then_some((*remote, public.as_str()))
        })
    }

    /// The owning remote and public workspace id of a composed workspace.
    pub(crate) fn workspace_owner(&self, ws_idx: usize) -> Option<(usize, &str)> {
        self.workspace_owners
            .get(ws_idx)
            .map(|(remote, public)| (*remote, public.as_str()))
    }

    /// Drops allocations for panes no longer present in any mirror.
    fn retain_known(&mut self, mirrors: &RemoteMirrors) {
        self.pane_ids.retain(|(remote, public), _| {
            mirrors
                .get(*remote)
                .is_some_and(|mirror| mirror.catalog.pane(public).is_some())
        });
    }
}

/// One mirror's contribution to the composed view.
struct MirrorProjection {
    workspaces: Vec<Workspace>,
    terminals: HashMap<TerminalId, TerminalState>,
    /// Composed (scoped) id of the mirror's focused workspace, when any.
    focused_workspace: Option<String>,
    /// Public workspace id per projected workspace, in order.
    workspace_publics: Vec<String>,
}

/// Projects one mirror's catalog into plain workspaces and terminals, with
/// all ids scoped to the owning remote.
fn project_mirror(
    mirror: &RemoteMirror,
    remote_index: usize,
    ids: &mut ComposeIds,
) -> MirrorProjection {
    let catalog = &mirror.catalog;
    let mut workspaces = Vec::new();
    let mut terminals = HashMap::new();
    let mut workspace_publics = Vec::new();
    for workspace_info in &catalog.workspaces {
        let mut tabs = Vec::new();
        let mut public_pane_numbers = HashMap::new();
        let mut active_tab = 0usize;
        let workspace_tabs: Vec<_> = catalog
            .tabs
            .iter()
            .filter(|tab| tab.workspace_id == workspace_info.workspace_id)
            .collect();
        for (tab_idx, tab_info) in workspace_tabs.iter().enumerate() {
            let tab_panes: Vec<&PaneInfo> = catalog
                .panes
                .iter()
                .filter(|pane| pane.tab_id == tab_info.tab_id)
                .collect();
            if tab_panes.is_empty() {
                continue;
            }
            let layout_snapshot = catalog.layouts.iter().find(|layout| {
                layout.workspace_id == workspace_info.workspace_id
                    && layout.tab_id == tab_info.tab_id
            });
            let (layout, root_pane, zoomed) =
                build_tab_layout(ids, remote_index, &tab_panes, layout_snapshot);

            let mut panes = HashMap::new();
            for pane in &tab_panes {
                let pane_id = ids.pane_id(remote_index, &pane.pane_id);
                let terminal_id = composed_terminal_id(remote_index, &pane.terminal_id);
                let mut pane_state = PaneState::new(terminal_id.clone());
                pane_state.seen = pane.agent_status != AgentStatus::Done;
                panes.insert(pane_id, pane_state);
                if let Some(number) = public_id_number(&pane.pane_id) {
                    public_pane_numbers.insert(pane_id, number);
                }
                terminals.insert(terminal_id.clone(), terminal_state_for(terminal_id, pane));
            }

            if tab_info.tab_id == workspace_info.active_tab_id || tab_info.focused {
                active_tab = tabs.len();
            }
            let custom_name = (!tab_info.label.is_empty()
                && tab_info.label != format!("tab {}", tab_idx + 1))
            .then(|| tab_info.label.clone());
            tabs.push(Tab::client_projection(
                tab_idx + 1,
                custom_name,
                root_pane,
                layout,
                panes,
                zoomed,
            ));
        }
        if tabs.is_empty() {
            continue;
        }
        workspace_publics.push(workspace_info.workspace_id.clone());
        workspaces.push(Workspace::client_projection(
            scoped_id(remote_index, &workspace_info.workspace_id),
            workspace_info.label.clone(),
            tabs,
            active_tab,
            public_pane_numbers,
        ));
    }

    let focused_workspace = catalog
        .focused_workspace_id
        .as_deref()
        .filter(|focused| workspace_publics.iter().any(|public| public == focused))
        .map(|focused| scoped_id(remote_index, focused));

    MirrorProjection {
        workspaces,
        terminals,
        focused_workspace,
        workspace_publics,
    }
}

/// Applies the composed workspaces plus the client chrome to `app`.
fn apply_composition(
    workspaces: Vec<Workspace>,
    terminals: HashMap<TerminalId, TerminalState>,
    focused_workspace: Option<&str>,
    chrome: &GlobalChrome,
    toast_context: &str,
    app: &mut AppState,
) {
    let active = focused_workspace
        .and_then(|focused| workspaces.iter().position(|ws| ws.id == focused))
        .or(if workspaces.is_empty() { None } else { Some(0) });

    app.workspaces = workspaces;
    app.terminals = terminals;
    app.active = active;
    app.selected = active
        .unwrap_or(0)
        .min(app.workspaces.len().saturating_sub(1));

    app.sidebar_collapsed = chrome.sidebar_collapsed;
    app.workspace_scroll = chrome.workspace_scroll;
    app.agent_panel_scroll = chrome.agent_panel_scroll;
    app.tab_scroll = chrome.tab_scroll;
    app.toast =
        chrome
            .connection_status
            .as_ref()
            .map(|status| crate::app::state::ToastNotification {
                kind: crate::app::state::ToastKind::NeedsAttention,
                title: status.clone(),
                context: toast_context.to_owned(),
                position: None,
                target: None,
            });
}

/// Projects the local mirror's catalog plus the client chrome into `app`.
///
/// Catalog-derived fields (workspaces, terminals, focus) are rebuilt from
/// scratch; chrome fields the user owns (mode, scroll positions, sidebar
/// state) are preserved or applied from `chrome`. The single-mirror path:
/// no chips, no attribution — exactly today's sidebar.
pub(crate) fn compose_into(
    mirror: &RemoteMirror,
    chrome: &GlobalChrome,
    ids: &mut ComposeIds,
    app: &mut AppState,
) {
    ids.pane_ids.retain(|(remote, public), _| {
        *remote == mirror.remote_index && mirror.catalog.pane(public).is_some()
    });
    let projection = project_mirror(mirror, mirror.remote_index, ids);
    ids.workspace_owners = projection
        .workspace_publics
        .iter()
        .map(|public| (mirror.remote_index, public.clone()))
        .collect();
    let focused = projection.focused_workspace.clone();
    apply_composition(
        projection.workspaces,
        projection.terminals,
        focused.as_deref(),
        chrome,
        &mirror.name,
        app,
    );
    app.remote_chips = Vec::new();
    app.workspace_remote_tags = Vec::new();
}

/// Composes every in-view mirror into `app`, flat and in descriptor order.
///
/// With two or more remotes in view every space carries a [`RemoteTag`]
/// (hue gutter plus dim remote token); with one, the view is exactly the
/// single-mirror composition. The chip strip model is populated whenever
/// more than one remote is configured, regardless of view membership —
/// filtered-out remotes stay connected and keep their mirrors. The focused
/// remote (falling back to the first in view) contributes the composed
/// focus.
pub(crate) fn compose_fleet_into(
    mirrors: &RemoteMirrors,
    descriptors: &[RemoteDescriptor],
    chrome: &GlobalChrome,
    ids: &mut ComposeIds,
    app: &mut AppState,
) {
    ids.retain_known(mirrors);
    let in_view = chrome.selection.in_view(descriptors);
    let focused_remote = chrome.selection.effective_focused_remote(descriptors);

    let mut workspaces = Vec::new();
    let mut terminals = HashMap::new();
    let mut owners = Vec::new();
    let mut tags = Vec::new();
    let mut focused_workspace = None;
    for descriptor in &in_view {
        let Some(mirror) = mirrors.get(descriptor.index) else {
            continue;
        };
        let projection = project_mirror(mirror, descriptor.index, ids);
        if descriptor.index == focused_remote {
            focused_workspace = projection.focused_workspace.clone();
        }
        for public in projection.workspace_publics {
            owners.push((descriptor.index, public));
            tags.push(RemoteTag {
                name: descriptor.name.clone(),
                hue_index: descriptor.hue_index,
            });
        }
        workspaces.extend(projection.workspaces);
        terminals.extend(projection.terminals);
    }
    ids.workspace_owners = owners;

    apply_composition(
        workspaces,
        terminals,
        focused_workspace.as_deref(),
        chrome,
        "remotes",
        app,
    );
    app.workspace_remote_tags = if in_view.len() >= 2 { tags } else { Vec::new() };
    app.remote_chips = if descriptors.len() >= 2 {
        super::fleet_view::remote_chip_states(mirrors, descriptors, &chrome.selection)
    } else {
        Vec::new()
    };
}

/// Maps a catalog pane to the plain terminal metadata the sidebar reads.
fn terminal_state_for(terminal_id: TerminalId, pane: &PaneInfo) -> TerminalState {
    let cwd = pane
        .cwd
        .as_deref()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("/"));
    let mut terminal = TerminalState::new(terminal_id, cwd);
    terminal.terminal_title = pane.terminal_title.clone();
    terminal.manual_label = pane.label.clone();
    terminal.agent_name = pane.display_agent.clone().or_else(|| pane.agent.clone());
    terminal.state = match pane.agent_status {
        AgentStatus::Idle | AgentStatus::Done => crate::detect::AgentState::Idle,
        AgentStatus::Working => crate::detect::AgentState::Working,
        AgentStatus::Blocked => crate::detect::AgentState::Blocked,
        AgentStatus::Unknown => crate::detect::AgentState::Unknown,
    };
    terminal.revision = pane.revision;
    terminal
}

/// Trailing number of a public id like `p_2_7`, for pane number badges.
fn public_id_number(public_id: &str) -> Option<usize> {
    public_id
        .rsplit('_')
        .next()
        .and_then(|tail| tail.parse().ok())
}

/// Rebuilds a tab's split tree from the exported layout snapshot.
///
/// The snapshot's split ids encode the tree path of every split
/// (`split_{idx}_{bits}`), and its pane list is the in-order traversal of
/// the leaves, so the tree rebuilds exactly. Without a usable snapshot the
/// panes fold into nested even splits — right geometry count, approximate
/// ratios — until the next `layout.updated` event lands.
fn build_tab_layout(
    ids: &mut ComposeIds,
    remote_index: usize,
    tab_panes: &[&PaneInfo],
    snapshot: Option<&PaneLayoutSnapshot>,
) -> (TileLayout, PaneId, bool) {
    // Focus authority: the catalog's pane.focused flags are kept current by
    // pane_focused events, while the layout snapshot's focused_pane_id only
    // refreshes on layout.updated (create/close/split/move) — so the pane
    // flags win and the snapshot is only a fallback.
    let focused_public = tab_panes
        .iter()
        .find(|pane| pane.focused)
        .map(|pane| pane.pane_id.as_str())
        .or_else(|| snapshot.map(|snapshot| snapshot.focused_pane_id.as_str()));
    let zoomed = snapshot.is_some_and(|snapshot| snapshot.zoomed);

    if let Some(snapshot) = snapshot {
        if let Some(layout) =
            layout_from_snapshot(ids, remote_index, snapshot, tab_panes, focused_public)
        {
            let root = leftmost_leaf(layout.root()).unwrap_or(layout.focused());
            return (layout, root, zoomed);
        }
    }

    // Fallback: fold the panes into nested even splits.
    let pane_ids: Vec<PaneId> = tab_panes
        .iter()
        .map(|pane| ids.pane_id(remote_index, &pane.pane_id))
        .collect();
    let mut node = Node::Pane(pane_ids[pane_ids.len() - 1]);
    for pane_id in pane_ids.iter().rev().skip(1) {
        node = Node::Split {
            direction: ratatui::layout::Direction::Horizontal,
            ratio: 0.5,
            first: Box::new(Node::Pane(*pane_id)),
            second: Box::new(node),
        };
    }
    let focus = focused_public
        .map(|public| ids.pane_id(remote_index, public))
        .unwrap_or(pane_ids[0]);
    let layout = TileLayout::from_saved(node, focus);
    (layout, pane_ids[0], zoomed)
}

/// Exact tree rebuild from split paths plus the in-order leaf list.
fn layout_from_snapshot(
    ids: &mut ComposeIds,
    remote_index: usize,
    snapshot: &PaneLayoutSnapshot,
    tab_panes: &[&PaneInfo],
    focused_public: Option<&str>,
) -> Option<TileLayout> {
    if snapshot.panes.len() != tab_panes.len() {
        return None;
    }
    let known: std::collections::HashSet<&str> =
        tab_panes.iter().map(|pane| pane.pane_id.as_str()).collect();
    if !snapshot
        .panes
        .iter()
        .all(|pane| known.contains(pane.pane_id.as_str()))
    {
        return None;
    }

    let mut splits = HashMap::new();
    for split in &snapshot.splits {
        let path = parse_split_path(&split.id)?;
        let direction = match split.direction {
            SplitDirection::Right => ratatui::layout::Direction::Horizontal,
            SplitDirection::Down => ratatui::layout::Direction::Vertical,
        };
        splits.insert(path, (direction, split.ratio));
    }

    let mut leaves = snapshot
        .panes
        .iter()
        .map(|pane| ids.pane_id(remote_index, &pane.pane_id))
        .collect::<Vec<_>>()
        .into_iter();
    let root = build_node(&splits, &mut leaves, Vec::new(), snapshot.splits.len() + 1)?;
    if leaves.next().is_some() {
        return None;
    }
    let focus = ids.pane_id(
        remote_index,
        focused_public.unwrap_or(snapshot.focused_pane_id.as_str()),
    );
    Some(TileLayout::from_saved(root, focus))
}

fn build_node(
    splits: &HashMap<Vec<bool>, (ratatui::layout::Direction, f32)>,
    leaves: &mut std::vec::IntoIter<PaneId>,
    path: Vec<bool>,
    depth_budget: usize,
) -> Option<Node> {
    if depth_budget == 0 {
        return None;
    }
    if let Some((direction, ratio)) = splits.get(&path) {
        let mut first_path = path.clone();
        first_path.push(false);
        let mut second_path = path;
        second_path.push(true);
        let first = build_node(splits, leaves, first_path, depth_budget - 1)?;
        let second = build_node(splits, leaves, second_path, depth_budget - 1)?;
        Some(Node::Split {
            direction: *direction,
            ratio: *ratio,
            first: Box::new(first),
            second: Box::new(second),
        })
    } else {
        leaves.next().map(Node::Pane)
    }
}

/// Parses the tree path out of an exported split id (`split_{idx}_{bits}`
/// or `split_{idx}_root`).
fn parse_split_path(id: &str) -> Option<Vec<bool>> {
    let rest = id.strip_prefix("split_")?;
    let (_, path) = rest.split_once('_')?;
    if path == "root" {
        return Some(Vec::new());
    }
    path.chars()
        .map(|c| match c {
            '0' => Some(false),
            '1' => Some(true),
            _ => None,
        })
        .collect()
}

fn leftmost_leaf(node: &Node) -> Option<PaneId> {
    match node {
        Node::Pane(id) => Some(*id),
        Node::Split { first, .. } => leftmost_leaf(first),
    }
}

/// Applies config-derived presentation to a fresh client `AppState`,
/// mirroring the assignments `App::new` makes for the server-owned state.
pub(crate) fn apply_client_config(app: &mut AppState, config: &crate::config::Config) {
    let (prefix_code, prefix_mods) = config.prefix_key();
    app.prefix_code = prefix_code;
    app.prefix_mods = prefix_mods;

    let (sidebar_min_width, sidebar_max_width) = crate::config::validated_sidebar_bounds(
        config.ui.sidebar_min_width,
        config.ui.sidebar_max_width,
    )
    .unwrap_or((18, 36));
    app.default_sidebar_width = config.ui.sidebar_width;
    app.sidebar_width = config.ui.sidebar_width;
    app.sidebar_min_width = sidebar_min_width;
    app.sidebar_max_width = sidebar_max_width;
    app.mobile_width_threshold = config.ui.mobile_width_threshold;
    app.sidebar_collapsed = config.ui.sidebar_start_collapsed;
    app.sidebar_collapsed_mode = config.ui.sidebar_collapsed_mode;
    app.agent_panel_sort = crate::app::agent_panel_sort_from_config(config.ui.agent_panel_sort);
    app.sidebar_agents = config.ui.sidebar.agents.clone();
    app.sidebar_spaces = config.ui.sidebar.spaces.clone();

    app.mouse_capture = config.ui.mouse_capture;
    app.copy_on_select = config.ui.copy_on_select;
    app.mouse_scroll_lines = config.ui.mouse_scroll_lines();
    app.confirm_close = config.ui.confirm_close;
    app.pane_borders = config.ui.pane_borders;
    app.pane_gaps = config.ui.pane_gaps;
    app.show_agent_labels_on_pane_borders = config.ui.show_agent_labels_on_pane_borders;
    app.hide_tab_bar_when_single_tab = config.ui.hide_tab_bar_when_single_tab;
    app.toast_config = config.ui.toast.clone();
    app.sound = config.ui.sound.clone();
    app.accent = crate::config::parse_color(&config.ui.accent);
    app.keybinds = config.keybinds();

    let theme_runtime = crate::app::theme_runtime_config(config, true);
    let (palette, theme_name) = crate::app::resolve_effective_theme(&theme_runtime, None);
    app.palette = palette;
    app.theme_name = theme_name;
    app.theme_runtime = theme_runtime;
}

/// Serves the mirror's pane replicas through the [`PaneContentSource`] seam
/// so the shared render path draws them exactly like live runtimes.
pub(crate) struct MirrorPaneSource<'a> {
    by_terminal: HashMap<TerminalId, &'a crate::terminal::replica::PaneReplica>,
}

impl<'a> MirrorPaneSource<'a> {
    pub(crate) fn new(mirror: &'a RemoteMirror) -> Self {
        let mut source = Self {
            by_terminal: HashMap::new(),
        };
        source.add_mirror(mirror);
        source
    }

    /// Serves the replicas of every in-view mirror, keyed by the composed
    /// (remote-scoped) terminal ids the fleet composition assigns.
    pub(crate) fn for_view(mirrors: &'a RemoteMirrors, in_view: &[usize]) -> Self {
        let mut source = Self {
            by_terminal: HashMap::new(),
        };
        for remote_index in in_view {
            if let Some(mirror) = mirrors.get(*remote_index) {
                source.add_mirror(mirror);
            }
        }
        source
    }

    fn add_mirror(&mut self, mirror: &'a RemoteMirror) {
        for (public_pane_id, stream_id) in &mirror.pane_streams {
            let Some(replica) = mirror.replicas.get(stream_id) else {
                continue;
            };
            let Some(pane) = mirror.catalog.pane(public_pane_id) else {
                continue;
            };
            self.by_terminal.insert(
                composed_terminal_id(mirror.remote_index, &pane.terminal_id),
                replica,
            );
        }
    }
}

impl PaneContentSource for MirrorPaneSource<'_> {
    fn pane_content(&self, terminal_id: &TerminalId) -> Option<&dyn PaneContent> {
        self.by_terminal
            .get(terminal_id)
            .map(|replica| *replica as &dyn PaneContent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;
    use ratatui::layout::Rect;
    use ratatui::Terminal;

    fn mirror_with_layout() -> crate::client_state::RemoteMirror {
        let mut mirror = crate::client_state::RemoteMirror::test_with_adversarial_catalog();
        let layout: PaneLayoutSnapshot = serde_json::from_value(serde_json::json!({
            "workspace_id": "ws_2",
            "tab_id": "t_2_1",
            "zoomed": false,
            "area": { "x": 0, "y": 0, "width": 80, "height": 20 },
            "focused_pane_id": "p_2_1",
            "panes": [
                { "pane_id": "p_2_1", "focused": true,
                  "rect": { "x": 0, "y": 0, "width": 40, "height": 20 } },
                { "pane_id": "p_2_10", "focused": false,
                  "rect": { "x": 40, "y": 0, "width": 40, "height": 20 } }
            ],
            "splits": [
                { "id": "split_0_root", "direction": "right", "ratio": 0.5,
                  "rect": { "x": 40, "y": 0, "width": 1, "height": 20 } }
            ]
        }))
        .expect("layout snapshot deserializes");
        mirror.catalog.layouts.push(layout);
        mirror
    }

    #[tokio::test]
    async fn compose_projects_the_catalog_into_app_state() {
        let mirror = mirror_with_layout();
        let chrome = GlobalChrome::new();
        let mut ids = ComposeIds::new();
        let mut app = AppState::test_new();

        compose_into(&mirror, &chrome, &mut ids, &mut app);

        assert_eq!(app.workspaces.len(), 2);
        assert_eq!(app.active, Some(0), "focused workspace ws_2 leads");
        assert_eq!(app.workspaces[0].id, "ws_2");
        assert_eq!(app.workspaces[0].tabs.len(), 1);
        assert_eq!(app.workspaces[0].tabs[0].layout.pane_count(), 2);
        assert_eq!(app.terminals.len(), 3);
        app.assert_invariants_for_test();

        // The exact split tree came back from the snapshot.
        crate::ui::compute_view(&mut app, Rect::new(0, 0, 106, 20));
        assert_eq!(app.view.pane_infos.len(), 2);
        let focused = app.workspaces[0].focused_pane_id().expect("focused pane");
        assert_eq!(ids.public_pane_id(focused), Some((0, "p_2_1")));

        // Composition is stable: pane ids survive a recompose.
        let before: Vec<_> = app.workspaces[0].tabs[0].layout.pane_ids();
        let mut app2 = AppState::test_new();
        compose_into(&mirror, &chrome, &mut ids, &mut app2);
        assert_eq!(app2.workspaces[0].tabs[0].layout.pane_ids(), before);
    }

    #[tokio::test]
    async fn panes_render_from_replicas_through_the_content_seam() {
        let mut mirror = crate::client_state::RemoteMirror::test_new();
        let snapshot: crate::api::schema::session::SessionSnapshot =
            serde_json::from_value(serde_json::json!({
                "version": "test",
                "protocol": 3,
                "focused_workspace_id": "ws_1",
                "focused_tab_id": "t_1_1",
                "focused_pane_id": "p_1_1",
                "workspaces": [{
                    "workspace_id": "ws_1", "number": 1, "label": "repo",
                    "focused": true, "pane_count": 1, "tab_count": 1,
                    "active_tab_id": "t_1_1", "agent_status": "idle"
                }],
                "tabs": [{
                    "tab_id": "t_1_1", "workspace_id": "ws_1", "number": 1,
                    "label": "shell", "focused": true, "pane_count": 1,
                    "agent_status": "idle"
                }],
                "panes": [{
                    "pane_id": "p_1_1", "terminal_id": "term_1",
                    "workspace_id": "ws_1", "tab_id": "t_1_1", "focused": true,
                    "agent_status": "idle", "revision": 1
                }],
                "layouts": [],
                "agents": []
            }))
            .expect("snapshot deserializes");
        mirror.catalog.resync(&snapshot, 1);
        let replica = crate::terminal::replica::PaneReplica::open(
            "REPLICA CONTENT",
            15,
            None,
            80,
            24,
            64 * 1024,
        )
        .expect("replica opens");
        mirror.stream_opened("p_1_1", 3, replica);
        mirror.assert_invariants_for_test();

        let chrome = GlobalChrome::new();
        let mut ids = ComposeIds::new();
        let mut app = AppState::test_new();
        compose_into(&mirror, &chrome, &mut ids, &mut app);
        app.mode = crate::app::Mode::Terminal;

        let area = Rect::new(0, 0, 106, 20);
        let source = MirrorPaneSource::new(&mirror);
        let _requests = crate::ui::compute_view_with_content(&mut app, &source, area);
        let mut terminal = Terminal::new(TestBackend::new(106, 20)).expect("test backend");
        terminal
            .draw(|frame| crate::ui::render_with_content(&app, &source, frame))
            .expect("render");
        let rendered: String = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect();
        assert!(
            rendered.contains("REPLICA CONTENT"),
            "replica screen must render: {rendered:?}"
        );
        assert!(rendered.contains("repo"), "sidebar shows the catalog label");
    }

    #[tokio::test]
    async fn pane_focused_events_move_the_composed_focus() {
        let mut mirror = mirror_with_layout();
        let chrome = GlobalChrome::new();
        let mut ids = ComposeIds::new();
        let mut app = AppState::test_new();
        compose_into(&mirror, &chrome, &mut ids, &mut app);
        let focused = app.workspaces[0].focused_pane_id().expect("focused pane");
        assert_eq!(ids.public_pane_id(focused), Some((0, "p_2_1")));

        // The server emits pane_focused without a layout.updated; the stale
        // layout snapshot must not pin the composed focus to the old pane.
        let focused_event: crate::api::schema::EventEnvelope =
            serde_json::from_value(serde_json::json!({
                "event": "pane_focused",
                "data": { "type": "pane_focused", "pane_id": "p_2_10", "workspace_id": "ws_2" }
            }))
            .expect("event deserializes");
        assert!(mirror.catalog.apply(42, &focused_event));

        compose_into(&mirror, &chrome, &mut ids, &mut app);
        let focused = app.workspaces[0].focused_pane_id().expect("focused pane");
        assert_eq!(
            ids.public_pane_id(focused),
            Some((0, "p_2_10")),
            "composed focus must follow pane_focused events, not the stale layout snapshot"
        );
        app.assert_invariants_for_test();
    }

    #[test]
    fn split_paths_parse_back_into_tree_positions() {
        assert_eq!(parse_split_path("split_0_root"), Some(Vec::new()));
        assert_eq!(parse_split_path("split_3_01"), Some(vec![false, true]));
        assert_eq!(parse_split_path("nonsense"), None);
    }

    // ---------------------------------------------------------------
    // Fleet composition: plain-data tests over multiple fixture mirrors.
    // ---------------------------------------------------------------

    fn connected_remote_mirror(index: usize, name: &str, label: &str) -> super::RemoteMirror {
        let mut mirror = super::RemoteMirror::new(index, name);
        mirror.connection.connect_started();
        mirror
            .connection
            .connected(crate::protocol::framed::NegotiatedSession {
                protocol: crate::protocol::framed::FRAMED_PROTOCOL_VERSION,
                capabilities: vec![crate::protocol::framed::CAPABILITY_PANE_STREAM.to_owned()],
            });
        let snapshot: crate::api::schema::session::SessionSnapshot =
            serde_json::from_value(serde_json::json!({
                "version": "test",
                "protocol": 3,
                "focused_workspace_id": "ws_1",
                "focused_tab_id": "t_1_1",
                "focused_pane_id": "p_1_1",
                "workspaces": [{
                    "workspace_id": "ws_1", "number": 1, "label": label,
                    "focused": true, "pane_count": 1, "tab_count": 1,
                    "active_tab_id": "t_1_1", "agent_status": "idle"
                }],
                "tabs": [{
                    "tab_id": "t_1_1", "workspace_id": "ws_1", "number": 1,
                    "label": "shell", "focused": true, "pane_count": 1,
                    "agent_status": "idle"
                }],
                "panes": [{
                    "pane_id": "p_1_1", "terminal_id": "term_1",
                    "workspace_id": "ws_1", "tab_id": "t_1_1", "focused": true,
                    "agent_status": "idle", "revision": 1
                }],
                "layouts": [],
                "agents": []
            }))
            .expect("snapshot deserializes");
        mirror.catalog.resync(&snapshot, 1);
        mirror
    }

    fn fleet_fixture() -> (
        crate::client_state::RemoteMirrors,
        Vec<crate::client_state::fleet_view::RemoteDescriptor>,
    ) {
        let mut mirrors = crate::client_state::RemoteMirrors::with_local();
        *mirrors.local_mut() = crate::client_state::RemoteMirror::test_with_adversarial_catalog();
        mirrors.insert(connected_remote_mirror(1, "buildbox", "builds"));
        // gpu-01 dropped offline after syncing: its last-known catalog stays
        // composed, its chip dot goes hollow.
        let mut offline = connected_remote_mirror(2, "gpu-01", "training");
        offline.connection_lost("bridge closed");
        mirrors.insert(offline);
        let descriptors = crate::client_state::fleet_view::remote_descriptors(&[
            crate::fleet::config::RemoteEntry {
                name: "buildbox".into(),
                target: "can@buildbox.example".into(),
                session: "default".into(),
                enabled: true,
            },
            crate::fleet::config::RemoteEntry {
                name: "gpu-01".into(),
                target: "can@gpu-01.example".into(),
                session: "default".into(),
                enabled: true,
            },
        ]);
        (mirrors, descriptors)
    }

    #[tokio::test]
    async fn fleet_composition_is_flat_ordered_and_attributed() {
        let (mirrors, descriptors) = fleet_fixture();
        let chrome = GlobalChrome::new();
        let mut ids = ComposeIds::new();
        let mut app = AppState::test_new();

        compose_fleet_into(&mirrors, &descriptors, &chrome, &mut ids, &mut app);

        // Flat, in descriptor order: local's two spaces, then one per remote.
        let ids_in_order: Vec<&str> = app.workspaces.iter().map(|ws| ws.id.as_str()).collect();
        assert_eq!(ids_in_order, vec!["ws_2", "ws_10", "r1:ws_1", "r2:ws_1"]);
        assert_eq!(app.active, Some(0), "local is focused; its ws_2 leads");
        app.assert_invariants_for_test();

        // Every space carries its remote tag (2+ remotes in view).
        let tags: Vec<(&str, usize)> = app
            .workspace_remote_tags
            .iter()
            .map(|tag| (tag.name.as_str(), tag.hue_index))
            .collect();
        assert_eq!(
            tags,
            vec![("local", 0), ("local", 0), ("buildbox", 1), ("gpu-01", 2)]
        );

        // Owners map back to the remotes' own public ids for the control plane.
        assert_eq!(ids.workspace_owner(0), Some((0, "ws_2")));
        assert_eq!(ids.workspace_owner(2), Some((1, "ws_1")));
        assert_eq!(ids.workspace_owner(3), Some((2, "ws_1")));

        // Chips: all three configured remotes, connection state in the dot.
        assert_eq!(app.remote_chips.len(), 3);
        assert!(app.remote_chips.iter().all(|chip| chip.in_view));
        assert_eq!(
            app.remote_chips[1].connection,
            crate::app::state::RemoteChipConnection::Connected
        );
        assert_eq!(
            app.remote_chips[2].connection,
            crate::app::state::RemoteChipConnection::Offline
        );

        // Terminals are remote-scoped so equal server ids never collide.
        assert!(app
            .terminals
            .keys()
            .any(|id| id.to_string().contains("r1:term_1")));
        assert!(app
            .terminals
            .keys()
            .any(|id| id.to_string().contains("r2:term_1")));
    }

    #[tokio::test]
    async fn chip_filtering_recomposes_without_touching_mirrors() {
        let (mirrors, descriptors) = fleet_fixture();
        let mut chrome = GlobalChrome::new();
        let mut ids = ComposeIds::new();
        let mut app = AppState::test_new();

        // Filter local out: focus falls to the first remote in view.
        chrome
            .selection
            .toggle(0, &descriptors)
            .expect("local out of view");
        compose_fleet_into(&mirrors, &descriptors, &chrome, &mut ids, &mut app);
        let ids_in_order: Vec<&str> = app.workspaces.iter().map(|ws| ws.id.as_str()).collect();
        assert_eq!(ids_in_order, vec!["r1:ws_1", "r2:ws_1"]);
        assert_eq!(app.active, Some(0), "buildbox's focused space leads");
        assert_eq!(app.workspace_remote_tags.len(), 2, "still a multi view");
        assert_eq!(app.remote_chips.len(), 3, "chips keep every remote");
        assert!(!app.remote_chips[0].in_view);
        app.assert_invariants_for_test();

        // Solo gpu-01: single-remote view collapses the attribution.
        chrome.selection.solo(2, &descriptors);
        compose_fleet_into(&mirrors, &descriptors, &chrome, &mut ids, &mut app);
        let ids_in_order: Vec<&str> = app.workspaces.iter().map(|ws| ws.id.as_str()).collect();
        assert_eq!(ids_in_order, vec!["r2:ws_1"]);
        assert!(
            app.workspace_remote_tags.is_empty(),
            "single-remote views carry no attribution"
        );
        assert_eq!(app.remote_chips.len(), 3, "the strip stays for un-soloing");
        app.assert_invariants_for_test();
    }

    #[tokio::test]
    async fn single_remote_fleet_composition_renders_exactly_todays_sidebar() {
        // Only the local runtime is configured: the fleet composition must
        // be byte-identical to the single-mirror composition — no strip, no
        // gutter, no tokens.
        let mut mirrors = crate::client_state::RemoteMirrors::with_local();
        *mirrors.local_mut() = crate::client_state::RemoteMirror::test_with_adversarial_catalog();
        let descriptors = crate::client_state::fleet_view::remote_descriptors(&[]);
        let chrome = GlobalChrome::new();

        let mut fleet_ids = ComposeIds::new();
        let mut fleet_app = AppState::test_new();
        compose_fleet_into(
            &mirrors,
            &descriptors,
            &chrome,
            &mut fleet_ids,
            &mut fleet_app,
        );
        assert!(fleet_app.remote_chips.is_empty());
        assert!(fleet_app.workspace_remote_tags.is_empty());

        let mut local_ids = ComposeIds::new();
        let mut local_app = AppState::test_new();
        compose_into(mirrors.local(), &chrome, &mut local_ids, &mut local_app);

        let render = |app: &mut AppState| {
            crate::ui::compute_view(app, Rect::new(0, 0, 106, 24));
            let mut terminal = Terminal::new(TestBackend::new(106, 24)).expect("test backend");
            terminal
                .draw(|frame| crate::ui::render(app, frame))
                .expect("render");
            terminal.backend().buffer().clone()
        };
        assert_eq!(
            render(&mut fleet_app),
            render(&mut local_app),
            "single-remote fleet view must be exactly today's render"
        );
    }

    #[tokio::test]
    async fn creation_targets_the_remote_owning_the_focused_context() {
        let (mirrors, descriptors) = fleet_fixture();
        let mut chrome = GlobalChrome::new();
        let mut ids = ComposeIds::new();
        let mut app = AppState::test_new();
        app.keybinds = crate::config::Config::default().keybinds();

        // Focus follows buildbox: creation goes there.
        chrome.selection.focused_remote = 1;
        compose_fleet_into(&mirrors, &descriptors, &chrome, &mut ids, &mut app);
        assert_eq!(app.workspaces[app.active.expect("active")].id, "r1:ws_1");
        assert_eq!(
            crate::client_state::fleet_view::creation_target_remote(&app, &ids, 0),
            1
        );

        // The full intent path: new tab lands on buildbox with its own
        // public workspace id, never the composed scoped id.
        let (remote, method) = super::super::intent::prefix_intent_method(
            crate::input::TerminalKey::new(
                crossterm::event::KeyCode::Char('c'),
                crossterm::event::KeyModifiers::empty(),
            ),
            &mirrors,
            &ids,
            &app,
        )
        .expect("new tab intent");
        assert_eq!(remote, 1);
        let crate::api::schema::Method::TabCreate(params) = method else {
            panic!("expected tab.create, got {method:?}");
        };
        assert_eq!(params.workspace_id.as_deref(), Some("ws_1"));

        // Creating a whole new space also follows the focused context.
        let (remote, method) = super::super::intent::mouse_intent_method(
            Some(crate::app::MouseAction::NewWorkspace),
            &mirrors,
            &ids,
            &app,
        )
        .expect("new space intent");
        assert_eq!(remote, 1);
        assert!(matches!(
            method,
            crate::api::schema::Method::WorkspaceCreate(_)
        ));
    }
}

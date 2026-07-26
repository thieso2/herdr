//! Client-side input intent interpretation.
//!
//! Prefix keys and chrome mouse actions resolve locally — against the
//! user's configured keybinds and the computed view geometry — into JSON
//! API methods dispatched over the framed control plane (`api.request`).
//! The server applies the mutation and the resulting catalog events update
//! the mirror; nothing here mutates session state directly.

#![cfg(unix)]

use crossterm::event::MouseEvent;

use crate::api::schema::{
    LayoutSetSplitRatioParams, Method, PaneDirection, PaneFocusDirectionParams, PaneSplitParams,
    PaneTarget, PaneZoomParams, SplitDirection, TabCreateParams, TabMoveParams, TabTarget,
    WorkspaceCreateParams, WorkspaceMoveParams, WorkspaceTarget,
};
use crate::app::AppState;
use crate::input::TerminalKey;

use super::chrome::GlobalChrome;
use super::compose::ComposeIds;
use super::fleet_view::RemoteDescriptor;
use super::run::Links;
use super::RemoteMirrors;

/// Resolves a prefix-mode key against the configured keybinds into a
/// control-plane method (or a local chrome mutation) and dispatches it to
/// the owning remote's session.
pub(super) fn dispatch_prefix_intent(
    key: TerminalKey,
    links: &mut Links,
    mirrors: &mut RemoteMirrors,
    ids: &mut ComposeIds,
    descriptors: &[RemoteDescriptor],
    app: &mut AppState,
    chrome: &mut GlobalChrome,
) {
    // Local chrome first: the sidebar belongs to this client.
    if app.keybinds.toggle_sidebar.matches_prefix_key(key) {
        app.sidebar_collapsed = !app.sidebar_collapsed;
        return;
    }

    // With nothing focused (for example a solo'd remote with no spaces
    // yet), creation still goes to the *effective* focused remote, never
    // silently to local — and never to a remote filtered out of view.
    let fallback_remote = chrome.selection.effective_focused_remote(descriptors);
    let Some((remote, method)) = prefix_intent_method(key, mirrors, ids, app, fallback_remote)
    else {
        return;
    };
    // Acting on a remote's content moves the client focus to that remote
    // (it wins the window title and the composed focus).
    chrome.selection.focused_remote = remote;
    super::run::send_api_request(links, remote, method);
}

/// The owning remote and JSON API method a prefix-mode key maps to, if any.
/// Pure: reads the keybinds, the composed state, and the owning remote's
/// catalog. Spaces, tabs, and panes belong to exactly one remote, so every
/// method targets the remote that owns the focused space or pane; creation
/// goes to the same owner (the focused context).
pub(super) fn prefix_intent_method(
    key: TerminalKey,
    mirrors: &RemoteMirrors,
    ids: &ComposeIds,
    app: &AppState,
    fallback_remote: usize,
) -> Option<(usize, Method)> {
    let target_remote = super::fleet_view::creation_target_remote(app, ids, fallback_remote);
    let catalog = &mirrors.get(target_remote)?.catalog;
    let keybinds = &app.keybinds;
    // Control-plane calls carry the remote's own public ids, never the
    // composed (remote-scoped) ids.
    let focused_workspace = app
        .active
        .and_then(|ws_idx| ids.workspace_owner(ws_idx))
        .map(|(_, public)| public.to_owned());
    let focused_pane = app
        .active
        .and_then(|idx| app.workspaces.get(idx))
        .and_then(|ws| ws.focused_pane_id())
        .and_then(|pane_id| ids.public_pane_id(pane_id))
        .filter(|(remote, _)| *remote == target_remote)
        .map(|(_, public)| public.to_owned());

    if keybinds.new_workspace.matches_prefix_key(key) {
        return Some((
            target_remote,
            Method::WorkspaceCreate(WorkspaceCreateParams {
                cwd: None,
                focus: true,
                label: None,
                env: Default::default(),
            }),
        ));
    }
    if keybinds.close_workspace.matches_prefix_key(key) {
        return Some((
            target_remote,
            Method::WorkspaceClose(WorkspaceTarget {
                workspace_id: focused_workspace?,
            }),
        ));
    }
    if keybinds.previous_workspace.matches_prefix_key(key)
        || keybinds.next_workspace.matches_prefix_key(key)
    {
        let forward = keybinds.next_workspace.matches_prefix_key(key);
        let ordered: Vec<&str> = catalog
            .workspaces
            .iter()
            .map(|ws| ws.workspace_id.as_str())
            .collect();
        let current = focused_workspace?;
        let position = ordered.iter().position(|id| **id == *current)?;
        let target = if forward {
            ordered.get(position + 1).or_else(|| ordered.first())?
        } else if position == 0 {
            ordered.last()?
        } else {
            ordered.get(position - 1)?
        };
        return Some((
            target_remote,
            Method::WorkspaceFocus(WorkspaceTarget {
                workspace_id: (*target).to_owned(),
            }),
        ));
    }
    if keybinds.new_tab.matches_prefix_key(key) {
        return Some((
            target_remote,
            Method::TabCreate(TabCreateParams {
                workspace_id: focused_workspace,
                cwd: None,
                focus: true,
                label: None,
                env: Default::default(),
            }),
        ));
    }
    if keybinds.previous_tab.matches_prefix_key(key) || keybinds.next_tab.matches_prefix_key(key) {
        let forward = keybinds.next_tab.matches_prefix_key(key);
        let workspace_id = focused_workspace?;
        let tabs: Vec<&str> = catalog
            .tabs
            .iter()
            .filter(|tab| tab.workspace_id == workspace_id)
            .map(|tab| tab.tab_id.as_str())
            .collect();
        let workspace = catalog.workspace(&workspace_id)?;
        let position = tabs.iter().position(|id| **id == workspace.active_tab_id)?;
        let target = if forward {
            tabs.get(position + 1).or_else(|| tabs.first())?
        } else if position == 0 {
            tabs.last()?
        } else {
            tabs.get(position - 1)?
        };
        return Some((
            target_remote,
            Method::TabFocus(TabTarget {
                tab_id: (*target).to_owned(),
            }),
        ));
    }
    if keybinds.close_tab.matches_prefix_key(key) {
        let workspace_id = focused_workspace?;
        let workspace = catalog.workspace(&workspace_id)?;
        return Some((
            target_remote,
            Method::TabClose(TabTarget {
                tab_id: workspace.active_tab_id.clone(),
            }),
        ));
    }
    if keybinds.split_horizontal.matches_prefix_key(key)
        || keybinds.split_vertical.matches_prefix_key(key)
    {
        // herdr naming: split_vertical is side by side (Right), and
        // split_horizontal is stacked (Down).
        let direction = if keybinds.split_vertical.matches_prefix_key(key) {
            SplitDirection::Right
        } else {
            SplitDirection::Down
        };
        return Some((
            target_remote,
            Method::PaneSplit(PaneSplitParams {
                workspace_id: None,
                target_pane_id: focused_pane,
                direction,
                ratio: None,
                cwd: None,
                focus: true,
                env: Default::default(),
            }),
        ));
    }
    if keybinds.close_pane.matches_prefix_key(key) {
        return Some((
            target_remote,
            Method::PaneClose(PaneTarget {
                pane_id: focused_pane?,
            }),
        ));
    }
    if keybinds.zoom.matches_prefix_key(key) {
        return Some((
            target_remote,
            Method::PaneZoom(PaneZoomParams {
                pane_id: focused_pane,
                mode: Default::default(),
            }),
        ));
    }
    for (bind, direction) in [
        (&keybinds.focus_pane_left, PaneDirection::Left),
        (&keybinds.focus_pane_down, PaneDirection::Down),
        (&keybinds.focus_pane_up, PaneDirection::Up),
        (&keybinds.focus_pane_right, PaneDirection::Right),
    ] {
        if bind.matches_prefix_key(key) {
            return Some((
                target_remote,
                Method::PaneFocusDirection(PaneFocusDirectionParams {
                    pane_id: focused_pane,
                    direction,
                }),
            ));
        }
    }
    None
}

/// Resolves a chrome mouse action (hit-tested against the computed view)
/// into a control-plane method and dispatches it.
pub(super) fn dispatch_mouse_intent(
    mouse: MouseEvent,
    links: &mut Links,
    mirrors: &mut RemoteMirrors,
    ids: &mut ComposeIds,
    descriptors: &[RemoteDescriptor],
    app: &mut AppState,
    chrome: &mut GlobalChrome,
) {
    // Hit testing runs on the composed state with no live runtimes: chrome
    // actions resolve exactly, and pane-content interactions (selection
    // drags, copy-on-select, scrollbar clicks and drags) run against the
    // replicas through the pane-content seam. Byte forwarding to
    // mouse-reporting panes already happened in the caller, so the empty
    // registry only mutes the residual forwarding paths.
    let mode_before = app.mode;
    let empty_runtimes = crate::terminal::TerminalRuntimeRegistry::new();
    let action = {
        let in_view: Vec<usize> = chrome
            .selection
            .in_view(descriptors)
            .iter()
            .map(|descriptor| descriptor.index)
            .collect();
        let source = super::compose::MirrorPaneSource::for_view(mirrors, &in_view);
        app.handle_mouse_with_content(&empty_runtimes, &source, mouse)
    };
    // A mouse-driven copy (copy_on_select drag release, double-click word
    // copies) lands in request_clipboard_write; the pure client is the
    // host terminal, so it goes straight out as OSC52.
    if let Some(content) = app.request_clipboard_write.take() {
        crate::selection::write_osc52_bytes(&content);
    }
    // handle_mouse can open modal surfaces. Context menus, confirm-close,
    // and rename dialogs are interpreted client-side below; anything else
    // reverts to the pre-click mode instead of trapping the user in a dead
    // modal.
    //
    // NOT CLOSED IN #21 — settings dialog and navigator under the flag.
    // Both are App-coupled, not AppState-coupled: every settings mutation
    // (SettingsAction::Save*) is an App method that does a config-file
    // load-modify-write plus an in-process runtime re-apply (theme reload,
    // sound engine, integration installs), and the navigator's open/refresh
    // path (App::open_navigator and Mode::Navigator key dispatch in
    // app/mod.rs) rebuilds its rows from live workspace runtimes. Closing
    // them needs either extracting those handlers into AppState-level
    // state machines like pure_client_modal_key, or a config surface on
    // the control plane; neither fits this change. What remains:
    // interpret MouseAction::Settings plus Mode::Settings and Mode::Navigator
    // client-side. The same applies to the worktree dialog modes, which additionally run git operations.
    if !matches!(
        app.mode,
        crate::app::Mode::Terminal
            | crate::app::Mode::Navigate
            | crate::app::Mode::Prefix
            | crate::app::Mode::Copy
            | crate::app::Mode::ContextMenu
            | crate::app::Mode::ConfirmClose
            | crate::app::Mode::GlobalMenu
            | crate::app::Mode::KeybindHelp
            | crate::app::Mode::RenameWorkspace
            | crate::app::Mode::RenameTab
            | crate::app::Mode::RenamePane
    ) {
        app.mode = mode_before;
        app.context_menu = None;
    }
    let fallback_remote = chrome.selection.effective_focused_remote(descriptors);
    let Some(action) = action else {
        return;
    };
    if matches!(
        action,
        crate::app::MouseAction::RenameModal(_)
            | crate::app::MouseAction::ConfirmCloseAccept
            | crate::app::MouseAction::ContextMenu { .. }
    ) {
        let methods = interpret_modal_mouse(action, mirrors, ids, app);
        // Modal methods target the workspace the modal acted on
        // (`app.selected` after interpretation); they go to its owner.
        let remote = ids
            .workspace_owner(app.selected)
            .map(|(remote, _)| remote)
            .unwrap_or(fallback_remote);
        for method in methods {
            super::run::send_api_request(links, remote, method);
        }
        return;
    }
    let Some((remote, method)) =
        mouse_intent_method(Some(action), mirrors, ids, app, fallback_remote)
    else {
        return;
    };
    // Clicking into a remote's content moves the client focus there.
    chrome.selection.focused_remote = remote;
    super::run::send_api_request(links, remote, method);
}

/// Interprets a modal mouse action (rename dialog buttons, confirm-close,
/// context menu items) into control-plane methods through the shared
/// pure-client modal state machines.
fn interpret_modal_mouse(
    action: crate::app::MouseAction,
    mirrors: &RemoteMirrors,
    ids: &ComposeIds,
    app: &mut AppState,
) -> Vec<Method> {
    let pane_public = |pane_id: crate::layout::PaneId| {
        ids.public_pane_id(pane_id)
            .map(|(_, public)| public.to_owned())
    };
    let tab_public = |ws_idx: usize, tab_idx: usize| {
        let (remote, workspace_public) = ids.workspace_owner(ws_idx)?;
        composed_tab_public_id(&mirrors.get(remote)?.catalog, workspace_public, tab_idx)
    };
    let modal_ids = crate::app::PureModalIds {
        pane_public: &pane_public,
        tab_public: &tab_public,
    };
    let mut methods = crate::app::pure_client_modal_mouse(app, action, &modal_ids);
    unscope_workspace_methods(&mut methods, app, ids);
    methods
}

/// Interprets a key in a pure-client modal mode (rename dialogs,
/// confirm-close, context menu) into control-plane methods and sends them
/// to the remote owning the modal's target workspace.
pub(super) fn dispatch_modal_key(
    key: crate::input::TerminalKey,
    links: &mut Links,
    mirrors: &mut RemoteMirrors,
    ids: &mut ComposeIds,
    app: &mut AppState,
    fallback_remote: usize,
) {
    let methods = {
        let pane_public = |pane_id: crate::layout::PaneId| {
            ids.public_pane_id(pane_id)
                .map(|(_, public)| public.to_owned())
        };
        let tab_public = |ws_idx: usize, tab_idx: usize| {
            let (remote, workspace_public) = ids.workspace_owner(ws_idx)?;
            composed_tab_public_id(&mirrors.get(remote)?.catalog, workspace_public, tab_idx)
        };
        let modal_ids = crate::app::PureModalIds {
            pane_public: &pane_public,
            tab_public: &tab_public,
        };
        let mut methods = crate::app::pure_client_modal_key(app, key.as_key_event(), &modal_ids);
        unscope_workspace_methods(&mut methods, app, ids);
        methods
    };
    let remote = ids
        .workspace_owner(app.selected)
        .map(|(remote, _)| remote)
        .unwrap_or(fallback_remote);
    for method in methods {
        super::run::send_api_request(links, remote, method);
    }
}

/// The modal state machines emit workspace ids straight from the composed
/// state, which scopes non-local ids (`rN:ws_x`); rewrite them to the
/// owning remote's public ids before they hit the wire.
fn unscope_workspace_methods(methods: &mut [Method], app: &AppState, ids: &ComposeIds) {
    for method in methods.iter_mut() {
        let workspace_id = match method {
            Method::WorkspaceRename(params) => &mut params.workspace_id,
            Method::WorkspaceClose(target) | Method::WorkspaceFocus(target) => {
                &mut target.workspace_id
            }
            _ => continue,
        };
        if let Some(public) = app
            .workspaces
            .iter()
            .position(|ws| ws.id == *workspace_id)
            .and_then(|ws_idx| ids.workspace_owner(ws_idx))
            .map(|(_, public)| public.to_owned())
        {
            *workspace_id = public;
        }
    }
}

/// Public tab id at a composed tab-bar position. The composed tab bar skips
/// tabs with no panes (compose_into), so positions resolve against the same
/// filtered view of the owning remote's catalog.
fn composed_tab_public_id(
    catalog: &super::SessionCatalog,
    workspace_id: &str,
    tab_idx: usize,
) -> Option<String> {
    catalog
        .tabs
        .iter()
        .filter(|tab| tab.workspace_id == workspace_id)
        .filter(|tab| catalog.panes.iter().any(|pane| pane.tab_id == tab.tab_id))
        .nth(tab_idx)
        .map(|tab| tab.tab_id.clone())
}

/// The owning remote and JSON API method a resolved chrome mouse action
/// maps to, if any. Focus actions target the clicked entity's owner;
/// creation targets the remote owning the focused context.
pub(super) fn mouse_intent_method(
    action: Option<crate::app::MouseAction>,
    mirrors: &RemoteMirrors,
    ids: &ComposeIds,
    app: &AppState,
    fallback_remote: usize,
) -> Option<(usize, Method)> {
    match action? {
        crate::app::MouseAction::NewWorkspace => {
            let remote = super::fleet_view::creation_target_remote(app, ids, fallback_remote);
            Some((
                remote,
                Method::WorkspaceCreate(WorkspaceCreateParams {
                    cwd: None,
                    focus: true,
                    label: None,
                    env: Default::default(),
                }),
            ))
        }
        crate::app::MouseAction::FocusWorkspace { ws_idx } => {
            let (remote, public) = ids.workspace_owner(ws_idx)?;
            Some((
                remote,
                Method::WorkspaceFocus(WorkspaceTarget {
                    workspace_id: public.to_owned(),
                }),
            ))
        }
        crate::app::MouseAction::FocusTab { tab_idx } => {
            let ws_idx = app.active?;
            let (remote, workspace_public) = ids.workspace_owner(ws_idx)?;
            let tab_id =
                composed_tab_public_id(&mirrors.get(remote)?.catalog, workspace_public, tab_idx)?;
            Some((remote, Method::TabFocus(TabTarget { tab_id })))
        }
        crate::app::MouseAction::FocusPane { pane_id, .. } => {
            let (remote, public) = ids.public_pane_id(pane_id)?;
            Some((
                remote,
                Method::PaneFocus(PaneTarget {
                    pane_id: public.to_owned(),
                }),
            ))
        }
        crate::app::MouseAction::MoveWorkspace {
            source_ws_idx,
            insert_idx,
        } => {
            let (remote, public) = ids.workspace_owner(source_ws_idx)?;
            // The drag speaks composed positions; the server speaks the
            // owning remote's index space, so count only that remote's
            // workspaces before the composed insert position.
            let insert_index = (0..insert_idx.min(app.workspaces.len()))
                .filter(|ws_idx| {
                    ids.workspace_owner(*ws_idx)
                        .is_some_and(|(owner, _)| owner == remote)
                })
                .count();
            Some((
                remote,
                Method::WorkspaceMove(WorkspaceMoveParams {
                    workspace_id: public.to_owned(),
                    insert_index,
                }),
            ))
        }
        crate::app::MouseAction::MoveTab {
            ws_idx,
            source_tab_idx,
            insert_idx,
        } => {
            let (remote, workspace_public) = ids.workspace_owner(ws_idx)?;
            let tab_id = composed_tab_public_id(
                &mirrors.get(remote)?.catalog,
                workspace_public,
                source_tab_idx,
            )?;
            Some((
                remote,
                Method::TabMove(TabMoveParams {
                    tab_id,
                    insert_index: insert_idx,
                }),
            ))
        }
        crate::app::MouseAction::SetSplitRatio { path, ratio } => {
            // The server resolves the focused tab on the remote owning the
            // focused workspace; the composed layout follows on the next
            // layout.updated event.
            let remote = app
                .active
                .and_then(|ws_idx| ids.workspace_owner(ws_idx))
                .map(|(remote, _)| remote)
                .unwrap_or(fallback_remote);
            Some((
                remote,
                Method::LayoutSetSplitRatio(LayoutSetSplitRatioParams {
                    tab_id: None,
                    pane_id: None,
                    path,
                    ratio,
                }),
            ))
        }
        // Modal actions are handled in interpret_modal_mouse. Settings stays
        // unsupported under the flag (see the NOT CLOSED note in
        // dispatch_mouse_intent for why and what remains); toast targets
        // have no composed equivalent yet.
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::MouseAction;
    use crossterm::event::{KeyCode, KeyModifiers};

    fn composed() -> (RemoteMirrors, ComposeIds, AppState) {
        let mut mirrors = RemoteMirrors::with_local();
        *mirrors.local_mut() = super::super::RemoteMirror::test_with_adversarial_catalog();
        let mut ids = ComposeIds::new();
        let mut app = AppState::test_new();
        super::super::compose::compose_into(
            mirrors.local(),
            &super::super::chrome::GlobalChrome::new(),
            &mut ids,
            &mut app,
        );
        app.keybinds = crate::config::Config::default().keybinds();
        (mirrors, ids, app)
    }

    fn key(code: KeyCode) -> TerminalKey {
        TerminalKey::new(code, KeyModifiers::empty())
    }

    /// The empty view is the state the "press prefix+shift+n" hint is
    /// shown in, and it is reachable per remote (a solo'd remote with no
    /// spaces). Creation must resolve there, on the in-view remote, for
    /// every shape a real terminal reports shift+n as: legacy uppercase,
    /// uppercase with shift, and the kitty base-key-plus-shifted-codepoint
    /// pair.
    #[test]
    fn new_workspace_intent_targets_the_in_view_remote_with_an_empty_catalog() {
        let mut mirrors = RemoteMirrors::with_local();
        mirrors.insert(super::super::RemoteMirror::new(1, "buildbox"));
        let ids = ComposeIds::new();
        let mut app = AppState::test_new();
        app.keybinds = crate::config::Config::default().keybinds();
        assert!(app.active.is_none(), "nothing focused in an empty view");

        for shape in [
            TerminalKey::new(KeyCode::Char('N'), KeyModifiers::empty()),
            TerminalKey::new(KeyCode::Char('N'), KeyModifiers::SHIFT),
            TerminalKey::new(KeyCode::Char('n'), KeyModifiers::SHIFT)
                .with_shifted_codepoint('N' as u32),
        ] {
            let (remote, method) =
                prefix_intent_method(shape, &mirrors, &ids, &app, 1).expect("new workspace intent");
            assert_eq!(remote, 1, "creation follows the effective focused remote");
            assert!(
                matches!(method, Method::WorkspaceCreate(_)),
                "expected workspace.create, got {method:?}"
            );
        }
    }

    #[test]
    fn prefix_keys_map_to_control_plane_methods() {
        let (mirrors, ids, app) = composed();

        // Default binds: c = new tab, % / " = splits, x = close pane,
        // arrows = pane focus (all through the configured keybinds).
        let (remote, method) =
            prefix_intent_method(key(KeyCode::Char('c')), &mirrors, &ids, &app, 0)
                .expect("new tab intent");
        assert_eq!(remote, 0, "adversarial fixture is local-only");
        let Method::TabCreate(params) = method else {
            panic!("expected tab.create, got {method:?}");
        };
        assert_eq!(params.workspace_id.as_deref(), Some("ws_2"));
        assert!(params.focus);

        let (remote, method) =
            prefix_intent_method(key(KeyCode::Char('v')), &mirrors, &ids, &app, 0)
                .expect("split intent");
        assert_eq!(remote, 0, "adversarial fixture is local-only");
        let Method::PaneSplit(params) = method else {
            panic!("expected pane.split, got {method:?}");
        };
        assert_eq!(params.direction, SplitDirection::Right);
        assert_eq!(params.target_pane_id.as_deref(), Some("p_2_1"));

        let (remote, method) =
            prefix_intent_method(key(KeyCode::Char('x')), &mirrors, &ids, &app, 0)
                .expect("close pane intent");
        assert_eq!(remote, 0, "adversarial fixture is local-only");
        let Method::PaneClose(target) = method else {
            panic!("expected pane.close, got {method:?}");
        };
        assert_eq!(target.pane_id, "p_2_1");

        let (remote, method) =
            prefix_intent_method(key(KeyCode::Char('l')), &mirrors, &ids, &app, 0)
                .expect("focus direction intent");
        assert_eq!(remote, 0, "adversarial fixture is local-only");
        let Method::PaneFocusDirection(params) = method else {
            panic!("expected pane.focus_direction, got {method:?}");
        };
        assert_eq!(params.direction, PaneDirection::Right);
    }

    #[test]
    fn workspace_cycling_wraps_through_the_catalog_order() {
        let (mirrors, ids, mut app) = composed();
        // next_workspace is unbound by default; bind it like a user would.
        app.keybinds.next_workspace = crate::config::ActionKeybinds::prefix("n");
        let (remote, method) =
            prefix_intent_method(key(KeyCode::Char('n')), &mirrors, &ids, &app, 0)
                .expect("next workspace intent");
        assert_eq!(remote, 0, "adversarial fixture is local-only");
        let Method::WorkspaceFocus(target) = method else {
            panic!("expected workspace.focus, got {method:?}");
        };
        // ws_2 is focused; ws_10 is the next in catalog order.
        assert_eq!(target.workspace_id, "ws_10");
    }

    #[test]
    fn unbound_prefix_keys_map_to_nothing() {
        let (mirrors, ids, app) = composed();
        assert!(prefix_intent_method(key(KeyCode::Char('~')), &mirrors, &ids, &app, 0).is_none());
    }

    #[test]
    fn tab_focus_index_skips_pane_less_catalog_tabs() {
        let (mut mirrors, ids, app) = composed();
        // A tab_created event landed before its pane_created: the catalog
        // briefly holds a pane-less tab the composed tab bar does not show.
        let empty_tab: crate::api::schema::tabs::TabInfo =
            serde_json::from_value(serde_json::json!({
                "tab_id": "t_2_0",
                "workspace_id": "ws_2",
                "number": 0,
                "label": "empty",
                "focused": false,
                "pane_count": 0,
                "agent_status": "idle"
            }))
            .expect("tab info deserializes");
        mirrors.local_mut().catalog.tabs.insert(0, empty_tab);

        // The composed tab bar shows only t_2_1, at index 0; clicking it must
        // not resolve to the invisible empty tab.
        let (remote, method) = mouse_intent_method(
            Some(MouseAction::FocusTab { tab_idx: 0 }),
            &mirrors,
            &ids,
            &app,
            0,
        )
        .expect("tab focus intent");
        assert_eq!(remote, 0, "adversarial fixture is local-only");
        let Method::TabFocus(target) = method else {
            panic!("expected tab.focus, got {method:?}");
        };
        assert_eq!(target.tab_id, "t_2_1");
    }

    #[tokio::test]
    async fn launcher_click_opens_a_global_menu_that_survives_dispatch() {
        let (mut mirrors, mut ids, mut app) = composed();
        app.mode = crate::app::Mode::Terminal;
        crate::ui::compute_view(&mut app, ratatui::layout::Rect::new(0, 0, 106, 20));
        let launcher = app.global_launcher_rect();

        let mut links = super::super::run::Links::new();
        let mut chrome = super::super::chrome::GlobalChrome::new();
        let mouse = MouseEvent {
            kind: crossterm::event::MouseEventKind::Down(crossterm::event::MouseButton::Left),
            column: launcher.x,
            row: launcher.y,
            modifiers: KeyModifiers::empty(),
        };
        dispatch_mouse_intent(
            mouse,
            &mut links,
            &mut mirrors,
            &mut ids,
            &[RemoteDescriptor::local()],
            &mut app,
            &mut chrome,
        );

        // The click opens the menu and the post-dispatch mode guard must
        // let it live: a reverted mode means a silently dead menu button.
        assert_eq!(app.mode, crate::app::Mode::GlobalMenu);
    }

    #[tokio::test]
    async fn pure_client_global_menu_offers_keybinds_add_remote_and_detach() {
        let (mut mirrors, mut ids, mut app) = composed();
        app.mode = crate::app::Mode::Terminal;
        app.pure_client = true;
        app.fleet_config_backed = true;
        app.detach_exits = true;
        crate::ui::compute_view(&mut app, ratatui::layout::Rect::new(0, 0, 106, 20));

        let mut links = super::super::run::Links::new();
        let mut chrome = super::super::chrome::GlobalChrome::new();
        let click = |column: u16, row: u16| MouseEvent {
            kind: crossterm::event::MouseEventKind::Down(crossterm::event::MouseButton::Left),
            column,
            row,
            modifiers: KeyModifiers::empty(),
        };

        // Settings and reload config have no client-side effect; they are
        // omitted so no menu row is silently dead. Add remote is the
        // inverse: it only exists here, and is the only way to add the
        // first remote when no chip strip is composed.
        assert_eq!(
            app.global_menu_labels(),
            vec!["keybinds", "add remote", "detach"]
        );

        let launcher = app.global_launcher_rect();
        let mut dispatch = |mouse, app: &mut AppState, chrome: &mut _| {
            dispatch_mouse_intent(
                mouse,
                &mut links,
                &mut mirrors,
                &mut ids,
                &[RemoteDescriptor::local()],
                app,
                chrome,
            );
        };
        dispatch(click(launcher.x, launcher.y), &mut app, &mut chrome);
        assert_eq!(app.mode, crate::app::Mode::GlobalMenu);

        // Row 1 is keybinds: the help overlay opens and survives dispatch.
        let menu = app.global_menu_rect();
        dispatch(click(menu.x + 2, menu.y + 1), &mut app, &mut chrome);
        assert_eq!(app.mode, crate::app::Mode::KeybindHelp);

        // Reopen and click outside: back to the base mode, nothing quits.
        dispatch(click(launcher.x, launcher.y), &mut app, &mut chrome);
        assert_eq!(app.mode, crate::app::Mode::GlobalMenu);
        dispatch(click(0, 0), &mut app, &mut chrome);
        assert_eq!(app.mode, crate::app::Mode::Terminal);
        assert!(!app.should_quit);

        // Row 2 is add remote: it asks the run loop for the dialog and
        // closes the menu; the flag is the client-chrome seam.
        dispatch(click(launcher.x, launcher.y), &mut app, &mut chrome);
        let menu = app.global_menu_rect();
        dispatch(click(menu.x + 2, menu.y + 2), &mut app, &mut chrome);
        assert!(app.request_add_remote, "the run loop opens the dialog");
        assert_eq!(app.mode, crate::app::Mode::Terminal);
        assert!(!app.should_quit);
        app.request_add_remote = false;

        // Row 3 is detach: the fleet client exits, remotes keep running.
        dispatch(click(launcher.x, launcher.y), &mut app, &mut chrome);
        let menu = app.global_menu_rect();
        dispatch(click(menu.x + 2, menu.y + 3), &mut app, &mut chrome);
        assert!(app.should_quit, "detach exits the pure client");
    }

    #[tokio::test]
    async fn right_click_opens_a_live_context_menu_that_esc_closes() {
        let (mut mirrors, mut ids, mut app) = composed();
        app.mode = crate::app::Mode::Terminal;
        crate::ui::compute_view(&mut app, ratatui::layout::Rect::new(0, 0, 106, 20));
        let inner = app.view.pane_infos.first().expect("pane info").inner_rect;

        let mut links = super::super::run::Links::new();
        let mut chrome = super::super::chrome::GlobalChrome::new();
        let mouse = MouseEvent {
            kind: crossterm::event::MouseEventKind::Down(crossterm::event::MouseButton::Right),
            column: inner.x + 1,
            row: inner.y + 1,
            modifiers: KeyModifiers::empty(),
        };
        dispatch_mouse_intent(
            mouse,
            &mut links,
            &mut mirrors,
            &mut ids,
            &[RemoteDescriptor::local()],
            &mut app,
            &mut chrome,
        );

        // Context menus are interpreted client-side now: the menu opens and
        // stays; Esc closes it back to a base mode.
        assert_eq!(app.mode, crate::app::Mode::ContextMenu);
        assert!(app.context_menu.is_some(), "context menu opened");
        dispatch_modal_key(
            crate::input::TerminalKey::new(KeyCode::Esc, KeyModifiers::empty()),
            &mut links,
            &mut mirrors,
            &mut ids,
            &mut app,
            0,
        );
        assert!(app.context_menu.is_none(), "esc closes the menu");
        assert_eq!(app.mode, crate::app::Mode::Terminal);
    }

    #[test]
    fn drag_actions_map_to_move_and_split_ratio_methods() {
        let (mirrors, ids, app) = composed();

        let (remote, method) = mouse_intent_method(
            Some(MouseAction::MoveWorkspace {
                source_ws_idx: 1,
                insert_idx: 0,
            }),
            &mirrors,
            &ids,
            &app,
            0,
        )
        .expect("workspace move intent");
        assert_eq!(remote, 0, "adversarial fixture is local-only");
        let Method::WorkspaceMove(params) = method else {
            panic!("expected workspace.move, got {method:?}");
        };
        assert_eq!(params.workspace_id, "ws_10");
        assert_eq!(params.insert_index, 0);

        let (remote, method) = mouse_intent_method(
            Some(MouseAction::MoveTab {
                ws_idx: 0,
                source_tab_idx: 0,
                insert_idx: 1,
            }),
            &mirrors,
            &ids,
            &app,
            0,
        )
        .expect("tab move intent");
        assert_eq!(remote, 0, "adversarial fixture is local-only");
        let Method::TabMove(params) = method else {
            panic!("expected tab.move, got {method:?}");
        };
        assert_eq!(params.tab_id, "t_2_1");

        let (remote, method) = mouse_intent_method(
            Some(MouseAction::SetSplitRatio {
                path: vec![false],
                ratio: 0.25,
            }),
            &mirrors,
            &ids,
            &app,
            0,
        )
        .expect("split ratio intent");
        assert_eq!(remote, 0, "adversarial fixture is local-only");
        let Method::LayoutSetSplitRatio(params) = method else {
            panic!("expected layout.set_split_ratio, got {method:?}");
        };
        assert_eq!(params.path, vec![false]);
        assert!((params.ratio - 0.25).abs() < f32::EPSILON);
    }

    #[test]
    fn rename_and_confirm_close_modals_interpret_into_methods() {
        let (mut mirrors, mut ids, mut app) = composed();
        let mut links = Links::new();

        // Rename workspace: type into the modal, Enter emits the rename.
        app.mode = crate::app::Mode::RenameWorkspace;
        app.selected = 0;
        app.name_input = "renamed".to_owned();
        app.name_input_replace_on_type = false;
        let methods = {
            let pane_public = |pane_id: crate::layout::PaneId| {
                ids.public_pane_id(pane_id)
                    .map(|(_, public)| public.to_owned())
            };
            let tab_public = |ws_idx: usize, tab_idx: usize| {
                let (remote, workspace_public) = ids.workspace_owner(ws_idx)?;
                composed_tab_public_id(&mirrors.get(remote)?.catalog, workspace_public, tab_idx)
            };
            let modal_ids = crate::app::PureModalIds {
                pane_public: &pane_public,
                tab_public: &tab_public,
            };
            crate::app::pure_client_modal_key(
                &mut app,
                crossterm::event::KeyEvent::new(KeyCode::Enter, KeyModifiers::empty()),
                &modal_ids,
            )
        };
        assert_eq!(methods.len(), 1);
        let Method::WorkspaceRename(params) = &methods[0] else {
            panic!("expected workspace.rename, got {methods:?}");
        };
        assert_eq!(params.workspace_id, "ws_2");
        assert_eq!(params.label, "renamed");
        assert_ne!(app.mode, crate::app::Mode::RenameWorkspace);

        // Confirm-close accept closes the selected workspace.
        app.mode = crate::app::Mode::ConfirmClose;
        app.selected = 1;
        dispatch_modal_key(
            crate::input::TerminalKey::new(KeyCode::Enter, KeyModifiers::empty()),
            &mut links,
            &mut mirrors,
            &mut ids,
            &mut app,
            0,
        );
        assert_ne!(app.mode, crate::app::Mode::ConfirmClose);
    }

    #[test]
    fn context_menu_pane_items_interpret_into_methods() {
        let (mirrors, ids, mut app) = composed();
        let pane_id = app.workspaces[0]
            .focused_pane_id()
            .expect("composed focused pane");
        let menu = crate::app::state::ContextMenuState {
            kind: crate::app::state::ContextMenuKind::Pane {
                ws_idx: 0,
                tab_idx: 0,
                pane_id,
                source_pane_id: None,
                has_manual_label: true,
            },
            x: 0,
            y: 0,
            list: crate::app::state::MenuListState::new(0),
        };
        let clear_idx = menu
            .items()
            .iter()
            .position(|item| *item == "Clear pane name")
            .expect("clear item");
        let methods = interpret_modal_mouse(
            crate::app::MouseAction::ContextMenu {
                menu,
                idx: clear_idx,
            },
            &mirrors,
            &ids,
            &mut app,
        );
        assert_eq!(methods.len(), 1);
        let Method::PaneRename(params) = &methods[0] else {
            panic!("expected pane.rename, got {methods:?}");
        };
        assert_eq!(params.pane_id, "p_2_1");
        assert_eq!(params.label, None);
    }

    #[test]
    fn creation_fallback_never_targets_a_remote_filtered_out_of_view() {
        // Nothing focused: creation falls back to the effective focused
        // remote, which must be in view.
        let mut mirrors = RemoteMirrors::with_local();
        mirrors.insert(super::super::RemoteMirror::new(1, "buildbox"));
        let mut ids = ComposeIds::new();
        let mut app = AppState::test_new();
        app.keybinds = crate::config::Config::default().keybinds();
        let descriptors =
            super::super::fleet_view::remote_descriptors(&[crate::fleet::config::RemoteEntry {
                name: "buildbox".into(),
                target: "can@buildbox.example".into(),
                session: "default".into(),
                enabled: true,
            }]);
        let mut links = Links::new();
        let mut chrome = GlobalChrome::new();
        // The user focused buildbox, then toggled its chip out of view.
        chrome.selection.focused_remote = 1;
        chrome
            .selection
            .toggle(1, &descriptors)
            .expect("filter buildbox out");
        assert!(app.active.is_none(), "nothing focused in the empty view");

        dispatch_prefix_intent(
            key(KeyCode::Char('c')),
            &mut links,
            &mut mirrors,
            &mut ids,
            &descriptors,
            &mut app,
            &mut chrome,
        );
        assert_eq!(
            chrome.selection.focused_remote, 0,
            "creation targets the effective (in-view) remote, not the hidden one"
        );
    }

    #[test]
    fn mouse_focus_actions_map_to_control_plane_methods() {
        let (mirrors, ids, app) = composed();

        let (remote, method) = mouse_intent_method(
            Some(MouseAction::FocusWorkspace { ws_idx: 1 }),
            &mirrors,
            &ids,
            &app,
            0,
        )
        .expect("workspace focus intent");
        assert_eq!(remote, 0, "adversarial fixture is local-only");
        let Method::WorkspaceFocus(target) = method else {
            panic!("expected workspace.focus, got {method:?}");
        };
        assert_eq!(target.workspace_id, "ws_10");

        let pane_id = app.workspaces[0]
            .focused_pane_id()
            .expect("composed focused pane");
        let (remote, method) = mouse_intent_method(
            Some(MouseAction::FocusPane { ws_idx: 0, pane_id }),
            &mirrors,
            &ids,
            &app,
            0,
        )
        .expect("pane focus intent");
        assert_eq!(remote, 0, "adversarial fixture is local-only");
        let Method::PaneFocus(target) = method else {
            panic!("expected pane.focus, got {method:?}");
        };
        assert_eq!(target.pane_id, "p_2_1");
    }
}

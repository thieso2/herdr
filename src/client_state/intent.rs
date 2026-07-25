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
    Method, PaneDirection, PaneFocusDirectionParams, PaneSplitParams, PaneTarget, PaneZoomParams,
    SplitDirection, TabCreateParams, TabTarget, WorkspaceCreateParams, WorkspaceTarget,
};
use crate::app::AppState;
use crate::input::TerminalKey;

use super::chrome::GlobalChrome;
use super::compose::ComposeIds;
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
    app: &mut AppState,
    chrome: &mut GlobalChrome,
) {
    // Local chrome first: the sidebar belongs to this client.
    if app.keybinds.toggle_sidebar.matches_prefix_key(key) {
        app.sidebar_collapsed = !app.sidebar_collapsed;
        return;
    }

    let Some((remote, method)) = prefix_intent_method(key, mirrors, ids, app) else {
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
) -> Option<(usize, Method)> {
    let target_remote =
        super::fleet_view::creation_target_remote(app, ids, super::LOCAL_REMOTE_INDEX);
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
    app: &mut AppState,
    chrome: &mut GlobalChrome,
) {
    // Hit testing runs on the composed state with no live runtimes: chrome
    // actions resolve exactly, pane-content interactions (selection drags)
    // simply find no runtime and do nothing.
    let mode_before = app.mode;
    let mut empty_runtimes = crate::terminal::TerminalRuntimeRegistry::new();
    let action = app.handle_mouse(&mut empty_runtimes, mouse);
    // handle_mouse can open modal surfaces (context menus, confirm-close,
    // rename) whose actions are not interpreted under the flag yet. Leaving
    // the app parked in such a mode would trap the user in a dead modal, so
    // revert to the pre-click mode and drop the modal state.
    if !matches!(
        app.mode,
        crate::app::Mode::Terminal | crate::app::Mode::Navigate | crate::app::Mode::Prefix
    ) {
        app.mode = mode_before;
        app.context_menu = None;
    }
    let Some((remote, method)) = mouse_intent_method(action, mirrors, ids, app) else {
        return;
    };
    // Clicking into a remote's content moves the client focus there.
    chrome.selection.focused_remote = remote;
    super::run::send_api_request(links, remote, method);
}

/// The owning remote and JSON API method a resolved chrome mouse action
/// maps to, if any. Focus actions target the clicked entity's owner;
/// creation targets the remote owning the focused context.
pub(super) fn mouse_intent_method(
    action: Option<crate::app::MouseAction>,
    mirrors: &RemoteMirrors,
    ids: &ComposeIds,
    app: &AppState,
) -> Option<(usize, Method)> {
    match action? {
        crate::app::MouseAction::NewWorkspace => {
            let remote =
                super::fleet_view::creation_target_remote(app, ids, super::LOCAL_REMOTE_INDEX);
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
            let catalog = &mirrors.get(remote)?.catalog;
            // The composed tab bar skips tabs with no panes (compose_into),
            // so the hit-tested index must be resolved against the same
            // filtered view of the owning remote's catalog.
            let tab_id = catalog
                .tabs
                .iter()
                .filter(|tab| tab.workspace_id == workspace_public)
                .filter(|tab| catalog.panes.iter().any(|pane| pane.tab_id == tab.tab_id))
                .nth(tab_idx)
                .map(|tab| tab.tab_id.clone())?;
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
        // Everything else (drag reordering, split-ratio drags, modals,
        // context menus, settings) stays unsupported under the flag for now.
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

    #[test]
    fn prefix_keys_map_to_control_plane_methods() {
        let (mirrors, ids, app) = composed();

        // Default binds: c = new tab, % / " = splits, x = close pane,
        // arrows = pane focus (all through the configured keybinds).
        let (remote, method) = prefix_intent_method(key(KeyCode::Char('c')), &mirrors, &ids, &app)
            .expect("new tab intent");
        assert_eq!(remote, 0, "adversarial fixture is local-only");
        let Method::TabCreate(params) = method else {
            panic!("expected tab.create, got {method:?}");
        };
        assert_eq!(params.workspace_id.as_deref(), Some("ws_2"));
        assert!(params.focus);

        let (remote, method) = prefix_intent_method(key(KeyCode::Char('v')), &mirrors, &ids, &app)
            .expect("split intent");
        assert_eq!(remote, 0, "adversarial fixture is local-only");
        let Method::PaneSplit(params) = method else {
            panic!("expected pane.split, got {method:?}");
        };
        assert_eq!(params.direction, SplitDirection::Right);
        assert_eq!(params.target_pane_id.as_deref(), Some("p_2_1"));

        let (remote, method) = prefix_intent_method(key(KeyCode::Char('x')), &mirrors, &ids, &app)
            .expect("close pane intent");
        assert_eq!(remote, 0, "adversarial fixture is local-only");
        let Method::PaneClose(target) = method else {
            panic!("expected pane.close, got {method:?}");
        };
        assert_eq!(target.pane_id, "p_2_1");

        let (remote, method) = prefix_intent_method(key(KeyCode::Char('l')), &mirrors, &ids, &app)
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
        let (remote, method) = prefix_intent_method(key(KeyCode::Char('n')), &mirrors, &ids, &app)
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
        assert!(prefix_intent_method(key(KeyCode::Char('~')), &mirrors, &ids, &app).is_none());
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
        )
        .expect("tab focus intent");
        assert_eq!(remote, 0, "adversarial fixture is local-only");
        let Method::TabFocus(target) = method else {
            panic!("expected tab.focus, got {method:?}");
        };
        assert_eq!(target.tab_id, "t_2_1");
    }

    #[tokio::test]
    async fn right_click_does_not_trap_the_client_in_a_dead_context_menu() {
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
            &mut app,
            &mut chrome,
        );

        assert_eq!(
            app.mode,
            crate::app::Mode::Terminal,
            "unsupported modal modes must not survive the dispatch"
        );
        assert!(app.context_menu.is_none(), "no dead context menu remains");
    }

    #[test]
    fn mouse_focus_actions_map_to_control_plane_methods() {
        let (mirrors, ids, app) = composed();

        let (remote, method) = mouse_intent_method(
            Some(MouseAction::FocusWorkspace { ws_idx: 1 }),
            &mirrors,
            &ids,
            &app,
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
        )
        .expect("pane focus intent");
        assert_eq!(remote, 0, "adversarial fixture is local-only");
        let Method::PaneFocus(target) = method else {
            panic!("expected pane.focus, got {method:?}");
        };
        assert_eq!(target.pane_id, "p_2_1");
    }
}

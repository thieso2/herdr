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

use super::compose::ComposeIds;
use super::run::SessionLink;
use super::RemoteMirrors;

/// Resolves a prefix-mode key against the configured keybinds into a
/// control-plane method (or a local chrome mutation) and dispatches it.
pub(super) fn dispatch_prefix_intent(
    key: TerminalKey,
    link: &mut SessionLink,
    mirrors: &mut RemoteMirrors,
    ids: &mut ComposeIds,
    app: &mut AppState,
) {
    // Local chrome first: the sidebar belongs to this client.
    if app.keybinds.toggle_sidebar.matches_prefix_key(key) {
        app.sidebar_collapsed = !app.sidebar_collapsed;
        return;
    }

    let Some(method) = prefix_intent_method(key, mirrors, ids, app) else {
        return;
    };
    super::run::send_api_request(link, method);
}

/// The JSON API method a prefix-mode key maps to, if any. Pure: reads the
/// keybinds, the composed state, and the catalog.
pub(super) fn prefix_intent_method(
    key: TerminalKey,
    mirrors: &RemoteMirrors,
    ids: &ComposeIds,
    app: &AppState,
) -> Option<Method> {
    let catalog = &mirrors.local().catalog;
    let keybinds = &app.keybinds;
    let focused_workspace = app
        .active
        .and_then(|idx| app.workspaces.get(idx))
        .map(|ws| ws.id.clone());
    let focused_pane = app
        .active
        .and_then(|idx| app.workspaces.get(idx))
        .and_then(|ws| ws.focused_pane_id())
        .and_then(|pane_id| ids.public_pane_id(pane_id))
        .map(str::to_owned);

    if keybinds.new_workspace.matches_prefix_key(key) {
        return Some(Method::WorkspaceCreate(WorkspaceCreateParams {
            cwd: None,
            focus: true,
            label: None,
            env: Default::default(),
        }));
    }
    if keybinds.close_workspace.matches_prefix_key(key) {
        return Some(Method::WorkspaceClose(WorkspaceTarget {
            workspace_id: focused_workspace?,
        }));
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
        return Some(Method::WorkspaceFocus(WorkspaceTarget {
            workspace_id: (*target).to_owned(),
        }));
    }
    if keybinds.new_tab.matches_prefix_key(key) {
        return Some(Method::TabCreate(TabCreateParams {
            workspace_id: focused_workspace,
            cwd: None,
            focus: true,
            label: None,
            env: Default::default(),
        }));
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
        return Some(Method::TabFocus(TabTarget {
            tab_id: (*target).to_owned(),
        }));
    }
    if keybinds.close_tab.matches_prefix_key(key) {
        let workspace_id = focused_workspace?;
        let workspace = catalog.workspace(&workspace_id)?;
        return Some(Method::TabClose(TabTarget {
            tab_id: workspace.active_tab_id.clone(),
        }));
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
        return Some(Method::PaneSplit(PaneSplitParams {
            workspace_id: None,
            target_pane_id: focused_pane,
            direction,
            ratio: None,
            cwd: None,
            focus: true,
            env: Default::default(),
        }));
    }
    if keybinds.close_pane.matches_prefix_key(key) {
        return Some(Method::PaneClose(PaneTarget {
            pane_id: focused_pane?,
        }));
    }
    if keybinds.zoom.matches_prefix_key(key) {
        return Some(Method::PaneZoom(PaneZoomParams {
            pane_id: focused_pane,
            mode: Default::default(),
        }));
    }
    for (bind, direction) in [
        (&keybinds.focus_pane_left, PaneDirection::Left),
        (&keybinds.focus_pane_down, PaneDirection::Down),
        (&keybinds.focus_pane_up, PaneDirection::Up),
        (&keybinds.focus_pane_right, PaneDirection::Right),
    ] {
        if bind.matches_prefix_key(key) {
            return Some(Method::PaneFocusDirection(PaneFocusDirectionParams {
                pane_id: focused_pane,
                direction,
            }));
        }
    }
    None
}

/// Resolves a chrome mouse action (hit-tested against the computed view)
/// into a control-plane method and dispatches it.
pub(super) fn dispatch_mouse_intent(
    mouse: MouseEvent,
    link: &mut SessionLink,
    mirrors: &mut RemoteMirrors,
    ids: &mut ComposeIds,
    app: &mut AppState,
) {
    // Hit testing runs on the composed state with no live runtimes: chrome
    // actions resolve exactly, pane-content interactions (selection drags)
    // simply find no runtime and do nothing.
    let mode_before = app.mode;
    let mut empty_runtimes = crate::terminal::TerminalRuntimeRegistry::new();
    let action = app.handle_mouse(&mut empty_runtimes, mouse);
    // handle_mouse can open modal surfaces. Context menus, confirm-close,
    // and rename dialogs are interpreted client-side below; anything else
    // (settings, worktree dialogs) stays unsupported under the flag, so
    // revert to the pre-click mode instead of trapping the user in a dead
    // modal.
    if !matches!(
        app.mode,
        crate::app::Mode::Terminal
            | crate::app::Mode::Navigate
            | crate::app::Mode::Prefix
            | crate::app::Mode::Copy
            | crate::app::Mode::ContextMenu
            | crate::app::Mode::ConfirmClose
            | crate::app::Mode::RenameWorkspace
            | crate::app::Mode::RenameTab
            | crate::app::Mode::RenamePane
    ) {
        app.mode = mode_before;
        app.context_menu = None;
    }
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
        for method in methods {
            super::run::send_api_request(link, method);
        }
        return;
    }
    let Some(method) = mouse_intent_method(Some(action), mirrors, ids, app) else {
        return;
    };
    super::run::send_api_request(link, method);
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
    let ws_ids: Vec<String> = app.workspaces.iter().map(|ws| ws.id.clone()).collect();
    let catalog = &mirrors.local().catalog;
    let pane_public =
        |pane_id: crate::layout::PaneId| ids.public_pane_id(pane_id).map(str::to_owned);
    let tab_public = |ws_idx: usize, tab_idx: usize| {
        composed_tab_public_id(catalog, ws_ids.get(ws_idx)?, tab_idx)
    };
    let modal_ids = crate::app::PureModalIds {
        pane_public: &pane_public,
        tab_public: &tab_public,
    };
    crate::app::pure_client_modal_mouse(app, action, &modal_ids)
}

/// Interprets a key in a pure-client modal mode (rename dialogs,
/// confirm-close, context menu) into control-plane methods.
pub(super) fn dispatch_modal_key(
    key: crate::input::TerminalKey,
    link: &mut SessionLink,
    mirrors: &mut RemoteMirrors,
    ids: &mut ComposeIds,
    app: &mut AppState,
) {
    let methods = {
        let ws_ids: Vec<String> = app.workspaces.iter().map(|ws| ws.id.clone()).collect();
        let catalog = &mirrors.local().catalog;
        let pane_public =
            |pane_id: crate::layout::PaneId| ids.public_pane_id(pane_id).map(str::to_owned);
        let tab_public = |ws_idx: usize, tab_idx: usize| {
            composed_tab_public_id(catalog, ws_ids.get(ws_idx)?, tab_idx)
        };
        let modal_ids = crate::app::PureModalIds {
            pane_public: &pane_public,
            tab_public: &tab_public,
        };
        crate::app::pure_client_modal_key(app, key.as_key_event(), &modal_ids)
    };
    for method in methods {
        super::run::send_api_request(link, method);
    }
}

/// Public tab id at a composed tab-bar position. The composed tab bar skips
/// tabs with no panes (compose_into), so positions resolve against the same
/// filtered view of the catalog.
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

/// The JSON API method a resolved chrome mouse action maps to, if any.
pub(super) fn mouse_intent_method(
    action: Option<crate::app::MouseAction>,
    mirrors: &RemoteMirrors,
    ids: &ComposeIds,
    app: &AppState,
) -> Option<Method> {
    let catalog = &mirrors.local().catalog;
    match action? {
        crate::app::MouseAction::NewWorkspace => {
            Some(Method::WorkspaceCreate(WorkspaceCreateParams {
                cwd: None,
                focus: true,
                label: None,
                env: Default::default(),
            }))
        }
        crate::app::MouseAction::FocusWorkspace { ws_idx } => {
            let workspace = app.workspaces.get(ws_idx)?;
            Some(Method::WorkspaceFocus(WorkspaceTarget {
                workspace_id: workspace.id.clone(),
            }))
        }
        crate::app::MouseAction::FocusTab { tab_idx } => {
            let workspace = app.active.and_then(|idx| app.workspaces.get(idx))?;
            let tab_id = composed_tab_public_id(catalog, &workspace.id, tab_idx)?;
            Some(Method::TabFocus(TabTarget { tab_id }))
        }
        crate::app::MouseAction::FocusPane { pane_id, .. } => {
            let public = ids.public_pane_id(pane_id)?;
            Some(Method::PaneFocus(PaneTarget {
                pane_id: public.to_owned(),
            }))
        }
        crate::app::MouseAction::MoveWorkspace {
            source_ws_idx,
            insert_idx,
        } => {
            let workspace = app.workspaces.get(source_ws_idx)?;
            Some(Method::WorkspaceMove(WorkspaceMoveParams {
                workspace_id: workspace.id.clone(),
                insert_index: insert_idx,
            }))
        }
        crate::app::MouseAction::MoveTab {
            ws_idx,
            source_tab_idx,
            insert_idx,
        } => {
            let workspace = app.workspaces.get(ws_idx)?;
            let tab_id = composed_tab_public_id(catalog, &workspace.id, source_tab_idx)?;
            Some(Method::TabMove(TabMoveParams {
                tab_id,
                insert_index: insert_idx,
            }))
        }
        crate::app::MouseAction::SetSplitRatio { path, ratio } => {
            // The server resolves the focused tab; the composed layout
            // follows on the next layout.updated event.
            Some(Method::LayoutSetSplitRatio(LayoutSetSplitRatioParams {
                tab_id: None,
                pane_id: None,
                path,
                ratio,
            }))
        }
        // Settings, toast targets, and modal actions are handled elsewhere
        // (modal actions in interpret_modal_mouse; settings stays
        // unsupported under the flag).
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
        let method = prefix_intent_method(key(KeyCode::Char('c')), &mirrors, &ids, &app)
            .expect("new tab intent");
        let Method::TabCreate(params) = method else {
            panic!("expected tab.create, got {method:?}");
        };
        assert_eq!(params.workspace_id.as_deref(), Some("ws_2"));
        assert!(params.focus);

        let method = prefix_intent_method(key(KeyCode::Char('v')), &mirrors, &ids, &app)
            .expect("split intent");
        let Method::PaneSplit(params) = method else {
            panic!("expected pane.split, got {method:?}");
        };
        assert_eq!(params.direction, SplitDirection::Right);
        assert_eq!(params.target_pane_id.as_deref(), Some("p_2_1"));

        let method = prefix_intent_method(key(KeyCode::Char('x')), &mirrors, &ids, &app)
            .expect("close pane intent");
        let Method::PaneClose(target) = method else {
            panic!("expected pane.close, got {method:?}");
        };
        assert_eq!(target.pane_id, "p_2_1");

        let method = prefix_intent_method(key(KeyCode::Char('l')), &mirrors, &ids, &app)
            .expect("focus direction intent");
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
        let method = prefix_intent_method(key(KeyCode::Char('n')), &mirrors, &ids, &app)
            .expect("next workspace intent");
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
        let method = mouse_intent_method(
            Some(MouseAction::FocusTab { tab_idx: 0 }),
            &mirrors,
            &ids,
            &app,
        )
        .expect("tab focus intent");
        let Method::TabFocus(target) = method else {
            panic!("expected tab.focus, got {method:?}");
        };
        assert_eq!(target.tab_id, "t_2_1");
    }

    #[tokio::test]
    async fn right_click_opens_a_live_context_menu_that_esc_closes() {
        let (mut mirrors, mut ids, mut app) = composed();
        app.mode = crate::app::Mode::Terminal;
        crate::ui::compute_view(&mut app, ratatui::layout::Rect::new(0, 0, 106, 20));
        let inner = app.view.pane_infos.first().expect("pane info").inner_rect;

        let mut link = super::super::run::SessionLink::Incompatible;
        let mouse = MouseEvent {
            kind: crossterm::event::MouseEventKind::Down(crossterm::event::MouseButton::Right),
            column: inner.x + 1,
            row: inner.y + 1,
            modifiers: KeyModifiers::empty(),
        };
        dispatch_mouse_intent(mouse, &mut link, &mut mirrors, &mut ids, &mut app);

        // Context menus are interpreted client-side now: the menu opens and
        // stays; Esc closes it back to a base mode.
        assert_eq!(app.mode, crate::app::Mode::ContextMenu);
        assert!(app.context_menu.is_some(), "context menu opened");
        dispatch_modal_key(
            crate::input::TerminalKey::new(KeyCode::Esc, KeyModifiers::empty()),
            &mut link,
            &mut mirrors,
            &mut ids,
            &mut app,
        );
        assert!(app.context_menu.is_none(), "esc closes the menu");
        assert_eq!(app.mode, crate::app::Mode::Terminal);
    }

    #[test]
    fn drag_actions_map_to_move_and_split_ratio_methods() {
        let (mirrors, ids, app) = composed();

        let method = mouse_intent_method(
            Some(MouseAction::MoveWorkspace {
                source_ws_idx: 1,
                insert_idx: 0,
            }),
            &mirrors,
            &ids,
            &app,
        )
        .expect("workspace move intent");
        let Method::WorkspaceMove(params) = method else {
            panic!("expected workspace.move, got {method:?}");
        };
        assert_eq!(params.workspace_id, "ws_10");
        assert_eq!(params.insert_index, 0);

        let method = mouse_intent_method(
            Some(MouseAction::MoveTab {
                ws_idx: 0,
                source_tab_idx: 0,
                insert_idx: 1,
            }),
            &mirrors,
            &ids,
            &app,
        )
        .expect("tab move intent");
        let Method::TabMove(params) = method else {
            panic!("expected tab.move, got {method:?}");
        };
        assert_eq!(params.tab_id, "t_2_1");

        let method = mouse_intent_method(
            Some(MouseAction::SetSplitRatio {
                path: vec![false],
                ratio: 0.25,
            }),
            &mirrors,
            &ids,
            &app,
        )
        .expect("split ratio intent");
        let Method::LayoutSetSplitRatio(params) = method else {
            panic!("expected layout.set_split_ratio, got {method:?}");
        };
        assert_eq!(params.path, vec![false]);
        assert!((params.ratio - 0.25).abs() < f32::EPSILON);
    }

    #[test]
    fn rename_and_confirm_close_modals_interpret_into_methods() {
        let (mut mirrors, mut ids, mut app) = composed();
        let mut link = super::super::run::SessionLink::Incompatible;

        // Rename workspace: type into the modal, Enter emits the rename.
        app.mode = crate::app::Mode::RenameWorkspace;
        app.selected = 0;
        app.name_input = "renamed".to_owned();
        app.name_input_replace_on_type = false;
        let methods = {
            let ws_ids: Vec<String> = app.workspaces.iter().map(|ws| ws.id.clone()).collect();
            let catalog = &mirrors.local().catalog;
            let pane_public = |pane_id: crate::layout::PaneId| {
                ids.public_pane_id(pane_id).map(str::to_owned)
            };
            let tab_public = |ws_idx: usize, tab_idx: usize| {
                composed_tab_public_id(catalog, ws_ids.get(ws_idx)?, tab_idx)
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
            &mut link,
            &mut mirrors,
            &mut ids,
            &mut app,
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
    fn mouse_focus_actions_map_to_control_plane_methods() {
        let (mirrors, ids, app) = composed();

        let method = mouse_intent_method(
            Some(MouseAction::FocusWorkspace { ws_idx: 1 }),
            &mirrors,
            &ids,
            &app,
        )
        .expect("workspace focus intent");
        let Method::WorkspaceFocus(target) = method else {
            panic!("expected workspace.focus, got {method:?}");
        };
        assert_eq!(target.workspace_id, "ws_10");

        let pane_id = app.workspaces[0]
            .focused_pane_id()
            .expect("composed focused pane");
        let method = mouse_intent_method(
            Some(MouseAction::FocusPane { ws_idx: 0, pane_id }),
            &mirrors,
            &ids,
            &app,
        )
        .expect("pane focus intent");
        let Method::PaneFocus(target) = method else {
            panic!("expected pane.focus, got {method:?}");
        };
        assert_eq!(target.pane_id, "p_2_1");
    }
}

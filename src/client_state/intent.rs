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
    let mut empty_runtimes = crate::terminal::TerminalRuntimeRegistry::new();
    let action = app.handle_mouse(&mut empty_runtimes, mouse);
    let Some(method) = mouse_intent_method(action, mirrors, ids, app) else {
        return;
    };
    super::run::send_api_request(link, method);
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
            let tab_id = catalog
                .tabs
                .iter()
                .filter(|tab| tab.workspace_id == workspace.id)
                .nth(tab_idx)
                .map(|tab| tab.tab_id.clone())?;
            Some(Method::TabFocus(TabTarget { tab_id }))
        }
        crate::app::MouseAction::FocusPane { pane_id, .. } => {
            let public = ids.public_pane_id(pane_id)?;
            Some(Method::PaneFocus(PaneTarget {
                pane_id: public.to_owned(),
            }))
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

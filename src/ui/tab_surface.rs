use ratatui::{layout::Rect, Frame};

use super::panes::{compute_pane_infos, plan_tab_panes_resizes, render_panes};
use crate::app::state::ViewState;
use crate::app::{AppState, Mode};
use crate::layout::{PaneInfo, SplitBorder};
use crate::protocol::CursorState;
use crate::terminal::{PaneContentSource, PaneResizeRequest};

pub(crate) struct TabSurfaceLayout {
    pub(crate) pane_infos: Vec<PaneInfo>,
    pub(crate) split_borders: Vec<SplitBorder>,
    pub(crate) resize_requests: Vec<PaneResizeRequest>,
}

#[derive(Clone, Copy)]
pub(crate) struct TabSurfaceView<'a> {
    pub(crate) pane_infos: &'a [PaneInfo],
    pub(crate) split_borders: &'a [SplitBorder],
}

impl ViewState {
    pub(crate) fn tab_surface(&self) -> TabSurfaceView<'_> {
        TabSurfaceView {
            pane_infos: &self.pane_infos,
            split_borders: &self.split_borders,
        }
    }
}

pub(crate) fn compute_tab_surface(
    app: &AppState,
    content: &dyn PaneContentSource,
    area: Rect,
    resize_panes: bool,
) -> TabSurfaceLayout {
    let split_borders = app
        .active
        .and_then(|i| app.workspaces.get(i))
        .map(|ws| {
            if ws.zoomed {
                Vec::new()
            } else {
                ws.layout.splits(area)
            }
        })
        .unwrap_or_default();
    let mut resize_requests = Vec::new();
    let pane_infos = compute_pane_infos(app, content, area, resize_panes, &mut resize_requests);

    TabSurfaceLayout {
        pane_infos,
        split_borders,
        resize_requests,
    }
}

pub(crate) fn plan_tab_surface_resizes(
    app: &AppState,
    content: &dyn PaneContentSource,
    tab: &crate::workspace::Tab,
    area: Rect,
    requests: &mut Vec<PaneResizeRequest>,
) {
    plan_tab_panes_resizes(app, content, tab, area, requests);
}

pub(crate) fn render_tab_surface(
    app: &AppState,
    content: &dyn PaneContentSource,
    surface: TabSurfaceView<'_>,
    frame: &mut Frame,
) {
    render_panes(
        app,
        content,
        frame,
        surface.pane_infos,
        surface.split_borders,
    );
}

pub(crate) fn tab_surface_hyperlinks(
    app: &AppState,
    content: &dyn PaneContentSource,
    surface: TabSurfaceView<'_>,
) -> Vec<((u16, u16), String, String)> {
    let Some(ws_idx) = app.active else {
        return Vec::new();
    };
    if app.workspaces.get(ws_idx).is_none() {
        return Vec::new();
    }

    let mut links = Vec::new();
    for info in surface.pane_infos {
        if let Some(runtime) = app.content_for_pane_in_workspace(content, ws_idx, info.id) {
            links.extend(runtime.visible_hyperlinks(info.inner_rect));
        }
    }
    links
}

pub(crate) fn tab_surface_cursor(
    app: &AppState,
    content: &dyn PaneContentSource,
    surface: TabSurfaceView<'_>,
) -> Option<CursorState> {
    if app.mode != Mode::Terminal {
        return None;
    }

    let ws_idx = app.active?;
    let info = surface.pane_infos.iter().find(|info| info.is_focused)?;
    if !app.pane_exposes_host_cursor(ws_idx, info.id) {
        return None;
    }
    let runtime = app.content_for_pane_in_workspace(content, ws_idx, info.id)?;
    if runtime.synchronized_output_active() {
        return None;
    }
    let scrolled_back = super::panes::pane_is_scrolled_back(runtime);
    let reveal = app.reveal_hidden_cursor_for_cjk_ime
        && (!app.cjk_ime_agent_filter_configured || {
            let detected = app
                .workspaces
                .get(ws_idx)
                .and_then(|ws| ws.terminal_id(info.id))
                .and_then(|terminal_id| app.terminals.get(terminal_id))
                .and_then(|terminal| terminal.detected_agent);
            detected.is_some_and(|agent| app.cjk_ime_agents.contains(&agent))
        });

    if let Some(cursor) = runtime.cursor_state(info.inner_rect, true) {
        let visible = if reveal {
            !scrolled_back
        } else {
            cursor.visible && !scrolled_back
        };
        Some(CursorState {
            x: cursor.x,
            y: cursor.y,
            visible,
            shape: if reveal && visible {
                app.cjk_ime_cursor_shape
            } else {
                cursor.shape
            },
        })
    } else if reveal && !scrolled_back {
        Some(CursorState {
            x: info.inner_rect.x,
            y: info.inner_rect.y,
            visible: true,
            shape: app.cjk_ime_cursor_shape,
        })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace::Workspace;
    use ratatui::backend::TestBackend;
    use ratatui::layout::Direction;
    use ratatui::Terminal;

    #[tokio::test]
    async fn explicit_surface_layout_drives_render_cursor_and_hyperlinks() {
        let uri = "https://example.com/surface";
        let mut workspace = Workspace::test_new("shell-workspace");
        let left = workspace.tabs[0].root_pane;
        let right = workspace.test_split(Direction::Horizontal);
        workspace.insert_test_runtime(
            left,
            crate::terminal::TerminalRuntime::test_with_screen_bytes(
                20,
                8,
                format!("\x1b]8;;{uri}\x1b\\LEFT\x1b]8;;\x1b\\").as_bytes(),
            ),
        );
        workspace.insert_test_runtime(
            right,
            crate::terminal::TerminalRuntime::test_with_screen_bytes(20, 8, b"RIGHT"),
        );

        let mut app = AppState::test_new();
        app.workspaces = vec![workspace];
        app.active = Some(0);
        app.selected = 0;
        app.mode = Mode::Terminal;

        let full_area = Rect::new(0, 0, 106, 20);
        crate::ui::compute_view(&mut app, full_area);
        let area = app.view.terminal_area;
        assert_eq!(area, Rect::new(26, 1, 80, 19));
        let surface = compute_tab_surface(
            &app,
            &crate::terminal::TerminalRuntimeRegistry::new(),
            area,
            false,
        );
        assert_eq!(surface.pane_infos.len(), 2);
        assert!(!surface.split_borders.is_empty());

        app.view.terminal_area = Rect::new(9, 8, 7, 6);
        app.view.pane_infos.clear();
        app.view.split_borders.clear();

        let surface_view = TabSurfaceView {
            pane_infos: &surface.pane_infos,
            split_borders: &surface.split_borders,
        };
        let mut terminal =
            Terminal::new(TestBackend::new(full_area.width, full_area.height)).unwrap();
        terminal
            .draw(|frame| {
                render_tab_surface(
                    &app,
                    &crate::terminal::TerminalRuntimeRegistry::new(),
                    surface_view,
                    frame,
                )
            })
            .unwrap();

        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("LEFT"), "surface: {rendered:?}");
        assert!(rendered.contains("RIGHT"), "surface: {rendered:?}");
        assert!(!rendered.contains("shell-workspace"));

        let links = tab_surface_hyperlinks(
            &app,
            &crate::terminal::TerminalRuntimeRegistry::new(),
            surface_view,
        );
        assert!(links
            .iter()
            .any(|(_, symbol, link)| { symbol == "L" && link == uri }));
        assert!(tab_surface_cursor(
            &app,
            &crate::terminal::TerminalRuntimeRegistry::new(),
            surface_view,
        )
        .is_some());
    }

    fn full_app_frame(app: &mut AppState, area: Rect) -> crate::protocol::FrameData {
        let (buffer, cursor) = crate::server::render_stream::render_virtual(app, area, true);
        let hyperlinks = crate::server::render_stream::visible_hyperlinks(
            app,
            &crate::terminal::TerminalRuntimeRegistry::new(),
        );
        crate::protocol::FrameData::from_ratatui_buffer_with_hyperlinks(
            &buffer,
            cursor,
            &hyperlinks,
        )
    }

    fn frame_digest(frame: &crate::protocol::FrameData) -> String {
        use sha2::{Digest, Sha256};

        let encoded = bincode::serde::encode_to_vec(frame, bincode::config::standard()).unwrap();
        format!("{:x}", Sha256::digest(encoded))
    }

    fn full_app_characterization_state(uri: &str) -> AppState {
        let mut workspace = Workspace::test_new("characterization");
        workspace.identity_cwd = std::path::PathBuf::from("characterization");
        workspace.cached_git_branch = None;
        workspace.cached_git_ahead_behind = None;
        workspace.cached_git_space = None;
        workspace.test_add_tab(Some("logs"));
        workspace.switch_tab(0);
        let left = workspace.tabs[0].root_pane;
        let right = workspace.test_split(Direction::Horizontal);
        workspace.insert_test_runtime(
            left,
            crate::terminal::TerminalRuntime::test_with_screen_bytes(
                40,
                10,
                format!("\x1b]8;;{uri}\x1b\\LINK\x1b]8;;\x1b\\").as_bytes(),
            ),
        );
        workspace.insert_test_runtime(
            right,
            crate::terminal::TerminalRuntime::test_with_screen_bytes(40, 10, b"RIGHT\r\nPANE"),
        );

        let mut app = AppState::test_new();
        app.workspaces = vec![workspace];
        app.active = Some(0);
        app.selected = 0;
        app.mode = Mode::Terminal;
        app
    }

    #[tokio::test]
    async fn desktop_full_app_semantic_frame_is_characterized() {
        let uri = "https://example.com/full-app";
        let mut app = full_app_characterization_state(uri);
        let frame = full_app_frame(&mut app, Rect::new(0, 0, 106, 20));

        assert_eq!((frame.width, frame.height), (106, 20));
        assert_eq!(app.view.sidebar_rect, Rect::new(0, 0, 26, 20));
        assert_eq!(app.view.tab_bar_rect, Rect::new(26, 0, 80, 1));
        assert_eq!(app.view.terminal_area, Rect::new(26, 1, 80, 19));
        assert_eq!(app.view.pane_infos.len(), 2);
        assert!(!app.view.split_borders.is_empty());
        assert!(frame.cursor.is_some());
        assert_eq!(frame.hyperlinks, vec![uri.to_owned()]);
        // Rebaselined for the sidebar section menus, for the same reason
        // as the render digest in `crate::ui`: both section headers gained
        // a right-aligned `▾` and the agents header lost the
        // `grouped`/`priority` label to the menu. Verified by reading the
        // frame's cells, not by accepting the new digest.
        assert_eq!(
            frame_digest(&frame),
            "8504721aa50ef88c62f90b1fc3745310ba33f8da52044a65efe8c5628be2d8e7"
        );
    }

    #[tokio::test]
    async fn mobile_full_app_semantic_frame_is_characterized() {
        let mut app = full_app_characterization_state("https://example.com/mobile");
        app.mode = Mode::Navigate;
        let frame = full_app_frame(&mut app, Rect::new(0, 0, 44, 20));

        assert_eq!((frame.width, frame.height), (44, 20));
        assert_eq!(app.view.layout, crate::app::state::ViewLayout::Mobile);
        assert_eq!(app.view.mobile_header_rect, Rect::new(0, 0, 44, 2));
        assert_eq!(app.view.terminal_area, Rect::new(0, 2, 44, 18));
        assert_eq!(frame.cursor, None);
        assert_eq!(
            frame_digest(&frame),
            "295608a66067f1e1f066c0adb3cf427e8a2d68bba8f68949fb72d464dcd8baab"
        );
    }
}

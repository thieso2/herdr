//! Read-only pane content seam between the UI layer and whatever owns the
//! terminal screens.
//!
//! The UI must not care whether pane content comes from server-owned live
//! runtimes ([`super::TerminalRuntimeRegistry`]) or from a client-side
//! replicated screen (a pane replica fed by the framed protocol). Everything
//! `compute_view`/`render` need from a pane funnels through [`PaneContent`],
//! and lookup by terminal id funnels through [`PaneContentSource`].
//!
//! Both traits are read-only by design: geometry mutation (pane resizing)
//! is *planned* by `compute_view` and applied by the caller, so a pure
//! client can translate resize plans into protocol messages instead.

use ratatui::{layout::Rect, Frame};

use super::{TerminalId, TerminalRuntime, TerminalRuntimeRegistry};

/// Read-only pane screen content as consumed by the UI layer.
pub(crate) trait PaneContent {
    /// Draws the pane's visible screen into `area`.
    fn render(&self, frame: &mut Frame, area: Rect, show_cursor: bool);

    /// Cursor position/shape for the pane when rendered into `area`.
    fn cursor_state(
        &self,
        area: Rect,
        show_cursor: bool,
    ) -> Option<crate::pane::TerminalCursorState>;

    /// Scrollback metrics for scrollbars and scrolled-back detection.
    fn scroll_metrics(&self) -> Option<crate::pane::ScrollMetrics>;

    /// True while the pane holds a synchronized-output batch open.
    fn synchronized_output_active(&self) -> bool;

    /// OSC 8 hyperlinks visible when rendered into `area`.
    fn visible_hyperlinks(&self, area: Rect) -> Vec<((u16, u16), String, String)>;

    /// For each candidate match, whether the screen text still matches.
    fn text_matches_are_current(
        &self,
        text_matches: &[crate::pane::TerminalTextMatch],
    ) -> Vec<bool>;

    /// Whether one candidate match still matches the screen text.
    fn text_match_is_current(&self, text_match: crate::pane::TerminalTextMatch) -> bool {
        self.text_matches_are_current(&[text_match])
            .first()
            .copied()
            .unwrap_or(false)
    }

    /// The pane's current working directory, when known live.
    fn cwd(&self) -> Option<std::path::PathBuf>;

    /// Scrolls the viewport up by `lines` rows into history.
    ///
    /// Scroll mutations take `&self`: the live runtime already synchronizes
    /// interior state behind its core lock, and the replica implementation
    /// on [`std::cell::RefCell`] borrows mutably per call.
    fn scroll_up(&self, lines: usize);

    /// Scrolls the viewport down by `lines` rows toward the live tail.
    fn scroll_down(&self, lines: usize);

    /// Sets the viewport offset in rows from the bottom of history.
    fn set_scroll_offset_from_bottom(&self, offset_from_bottom: usize);

    /// Searches the pane's screen text (scrollback plus viewport).
    fn search_text_matches(
        &self,
        query: &str,
        case_sensitive: bool,
    ) -> Vec<crate::pane::TerminalTextMatch>;

    /// Word-motion target from a text position, for copy-mode vi motions.
    fn word_motion_target(
        &self,
        row: u32,
        col: u16,
        motion: crate::pane::TerminalWordMotion,
    ) -> Option<crate::pane::TerminalTextPoint>;

    /// Extracts the selected text from the pane's screen rows.
    fn extract_selection(&self, selection: &crate::selection::Selection) -> Option<String>;

    /// Visible kitty image placements, fetching pixel data only for images
    /// `needs_data` accepts. Dyn-compatible flavor of the runtime's generic
    /// method so host graphics painting works through the seam.
    fn kitty_image_placements_with_data_filter(
        &self,
        needs_data: &mut dyn FnMut(crate::ghostty::KittyImageDescriptor) -> bool,
    ) -> Vec<crate::ghostty::KittyImagePlacement>;
}

impl PaneContent for TerminalRuntime {
    fn render(&self, frame: &mut Frame, area: Rect, show_cursor: bool) {
        TerminalRuntime::render(self, frame, area, show_cursor);
    }

    fn cursor_state(
        &self,
        area: Rect,
        show_cursor: bool,
    ) -> Option<crate::pane::TerminalCursorState> {
        TerminalRuntime::cursor_state(self, area, show_cursor)
    }

    fn scroll_metrics(&self) -> Option<crate::pane::ScrollMetrics> {
        TerminalRuntime::scroll_metrics(self)
    }

    fn synchronized_output_active(&self) -> bool {
        TerminalRuntime::synchronized_output_active(self)
    }

    fn visible_hyperlinks(&self, area: Rect) -> Vec<((u16, u16), String, String)> {
        TerminalRuntime::visible_hyperlinks(self, area)
    }

    fn text_matches_are_current(
        &self,
        text_matches: &[crate::pane::TerminalTextMatch],
    ) -> Vec<bool> {
        TerminalRuntime::text_matches_are_current(self, text_matches)
    }

    fn cwd(&self) -> Option<std::path::PathBuf> {
        TerminalRuntime::cwd(self)
    }

    fn scroll_up(&self, lines: usize) {
        TerminalRuntime::scroll_up(self, lines);
    }

    fn scroll_down(&self, lines: usize) {
        TerminalRuntime::scroll_down(self, lines);
    }

    fn set_scroll_offset_from_bottom(&self, offset_from_bottom: usize) {
        TerminalRuntime::set_scroll_offset_from_bottom(self, offset_from_bottom);
    }

    fn search_text_matches(
        &self,
        query: &str,
        case_sensitive: bool,
    ) -> Vec<crate::pane::TerminalTextMatch> {
        TerminalRuntime::search_text_matches(self, query, case_sensitive)
    }

    fn text_match_is_current(&self, text_match: crate::pane::TerminalTextMatch) -> bool {
        TerminalRuntime::text_match_is_current(self, text_match)
    }

    fn word_motion_target(
        &self,
        row: u32,
        col: u16,
        motion: crate::pane::TerminalWordMotion,
    ) -> Option<crate::pane::TerminalTextPoint> {
        TerminalRuntime::word_motion_target(self, row, col, motion)
    }

    fn extract_selection(&self, selection: &crate::selection::Selection) -> Option<String> {
        TerminalRuntime::extract_selection(self, selection)
    }

    fn kitty_image_placements_with_data_filter(
        &self,
        needs_data: &mut dyn FnMut(crate::ghostty::KittyImageDescriptor) -> bool,
    ) -> Vec<crate::ghostty::KittyImagePlacement> {
        TerminalRuntime::kitty_image_placements_with_data_filter(self, needs_data)
    }
}

/// The replica implementation lives on `RefCell<PaneReplica>`: copy-mode
/// scrolling mutates the replica's viewport through the shared reference the
/// UI seam hands out, and the pure client is single-threaded per mirror so a
/// `RefCell` is the exact fit (no lock, borrow bugs panic in tests).
impl PaneContent for std::cell::RefCell<super::replica::PaneReplica> {
    fn render(&self, frame: &mut Frame, area: Rect, show_cursor: bool) {
        crate::pane::render_plain_terminal(self.borrow().terminal(), frame, area);
        // Mirror of GhosttyPane::render: the focused pane places the host
        // cursor. Implemented via cursor_state so the two can never diverge.
        if let Some(cursor) = self
            .cursor_state(area, show_cursor)
            .filter(|cursor| cursor.visible)
        {
            frame.set_cursor_position((cursor.x, cursor.y));
        }
    }

    fn cursor_state(
        &self,
        area: Rect,
        show_cursor: bool,
    ) -> Option<crate::pane::TerminalCursorState> {
        if !show_cursor {
            return None;
        }
        let cursor = crate::pane::plain_terminal_cursor_state(self.borrow().terminal())?;
        if cursor.x >= area.width || cursor.y >= area.height {
            return None;
        }
        // Frame-absolute coordinates, matching PaneRuntime::cursor_state:
        // callers (copy-mode entry, tab_surface) subtract the pane rect.
        Some(crate::pane::TerminalCursorState {
            x: area.x + cursor.x,
            y: area.y + cursor.y,
            visible: cursor.visible,
            shape: cursor.shape,
        })
    }

    fn scroll_metrics(&self) -> Option<crate::pane::ScrollMetrics> {
        super::replica::PaneReplica::scroll_metrics(&self.borrow()).ok()
    }

    fn synchronized_output_active(&self) -> bool {
        self.borrow()
            .terminal()
            .mode_get(crate::ghostty::MODE_SYNCHRONIZED_OUTPUT)
            .unwrap_or(false)
    }

    fn visible_hyperlinks(&self, area: Rect) -> Vec<((u16, u16), String, String)> {
        crate::pane::plain_terminal_visible_hyperlinks(self.borrow().terminal(), area)
    }

    fn text_matches_are_current(
        &self,
        text_matches: &[crate::pane::TerminalTextMatch],
    ) -> Vec<bool> {
        crate::pane::plain_terminal_text_matches_are_current(self.borrow().terminal(), text_matches)
    }

    fn cwd(&self) -> Option<std::path::PathBuf> {
        // The catalog carries the pane cwd as a server fact; the replica has
        // no live process to ask.
        None
    }

    fn scroll_up(&self, lines: usize) {
        self.borrow_mut()
            .scroll_delta(-(lines.min(isize::MAX as usize) as isize));
    }

    fn scroll_down(&self, lines: usize) {
        self.borrow_mut()
            .scroll_delta(lines.min(isize::MAX as usize) as isize);
    }

    fn set_scroll_offset_from_bottom(&self, offset_from_bottom: usize) {
        self.borrow_mut()
            .set_scroll_offset_from_bottom(offset_from_bottom);
    }

    fn search_text_matches(
        &self,
        query: &str,
        case_sensitive: bool,
    ) -> Vec<crate::pane::TerminalTextMatch> {
        crate::pane::plain_terminal_search_text_matches(
            self.borrow().terminal(),
            query,
            case_sensitive,
        )
    }

    fn word_motion_target(
        &self,
        row: u32,
        col: u16,
        motion: crate::pane::TerminalWordMotion,
    ) -> Option<crate::pane::TerminalTextPoint> {
        crate::pane::plain_terminal_word_motion_target(self.borrow().terminal(), row, col, motion)
    }

    fn extract_selection(&self, selection: &crate::selection::Selection) -> Option<String> {
        crate::pane::plain_terminal_extract_selection(self.borrow().terminal(), selection)
    }

    fn kitty_image_placements_with_data_filter(
        &self,
        needs_data: &mut dyn FnMut(crate::ghostty::KittyImageDescriptor) -> bool,
    ) -> Vec<crate::ghostty::KittyImagePlacement> {
        self.borrow()
            .terminal()
            .kitty_image_placements_with_data_filter(needs_data)
            .unwrap_or_default()
    }
}

/// Lookup of read-only pane content by durable terminal id.
pub(crate) trait PaneContentSource {
    fn pane_content(&self, terminal_id: &TerminalId) -> Option<&dyn PaneContent>;
}

impl PaneContentSource for TerminalRuntimeRegistry {
    fn pane_content(&self, terminal_id: &TerminalId) -> Option<&dyn PaneContent> {
        self.get(terminal_id).map(|runtime| runtime as _)
    }
}

/// A [`PaneContentSource`] with no content, for pure-geometry computation in
/// tests and previews.
pub(crate) struct EmptyPaneContentSource;

impl PaneContentSource for EmptyPaneContentSource {
    fn pane_content(&self, _terminal_id: &TerminalId) -> Option<&dyn PaneContent> {
        None
    }
}

/// A pane resize implied by newly computed view geometry.
///
/// `compute_view` plans these instead of resizing runtimes itself; the caller
/// applies them against live runtimes or translates them into protocol
/// messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PaneResizeRequest {
    pub(crate) terminal_id: TerminalId,
    pub(crate) rows: u16,
    pub(crate) cols: u16,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    fn replica(
        snapshot: &str,
        cols: u16,
        rows: u16,
    ) -> RefCell<crate::terminal::replica::PaneReplica> {
        RefCell::new(
            crate::terminal::replica::PaneReplica::open(snapshot, 0, None, cols, rows, 64 * 1024)
                .expect("replica opens"),
        )
    }

    /// The replica-backed cursor_state must honor the same coordinate
    /// contract as the runtime-backed one: frame-absolute coordinates
    /// (area offset applied), not pane-local viewport coordinates.
    #[tokio::test]
    async fn replica_cursor_state_matches_runtime_coordinates() {
        let runtime = TerminalRuntime::test_with_screen_bytes(20, 5, b"left");
        let replica = replica("left", 20, 5);
        let area = Rect::new(5, 3, 20, 5);

        let from_runtime = PaneContent::cursor_state(&runtime, area, true).expect("runtime cursor");
        let from_replica = PaneContent::cursor_state(&replica, area, true).expect("replica cursor");
        assert_eq!(
            (from_replica.x, from_replica.y, from_replica.visible),
            (from_runtime.x, from_runtime.y, from_runtime.visible),
            "replica cursor_state must return frame-absolute coordinates like the runtime"
        );

        assert!(PaneContent::cursor_state(&replica, area, false).is_none());
    }

    /// The pure client draws through `PaneContent::render`; the focused
    /// pane's host cursor exists only if the replica implementation sets the
    /// frame cursor position like `GhosttyPane::render` does.
    #[test]
    fn replica_render_sets_the_frame_cursor_when_shown() {
        let replica = replica("left", 20, 5);
        let area = Rect::new(5, 3, 20, 5);
        let backend = ratatui::backend::TestBackend::new(40, 10);
        let mut terminal = ratatui::Terminal::new(backend).expect("test terminal");
        terminal
            .draw(|frame| PaneContent::render(&replica, frame, area, true))
            .expect("draw");
        terminal
            .backend_mut()
            .assert_cursor_position((area.x + 4, area.y));
    }

    /// An unfocused pane (`show_cursor == false`) and a pane that hid its
    /// cursor via DECTCEM must not place the host cursor.
    #[test]
    fn replica_render_leaves_the_cursor_unset_when_hidden() {
        use ratatui::backend::Backend;

        let unfocused = replica("ab", 20, 5);
        let area = Rect::new(5, 3, 20, 5);
        let backend = ratatui::backend::TestBackend::new(40, 10);
        let mut terminal = ratatui::Terminal::new(backend).expect("test terminal");
        terminal
            .draw(|frame| PaneContent::render(&unfocused, frame, area, false))
            .expect("draw");
        let position = terminal
            .backend_mut()
            .get_cursor_position()
            .expect("cursor position");
        assert_ne!((position.x, position.y), (area.x + 2, area.y));

        let hidden = replica("ab\x1b[?25l", 20, 5);
        terminal
            .draw(|frame| PaneContent::render(&hidden, frame, area, true))
            .expect("draw");
        let position = terminal
            .backend_mut()
            .get_cursor_position()
            .expect("cursor position");
        assert_ne!((position.x, position.y), (area.x + 2, area.y));
    }
}

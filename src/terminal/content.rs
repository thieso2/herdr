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

    /// The pane's current working directory, when known live.
    fn cwd(&self) -> Option<std::path::PathBuf>;
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
}

impl PaneContent for super::replica::PaneReplica {
    fn render(&self, frame: &mut Frame, area: Rect, _show_cursor: bool) {
        crate::pane::render_plain_terminal(self.terminal(), frame, area);
    }

    fn cursor_state(
        &self,
        area: Rect,
        show_cursor: bool,
    ) -> Option<crate::pane::TerminalCursorState> {
        if !show_cursor {
            return None;
        }
        let cursor = crate::pane::plain_terminal_cursor_state(self.terminal())?;
        if cursor.x >= area.width || cursor.y >= area.height {
            return None;
        }
        Some(cursor)
    }

    fn scroll_metrics(&self) -> Option<crate::pane::ScrollMetrics> {
        super::replica::PaneReplica::scroll_metrics(self).ok()
    }

    fn synchronized_output_active(&self) -> bool {
        self.terminal()
            .mode_get(crate::ghostty::MODE_SYNCHRONIZED_OUTPUT)
            .unwrap_or(false)
    }

    fn visible_hyperlinks(&self, area: Rect) -> Vec<((u16, u16), String, String)> {
        crate::pane::plain_terminal_visible_hyperlinks(self.terminal(), area)
    }

    fn text_matches_are_current(
        &self,
        text_matches: &[crate::pane::TerminalTextMatch],
    ) -> Vec<bool> {
        // Copy-mode search runs against live runtimes only; a replica has no
        // search index yet, so no candidate can be confirmed current.
        vec![false; text_matches.len()]
    }

    fn cwd(&self) -> Option<std::path::PathBuf> {
        // The catalog carries the pane cwd as a server fact; the replica has
        // no live process to ask.
        None
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

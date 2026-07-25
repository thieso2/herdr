//! Client-side terminal replica for streamed panes.
//!
//! A [`PaneReplica`] is a headless ghostty terminal fed exclusively by server
//! bytes: the `stream.open` snapshot seeds it, the live DATA tail keeps it
//! current, and `stream.history` pages backfill scrollback on demand. It never
//! answers device queries — bytes go in through bare [`Terminal::write`], and
//! the authoritative server pane already replied to DA/DECRQM/XTGETTCAP.
//!
//! Scrolling a replica is fully local. When the viewport approaches the top
//! of loaded history, the pure paging policy ([`plan_backfill`]) decides the
//! next `stream.history` fetch, and [`PaneReplica::apply_history_response`]
//! prepends the page by rebuilding the terminal from the raw-page deque plus
//! a local dump. The server's cursor contract (pages are byte-contiguous
//! slices of one immutable capture) plus the single-in-flight fetch this
//! module enforces make the rebuilt history gap-free and duplicate-free.

// The TUI client wiring for pane replicas arrives with #20; until then only
// tests exercise this module in-tree, so the dead-code lint has nothing to
// anchor on yet.
#![allow(dead_code)]

use std::collections::VecDeque;

use crate::ghostty::{ActiveScreen, Error as GhosttyError, Terminal, TerminalScrollbar};
use crate::pane::ScrollMetrics;
use crate::protocol::framed::{
    parse_stream_history, stream_history_request, HISTORY_FETCH_MAX_BYTES,
    HISTORY_PAGE_DEFAULT_BYTES,
};

/// Why the client is consulting the paging policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackfillTrigger {
    /// The pane was just attached; the focused pane gets one eager page so
    /// the first wheel tick already has history behind it.
    Attach {
        /// Whether the pane is the focused one at attach time.
        focused: bool,
    },
    /// The user scrolled; metrics describe the post-scroll viewport and a
    /// lazy page is fetched two screens ahead of the loaded top.
    Scroll,
    /// The user jumped to the top of history: one large fetch instead of a
    /// page-by-page crawl.
    JumpToTop,
}

/// Everything the pure paging policy looks at. Kept as plain data so policy
/// tests need no terminal, no sockets, and no replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackfillState {
    /// Current scroll metrics of the replica viewport.
    pub metrics: ScrollMetrics,
    /// True when no older history can or should be fetched: the server
    /// reported the top, the cursor is gone, or the local scrollback budget
    /// is spent.
    pub history_exhausted: bool,
    /// True while a `stream.history` request is outstanding. The policy
    /// never stacks fetches, which also keeps page application ordered.
    pub fetch_in_flight: bool,
    /// Bytes left in the replica's scrollback budget.
    pub budget_remaining: usize,
}

/// Pure paging-policy decision: the `max_bytes` of the `stream.history`
/// fetch to issue now, or `None` when no fetch should happen.
pub fn plan_backfill(trigger: BackfillTrigger, state: BackfillState) -> Option<usize> {
    if state.history_exhausted || state.fetch_in_flight || state.budget_remaining == 0 {
        return None;
    }
    let page = HISTORY_PAGE_DEFAULT_BYTES.min(state.budget_remaining);
    match trigger {
        BackfillTrigger::Attach { focused } => focused.then_some(page),
        BackfillTrigger::Scroll => {
            let metrics = state.metrics;
            let lookahead = metrics
                .offset_from_bottom
                .saturating_add(metrics.viewport_rows.saturating_mul(2));
            (lookahead >= metrics.max_offset_from_bottom).then_some(page)
        }
        BackfillTrigger::JumpToTop => Some(HISTORY_FETCH_MAX_BYTES.min(state.budget_remaining)),
    }
}

/// Client replica of one streamed pane's terminal.
///
/// Owns a headless [`Terminal`] plus the raw material needed to rebuild it
/// when history is prepended: the deque of fetched-but-unbaked history pages
/// and the bookkeeping of the opaque history cursor. Capped at its own
/// scrollback budget; there is no global cap and no cross-pane eviction.
pub struct PaneReplica {
    terminal: Terminal,
    cols: u16,
    rows: u16,
    cell_px: (u32, u32),
    scrollback_limit_bytes: usize,
    /// Fetched history pages not yet baked into the terminal; front is
    /// oldest. Pages queue here while the replica sits on the alternate
    /// screen and drain into the terminal on the next rebuild.
    pages: VecDeque<String>,
    pages_bytes: usize,
    /// True when the newest queued page (the deque back, which abuts the
    /// already-baked content on rebuild) ends on a mid-line cut: a younger
    /// page's hard-capped start cut a logical line there, so the rebuild
    /// must join it to the local dump without fabricating a line break.
    newest_page_cut_mid_line: bool,
    /// Cursor for the next older page; `None` once the top was reached or
    /// paging stopped.
    next_cursor: Option<String>,
    at_history_top: bool,
    budget_exhausted: bool,
    /// Total history bytes accepted so far, counted against the budget.
    fetched_bytes: usize,
    fetch_in_flight: bool,
    /// Pane output byte sequence the replica has consumed up to: the
    /// `stream.open` sequence plus every DATA tail byte since. Bytes of a
    /// trailing incomplete escape sequence are counted here but sit in
    /// `held_tail` until their continuation arrives.
    applied_sequence: u64,
    /// Consumed-but-unwritten tail bytes: the trailing incomplete escape
    /// sequence or UTF-8 codepoint, held back so the terminal parser always
    /// rests between complete sequences when a rebuild replaces it.
    held_tail: Vec<u8>,
    /// Sequence-scanner state after every consumed tail byte.
    scan_state: VtScanState,
    /// Sequence-scanner state after every byte actually written to the
    /// terminal. `Ground` whenever `held_tail` captures the incomplete
    /// suffix; mid-sequence only after an oversized held tail was flushed
    /// through, which defers rebuilds until the sequence terminates.
    written_state: VtScanState,
    /// Terminal row total right after the last bake (open/rebuild/resize).
    baked_total_rows: usize,
    /// Anchored row count and text of the topmost scrollback rows (up to
    /// [`HISTORY_ANCHOR_ROWS`]) at the last bake, recorded only while those
    /// rows sit in (immutable) scrollback; `None` while the top row is still
    /// on the active screen and can be legitimately rewritten. Several rows
    /// are anchored instead of one so identical repetitive top rows are far
    /// less likely to collide after an eviction.
    history_anchor: Option<(usize, String)>,
}

impl PaneReplica {
    /// Seeds a replica from a `stream.open` result. The snapshot is written
    /// into a fresh terminal at home position; it carries its own mode,
    /// cursor, and style state, so nothing else needs asserting.
    pub fn open(
        snapshot: &str,
        sequence: u64,
        history_cursor: Option<String>,
        cols: u16,
        rows: u16,
        scrollback_limit_bytes: usize,
    ) -> Result<Self, GhosttyError> {
        let cols = cols.max(1);
        let rows = rows.max(1);
        let mut terminal = Terminal::new(cols, rows, scrollback_limit_bytes)?;
        terminal.write(snapshot.as_bytes());
        let mut replica = Self {
            terminal,
            cols,
            rows,
            cell_px: (1, 1),
            scrollback_limit_bytes,
            pages: VecDeque::new(),
            pages_bytes: 0,
            newest_page_cut_mid_line: false,
            next_cursor: history_cursor.filter(|cursor| !cursor.is_empty()),
            at_history_top: false,
            budget_exhausted: false,
            fetched_bytes: 0,
            fetch_in_flight: false,
            applied_sequence: sequence,
            held_tail: Vec::new(),
            scan_state: VtScanState::Ground,
            written_state: VtScanState::Ground,
            baked_total_rows: 0,
            history_anchor: None,
        };
        replica.rebase_history_anchor()?;
        Ok(replica)
    }

    /// Read access to the replicated terminal, for rendering and text
    /// extraction. All mutation goes through the replica so its bookkeeping
    /// stays consistent.
    pub fn terminal(&self) -> &Terminal {
        &self.terminal
    }

    /// Pane output byte sequence applied so far. The next DATA tail byte is
    /// byte `applied_sequence() + 1` of the pane's output.
    pub fn applied_sequence(&self) -> u64 {
        self.applied_sequence
    }

    /// Applies live DATA tail bytes. Returns the number of rows prepended by
    /// a deferred rebuild that ran because queued pages became applicable
    /// (the tail left the alternate screen); callers must re-base
    /// absolute-row state (selection, copy-mode cursor) by that amount.
    ///
    /// A trailing incomplete escape sequence or UTF-8 codepoint is held back
    /// from the terminal until its continuation arrives, so a rebuild (which
    /// replaces the terminal and its VT parser) can never land mid-sequence
    /// and turn continuation bytes into literal text.
    pub fn apply_tail(&mut self, bytes: &[u8]) -> Result<usize, GhosttyError> {
        self.applied_sequence = self.applied_sequence.saturating_add(bytes.len() as u64);
        self.ingest_tail(bytes);
        if !self.pages.is_empty()
            && self.written_state == VtScanState::Ground
            && self.terminal.active_screen()? == ActiveScreen::Primary
        {
            return self.rebuild();
        }
        Ok(0)
    }

    /// Writes tail bytes to the terminal up to the last point where the byte
    /// stream rests between complete sequences and codepoints, holding the
    /// incomplete suffix in `held_tail` for the next chunk.
    fn ingest_tail(&mut self, bytes: &[u8]) {
        let mut state = self.scan_state;
        let mut flush_upto = None;
        for (index, &byte) in bytes.iter().enumerate() {
            state = vt_scan_advance(state, byte);
            if state == VtScanState::Ground {
                flush_upto = Some(index + 1);
            }
        }
        self.scan_state = state;
        match flush_upto {
            Some(upto) => {
                self.held_tail.extend_from_slice(&bytes[..upto]);
                let complete = std::mem::take(&mut self.held_tail);
                self.terminal.write(&complete);
                self.written_state = VtScanState::Ground;
                self.held_tail.extend_from_slice(&bytes[upto..]);
            }
            None => self.held_tail.extend_from_slice(bytes),
        }
        if self.held_tail.len() > HELD_TAIL_MAX_BYTES {
            // A pathological giant sequence (huge OSC/APC payload) is written
            // through instead of buffered without bound. `written_state` then
            // tracks the mid-sequence parser and defers rebuilds until the
            // sequence terminates.
            let flushed = std::mem::take(&mut self.held_tail);
            self.terminal.write(&flushed);
            self.written_state = self.scan_state;
        }
    }

    /// Scroll metrics of the replica viewport, shaped exactly like the pane
    /// runtime's so `src/ui/scrollbar.rs` works unchanged.
    pub fn scroll_metrics(&self) -> Result<ScrollMetrics, GhosttyError> {
        let scrollbar = self.terminal.scrollbar()?;
        Ok(ScrollMetrics {
            offset_from_bottom: scrollbar
                .total
                .saturating_sub(scrollbar.offset + scrollbar.len),
            max_offset_from_bottom: scrollbar.total.saturating_sub(scrollbar.len),
            viewport_rows: scrollbar.len,
        })
    }

    /// Scrolls the viewport by a row delta; fully local.
    pub fn scroll_delta(&mut self, delta: isize) {
        self.terminal.scroll_viewport_delta(delta);
    }

    /// Sets the viewport offset from the bottom, clamped to history.
    pub fn set_scroll_offset_from_bottom(&mut self, offset_from_bottom: usize) {
        let Ok(scrollbar) = self.terminal.scrollbar() else {
            self.terminal.scroll_viewport_bottom();
            return;
        };
        let max_offset = scrollbar.total.saturating_sub(scrollbar.len);
        let offset_from_bottom = offset_from_bottom.min(max_offset);
        if offset_from_bottom == 0 {
            self.terminal.scroll_viewport_bottom();
        } else {
            self.terminal
                .scroll_viewport_row(max_offset - offset_from_bottom);
        }
    }

    /// The paging-policy inputs describing this replica right now.
    pub fn backfill_state(&self) -> Result<BackfillState, GhosttyError> {
        Ok(BackfillState {
            metrics: self.scroll_metrics()?,
            history_exhausted: self.history_exhausted(),
            fetch_in_flight: self.fetch_in_flight,
            budget_remaining: self.budget_remaining(),
        })
    }

    /// True when no older history remains to fetch.
    pub fn history_exhausted(&self) -> bool {
        self.at_history_top || self.budget_exhausted || self.next_cursor.is_none()
    }

    /// Whether a lazy scroll-ahead backfill should be issued for the current
    /// viewport position.
    pub fn needs_backfill(&self) -> Result<bool, GhosttyError> {
        Ok(plan_backfill(BackfillTrigger::Scroll, self.backfill_state()?).is_some())
    }

    /// Builds the `stream.history` control request the policy asks for, or
    /// `None` when no fetch should happen. Marks the fetch in flight, so at
    /// most one request is outstanding and pages apply strictly in order.
    pub fn take_backfill_request(
        &mut self,
        id: &str,
        trigger: BackfillTrigger,
    ) -> Result<Option<serde_json::Value>, GhosttyError> {
        let Some(max_bytes) = plan_backfill(trigger, self.backfill_state()?) else {
            return Ok(None);
        };
        let Some(cursor) = self.next_cursor.as_deref() else {
            return Ok(None);
        };
        let request = stream_history_request(id, cursor, max_bytes);
        self.fetch_in_flight = true;
        Ok(Some(request))
    }

    /// Applies a `stream.history` control response: parses the page, advances
    /// the cursor bookkeeping, and prepends the content by rebuilding the
    /// terminal. Returns the number of rows prepended so callers can re-base
    /// absolute-row state (selection anchors, copy-mode cursor) and
    /// invalidate search matches, exactly as they would after a resize.
    ///
    /// On the alternate screen the page is queued instead — replaying primary
    /// history there would corrupt the alternate buffer — and the rebuild
    /// runs when the tail returns to the primary screen.
    pub fn apply_history_response(
        &mut self,
        response: &serde_json::Value,
    ) -> Result<usize, HistoryApplyError> {
        self.fetch_in_flight = false;
        let page = parse_stream_history(response).map_err(HistoryApplyError::Response)?;
        self.at_history_top = page.at_top;
        self.next_cursor = page.next_cursor;
        if page.content.is_empty() {
            return Ok(0);
        }
        self.fetched_bytes = self.fetched_bytes.saturating_add(page.content.len());
        if self.fetched_bytes >= self.scrollback_limit_bytes {
            self.budget_exhausted = true;
        }
        self.pages_bytes = self.pages_bytes.saturating_add(page.content.len());
        if self.pages.is_empty() {
            // This page becomes the deque back: the one whose end abuts the
            // already-baked content on the next rebuild. Later pushes only
            // prepend older pages, which stay byte-contiguous in the deque.
            self.newest_page_cut_mid_line = page.end_cut_mid_line;
        }
        self.pages.push_front(page.content);
        self.enforce_page_budget();
        if self
            .terminal
            .active_screen()
            .map_err(HistoryApplyError::Terminal)?
            != ActiveScreen::Primary
            || self.written_state != VtScanState::Ground
        {
            // Queued: the rebuild runs when the tail returns to the primary
            // screen (and, after an oversized held-tail flush, when the
            // terminal parser is back between sequences).
            return Ok(0);
        }
        self.rebuild().map_err(HistoryApplyError::Terminal)
    }

    /// Resizes the replica. The terminal reflows and preserves scrollback;
    /// queued raw pages are unwrapped ANSI, so a later rebuild reflows them
    /// at the new width too.
    pub fn resize(
        &mut self,
        cols: u16,
        rows: u16,
        cell_width_px: u32,
        cell_height_px: u32,
    ) -> Result<(), GhosttyError> {
        let cols = cols.max(1);
        let rows = rows.max(1);
        self.terminal
            .resize(cols, rows, cell_width_px, cell_height_px)?;
        self.cols = cols;
        self.rows = rows;
        self.cell_px = (cell_width_px.max(1), cell_height_px.max(1));
        // Reflow legitimately changes the row total and the top row content;
        // re-anchor eviction detection on the reflowed state.
        self.rebase_history_anchor()?;
        Ok(())
    }

    /// Drops oldest queued history when the deque overruns the scrollback
    /// budget, keeping the surviving front newline-aligned so replay never
    /// starts mid-line. Dropping history means older pages can no longer be
    /// stitched on gap-free, so paging stops here.
    fn enforce_page_budget(&mut self) {
        while self.pages_bytes > self.scrollback_limit_bytes {
            let Some(front) = self.pages.front_mut() else {
                self.pages_bytes = 0;
                break;
            };
            let excess = self.pages_bytes - self.scrollback_limit_bytes;
            self.budget_exhausted = true;
            self.at_history_top = false;
            self.next_cursor = None;
            if front.len() <= excess {
                self.pages_bytes -= front.len();
                self.pages.pop_front();
                continue;
            }
            let kept = keep_tail_newline_aligned(front, front.len() - excess);
            self.pages_bytes -= front.len() - kept.len();
            if kept.is_empty() {
                self.pages.pop_front();
            } else {
                *front = kept;
            }
            break;
        }
    }

    /// Rebuilds the terminal with the queued pages prepended: dump the
    /// current replica through the existing replay machinery
    /// ([`Terminal::stream_seed`]), then replay pages (oldest first), the
    /// local history dump, and the state-carrying snapshot into a fresh
    /// terminal. Returns the number of rows added at the top.
    ///
    /// Only called on the primary screen. The viewport offset from the
    /// bottom is preserved, which keeps the same content in view because
    /// prepending never moves rows relative to the bottom.
    fn rebuild(&mut self) -> Result<usize, GhosttyError> {
        let scrollbar = self.terminal.scrollbar()?;
        let offset_from_bottom = scrollbar
            .total
            .saturating_sub(scrollbar.offset + scrollbar.len);
        let old_total = scrollbar.total;

        if self.top_rows_lost_since_bake(&scrollbar)? {
            // Scrollback rows were lost since the last bake: ghostty evicted
            // its oldest rows under cell-memory pressure (its scrollback
            // accounting differs from the replica's raw-byte budget) or the
            // pane erased scrollback (ED 3). The history cursor points just
            // above rows that no longer exist, so stitching queued pages
            // under the truncated content would hide a gap. Stop paging.
            tracing::debug!(
                "pane replica lost scrollback rows since last bake; stopping history backfill"
            );
            self.stop_backfill();
            self.pages.clear();
            self.pages_bytes = 0;
            self.newest_page_cut_mid_line = false;
            self.rebase_history_anchor()?;
            return Ok(0);
        }
        // Every newline in the queued pages must contribute at least one new
        // row to the rebuilt terminal; fewer means the fresh terminal
        // front-evicted while baking. This is a lower bound: soft-wrapped
        // logical lines render to more rows than their newline count, so a
        // small bake-time eviction can hide behind the soft-wrap surplus.
        // Computing the exact rendered row count would require re-rendering
        // the pages at the current width; the residual failure mode is
        // bounded (a visual gap in the oldest history), so the lower bound
        // is kept.
        let page_line_rows: usize = self
            .pages
            .iter()
            .map(|page| page.matches('\n').count())
            .sum();

        let seed = self.terminal.stream_seed()?;

        let mut fresh = Terminal::new(self.cols, self.rows, self.scrollback_limit_bytes)?;
        fresh.resize(self.cols, self.rows, self.cell_px.0, self.cell_px.1)?;
        let mut ends_with_newline = true;
        for page in &self.pages {
            fresh.write(page.as_bytes());
            ends_with_newline = page.ends_with('\n');
        }
        if !seed.history.is_empty() {
            if !ends_with_newline && !self.newest_page_cut_mid_line {
                // The newest page ends at a genuine row boundary whose row
                // carries no trailing newline (the bottom of the server
                // capture); keep it from merging with the local dump's first
                // row. When the server flagged the page end as a hard-capped
                // mid-line cut instead, the local dump's first row continues
                // the same logical line, so the page joins it directly and a
                // fabricated break would split the line at every page
                // boundary.
                fresh.write(b"\r\n");
            }
            fresh.write(seed.history.as_bytes());
        }
        // Scroll the replayed history fully into scrollback so the active
        // screen starts blank, then place the snapshot at home. This keeps
        // the snapshot's absolute cursor/row positions identical to the
        // live screen instead of anchoring its content at the bottom.
        fresh.write("\r\n".repeat(self.rows as usize).as_bytes());
        // The content-only history dump carries no trailing style reset, so
        // clear leaked SGR state before the snapshot replays its own.
        fresh.write(b"\x1b[H\x1b[0m");
        fresh.write(seed.snapshot.as_bytes());

        let new_total = fresh.scrollbar()?.total;
        self.terminal = fresh;
        self.pages.clear();
        self.pages_bytes = 0;
        self.newest_page_cut_mid_line = false;
        self.set_scroll_offset_from_bottom(offset_from_bottom);
        if new_total < old_total.saturating_add(page_line_rows) {
            // The fresh terminal evicted its oldest rows while the pages and
            // the local dump were baked in: the terminal is full in ghostty's
            // cell-memory accounting even though the raw-byte budget is not.
            // The content is still contiguous — only the oldest rows fell
            // off — but the cursor now points above evicted rows, so further
            // paging would stitch a gap. Stop here.
            tracing::debug!(
                "pane replica evicted rows while baking history; stopping history backfill"
            );
            self.stop_backfill();
        }
        self.rebase_history_anchor()?;
        Ok(new_total.saturating_sub(old_total))
    }

    /// Bytes left in the replica's scrollback budget.
    fn budget_remaining(&self) -> usize {
        if self.budget_exhausted {
            return 0;
        }
        self.scrollback_limit_bytes
            .saturating_sub(self.fetched_bytes)
    }

    /// True when scrollback rows above the last-baked content are gone
    /// (ghostty eviction or an ED 3 erase), observed as a shrunken row total
    /// or changed anchor rows. Scrollback rows are immutable while they sit
    /// above the active screen, so a text change in the anchored top rows
    /// means the original rows were dropped and different content slid up
    /// into their place. Detection stays probabilistic in one pathological
    /// shape: content whose every row is identical can slide up by a whole
    /// number of rows and still match the anchor while a tail flood keeps
    /// the row total above the floor. The bounded failure mode is a visual
    /// gap in the oldest history.
    fn top_rows_lost_since_bake(
        &self,
        scrollbar: &TerminalScrollbar,
    ) -> Result<bool, GhosttyError> {
        if scrollbar.total < self.baked_total_rows {
            return Ok(true);
        }
        match &self.history_anchor {
            Some((rows, anchor)) => Ok(self.top_rows_text(*rows)? != *anchor),
            None => Ok(false),
        }
    }

    fn top_rows_text(&self, rows: usize) -> Result<String, GhosttyError> {
        let last_row = u32::try_from(rows.saturating_sub(1)).unwrap_or(0);
        self.terminal
            .read_text_screen((0, 0), (self.cols.saturating_sub(1), last_row), false)
    }

    /// Re-anchors eviction detection on the terminal's current content:
    /// records the row total and, once rows have scrolled into (immutable)
    /// scrollback, the text of the topmost few. Runs after open, every
    /// rebuild, and every resize, because baking and reflow legitimately
    /// change both.
    fn rebase_history_anchor(&mut self) -> Result<(), GhosttyError> {
        let scrollbar = self.terminal.scrollbar()?;
        self.baked_total_rows = scrollbar.total;
        let scrollback_rows = scrollbar.total.saturating_sub(scrollbar.len);
        self.history_anchor = if scrollback_rows > 0 {
            let rows = scrollback_rows.min(HISTORY_ANCHOR_ROWS);
            Some((rows, self.top_rows_text(rows)?))
        } else {
            None
        };
        Ok(())
    }

    /// Stops all further history paging: older pages can no longer be
    /// stitched on gap-free.
    fn stop_backfill(&mut self) {
        self.budget_exhausted = true;
        self.next_cursor = None;
    }
}

/// Held-back tail bytes are capped so a pathological giant string sequence
/// (huge OSC/APC payload) cannot buffer without bound; past the cap the bytes
/// are written through and rebuilds are deferred instead.
const HELD_TAIL_MAX_BYTES: usize = 64 * 1024;

/// Number of topmost scrollback rows recorded for eviction detection. One
/// row is enough for correctness on distinct content; several rows keep
/// identical repetitive top rows from colliding after an eviction.
const HISTORY_ANCHOR_ROWS: usize = 8;

/// Streaming scanner state tracking whether a VT byte stream currently rests
/// between complete escape sequences and UTF-8 codepoints ("ground"). Used to
/// hold back a trailing incomplete sequence from the replica terminal so a
/// rebuild never swaps out the VT parser mid-sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VtScanState {
    /// Between complete sequences and codepoints.
    Ground,
    /// Inside a multi-byte UTF-8 codepoint; `remaining` continuation bytes
    /// are still expected.
    Utf8 { remaining: u8 },
    /// After a bare ESC.
    Escape,
    /// Inside `ESC <intermediate...>` waiting for the final byte.
    EscapeIntermediate,
    /// Inside a CSI sequence waiting for its final byte.
    Csi,
    /// Inside an OSC/DCS/SOS/PM/APC string sequence waiting for its
    /// terminator (ST, or BEL for OSC).
    StringSeq { osc: bool },
    /// Saw ESC inside a string sequence; `\` completes the ST terminator.
    StringSeqEscape { osc: bool },
}

/// Advances the scanner by one byte. Deliberately lenient: malformed input
/// falls back toward `Ground` so the scanner can only hold bytes back for
/// genuine sequence prefixes, never wedge.
fn vt_scan_advance(state: VtScanState, byte: u8) -> VtScanState {
    use VtScanState::*;
    match state {
        Ground => match byte {
            0x1b => Escape,
            0xc2..=0xdf => Utf8 { remaining: 1 },
            0xe0..=0xef => Utf8 { remaining: 2 },
            0xf0..=0xf4 => Utf8 { remaining: 3 },
            _ => Ground,
        },
        Utf8 { remaining } => match byte {
            0x80..=0xbf if remaining > 1 => Utf8 {
                remaining: remaining - 1,
            },
            0x80..=0xbf => Ground,
            // Invalid continuation: rescan the byte from ground so a stray
            // lead byte cannot wedge the scanner.
            _ => vt_scan_advance(Ground, byte),
        },
        Escape => match byte {
            b'[' => Csi,
            b']' => StringSeq { osc: true },
            b'P' | b'X' | b'^' | b'_' => StringSeq { osc: false },
            0x20..=0x2f => EscapeIntermediate,
            0x1b => Escape,
            _ => Ground,
        },
        EscapeIntermediate => match byte {
            0x20..=0x2f => EscapeIntermediate,
            0x1b => Escape,
            _ => Ground,
        },
        Csi => match byte {
            0x40..=0x7e => Ground,
            0x1b => Escape,
            _ => Csi,
        },
        StringSeq { osc } => match byte {
            0x07 if osc => Ground,
            0x1b => StringSeqEscape { osc },
            _ => StringSeq { osc },
        },
        StringSeqEscape { osc } => match byte {
            b'\\' => Ground,
            0x07 if osc => Ground,
            0x1b => StringSeqEscape { osc },
            _ => StringSeq { osc },
        },
    }
}

/// A `stream.history` response that could not be applied.
#[derive(Debug)]
pub enum HistoryApplyError {
    /// The response was an error or not a `stream_history` result.
    Response(String),
    /// The replica terminal rejected an operation.
    Terminal(GhosttyError),
}

impl std::fmt::Display for HistoryApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Response(message) => write!(f, "{message}"),
            Self::Terminal(error) => write!(f, "replica terminal error: {error:?}"),
        }
    }
}

/// Keeps at most `max_bytes` of the tail of `text`, snapping the cut to a
/// char boundary and then past the next newline so the survivor never starts
/// mid-line or inside an escape sequence. Returns an empty string when the
/// tail holds no newline. Same shape as the handoff history truncation.
fn keep_tail_newline_aligned(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_owned();
    }
    let mut start = text.len() - max_bytes;
    while !text.is_char_boundary(start) {
        start += 1;
    }
    let Some(newline_offset) = text[start..].find('\n') else {
        return String::new();
    };
    start += newline_offset + 1;
    text[start..].to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::framed::{
        decode_history_cursor, encode_history_cursor, history_page_end_cut_mid_line,
        history_page_start, HistoryCursor, StreamHistoryParams,
    };

    /// Simulates the framed server's `stream.history` handler over an
    /// immutable capture, using the same cursor codec and page slicing, so
    /// the client walks the real cursor/sequence contract without sockets.
    struct TestHistoryServer {
        capture: String,
    }

    impl TestHistoryServer {
        fn from_terminal(
            terminal: &Terminal,
        ) -> (Self, crate::ghostty::TerminalStreamSeed, String) {
            let seed = terminal.stream_seed().unwrap();
            let server = Self {
                capture: seed.history.clone(),
            };
            let cursor = server.cursor(server.capture.len());
            (server, seed, cursor)
        }

        fn cursor(&self, offset: usize) -> String {
            encode_history_cursor(&HistoryCursor {
                pane_id: "p_1".to_owned(),
                sequence: 0,
                stream_id: 1,
                offset: offset as u64,
            })
        }

        /// Serves one page for a `stream.history` request built by the
        /// replica, overriding the byte budget so tests can force small
        /// pages the production policy would never request.
        fn serve_with_budget(
            &self,
            request: &serde_json::Value,
            max_bytes: usize,
        ) -> serde_json::Value {
            let params: StreamHistoryParams =
                serde_json::from_value(request["params"].clone()).unwrap();
            let cursor = decode_history_cursor(&params.cursor).expect("decodable cursor");
            let end = usize::try_from(cursor.offset).unwrap();
            assert!(end <= self.capture.len(), "cursor inside the capture");
            assert!(self.capture.is_char_boundary(end));
            let start = history_page_start(&self.capture, end, max_bytes);
            let next_cursor = (start > 0).then(|| self.cursor(start));
            serde_json::json!({
                "id": request["id"],
                "result": {
                    "type": "stream_history",
                    "stream_id": 1,
                    "content": &self.capture[start..end],
                    "next_cursor": next_cursor,
                    "at_top": start == 0,
                    "end_cut_mid_line": history_page_end_cut_mid_line(&self.capture, end),
                },
            })
        }

        fn serve(&self, request: &serde_json::Value) -> serde_json::Value {
            let params: StreamHistoryParams =
                serde_json::from_value(request["params"].clone()).unwrap();
            let max_bytes = usize::try_from(params.max_bytes.unwrap()).unwrap();
            self.serve_with_budget(request, max_bytes)
        }
    }

    fn screen_text(terminal: &Terminal) -> String {
        let cols = terminal.cols().unwrap();
        let total = terminal.total_rows().unwrap();
        terminal
            .read_text_screen((0, 0), (cols.saturating_sub(1), (total - 1) as u32), false)
            .unwrap()
    }

    /// Walks the replica's own paging policy against the test server until
    /// history is exhausted, forcing `page_bytes`-sized pages.
    fn backfill_all(replica: &mut PaneReplica, server: &TestHistoryServer, page_bytes: usize) {
        let mut fetches = 0;
        while let Some(request) = replica
            .take_backfill_request("t", BackfillTrigger::JumpToTop)
            .unwrap()
        {
            let response = server.serve_with_budget(&request, page_bytes);
            replica.apply_history_response(&response).unwrap();
            fetches += 1;
            assert!(fetches < 10_000, "backfill must terminate");
        }
    }

    fn metrics(offset: usize, max: usize, rows: usize) -> ScrollMetrics {
        ScrollMetrics {
            offset_from_bottom: offset,
            max_offset_from_bottom: max,
            viewport_rows: rows,
        }
    }

    fn ready_state(metrics: ScrollMetrics) -> BackfillState {
        BackfillState {
            metrics,
            history_exhausted: false,
            fetch_in_flight: false,
            budget_remaining: 10_000_000,
        }
    }

    #[test]
    fn plan_backfill_fetches_one_eager_page_for_focused_attach_only() {
        let state = ready_state(metrics(0, 500, 24));
        assert_eq!(
            plan_backfill(BackfillTrigger::Attach { focused: true }, state),
            Some(HISTORY_PAGE_DEFAULT_BYTES)
        );
        assert_eq!(
            plan_backfill(BackfillTrigger::Attach { focused: false }, state),
            None
        );
    }

    #[test]
    fn plan_backfill_scroll_triggers_two_screens_ahead_of_loaded_top() {
        // Far from the top: no fetch.
        assert_eq!(
            plan_backfill(BackfillTrigger::Scroll, ready_state(metrics(10, 500, 24))),
            None
        );
        // Exactly two screens from the top: fetch.
        assert_eq!(
            plan_backfill(BackfillTrigger::Scroll, ready_state(metrics(452, 500, 24))),
            Some(HISTORY_PAGE_DEFAULT_BYTES)
        );
        // At the loaded top: fetch.
        assert_eq!(
            plan_backfill(BackfillTrigger::Scroll, ready_state(metrics(500, 500, 24))),
            Some(HISTORY_PAGE_DEFAULT_BYTES)
        );
    }

    #[test]
    fn plan_backfill_jump_to_top_is_one_large_fetch() {
        let mut roomy = ready_state(metrics(0, 500, 24));
        roomy.budget_remaining = 2 * HISTORY_FETCH_MAX_BYTES;
        assert_eq!(
            plan_backfill(BackfillTrigger::JumpToTop, roomy),
            Some(HISTORY_FETCH_MAX_BYTES)
        );
        // The large fetch never exceeds the remaining scrollback budget.
        let mut tight = ready_state(metrics(0, 500, 24));
        tight.budget_remaining = 1_000_000;
        assert_eq!(
            plan_backfill(BackfillTrigger::JumpToTop, tight),
            Some(1_000_000)
        );
    }

    #[test]
    fn plan_backfill_never_stacks_or_overruns() {
        let base = ready_state(metrics(500, 500, 24));
        let mut in_flight = base;
        in_flight.fetch_in_flight = true;
        assert_eq!(plan_backfill(BackfillTrigger::Scroll, in_flight), None);
        assert_eq!(plan_backfill(BackfillTrigger::JumpToTop, in_flight), None);

        let mut exhausted = base;
        exhausted.history_exhausted = true;
        assert_eq!(plan_backfill(BackfillTrigger::Scroll, exhausted), None);

        let mut spent = base;
        spent.budget_remaining = 0;
        assert_eq!(plan_backfill(BackfillTrigger::JumpToTop, spent), None);

        // A small remaining budget shrinks the page instead of skipping it.
        let mut small = base;
        small.budget_remaining = 1024;
        assert_eq!(plan_backfill(BackfillTrigger::Scroll, small), Some(1024));
    }

    #[test]
    fn replica_seeds_from_snapshot_and_follows_live_tail() {
        let mut server = Terminal::new(40, 6, 1_000_000).unwrap();
        for line in 0..30 {
            server.write(format!("\x1b[3{}mline {line}\x1b[0m\r\n", line % 8).as_bytes());
        }
        server.write(b"\x1b[?2004h\x1b[1mprompt> ");
        let seed = server.stream_seed().unwrap();

        let mut replica =
            PaneReplica::open(&seed.snapshot, 77, None, seed.cols, seed.rows, 1_000_000).unwrap();
        assert_eq!(replica.applied_sequence(), 77);

        // The snapshot alone reproduces the visible screen and live modes.
        let cols = seed.cols;
        let rows = seed.rows;
        let visible = |t: &Terminal| {
            let total = t.total_rows().unwrap();
            let top = (total - rows as usize) as u32;
            t.read_text_screen((0, top), (cols - 1, (total - 1) as u32), false)
                .unwrap()
        };
        assert_eq!(visible(replica.terminal()), visible(&server));
        assert!(replica.terminal().mode_get(2004).unwrap());

        // The live tail keeps both sides identical, byte for byte.
        let tail = b"typed\r\n\x1b[42mgreen row\x1b[0m\r\nmore output\r\n";
        server.write(tail);
        let rows_prepended = replica.apply_tail(tail).unwrap();
        assert_eq!(rows_prepended, 0);
        assert_eq!(replica.applied_sequence(), 77 + tail.len() as u64);
        assert_eq!(visible(replica.terminal()), visible(&server));
    }

    #[test]
    fn backfill_rebuild_matches_locally_fed_terminal_and_survives_resize() {
        use std::fmt::Write as _;

        // Template: deep_scrollback_resize_preserves_unicode_and_hyperlinks.
        let mut input = String::from("\x1b]8;;https://example.com\x1b\\FIRST 🇧🇷\x1b]8;;\x1b\\\r\n");
        for line in 0..2_000 {
            write!(input, "{line:05} 👨‍👩‍👧\r\n").unwrap();
        }
        input.push_str("prompt> ");

        let mut local = Terminal::new(20, 5, 100_000_000).unwrap();
        local.write(input.as_bytes());

        let mut streamed = Terminal::new(20, 5, 100_000_000).unwrap();
        streamed.write(input.as_bytes());
        let (server, seed, cursor) = TestHistoryServer::from_terminal(&streamed);

        let mut replica = PaneReplica::open(
            &seed.snapshot,
            0,
            Some(cursor),
            seed.cols,
            seed.rows,
            100_000_000,
        )
        .unwrap();

        // Page the entire history back in small chunks: many rebuild rounds.
        backfill_all(&mut replica, &server, 8 * 1024);
        assert!(replica.history_exhausted());

        // The replica reaches the same depth with identical content: the
        // cursor contract left no gaps and no duplicates.
        assert_eq!(
            replica.terminal().total_rows().unwrap(),
            local.total_rows().unwrap()
        );
        assert_eq!(screen_text(replica.terminal()), screen_text(&local));

        // The very top of history is intact and reachable by purely local
        // scrolling. Hyperlink URIs are not asserted: the VT formatter does
        // not emit inline OSC 8 in content dumps yet
        // (vendor formatter.zig "only emit hyperlinks for HTML"), so every
        // dump/replay path — handoff seeding and history pages alike —
        // carries link text but not link targets.
        replica.scroll_delta(-100_000);
        assert!(replica
            .terminal()
            .read_text_viewport((0, 0), (19, 0), false)
            .unwrap()
            .starts_with("FIRST 🇧🇷"));
        replica.set_scroll_offset_from_bottom(0);

        // Resize reflows the backfilled history exactly like a local pane.
        replica.resize(10, 5, 8, 16).unwrap();
        local.resize(10, 5, 8, 16).unwrap();
        assert_eq!(screen_text(replica.terminal()), screen_text(&local));
    }

    #[test]
    fn mid_line_cut_pages_reassemble_split_logical_line_without_fabricated_breaks() {
        use std::fmt::Write as _;

        // One newline-free logical line far larger than the page budget, so
        // several hard-capped mid-line cuts land inside it and every rebuild
        // joins a cut page end onto already-baked content.
        let mut long_line = String::new();
        for segment in 0..3_000 {
            write!(long_line, "seg{segment:06} ").unwrap();
        }
        assert!(long_line.len() >= 30_000);
        assert!(!long_line.contains('\n'));
        // Enough short lines after the long one that the active screen holds
        // only whole lines: a logical line spanning the history/screen
        // boundary is truncated there by the snapshot+history capture design
        // itself, which is not what this test pins down.
        let mut input = format!("head line\r\n{long_line}\r\n");
        for tail_line in 0..8 {
            write!(input, "tail {tail_line}\r\n").unwrap();
        }
        input.push_str("prompt> ");

        let mut local = Terminal::new(40, 6, 100_000_000).unwrap();
        local.write(input.as_bytes());

        let mut streamed = Terminal::new(40, 6, 100_000_000).unwrap();
        streamed.write(input.as_bytes());
        let (server, seed, cursor) = TestHistoryServer::from_terminal(&streamed);
        let mut replica = PaneReplica::open(
            &seed.snapshot,
            0,
            Some(cursor),
            seed.cols,
            seed.rows,
            100_000_000,
        )
        .unwrap();

        // Page-budget pages: each rebuild bakes one page, so the next page's
        // end is the previous page's hard-capped mid-line start.
        backfill_all(&mut replica, &server, 4 * 1024);
        assert!(replica.history_exhausted());

        // The reassembled replica must match an unsplit reference terminal
        // exactly: same row total and identical text, which pins the logical
        // line count. A fabricated break per page boundary would split the
        // long line into one logical line per page and add rows.
        assert_eq!(
            replica.terminal().total_rows().unwrap(),
            local.total_rows().unwrap()
        );
        let replica_text = screen_text(replica.terminal());
        let local_text = screen_text(&local);
        assert_eq!(replica_text.lines().count(), local_text.lines().count());
        assert_eq!(replica_text, local_text);
    }

    #[test]
    fn backfill_preserves_scroll_position_and_reports_prepended_rows() {
        use std::fmt::Write as _;

        let mut input = String::new();
        for line in 0..300 {
            write!(input, "row {line:04}\r\n").unwrap();
        }
        input.push_str("bottom");

        let mut streamed = Terminal::new(40, 5, 10_000_000).unwrap();
        streamed.write(input.as_bytes());
        let (server, seed, cursor) = TestHistoryServer::from_terminal(&streamed);

        let mut replica = PaneReplica::open(
            &seed.snapshot,
            0,
            Some(cursor),
            seed.cols,
            seed.rows,
            10_000_000,
        )
        .unwrap();
        assert_eq!(replica.scroll_metrics().unwrap().max_offset_from_bottom, 0);

        // Scroll is pinned at the loaded top; the policy wants a page.
        assert!(replica.needs_backfill().unwrap());
        let request = replica
            .take_backfill_request("r1", BackfillTrigger::Scroll)
            .unwrap()
            .expect("fetch planned");
        // In flight: no second fetch is planned.
        assert!(replica
            .take_backfill_request("r2", BackfillTrigger::Scroll)
            .unwrap()
            .is_none());

        let response = server.serve_with_budget(&request, 2_000);
        let before = replica.scroll_metrics().unwrap();
        let rows_added = replica.apply_history_response(&response).unwrap();
        assert!(rows_added > 0, "backfill must prepend rows");
        let after = replica.scroll_metrics().unwrap();
        // Same content stays in view: the offset from the bottom held while
        // the reachable history above it grew.
        assert_eq!(after.offset_from_bottom, before.offset_from_bottom);
        assert_eq!(
            after.max_offset_from_bottom,
            before.max_offset_from_bottom + rows_added
        );
        // The newly loaded rows are immediately reachable by local scroll.
        replica.set_scroll_offset_from_bottom(after.max_offset_from_bottom);
        let top_row = replica
            .terminal()
            .read_text_screen((0, 0), (39, 0), false)
            .unwrap();
        assert!(top_row.starts_with("row "), "top row is history: {top_row}");
    }

    #[test]
    fn pages_queue_on_alternate_screen_and_apply_on_primary_return() {
        use std::fmt::Write as _;

        let mut input = String::new();
        for line in 0..100 {
            write!(input, "scroll {line:03}\r\n").unwrap();
        }
        input.push_str("shell$");

        let mut streamed = Terminal::new(40, 5, 10_000_000).unwrap();
        streamed.write(input.as_bytes());
        let (server, seed, cursor) = TestHistoryServer::from_terminal(&streamed);

        let mut replica = PaneReplica::open(
            &seed.snapshot,
            0,
            Some(cursor),
            seed.cols,
            seed.rows,
            10_000_000,
        )
        .unwrap();

        // The pane enters the alternate screen before the page arrives.
        replica.apply_tail(b"\x1b[?1049h\x1b[HALTSCREEN").unwrap();
        let request = replica
            .take_backfill_request("r1", BackfillTrigger::JumpToTop)
            .unwrap()
            .expect("fetch planned");
        let response = server.serve(&request);
        let rows_added = replica.apply_history_response(&response).unwrap();
        // Queued, not applied: the alternate screen keeps its content and
        // has no scrollback to corrupt.
        assert_eq!(rows_added, 0);
        assert_eq!(replica.scroll_metrics().unwrap().max_offset_from_bottom, 0);
        assert!(replica
            .terminal()
            .read_text_viewport((0, 0), (8, 0), false)
            .unwrap()
            .starts_with("ALTSCREEN"));

        // Leaving the alternate screen applies the queued page.
        let rows_added = replica.apply_tail(b"\x1b[?1049l").unwrap();
        assert!(rows_added > 0, "queued page applies on primary return");
        replica.set_scroll_offset_from_bottom(usize::MAX);
        assert!(replica
            .terminal()
            .read_text_screen((0, 0), (39, 0), false)
            .unwrap()
            .starts_with("scroll 000"));
    }

    #[test]
    fn replica_caps_history_at_its_own_scrollback_budget() {
        use std::fmt::Write as _;

        let mut input = String::new();
        for line in 0..400 {
            write!(input, "budget line {line:04}\r\n").unwrap();
        }
        input.push_str("end");

        let mut streamed = Terminal::new(40, 5, 10_000_000).unwrap();
        streamed.write(input.as_bytes());
        let (server, seed, cursor) = TestHistoryServer::from_terminal(&streamed);

        // A replica whose budget is far below the server's history.
        let budget = 2_048;
        let mut replica = PaneReplica::open(
            &seed.snapshot,
            0,
            Some(cursor),
            seed.cols,
            seed.rows,
            budget,
        )
        .unwrap();

        let request = replica
            .take_backfill_request("r1", BackfillTrigger::JumpToTop)
            .unwrap()
            .expect("fetch planned");
        let response = server.serve_with_budget(&request, 3 * budget);
        replica.apply_history_response(&response).unwrap();

        // The budget is spent: paging stops even though older history
        // remains on the server.
        assert!(replica.history_exhausted());
        assert!(!replica.needs_backfill().unwrap());
        assert!(replica
            .take_backfill_request("r2", BackfillTrigger::JumpToTop)
            .unwrap()
            .is_none());

        // The replica kept a bounded amount of history and its oldest kept
        // row starts on a line boundary, not mid-line.
        replica.set_scroll_offset_from_bottom(usize::MAX);
        let top_row = replica
            .terminal()
            .read_text_screen((0, 0), (39, 0), false)
            .unwrap();
        assert!(
            top_row.starts_with("budget line "),
            "front drop must stay newline-aligned: {top_row}"
        );
    }

    #[test]
    fn oversized_queued_page_is_front_trimmed_newline_aligned() {
        let mut page = String::new();
        for line in 0..50 {
            page.push_str(&format!("drop {line:02}\r\n"));
        }
        let mut replica = PaneReplica::open("seed", 0, Some("cursor".into()), 40, 5, 256).unwrap();
        // Bypass the terminal: apply a response whose single page overruns
        // the whole budget while on the alternate screen, so the page stays
        // queued and the trim is observable.
        replica.apply_tail(b"\x1b[?1049h").unwrap();
        let response = serde_json::json!({
            "id": "r1",
            "result": {
                "type": "stream_history",
                "stream_id": 1,
                "content": page,
                "next_cursor": "older",
                "at_top": false,
            },
        });
        replica.apply_history_response(&response).unwrap();
        assert!(replica.history_exhausted(), "dropping history stops paging");
        assert_eq!(replica.pages.len(), 1);
        let kept = replica.pages.front().unwrap();
        assert!(kept.len() <= 256);
        assert!(kept.starts_with("drop "), "trim keeps whole lines: {kept}");
        assert_eq!(replica.pages_bytes, kept.len());
    }

    #[test]
    fn copy_mode_selection_extraction_matches_local_pane_over_backfilled_rows() {
        use std::fmt::Write as _;

        let mut input = String::new();
        for line in 0..500 {
            write!(input, "copy {line:04} lorem ipsum dolor\r\n").unwrap();
        }
        input.push_str("prompt> ");

        let mut local = Terminal::new(30, 6, 10_000_000).unwrap();
        local.write(input.as_bytes());

        let mut streamed = Terminal::new(30, 6, 10_000_000).unwrap();
        streamed.write(input.as_bytes());
        let (server, seed, cursor) = TestHistoryServer::from_terminal(&streamed);
        let mut replica = PaneReplica::open(
            &seed.snapshot,
            0,
            Some(cursor),
            seed.cols,
            seed.rows,
            10_000_000,
        )
        .unwrap();
        backfill_all(&mut replica, &server, 4 * 1024);

        assert_eq!(
            replica.terminal().total_rows().unwrap(),
            local.total_rows().unwrap()
        );
        let total = local.total_rows().unwrap() as u32;

        // Copy-mode and selections read text through the exact
        // extract_selection shape: read_text_screen over absolute screen
        // rows. Every probed range over backfilled history must extract the
        // identical text a locally fed pane would produce.
        let probes = [
            ((0u16, 0u32), (29u16, 3u32)),                   // deep history block
            ((5, total / 2), (20, total / 2 + 2)),           // mid-history span
            ((0, total.saturating_sub(6)), (29, total - 1)), // active screen
            ((3, 1), (7, 1)),                                // single-row word
        ];
        for (start, end) in probes {
            let expected = local.read_text_screen(start, end, false).unwrap();
            let extracted = replica
                .terminal()
                .read_text_screen(start, end, false)
                .unwrap();
            assert_eq!(extracted, expected, "range {start:?}..{end:?}");
            assert!(!expected.is_empty());
        }
    }

    #[test]
    fn split_escape_and_utf8_across_rebuilds_still_render_intact() {
        use std::fmt::Write as _;

        let mut input = String::new();
        for line in 0..100 {
            write!(input, "hist {line:03}\r\n").unwrap();
        }
        input.push_str("prompt> ");

        let mut streamed = Terminal::new(40, 5, 10_000_000).unwrap();
        streamed.write(input.as_bytes());
        let (server, seed, cursor) = TestHistoryServer::from_terminal(&streamed);
        let mut replica = PaneReplica::open(
            &seed.snapshot,
            0,
            Some(cursor),
            seed.cols,
            seed.rows,
            10_000_000,
        )
        .unwrap();

        // The DATA tail splits an SGR sequence across chunks and a history
        // page (with its terminal-replacing rebuild) lands in the gap.
        replica.apply_tail(b"plain \x1b[3").unwrap();
        let request = replica
            .take_backfill_request("r1", BackfillTrigger::JumpToTop)
            .unwrap()
            .expect("fetch planned");
        let rows_added = replica
            .apply_history_response(&server.serve_with_budget(&request, 600))
            .unwrap();
        assert!(rows_added > 0, "the rebuild must run mid-split");

        // The continuation completes the CSI; the next chunk then ends with
        // three of the four bytes of a UTF-8 emoji and another rebuild lands
        // in that gap too.
        let globe = "🌍".as_bytes();
        let mut chunk = b"1mred\x1b[0m ".to_vec();
        chunk.extend_from_slice(&globe[..3]);
        replica.apply_tail(&chunk).unwrap();
        let request = replica
            .take_backfill_request("r2", BackfillTrigger::JumpToTop)
            .unwrap()
            .expect("second fetch planned");
        let rows_added = replica
            .apply_history_response(&server.serve_with_budget(&request, 600))
            .unwrap();
        assert!(rows_added > 0, "the second rebuild must run mid-codepoint");
        let mut tail_end = globe[3..].to_vec();
        tail_end.extend_from_slice(b" end");
        replica.apply_tail(&tail_end).unwrap();

        let total_tail = "plain \x1b[31mred\x1b[0m 🌍 end";
        assert_eq!(
            replica.applied_sequence(),
            total_tail.len() as u64,
            "held-back bytes still count as consumed"
        );

        // A locally fed terminal that saw the same bytes unsplit is the
        // ground truth: the split sequences must still act as one.
        let mut local = Terminal::new(40, 5, 10_000_000).unwrap();
        local.write(input.as_bytes());
        local.write(total_tail.as_bytes());
        let visible = |t: &Terminal| {
            let total = t.total_rows().unwrap();
            let top = (total - 5) as u32;
            t.read_text_screen((0, top), (39, (total - 1) as u32), false)
                .unwrap()
        };
        assert_eq!(visible(replica.terminal()), visible(&local));
        assert!(
            !visible(replica.terminal()).contains("1mred"),
            "split CSI must never render literally"
        );
    }

    #[test]
    fn backfill_stops_without_gap_when_ghostty_evicts_while_baking() {
        use std::fmt::Write as _;

        let mut input = String::new();
        for line in 0..20_000 {
            write!(input, "evict line {line:05}\r\n").unwrap();
        }
        input.push_str("bottom> ");

        let mut streamed = Terminal::new(40, 6, 100_000_000).unwrap();
        streamed.write(input.as_bytes());
        let (server, seed, cursor) = TestHistoryServer::from_terminal(&streamed);

        // The raw-byte budget (1 MB) covers the whole capture (~360 KB), but
        // ghostty's cell-memory accounting for a 1 MB terminal cannot hold
        // 20k rows: baking must evict, and paging must stop instead of
        // stitching older pages onto a dump missing evicted rows.
        let mut replica = PaneReplica::open(
            &seed.snapshot,
            0,
            Some(cursor),
            seed.cols,
            seed.rows,
            1_000_000,
        )
        .unwrap();
        backfill_all(&mut replica, &server, 32 * 1024);
        assert!(replica.history_exhausted());

        // Contiguity: everything the replica retained must be a gap-free
        // suffix of an unlimited reference terminal fed the same bytes.
        let mut reference = Terminal::new(40, 6, 100_000_000).unwrap();
        reference.write(input.as_bytes());
        let ref_text = screen_text(&reference);
        let replica_text = screen_text(replica.terminal());
        let ref_lines: Vec<&str> = ref_text.lines().collect();
        let replica_lines: Vec<&str> = replica_text.lines().collect();
        assert!(
            replica_lines.len() < ref_lines.len(),
            "eviction must have trimmed the replica's history"
        );
        assert_eq!(
            replica_lines[..],
            ref_lines[ref_lines.len() - replica_lines.len()..],
            "replica content must be a contiguous suffix, never gapped"
        );
    }

    #[test]
    fn tail_output_eviction_stops_paging_instead_of_stitching_a_gap() {
        use std::fmt::Write as _;

        let mut input = String::new();
        for line in 0..2_000 {
            write!(input, "old {line:04}\r\n").unwrap();
        }
        input.push_str("shell$ ");

        let mut streamed = Terminal::new(40, 6, 100_000_000).unwrap();
        streamed.write(input.as_bytes());
        let (server, seed, cursor) = TestHistoryServer::from_terminal(&streamed);
        // Raw budget far above every fetch below, so only eviction detection
        // can stop paging.
        let mut replica = PaneReplica::open(
            &seed.snapshot,
            0,
            Some(cursor),
            seed.cols,
            seed.rows,
            400_000,
        )
        .unwrap();

        // Bake one page so the replica holds real backfilled scrollback.
        let request = replica
            .take_backfill_request("r1", BackfillTrigger::JumpToTop)
            .unwrap()
            .expect("fetch planned");
        let rows_added = replica
            .apply_history_response(&server.serve_with_budget(&request, 4_096))
            .unwrap();
        assert!(rows_added > 0);
        assert!(!replica.history_exhausted());

        // A tail flood overruns ghostty's cell memory: the oldest rows —
        // including the baked page — are evicted while the raw budget is
        // still far from spent.
        let mut flood = String::new();
        for line in 0..30_000 {
            write!(flood, "flood {line:05}\r\n").unwrap();
        }
        replica.apply_tail(flood.as_bytes()).unwrap();

        // The next page would stitch under content whose top rows are gone;
        // the replica must refuse and stop paging instead.
        let request = replica
            .take_backfill_request("r2", BackfillTrigger::JumpToTop)
            .unwrap()
            .expect("second fetch planned");
        let rows_added = replica
            .apply_history_response(&server.serve_with_budget(&request, 4_096))
            .unwrap();
        assert_eq!(
            rows_added, 0,
            "no page may be stitched over an eviction gap"
        );
        assert!(replica.history_exhausted());
        assert!(replica
            .take_backfill_request("r3", BackfillTrigger::JumpToTop)
            .unwrap()
            .is_none());

        // Contiguity: the replica content is a gap-free suffix of a
        // reference terminal fed the same bytes with unlimited scrollback.
        let mut reference = Terminal::new(40, 6, 100_000_000).unwrap();
        reference.write(input.as_bytes());
        reference.write(flood.as_bytes());
        let ref_text = screen_text(&reference);
        let replica_text = screen_text(replica.terminal());
        let ref_lines: Vec<&str> = ref_text.lines().collect();
        let replica_lines: Vec<&str> = replica_text.lines().collect();
        assert!(!replica_lines.is_empty());
        assert_eq!(
            replica_lines[..],
            ref_lines[ref_lines.len() - replica_lines.len()..],
            "replica content must be a contiguous suffix, never gapped"
        );
    }

    #[test]
    fn vt_scan_advance_ground_contract() {
        let scan = |bytes: &[u8]| {
            bytes.iter().fold(VtScanState::Ground, |state, &byte| {
                vt_scan_advance(state, byte)
            })
        };
        // Complete sequences end at ground.
        assert_eq!(scan(b"plain text"), VtScanState::Ground);
        assert_eq!(scan(b"\x1b[31m"), VtScanState::Ground);
        assert_eq!(scan(b"\x1b[?1049h"), VtScanState::Ground);
        assert_eq!(scan(b"\x1b(B"), VtScanState::Ground);
        assert_eq!(scan(b"\x1b]0;title\x07"), VtScanState::Ground);
        assert_eq!(scan(b"\x1b]8;;http://x\x1b\\"), VtScanState::Ground);
        assert_eq!(scan(b"\x1bP1$r\x1b\\"), VtScanState::Ground);
        assert_eq!(scan("🌍".as_bytes()), VtScanState::Ground);
        // Incomplete prefixes are not ground.
        assert_ne!(scan(b"\x1b"), VtScanState::Ground);
        assert_ne!(scan(b"\x1b[3"), VtScanState::Ground);
        assert_ne!(scan(b"\x1b]0;tit"), VtScanState::Ground);
        assert_ne!(scan(b"\x1b]0;tit\x1b"), VtScanState::Ground);
        assert_ne!(scan(b"\x1b("), VtScanState::Ground);
        assert_ne!(scan(&"🌍".as_bytes()[..3]), VtScanState::Ground);
        // A stray continuation byte cannot wedge the scanner.
        assert_eq!(scan(b"\xf0\x41"), VtScanState::Ground);
    }

    #[test]
    fn history_error_response_clears_in_flight_and_surfaces_error() {
        let mut replica =
            PaneReplica::open("seed", 0, Some("cursor".into()), 40, 5, 10_000).unwrap();
        let request = replica
            .take_backfill_request("r1", BackfillTrigger::JumpToTop)
            .unwrap();
        assert!(request.is_some());
        let error = serde_json::json!({
            "id": "r1",
            "error": {"code": "invalid_cursor", "message": "stale"},
        });
        assert!(replica.apply_history_response(&error).is_err());
        // The failed fetch is no longer in flight, so a retry can be
        // planned (the cursor is still set).
        assert!(!replica.backfill_state().unwrap().fetch_in_flight);
    }

    #[test]
    fn keep_tail_newline_aligned_contract() {
        assert_eq!(keep_tail_newline_aligned("a\r\nb", 64), "a\r\nb");
        assert_eq!(keep_tail_newline_aligned("aaa\r\nbbb\r\nccc", 8), "ccc");
        assert_eq!(keep_tail_newline_aligned("no newline at all", 4), "");
        // Multi-byte characters never split.
        let text = format!("top\r\n{}\r\ntail", "👨‍👩‍👧".repeat(10));
        let kept = keep_tail_newline_aligned(&text, 6);
        assert_eq!(kept, "tail");
    }
}

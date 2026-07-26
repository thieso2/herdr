//! Pane scroll and raw input routing shared by every writable pane client.
//!
//! Scrolling a pane is not one behavior: it depends on what the program in
//! the pane asked for. Programs that requested mouse reporting get encoded
//! wheel events, programs on the alternate screen without mouse reporting get
//! alternate-scroll keys, and everything else scrolls the pane's own
//! scrollback. Page keys follow the pane's declared page-key policy. Both the
//! legacy direct-attach path and framed write-mode pane streams route through
//! this module so the rules stay in one place.

use bytes::Bytes;
use crossterm::event::{KeyModifiers, MouseEventKind};

use crate::terminal::TerminalRuntime;

/// Which way a scroll request moves the pane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PaneScrollDirection {
    Up,
    Down,
}

/// What produced a scroll request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PaneScrollSource {
    /// A wheel notch, with the pointer position carried alongside.
    Wheel,
    /// An unmodified PageUp/PageDown press.
    PageKey,
}

/// One scroll request against a pane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PaneScrollRequest {
    pub(crate) source: PaneScrollSource,
    pub(crate) direction: PaneScrollDirection,
    /// Rows to move; clamped to at least one.
    pub(crate) lines: u16,
    pub(crate) column: Option<u16>,
    pub(crate) row: Option<u16>,
    /// Crossterm modifier bits active with the event.
    pub(crate) modifiers: u8,
}

/// Default page-key bytes forwarded when the pane wants the keys themselves.
pub(crate) fn page_key_input(direction: PaneScrollDirection) -> Vec<u8> {
    match direction {
        PaneScrollDirection::Up => b"\x1b[5~".to_vec(),
        PaneScrollDirection::Down => b"\x1b[6~".to_vec(),
    }
}

/// Applies a scroll request using the pane's own wheel and page-key routing.
pub(crate) fn apply_pane_scroll(
    runtime: &TerminalRuntime,
    request: PaneScrollRequest,
) -> Result<(), String> {
    apply_pane_scroll_with_page_input(runtime, request, None)
}

/// Same as [`apply_pane_scroll`], but lets the caller supply the exact page-key
/// bytes it received instead of the canonical sequence.
pub(crate) fn apply_pane_scroll_with_page_input(
    runtime: &TerminalRuntime,
    request: PaneScrollRequest,
    page_key_bytes: Option<Vec<u8>>,
) -> Result<(), String> {
    let wheel_kind = match request.direction {
        PaneScrollDirection::Up => MouseEventKind::ScrollUp,
        PaneScrollDirection::Down => MouseEventKind::ScrollDown,
    };
    let lines = request.lines.max(1) as usize;

    if request.source == PaneScrollSource::PageKey {
        let host_scroll = runtime
            .input_state()
            .is_some_and(crate::pane::InputState::plain_page_keys_use_host_scrollback);
        if host_scroll {
            match request.direction {
                PaneScrollDirection::Up => runtime.scroll_up(lines),
                PaneScrollDirection::Down => runtime.scroll_down(lines),
            }
            return Ok(());
        }
        let input = page_key_bytes.unwrap_or_else(|| page_key_input(request.direction));
        return apply_pane_input(runtime, input);
    }

    match runtime.wheel_routing() {
        Some(crate::pane::WheelRouting::MouseReport) => {
            runtime.scroll_reset();
            let column = request.column.unwrap_or(0);
            let row = request.row.unwrap_or(0);
            let Some(bytes) = runtime.encode_mouse_wheel(
                wheel_kind,
                column,
                row,
                KeyModifiers::from_bits_truncate(request.modifiers),
            ) else {
                return Err(format!(
                    "failed to encode terminal attach mouse wheel event: {wheel_kind:?}"
                ));
            };
            runtime
                .try_send_bytes(Bytes::from(bytes))
                .map_err(|err| format!("terminal attach mouse wheel input failed: {err}"))?;
        }
        Some(crate::pane::WheelRouting::AlternateScroll) => {
            runtime.scroll_reset();
            let Some(bytes) = runtime.encode_alternate_scroll(wheel_kind) else {
                return Ok(());
            };
            runtime
                .try_send_bytes(Bytes::from(bytes))
                .map_err(|err| format!("terminal attach alternate scroll input failed: {err}"))?;
        }
        Some(crate::pane::WheelRouting::HostScroll) | None => match request.direction {
            PaneScrollDirection::Up => runtime.scroll_up(lines),
            PaneScrollDirection::Down => runtime.scroll_down(lines),
        },
    }
    Ok(())
}

/// Writes raw client input to the pane, snapping the viewport back to the
/// live tail first the way a terminal does when you type.
pub(crate) fn apply_pane_input(runtime: &TerminalRuntime, data: Vec<u8>) -> Result<(), String> {
    runtime.scroll_reset();
    runtime
        .try_send_bytes(Bytes::from(data))
        .map_err(|err| format!("terminal attach input failed: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_key_input_uses_the_canonical_sequences() {
        assert_eq!(page_key_input(PaneScrollDirection::Up), b"\x1b[5~");
        assert_eq!(page_key_input(PaneScrollDirection::Down), b"\x1b[6~");
    }
}

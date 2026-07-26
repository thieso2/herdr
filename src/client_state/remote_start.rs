//! Start-a-stopped-remote confirmation prompt.
//!
//! A remote that is reachable, has herdr installed, and has no server
//! running parks in [`super::ClientConnectionState::Stopped`]. Starting one
//! spawns a daemon on someone else's machine that outlives the connection,
//! so it never happens behind the user's back: this prompt is how they say
//! yes, and dismissing it leaves the remote dimmed until they do.
//!
//! Pure data plus pure key interpretation, following the
//! [`super::remote_edit`] dialog grammar: geometry and drawing live in the
//! render layer, the IO (the `remote.start` request) in the run loop.

use crossterm::event::{KeyCode, KeyModifiers};

use crate::input::TerminalKey;

/// State of the "start this remote?" confirmation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteStartPrompt {
    /// Which remote the prompt is about.
    pub(crate) remote: usize,
    /// Its display name, for the prompt body and the request.
    pub(crate) name: String,
    /// Why it is stopped, phrased as the status line the CLI also shows.
    pub(crate) status: String,
    /// Set when a start was attempted and failed; the prompt stays open so
    /// the reason is readable and the user can retry.
    pub(crate) error: Option<String>,
    /// A start is in flight. The prompt stays open and refuses a second
    /// approval, so an impatient double-press cannot spawn two daemons.
    pub(crate) starting: bool,
}

/// What a key did to the prompt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoteStartKeyResult {
    /// The prompt consumed the key without deciding anything.
    Ignored,
    /// Enter or `y`: the run loop starts the remote.
    Start,
    /// Esc or `n`: leave it stopped and close.
    Dismiss,
}

/// Pure key interpretation. Deliberately narrow: this prompt writes to
/// another machine, so only an unambiguous yes starts anything, and every
/// unrecognized key is ignored rather than guessed at.
pub(crate) fn remote_start_apply_key(
    prompt: &RemoteStartPrompt,
    key: TerminalKey,
) -> RemoteStartKeyResult {
    if key.modifiers.contains(KeyModifiers::CONTROL) {
        return RemoteStartKeyResult::Ignored;
    }
    // While a start is in flight the only answer left is to walk away: a
    // second yes would spawn a second daemon.
    if prompt.starting {
        return match key.code {
            KeyCode::Esc | KeyCode::Char('q') => RemoteStartKeyResult::Dismiss,
            _ => RemoteStartKeyResult::Ignored,
        };
    }
    match key.code {
        KeyCode::Enter | KeyCode::Char('y') | KeyCode::Char('Y') => RemoteStartKeyResult::Start,
        KeyCode::Esc | KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Char('q') => {
            RemoteStartKeyResult::Dismiss
        }
        _ => RemoteStartKeyResult::Ignored,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(code: KeyCode) -> TerminalKey {
        TerminalKey::new(code, KeyModifiers::empty())
    }

    fn ctrl(c: char) -> TerminalKey {
        TerminalKey::new(KeyCode::Char(c), KeyModifiers::CONTROL)
    }

    fn prompt(starting: bool) -> RemoteStartPrompt {
        RemoteStartPrompt {
            remote: 1,
            name: "gpu-1".into(),
            status: "no server running".into(),
            error: None,
            starting,
        }
    }

    #[test]
    fn a_start_in_flight_refuses_a_second_yes() {
        // Approving twice would spawn a second daemon on that host; only
        // walking away is still available.
        let busy = prompt(true);
        for approve in [KeyCode::Enter, KeyCode::Char('y')] {
            assert_eq!(
                remote_start_apply_key(&busy, key(approve)),
                RemoteStartKeyResult::Ignored,
                "{approve:?}"
            );
        }
        assert_eq!(
            remote_start_apply_key(&busy, key(KeyCode::Esc)),
            RemoteStartKeyResult::Dismiss,
            "walking away stays available while a start is in flight"
        );
    }

    #[test]
    fn only_an_unambiguous_yes_starts_a_remote() {
        // Starting writes a daemon to another machine, so a stray keypress
        // must never be read as approval.
        for approve in [KeyCode::Enter, KeyCode::Char('y'), KeyCode::Char('Y')] {
            assert_eq!(
                remote_start_apply_key(&prompt(false), key(approve)),
                RemoteStartKeyResult::Start,
                "{approve:?}"
            );
        }
        for dismiss in [
            KeyCode::Esc,
            KeyCode::Char('n'),
            KeyCode::Char('N'),
            KeyCode::Char('q'),
        ] {
            assert_eq!(
                remote_start_apply_key(&prompt(false), key(dismiss)),
                RemoteStartKeyResult::Dismiss,
                "{dismiss:?}"
            );
        }
        for ignored in [KeyCode::Tab, KeyCode::Char('x'), KeyCode::Backspace] {
            assert_eq!(
                remote_start_apply_key(&prompt(false), key(ignored)),
                RemoteStartKeyResult::Ignored,
                "{ignored:?}"
            );
        }
        // Ctrl-y is a terminal control code, not approval.
        assert_eq!(
            remote_start_apply_key(&prompt(false), ctrl('y')),
            RemoteStartKeyResult::Ignored
        );
    }
}

//! Remotes list modal state and pure key interpretation.
//!
//! The fleet as a list: every configured entry, including disabled ones,
//! with its live connection status. It owns list-shaped operations only —
//! reorder, enable/disable, remove, start/stop — and hands field editing to
//! the existing single-remote dialog rather than rebuilding it.
//!
//! Same three-layer split as that dialog: plain data and pure key
//! interpretation here, geometry and drawing in `crate::ui::remote_chips`,
//! and all IO in the run loop. Every action commits immediately and
//! individually through the transactional fleet-config update, so there is
//! no draft to reconcile and closing never discards work.

use crossterm::event::{KeyCode, KeyModifiers};

use crate::fleet::config::{MoveDirection, RemoteEntry};
use crate::input::TerminalKey;

/// A remote's live connection state, as the list shows it.
///
/// Distinct from the chip's vocabulary in one way that matters here: a
/// disabled remote has no connection at all, because it has no descriptor
/// and no mirror. The list is the only surface that shows those, so it is
/// the only one that needs the state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoteListStatus {
    Connected,
    Connecting,
    Offline,
    Stopped,
    /// Configured but deliberately taken out of the fleet.
    Disabled,
    /// Enabled, but the client holds no mirror for it yet.
    Unknown,
}

/// One row: a configured entry plus how it is doing right now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteListRow {
    pub(crate) entry: RemoteEntry,
    pub(crate) status: RemoteListStatus,
}

/// State of the remotes list modal.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RemoteListState {
    pub(crate) rows: Vec<RemoteListRow>,
    /// Index of the selected row. Presentation only: every mutation is
    /// addressed by name, so a list something else reordered cannot make
    /// this index act on the wrong remote.
    pub(crate) selected: usize,
    /// A refused write, shown in the modal. The file is untouched and the
    /// modal stays open.
    pub(crate) error: Option<String>,
}

impl RemoteListState {
    pub(crate) fn new(rows: Vec<RemoteListRow>) -> Self {
        Self {
            rows,
            selected: 0,
            error: None,
        }
    }

    pub(crate) fn selected_row(&self) -> Option<&RemoteListRow> {
        self.rows.get(self.selected)
    }

    /// The name of the selected entry, which is what every action is keyed
    /// on.
    pub(crate) fn selected_name(&self) -> Option<&str> {
        self.selected_row().map(|row| row.entry.name.as_str())
    }

    /// Replaces the rows after a write, keeping the selection on the entry
    /// the user had selected.
    ///
    /// Selection follows the *name*, not the row number: a commit can
    /// reorder the list, and an external edit can change it under us. When
    /// the tracked entry is gone entirely, fall back to the nearest
    /// surviving row rather than jumping to the top.
    pub(crate) fn reload(&mut self, rows: Vec<RemoteListRow>) {
        let tracked = self.selected_name().map(|name| name.to_owned());
        let previous = self.selected;
        self.rows = rows;
        self.selected = tracked
            .and_then(|name| {
                self.rows
                    .iter()
                    .position(|row| row.entry.name == name)
            })
            .unwrap_or_else(|| previous.min(self.rows.len().saturating_sub(1)));
    }
}

/// Builds the modal's rows from the config plus live connection state.
///
/// The config is the source of rows, not the descriptor list: descriptors
/// exclude disabled entries, and a list that hides a disabled remote gives
/// the user no way to find it again and re-enable it. Live status is
/// correlated by *name*, the same key every mutation uses.
pub(crate) fn remote_list_rows(
    entries: &[RemoteEntry],
    descriptors: &[super::fleet_view::RemoteDescriptor],
    mirrors: &super::RemoteMirrors,
) -> Vec<RemoteListRow> {
    entries
        .iter()
        .map(|entry| {
            let status = if !entry.enabled {
                RemoteListStatus::Disabled
            } else {
                descriptors
                    .iter()
                    .find(|descriptor| descriptor.name == entry.name)
                    .and_then(|descriptor| mirrors.get(descriptor.index))
                    .map(|mirror| match &mirror.connection {
                        super::ClientConnectionState::Connected { .. } => {
                            RemoteListStatus::Connected
                        }
                        super::ClientConnectionState::Connecting { .. } => {
                            RemoteListStatus::Connecting
                        }
                        super::ClientConnectionState::Stopped { .. } => RemoteListStatus::Stopped,
                        super::ClientConnectionState::Offline { .. }
                        | super::ClientConnectionState::Incompatible { .. } => {
                            RemoteListStatus::Offline
                        }
                        super::ClientConnectionState::Disconnected => RemoteListStatus::Unknown,
                    })
                    .unwrap_or(RemoteListStatus::Unknown)
            };
            RemoteListRow {
                entry: entry.clone(),
                status,
            }
        })
        .collect()
}

impl RemoteListStatus {
    /// The dot glyph, in the chip strip's vocabulary so the two surfaces
    /// agree on what a remote's state looks like.
    pub(crate) fn dot(self) -> &'static str {
        match self {
            Self::Connected => "●",
            Self::Connecting => "◐",
            Self::Stopped => "◍",
            Self::Offline | Self::Unknown => "○",
            Self::Disabled => "·",
        }
    }

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Connected => "connected",
            Self::Connecting => "connecting",
            Self::Stopped => "stopped",
            Self::Offline => "offline",
            Self::Disabled => "disabled",
            Self::Unknown => "",
        }
    }
}

/// What a key asked the run loop to do.
///
/// Every variant that writes carries the entry's *name*: each closure runs
/// against a list loaded inside the fleet lock, so an index-based mutation
/// applied to a list something else reordered would corrupt it silently,
/// while a name-based one is either correct or a no-op.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RemoteListKeyResult {
    /// Consumed: selection moved, or an error was cleared.
    Consumed,
    /// Nothing this modal handles.
    Ignored,
    /// Move the named entry one slot.
    Reorder(String, MoveDirection),
    /// Flip the named entry's `enabled` flag.
    ToggleEnabled(String),
    /// Hand the named entry to the single-remote field dialog.
    Edit(String),
    /// Remove the named entry from the fleet.
    Remove(String),
    /// Start the named remote's server, or stop it if it is running.
    StartStop(String),
    /// `[done]` or Escape. Not a revert: every action already took effect.
    Close,
}

/// Pure key interpretation for the modal: state plus key in, an action out.
///
/// Follows the single-remote dialog's grammar so the two surfaces feel the
/// same. Plain arrows move the selection; shifted arrows move the selected
/// *row*, which is the reorder.
pub(crate) fn remote_list_apply_key(
    state: &mut RemoteListState,
    key: TerminalKey,
) -> RemoteListKeyResult {
    let shifted = key.modifiers.contains(KeyModifiers::SHIFT);
    // Any key clears a stale error: the next action reports its own.
    let name = state.selected_name().map(|name| name.to_owned());

    match key.code {
        KeyCode::Esc | KeyCode::Char('q') => RemoteListKeyResult::Close,

        KeyCode::Up | KeyCode::Char('k') if shifted => name
            .map(|name| RemoteListKeyResult::Reorder(name, MoveDirection::Up))
            .unwrap_or(RemoteListKeyResult::Consumed),
        KeyCode::Down | KeyCode::Char('j') if shifted => name
            .map(|name| RemoteListKeyResult::Reorder(name, MoveDirection::Down))
            .unwrap_or(RemoteListKeyResult::Consumed),

        KeyCode::Up | KeyCode::Char('k') => {
            state.error = None;
            state.selected = state.selected.saturating_sub(1);
            RemoteListKeyResult::Consumed
        }
        KeyCode::Down | KeyCode::Char('j') => {
            state.error = None;
            let last = state.rows.len().saturating_sub(1);
            state.selected = state.selected.saturating_add(1).min(last);
            RemoteListKeyResult::Consumed
        }
        KeyCode::Home => {
            state.error = None;
            state.selected = 0;
            RemoteListKeyResult::Consumed
        }
        KeyCode::End => {
            state.error = None;
            state.selected = state.rows.len().saturating_sub(1);
            RemoteListKeyResult::Consumed
        }

        KeyCode::Enter => name
            .map(RemoteListKeyResult::Edit)
            .unwrap_or(RemoteListKeyResult::Consumed),
        KeyCode::Char(' ') => name
            .map(RemoteListKeyResult::ToggleEnabled)
            .unwrap_or(RemoteListKeyResult::Consumed),
        KeyCode::Char('s') => name
            .map(RemoteListKeyResult::StartStop)
            .unwrap_or(RemoteListKeyResult::Consumed),
        KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => name
            .map(RemoteListKeyResult::Remove)
            .unwrap_or(RemoteListKeyResult::Consumed),
        KeyCode::Delete => name
            .map(RemoteListKeyResult::Remove)
            .unwrap_or(RemoteListKeyResult::Consumed),

        _ => RemoteListKeyResult::Ignored,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(code: KeyCode) -> TerminalKey {
        TerminalKey::new(code, KeyModifiers::empty())
    }

    fn shift(code: KeyCode) -> TerminalKey {
        TerminalKey::new(code, KeyModifiers::SHIFT)
    }

    fn row(name: &str, enabled: bool, status: RemoteListStatus) -> RemoteListRow {
        RemoteListRow {
            entry: RemoteEntry {
                name: name.to_owned(),
                target: Some(format!("can@{name}.example")),
                session: "default".to_owned(),
                enabled,
                hue: None,
            },
            status,
        }
    }

    fn three() -> RemoteListState {
        RemoteListState::new(vec![
            row("a", true, RemoteListStatus::Connected),
            row("b", false, RemoteListStatus::Disabled),
            row("c", true, RemoteListStatus::Stopped),
        ])
    }

    #[test]
    fn selection_moves_and_clamps_at_both_ends() {
        let mut state = three();
        assert_eq!(state.selected, 0);

        assert_eq!(
            remote_list_apply_key(&mut state, key(KeyCode::Up)),
            RemoteListKeyResult::Consumed
        );
        assert_eq!(state.selected, 0, "clamps at the top");

        remote_list_apply_key(&mut state, key(KeyCode::Down));
        remote_list_apply_key(&mut state, key(KeyCode::Down));
        assert_eq!(state.selected, 2);
        remote_list_apply_key(&mut state, key(KeyCode::Down));
        assert_eq!(state.selected, 2, "clamps at the bottom");

        remote_list_apply_key(&mut state, key(KeyCode::Home));
        assert_eq!(state.selected, 0);
        remote_list_apply_key(&mut state, key(KeyCode::End));
        assert_eq!(state.selected, 2);
    }

    #[test]
    fn every_action_is_keyed_by_name_not_by_row() {
        let mut state = three();
        state.selected = 1;

        assert_eq!(
            remote_list_apply_key(&mut state, shift(KeyCode::Up)),
            RemoteListKeyResult::Reorder("b".into(), MoveDirection::Up)
        );
        assert_eq!(
            remote_list_apply_key(&mut state, shift(KeyCode::Down)),
            RemoteListKeyResult::Reorder("b".into(), MoveDirection::Down)
        );
        assert_eq!(
            remote_list_apply_key(&mut state, key(KeyCode::Char(' '))),
            RemoteListKeyResult::ToggleEnabled("b".into())
        );
        assert_eq!(
            remote_list_apply_key(&mut state, key(KeyCode::Enter)),
            RemoteListKeyResult::Edit("b".into())
        );
        assert_eq!(
            remote_list_apply_key(&mut state, key(KeyCode::Char('s'))),
            RemoteListKeyResult::StartStop("b".into())
        );
        assert_eq!(
            remote_list_apply_key(&mut state, key(KeyCode::Delete)),
            RemoteListKeyResult::Remove("b".into())
        );
        assert_eq!(
            remote_list_apply_key(
                &mut state,
                TerminalKey::new(KeyCode::Char('d'), KeyModifiers::CONTROL)
            ),
            RemoteListKeyResult::Remove("b".into())
        );
    }

    #[test]
    fn escape_and_q_close_without_reverting() {
        // `esc` is not a revert: the actions already took effect on the
        // running fleet, and rewriting the file would not undo a started
        // server or a disconnected remote.
        let mut state = three();
        assert_eq!(
            remote_list_apply_key(&mut state, key(KeyCode::Esc)),
            RemoteListKeyResult::Close
        );
        assert_eq!(
            remote_list_apply_key(&mut state, key(KeyCode::Char('q'))),
            RemoteListKeyResult::Close
        );
    }

    #[test]
    fn an_empty_fleet_swallows_actions_rather_than_naming_nothing() {
        let mut state = RemoteListState::new(Vec::new());
        for code in [
            KeyCode::Enter,
            KeyCode::Char(' '),
            KeyCode::Char('s'),
            KeyCode::Delete,
        ] {
            assert_eq!(
                remote_list_apply_key(&mut state, key(code)),
                RemoteListKeyResult::Consumed
            );
        }
        assert_eq!(state.selected, 0);
    }

    #[test]
    fn a_reload_keeps_the_selection_on_the_entry_the_user_picked() {
        let mut state = three();
        state.selected = 2;
        assert_eq!(state.selected_name(), Some("c"));

        // A commit reordered the list; selection follows the name.
        state.reload(vec![
            row("c", true, RemoteListStatus::Stopped),
            row("a", true, RemoteListStatus::Connected),
            row("b", false, RemoteListStatus::Disabled),
        ]);
        assert_eq!(state.selected, 0);
        assert_eq!(state.selected_name(), Some("c"));

        // Something else removed it: fall back to the nearest surviving row
        // rather than jumping to the top.
        state.selected = 2;
        state.reload(vec![
            row("c", true, RemoteListStatus::Stopped),
            row("a", true, RemoteListStatus::Connected),
        ]);
        assert_eq!(state.selected, 1);

        state.reload(Vec::new());
        assert_eq!(state.selected, 0, "an empty fleet has nowhere to be");
        assert_eq!(state.selected_name(), None);
    }

    #[test]
    fn an_error_from_a_refused_write_survives_until_the_next_move() {
        let mut state = three();
        state.error = Some("duplicate remote name 'a'".to_owned());

        // Asking for another write does not clear it; the write's own
        // outcome will.
        remote_list_apply_key(&mut state, key(KeyCode::Enter));
        assert!(state.error.is_some(), "the modal stays open with its error");

        remote_list_apply_key(&mut state, key(KeyCode::Down));
        assert_eq!(state.error, None, "moving on clears it");
    }
}

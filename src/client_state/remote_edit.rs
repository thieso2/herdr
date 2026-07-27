//! Add/edit-remote dialog state and pure key interpretation.
//!
//! The dialog is pure-client chrome: plain data here, geometry and drawing
//! in `crate::ui::remote_chips`, IO (config save, fleet reconcile) in the
//! run loop. Field editing, focus cycling, and submit/cancel resolution are
//! pure so the whole dialog grammar is testable without a terminal.

use crossterm::event::{KeyCode, KeyModifiers};

use crate::input::TerminalKey;

/// Dialog fields, in focus order.
pub(crate) const REMOTE_EDIT_FIELDS: usize = 3;

/// State of the add/edit-remote dialog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteEditState {
    /// `Some(name)` when editing an existing remote; `None` when adding.
    pub(crate) original_name: Option<String>,
    pub(crate) name: String,
    pub(crate) target: String,
    pub(crate) session: String,
    /// 0 = name, 1 = target, 2 = session.
    pub(crate) focused_field: usize,
    pub(crate) error: Option<String>,
    /// The edited entry's `enabled` flag, carried through untouched. The
    /// dialog edits fields only: changing a hostname must not silently
    /// reconnect a machine the user deliberately took out of the fleet.
    pub(crate) enabled: bool,
    /// The edited entry's stored hue, carried through so a field edit never
    /// recolours the remote. `None` when adding: the config layer allocates.
    pub(crate) hue: Option<usize>,
}

// Hand-written rather than derived: a dialog with no entry behind it is an
// *add*, and a remote you just added is enabled.
impl Default for RemoteEditState {
    fn default() -> Self {
        Self {
            original_name: None,
            name: String::new(),
            target: String::new(),
            session: String::new(),
            focused_field: 0,
            error: None,
            enabled: true,
            hue: None,
        }
    }
}

impl RemoteEditState {
    pub(crate) fn add() -> Self {
        Self {
            session: crate::session::DEFAULT_SESSION_NAME.to_owned(),
            ..Self::default()
        }
    }

    pub(crate) fn edit(entry: &crate::fleet::config::RemoteEntry) -> Self {
        Self {
            original_name: Some(entry.name.clone()),
            name: entry.name.clone(),
            target: entry.target.clone().unwrap_or_default(),
            session: entry.session.clone(),
            focused_field: 0,
            error: None,
            enabled: entry.enabled,
            hue: entry.hue,
        }
    }

    pub(crate) fn is_edit(&self) -> bool {
        self.original_name.is_some()
    }

    fn focused_input(&mut self) -> &mut String {
        match self.focused_field {
            0 => &mut self.name,
            1 => &mut self.target,
            _ => &mut self.session,
        }
    }

    /// The entry this dialog describes, validated. Renames keep the entry
    /// keyed by the new name; the run loop removes the original first.
    pub(crate) fn entry(&self) -> Result<crate::fleet::config::RemoteEntry, String> {
        let entry = crate::fleet::config::RemoteEntry {
            name: self.name.trim().to_owned(),
            // An empty target field is a local runtime, not an error: it is
            // how the dialog expresses "this machine" without ssh.
            target: {
                let target = self.target.trim();
                (!target.is_empty()).then(|| target.to_owned())
            },
            session: {
                let session = self.session.trim();
                if session.is_empty() {
                    crate::session::DEFAULT_SESSION_NAME.to_owned()
                } else {
                    session.to_owned()
                }
            },
            enabled: self.enabled,
            hue: self.hue,
        };
        crate::fleet::config::validate_entry(&entry)?;
        Ok(entry)
    }
}

/// What a key did to the dialog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoteEditKeyResult {
    /// The dialog consumed the key (edit, focus move, or error clear).
    Edited,
    /// Enter: the run loop validates and saves.
    Submit,
    /// Esc: close without saving.
    Cancel,
    /// Ctrl-d on an existing remote: the run loop removes it.
    Remove,
}

/// Pure key interpretation for the dialog. Follows the worktree dialog
/// grammar: Esc closes, Enter submits, Tab/Down and BackTab/Up cycle
/// fields, printable keys edit the focused field.
pub(crate) fn remote_edit_apply_key(
    state: &mut RemoteEditState,
    key: TerminalKey,
) -> RemoteEditKeyResult {
    match key.code {
        KeyCode::Esc => return RemoteEditKeyResult::Cancel,
        KeyCode::Enter => return RemoteEditKeyResult::Submit,
        KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            if state.is_edit() {
                return RemoteEditKeyResult::Remove;
            }
        }
        KeyCode::Tab | KeyCode::Down => {
            state.focused_field = (state.focused_field + 1) % REMOTE_EDIT_FIELDS;
        }
        KeyCode::BackTab | KeyCode::Up => {
            state.focused_field =
                (state.focused_field + REMOTE_EDIT_FIELDS - 1) % REMOTE_EDIT_FIELDS;
        }
        KeyCode::Backspace => {
            state.focused_input().pop();
            state.error = None;
        }
        KeyCode::Char(c) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
            state.focused_input().push(c);
            state.error = None;
        }
        _ => {}
    }
    RemoteEditKeyResult::Edited
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

    #[test]
    fn dialog_grammar_edits_cycles_submits_and_cancels() {
        let mut state = RemoteEditState::add();
        assert_eq!(state.session, "default");
        assert!(!state.is_edit());

        for c in "gpu-01".chars() {
            assert_eq!(
                remote_edit_apply_key(&mut state, key(KeyCode::Char(c))),
                RemoteEditKeyResult::Edited
            );
        }
        remote_edit_apply_key(&mut state, key(KeyCode::Tab));
        for c in "can@gpu".chars() {
            remote_edit_apply_key(&mut state, key(KeyCode::Char(c)));
        }
        remote_edit_apply_key(&mut state, key(KeyCode::Backspace));
        remote_edit_apply_key(&mut state, key(KeyCode::Backspace));
        remote_edit_apply_key(&mut state, key(KeyCode::Backspace));
        for c in "gpu1.example".chars() {
            remote_edit_apply_key(&mut state, key(KeyCode::Char(c)));
        }
        assert_eq!(state.name, "gpu-01");
        assert_eq!(state.target, "can@gpu1.example");

        // BackTab wraps backwards; Down wraps forwards.
        remote_edit_apply_key(&mut state, key(KeyCode::BackTab));
        assert_eq!(state.focused_field, 0);
        remote_edit_apply_key(&mut state, key(KeyCode::Down));
        remote_edit_apply_key(&mut state, key(KeyCode::Down));
        assert_eq!(state.focused_field, 2);
        remote_edit_apply_key(&mut state, key(KeyCode::Down));
        assert_eq!(state.focused_field, 0);

        assert_eq!(
            remote_edit_apply_key(&mut state, key(KeyCode::Enter)),
            RemoteEditKeyResult::Submit
        );
        assert_eq!(
            remote_edit_apply_key(&mut state, key(KeyCode::Esc)),
            RemoteEditKeyResult::Cancel
        );

        let entry = state.entry().expect("valid entry");
        assert_eq!(entry.name, "gpu-01");
        assert_eq!(entry.target.as_deref(), Some("can@gpu1.example"));
        assert_eq!(entry.session, "default");
        assert!(entry.enabled);
    }

    #[test]
    fn validation_errors_surface_and_typing_clears_them() {
        let mut state = RemoteEditState::add();
        state.name = "has space".to_owned();
        state.target = "host".to_owned();
        let err = state.entry().expect_err("illegal name");
        assert!(err.contains("may only contain"), "{err}");
        state.error = Some(err);
        remote_edit_apply_key(&mut state, key(KeyCode::Char('x')));
        assert_eq!(state.error, None, "typing clears the error row");
    }

    #[test]
    fn an_empty_target_field_saves_a_local_runtime() {
        // Regression: the dialog used to refuse an empty target, and `local`
        // was a reserved name. Both were consequences of the implicit remote
        // #0; a target-less entry is now how you add your own box.
        let mut state = RemoteEditState::add();
        state.name = "local".to_owned();
        let entry = state.entry().expect("a target-less entry is valid");
        assert_eq!(entry.name, "local");
        assert_eq!(entry.target, None);
        assert!(entry.is_local());
        assert_eq!(entry.session, "default");

        // Whitespace is not a target either.
        state.target = "   ".to_owned();
        assert_eq!(state.entry().expect("still local").target, None);

        // Editing one round-trips the field.
        let round_trip = RemoteEditState::edit(&entry);
        assert_eq!(round_trip.target, "", "no target renders as an empty field");
        assert_eq!(round_trip.entry().expect("valid").target, None);
    }

    #[test]
    fn editing_a_disabled_remote_leaves_it_disabled_and_keeps_its_hue() {
        // The dialog used to hardcode `enabled: true`, which was harmless
        // only while nothing could set it false. With a toggle in the
        // remotes list, changing a hostname would silently reconnect a
        // machine the user deliberately took out of the fleet.
        let disabled = crate::fleet::config::RemoteEntry {
            name: "gpu".into(),
            target: Some("can@gpu".into()),
            session: "work".into(),
            enabled: false,
            hue: Some(2),
        };

        let mut state = RemoteEditState::edit(&disabled);
        remote_edit_apply_key(&mut state, key(KeyCode::Tab));
        for c in "-new".chars() {
            remote_edit_apply_key(&mut state, key(KeyCode::Char(c)));
        }

        let edited = state.entry().expect("valid entry");
        assert!(!edited.enabled, "a field edit does not re-enable a remote");
        assert_eq!(edited.hue, Some(2), "a field edit does not recolour it");
        assert_eq!(edited.target.as_deref(), Some("can@gpu-new"));

        // Adding is unaffected: a remote you just added is enabled.
        let mut added = RemoteEditState::add();
        added.name = "new".to_owned();
        let entry = added.entry().expect("valid entry");
        assert!(entry.enabled);
        assert_eq!(entry.hue, None, "the config layer allocates the hue");
    }

    #[test]
    fn remove_applies_only_to_existing_remotes() {
        let mut add = RemoteEditState::add();
        assert_eq!(
            remote_edit_apply_key(&mut add, ctrl('d')),
            RemoteEditKeyResult::Edited,
            "nothing to remove while adding"
        );

        let entry = crate::fleet::config::RemoteEntry {
            name: "gpu".into(),
            target: Some("can@gpu".into()),
            session: "work".into(),
            enabled: true,
            hue: None,
        };
        let mut edit = RemoteEditState::edit(&entry);
        assert!(edit.is_edit());
        assert_eq!(edit.session, "work");
        assert_eq!(
            remote_edit_apply_key(&mut edit, ctrl('d')),
            RemoteEditKeyResult::Remove
        );
    }
}

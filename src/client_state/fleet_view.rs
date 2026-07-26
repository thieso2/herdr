//! Plain-data fleet view model for the pure client.
//!
//! [`FleetSelection`] is the client-owned view membership: which remotes are
//! composed into the sidebar, and which remote holds the focus. Chips,
//! per-space attribution, creation targeting, notification labeling, and
//! window-title selection are all pure functions over this state plus the
//! keyed [`super::RemoteMirrors`] — no sockets, PTYs, or SSH anywhere.
//!
//! View membership is presentation state: toggling a chip never connects or
//! disconnects anything. Every configured remote keeps its mirror and its
//! connection regardless of selection.

use std::collections::{BTreeMap, BTreeSet};

use crate::app::state::{RemoteChipConnection, RemoteChipState};

use super::connection::ClientConnectionState;
#[cfg(test)]
use super::LOCAL_REMOTE_NAME;
use super::{RemoteMirrors, LOCAL_REMOTE_INDEX};

/// One remote the client composes over: the implicit local runtime plus the
/// enabled fleet config entries, in config order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteDescriptor {
    /// Stable per-run remote index; 0 is the local server.
    pub(crate) index: usize,
    pub(crate) name: String,
    /// Identity hue index (into `Palette::remote_hue`), by config order.
    pub(crate) hue_index: usize,
    /// SSH target; `None` for the implicit local runtime.
    pub(crate) target: Option<String>,
    /// Remote herdr session name.
    pub(crate) session: String,
    /// Remote herdr binary to exec for the bridge; `None` runs `herdr` from
    /// the remote PATH, which is what saved fleet remotes do. A `--remote`
    /// launch pins the binary it discovered or installed.
    pub(crate) program: Option<String>,
}

impl RemoteDescriptor {
    /// The descriptor the implicit remote #0 used to be. Test-only: a local
    /// runtime is now an ordinary target-less config entry.
    #[cfg(test)]
    pub(crate) fn local() -> Self {
        Self {
            index: LOCAL_REMOTE_INDEX,
            name: LOCAL_REMOTE_NAME.to_owned(),
            hue_index: 0,
            target: None,
            session: crate::session::DEFAULT_SESSION_NAME.to_owned(),
            program: None,
        }
    }

    /// The single descriptor of an ephemeral `--remote` fleet-of-one: not in
    /// `remotes.toml`, alive only for this launch, and sitting at index 0
    /// because there is no local runtime in the view.
    #[cfg(unix)]
    pub(crate) fn ephemeral(
        name: impl Into<String>,
        target: impl Into<String>,
        session: impl Into<String>,
        program: Option<String>,
    ) -> Self {
        Self {
            index: LOCAL_REMOTE_INDEX,
            name: name.into(),
            hue_index: 0,
            target: Some(target.into()),
            session: session.into(),
            program,
        }
    }
}

/// The composed remote list: every *enabled* fleet config entry in file
/// order, and nothing else. There is no implicit local runtime — an entry
/// with no target is a local one, and it is in this list only because the
/// config asked for it. Hues follow list order so they stay stable while the
/// config file does.
pub(crate) fn remote_descriptors(
    entries: &[crate::fleet::config::RemoteEntry],
) -> Vec<RemoteDescriptor> {
    entries
        .iter()
        .filter(|entry| entry.enabled)
        .enumerate()
        .map(|(index, entry)| RemoteDescriptor {
            index,
            name: entry.name.clone(),
            hue_index: index,
            target: entry.target.clone(),
            session: entry.session.clone(),
            program: None,
        })
        .collect()
}

/// Client-owned view membership over the fleet. Stores the *hidden* set so
/// newly configured remotes default into view.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct FleetSelection {
    hidden: BTreeSet<usize>,
    /// The remote whose focused workspace owns the composed focus (and the
    /// window title). Falls back to the first remote in view when this one
    /// is filtered out or gone.
    pub(crate) focused_remote: usize,
}

impl FleetSelection {
    // Chrome constructs selections via `Default`; `new` reads better at
    // test call sites.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn is_in_view(&self, index: usize) -> bool {
        !self.hidden.contains(&index)
    }

    /// The in-view subset of `descriptors`, in order.
    pub(crate) fn in_view<'a>(
        &self,
        descriptors: &'a [RemoteDescriptor],
    ) -> Vec<&'a RemoteDescriptor> {
        descriptors
            .iter()
            .filter(|descriptor| self.is_in_view(descriptor.index))
            .collect()
    }

    /// The remote whose focus wins: `focused_remote` when in view, else the
    /// first remote in view.
    pub(crate) fn effective_focused_remote(&self, descriptors: &[RemoteDescriptor]) -> usize {
        if self.is_in_view(self.focused_remote)
            && descriptors
                .iter()
                .any(|descriptor| descriptor.index == self.focused_remote)
        {
            return self.focused_remote;
        }
        self.in_view(descriptors)
            .first()
            .map(|descriptor| descriptor.index)
            .unwrap_or(LOCAL_REMOTE_INDEX)
    }

    /// Toggles one remote's view membership. Refuses to empty the view:
    /// at least one remote stays in view. Never touches connections.
    pub(crate) fn toggle(
        &mut self,
        index: usize,
        descriptors: &[RemoteDescriptor],
    ) -> Result<(), &'static str> {
        if self.is_in_view(index) {
            if self.in_view(descriptors).len() <= 1 {
                return Err("at least one remote stays in view");
            }
            self.hidden.insert(index);
        } else {
            self.hidden.remove(&index);
        }
        Ok(())
    }

    /// Solos one remote: it becomes the only remote in view (and takes the
    /// focus). Never touches connections.
    pub(crate) fn solo(&mut self, index: usize, descriptors: &[RemoteDescriptor]) {
        self.hidden = descriptors
            .iter()
            .map(|descriptor| descriptor.index)
            .filter(|other| *other != index)
            .collect();
        self.focused_remote = index;
    }

    /// Remaps selection state across a config change. Hidden and focus
    /// state follow remote *identity* (the config name), not the index:
    /// removing or reordering remotes shifts later indices, and positional
    /// retention would silently transfer filter or focus state to a
    /// different remote.
    pub(crate) fn remap(&mut self, old: &[RemoteDescriptor], new: &[RemoteDescriptor]) {
        let new_index_of = |name: &str| {
            new.iter()
                .find(|descriptor| descriptor.name == name)
                .map(|descriptor| descriptor.index)
        };
        self.hidden = self
            .hidden
            .iter()
            .filter_map(|index| old.iter().find(|descriptor| descriptor.index == *index))
            .filter_map(|descriptor| new_index_of(&descriptor.name))
            .collect();
        self.focused_remote = old
            .iter()
            .find(|descriptor| descriptor.index == self.focused_remote)
            .and_then(|descriptor| new_index_of(&descriptor.name))
            .unwrap_or(LOCAL_REMOTE_INDEX);
        // A config change must never leave the view empty: when everything
        // still configured is hidden (for example the soloed remote was
        // removed), the whole fleet comes back into view.
        if self.in_view(new).is_empty() {
            self.hidden.clear();
        }
    }
}

fn chip_connection(connection: &ClientConnectionState) -> RemoteChipConnection {
    match connection {
        ClientConnectionState::Connected { .. } => RemoteChipConnection::Connected,
        ClientConnectionState::Connecting { .. } => RemoteChipConnection::Connecting,
        ClientConnectionState::Disconnected | ClientConnectionState::Offline { .. } => {
            RemoteChipConnection::Offline
        }
        ClientConnectionState::Incompatible { .. } => RemoteChipConnection::Incompatible,
    }
}

/// The chip strip model: one chip per configured remote, dot = connection
/// state, membership = selection. Pure data for the render layer.
pub(crate) fn remote_chip_states(
    mirrors: &RemoteMirrors,
    descriptors: &[RemoteDescriptor],
    selection: &FleetSelection,
) -> Vec<RemoteChipState> {
    descriptors
        .iter()
        .map(|descriptor| RemoteChipState {
            name: descriptor.name.clone(),
            hue_index: descriptor.hue_index,
            in_view: selection.is_in_view(descriptor.index),
            connection: mirrors
                .get(descriptor.index)
                .map(|mirror| chip_connection(&mirror.connection))
                .unwrap_or(RemoteChipConnection::Offline),
        })
        .collect()
}

/// The remote that owns creation: the owner of the focused space (or pane),
/// falling back to `fallback` (the effective focused remote) when nothing is
/// focused. New spaces and panes are created on this remote.
pub(crate) fn creation_target_remote(
    app: &crate::app::AppState,
    ids: &super::compose::ComposeIds,
    fallback: usize,
) -> usize {
    app.active
        .and_then(|ws_idx| ids.workspace_owner(ws_idx))
        .map(|(remote, _)| remote)
        .unwrap_or(fallback)
}

/// The focused remote wins the window title; other remotes' titles are
/// retained but not shown.
pub(crate) fn select_window_title(
    titles: &BTreeMap<usize, String>,
    focused_remote: usize,
) -> Option<&str> {
    titles.get(&focused_remote).map(String::as_str)
}

/// Labels a notification message with its origin remote. With more than
/// one remote configured every notification carries its remote's name -
/// the local runtime is one fleet member among others. Single-remote
/// clients stay exactly as today.
pub(crate) fn labeled_notification_message(
    remote_name: &str,
    configured_remotes: usize,
    message: &str,
) -> String {
    if configured_remotes > 1 {
        format!("[{remote_name}] {message}")
    } else {
        message.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fleet::config::RemoteEntry;

    fn entry(name: &str, enabled: bool) -> RemoteEntry {
        RemoteEntry {
            name: name.to_owned(),
            target: Some(format!("can@{name}.example")),
            session: "work".to_owned(),
            enabled,
        }
    }

    /// A local runtime entry. Selection, filtering and remap tests put one
    /// first so they keep exercising a fleet whose head is local - the shape
    /// the implicit remote #0 used to force, now spelled out in config.
    fn local(name: &str) -> RemoteEntry {
        RemoteEntry {
            name: name.to_owned(),
            target: None,
            session: "default".to_owned(),
            enabled: true,
        }
    }

    #[test]
    fn descriptors_are_exactly_the_enabled_entries_in_config_order() {
        // Regression: an implicit `local` used to be prepended here, so no
        // config could remove it and its name was reserved. The fleet is now
        // exactly what the file configures - local runtimes included.
        let descriptors = remote_descriptors(&[
            entry("buildbox", true),
            entry("dark", false),
            entry("gpu-01", true),
        ]);
        let names: Vec<&str> = descriptors.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(names, vec!["buildbox", "gpu-01"]);
        assert!(remote_descriptors(&[]).is_empty(), "no implicit runtime");

        // A target-less entry is a local runtime, wherever config puts it.
        let mixed = remote_descriptors(&[entry("gpu-01", true), local("me")]);
        assert_eq!(mixed[1].name, "me");
        assert_eq!(mixed[1].target, None, "no ssh for a local runtime");
        assert_eq!(mixed[1].index, 1);

        let descriptors = remote_descriptors(&[
            local("me"),
            entry("buildbox", true),
            entry("dark", false),
            entry("gpu-01", true),
        ]);
        let names: Vec<&str> = descriptors.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(names, vec!["me", "buildbox", "gpu-01"]);
        let indices: Vec<usize> = descriptors.iter().map(|d| d.index).collect();
        assert_eq!(indices, vec![0, 1, 2]);
        assert_eq!(descriptors[0].target, None);
        assert_eq!(descriptors[2].target.as_deref(), Some("can@gpu-01.example"));
        assert_eq!(descriptors[2].hue_index, 2);
    }

    #[cfg(unix)]
    #[test]
    fn an_ephemeral_remote_is_a_fleet_of_one_at_index_zero() {
        let descriptor = RemoteDescriptor::ephemeral(
            "buildbox.example",
            "can@buildbox.example",
            "work",
            Some("/home/can/.local/bin/herdr".to_string()),
        );
        // Remote #0 with an ssh target: there is no local runtime in view,
        // so the bridge transport is chosen by the target, not the index.
        assert_eq!(descriptor.index, LOCAL_REMOTE_INDEX);
        assert_eq!(descriptor.target.as_deref(), Some("can@buildbox.example"));
        assert_eq!(descriptor.session, "work");
        assert_eq!(
            descriptor.program.as_deref(),
            Some("/home/can/.local/bin/herdr")
        );
        // A saved fleet remote keeps resolving the fork on the remote PATH.
        assert!(remote_descriptors(&[entry("a", true)])[0].program.is_none());
    }

    #[test]
    fn toggle_filters_without_ever_emptying_the_view() {
        let descriptors = remote_descriptors(&[local("me"), entry("a", true), entry("b", true)]);
        let mut selection = FleetSelection::new();
        assert!(selection.is_in_view(0) && selection.is_in_view(1) && selection.is_in_view(2));

        selection.toggle(1, &descriptors).expect("filter b out");
        assert!(!selection.is_in_view(1));
        selection.toggle(0, &descriptors).expect("filter local out");
        assert_eq!(
            selection.toggle(2, &descriptors),
            Err("at least one remote stays in view"),
            "the last remote in view cannot be filtered out"
        );
        assert!(selection.is_in_view(2));

        selection.toggle(0, &descriptors).expect("local back in");
        assert!(selection.is_in_view(0));
    }

    #[test]
    fn solo_leaves_exactly_one_remote_in_view_and_focuses_it() {
        let descriptors = remote_descriptors(&[local("me"), entry("a", true), entry("b", true)]);
        let mut selection = FleetSelection::new();
        selection.solo(1, &descriptors);
        assert!(!selection.is_in_view(0));
        assert!(selection.is_in_view(1));
        assert!(!selection.is_in_view(2));
        assert_eq!(selection.focused_remote, 1);
        assert_eq!(selection.effective_focused_remote(&descriptors), 1);

        // Toggling another remote back in un-solos without dropping a.
        selection.toggle(2, &descriptors).expect("b back in view");
        assert_eq!(selection.in_view(&descriptors).len(), 2);
    }

    #[test]
    fn focused_remote_falls_back_to_the_first_in_view() {
        let descriptors = remote_descriptors(&[local("me"), entry("a", true)]);
        let mut selection = FleetSelection::new();
        selection.focused_remote = 1;
        assert_eq!(selection.effective_focused_remote(&descriptors), 1);
        selection.toggle(1, &descriptors).expect("filter a out");
        assert_eq!(
            selection.effective_focused_remote(&descriptors),
            0,
            "a filtered-out remote cannot keep the focus"
        );
    }

    #[test]
    fn remap_drops_selection_state_for_removed_remotes() {
        let descriptors = remote_descriptors(&[local("me"), entry("a", true), entry("b", true)]);
        let mut selection = FleetSelection::new();
        selection.solo(2, &descriptors);

        // b is removed from the config: the solo selection must repair.
        let shrunk = remote_descriptors(&[local("me"), entry("a", true)]);
        selection.remap(&descriptors, &shrunk);
        assert!(descriptors.len() > shrunk.len());
        assert_eq!(selection.effective_focused_remote(&shrunk), 0);
        assert!(selection.is_in_view(0) || selection.is_in_view(1));
    }

    #[test]
    fn remap_keys_selection_by_remote_identity_across_removals() {
        let old =
            remote_descriptors(&[local("me"), entry("buildbox", true), entry("gpu-01", true)]);
        let mut selection = FleetSelection::new();
        selection.toggle(1, &old).expect("hide buildbox");
        selection.focused_remote = 2; // gpu-01

        // buildbox is removed: gpu-01 shifts from index 2 to index 1 and
        // must carry its own selection state, not inherit buildbox's.
        let new = remote_descriptors(&[local("me"), entry("gpu-01", true)]);
        selection.remap(&old, &new);
        assert!(
            selection.is_in_view(1),
            "gpu-01 must not inherit buildbox's hidden state"
        );
        assert_eq!(
            selection.focused_remote, 1,
            "focus follows gpu-01 to its new index"
        );
        assert_eq!(selection.effective_focused_remote(&new), 1);
    }

    #[test]
    fn chip_states_carry_connection_membership_and_hue() {
        let descriptors = remote_descriptors(&[local("me"), entry("a", true), entry("b", true)]);
        let mut selection = FleetSelection::new();
        selection.toggle(2, &descriptors).expect("filter b");

        let mut mirrors = RemoteMirrors::with_local();
        mirrors.insert(super::super::RemoteMirror::new(1, "a"));
        mirrors.insert(super::super::RemoteMirror::new(2, "b"));
        mirrors.local_mut().connection.connect_started();
        mirrors
            .local_mut()
            .connection
            .connected(crate::protocol::framed::NegotiatedSession {
                protocol: crate::protocol::framed::FRAMED_PROTOCOL_VERSION,
                capabilities: Vec::new(),
            });
        if let Some(mirror) = mirrors.get_mut(1) {
            mirror.connection.connect_started();
        }
        if let Some(mirror) = mirrors.get_mut(2) {
            mirror.connection.incompatible(
                crate::protocol::framed::HelloRemedy::UpgradeServer,
                "windows do not overlap",
            );
        }

        let chips = remote_chip_states(&mirrors, &descriptors, &selection);
        assert_eq!(chips.len(), 3);
        assert_eq!(chips[0].connection, RemoteChipConnection::Connected);
        assert!(chips[0].in_view);
        assert_eq!(chips[1].connection, RemoteChipConnection::Connecting);
        assert_eq!(chips[2].connection, RemoteChipConnection::Incompatible);
        assert!(!chips[2].in_view, "filtered chip stays configured");
        assert_eq!(chips[2].hue_index, 2);
    }

    #[test]
    fn window_title_selection_follows_the_focused_remote() {
        let mut titles = BTreeMap::new();
        titles.insert(0, "local title".to_owned());
        titles.insert(1, "gpu title".to_owned());
        assert_eq!(select_window_title(&titles, 1), Some("gpu title"));
        assert_eq!(select_window_title(&titles, 0), Some("local title"));
        assert_eq!(select_window_title(&titles, 2), None);
    }

    #[test]
    fn notification_labels_name_the_remote_only_in_a_real_fleet() {
        assert_eq!(
            labeled_notification_message("gpu-01", 3, "agent done"),
            "[gpu-01] agent done"
        );
        assert_eq!(
            labeled_notification_message("local", 3, "agent done"),
            "[local] agent done",
            "the local runtime is labeled like any other fleet member"
        );
        assert_eq!(
            labeled_notification_message("gpu-01", 1, "agent done"),
            "agent done",
            "single-remote fleets stay exactly as today"
        );
    }
}

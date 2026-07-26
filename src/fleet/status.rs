//! Mapping from fleet state to the API's `RemoteInfo` rows.
//!
//! Shared by the server-side `remote.*` handlers and the CLI's
//! offline fallback, so both render the same shape. The fleet is exactly
//! what `remotes.toml` configures: there is no implicit local runtime, and a
//! target-less entry reports the `local` state.

use crate::api::schema::{RemoteConnectionStateInfo, RemoteInfo};

use super::config::RemoteEntry;
use super::manager::{RemoteStatus, RemoteStatusKind};

/// Maps live fleet statuses to API remote infos, in config order.
pub fn remote_infos_from_statuses(statuses: &[RemoteStatus]) -> Vec<RemoteInfo> {
    let mut remotes = Vec::with_capacity(statuses.len());
    for (index, status) in statuses.iter().enumerate() {
        let (state, attempt, retry_in_ms, last_error) = match &status.kind {
            RemoteStatusKind::Local => (RemoteConnectionStateInfo::Local, None, None, None),
            RemoteStatusKind::Disabled => (RemoteConnectionStateInfo::Disabled, None, None, None),
            RemoteStatusKind::Connecting { attempt } => (
                RemoteConnectionStateInfo::Connecting,
                Some(*attempt),
                None,
                None,
            ),
            RemoteStatusKind::Connected => (RemoteConnectionStateInfo::Connected, None, None, None),
            RemoteStatusKind::Offline {
                attempt,
                retry_in,
                last_error,
            } => (
                RemoteConnectionStateInfo::Offline,
                Some(*attempt),
                Some(retry_in.as_millis().min(u128::from(u64::MAX)) as u64),
                (!last_error.is_empty()).then(|| last_error.clone()),
            ),
            RemoteStatusKind::Incompatible { message } => (
                RemoteConnectionStateInfo::Incompatible,
                None,
                None,
                (!message.is_empty()).then(|| message.clone()),
            ),
        };
        remotes.push(RemoteInfo {
            index,
            name: status.entry.name.clone(),
            target: status.entry.target.clone(),
            session: Some(status.entry.session.clone()),
            enabled: status.entry.enabled,
            state,
            attempt,
            retry_in_ms,
            last_error,
        });
    }
    remotes
}

/// Maps bare config entries (no live fleet) to API remote infos with the
/// `unknown` connection state, in config order. This keeps `remote list`
/// offline-capable: the config is readable even when nothing is connected.
pub fn remote_infos_from_entries(entries: &[RemoteEntry]) -> Vec<RemoteInfo> {
    let mut remotes = Vec::with_capacity(entries.len());
    for (index, entry) in entries.iter().enumerate() {
        remotes.push(RemoteInfo {
            index,
            name: entry.name.clone(),
            target: entry.target.clone(),
            session: Some(entry.session.clone()),
            enabled: entry.enabled,
            state: match (entry.enabled, entry.is_local()) {
                (false, _) => RemoteConnectionStateInfo::Disabled,
                // A local runtime needs no probe to know its own state.
                (true, true) => RemoteConnectionStateInfo::Local,
                (true, false) => RemoteConnectionStateInfo::Unknown,
            },
            attempt: None,
            retry_in_ms: None,
            last_error: None,
        });
    }
    remotes
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn entry(name: &str, enabled: bool) -> RemoteEntry {
        RemoteEntry {
            name: name.to_string(),
            target: Some(format!("can@{name}.example")),
            session: "work".to_string(),
            enabled,
        }
    }

    fn local_entry(name: &str, session: &str, enabled: bool) -> RemoteEntry {
        RemoteEntry {
            name: name.to_string(),
            target: None,
            session: session.to_string(),
            enabled,
        }
    }

    #[test]
    fn an_empty_fleet_lists_nothing() {
        // Regression: `local` used to be an implicit remote #0 that no
        // config could remove. The fleet is now exactly what is configured,
        // so an empty config lists an empty fleet.
        assert!(remote_infos_from_statuses(&[]).is_empty());
        assert!(remote_infos_from_entries(&[]).is_empty());
    }

    #[test]
    fn a_target_less_entry_is_a_local_runtime_in_config_order() {
        // Local runtimes are ordinary entries: they take their configured
        // name and session, and they sit wherever the file puts them.
        let entries = [
            entry("gpu-1", true),
            local_entry("me", "default", true),
            local_entry("scratch", "scratch", true),
        ];
        let remotes = remote_infos_from_entries(&entries);
        assert_eq!(remotes.len(), 3);

        assert_eq!(remotes[0].index, 0);
        assert_eq!(remotes[0].name, "gpu-1");
        assert_eq!(remotes[0].state, RemoteConnectionStateInfo::Unknown);

        // No probe is needed for a runtime on this machine's own socket.
        assert_eq!(remotes[1].name, "me");
        assert_eq!(remotes[1].target, None);
        assert_eq!(remotes[1].state, RemoteConnectionStateInfo::Local);
        assert_eq!(remotes[1].session.as_deref(), Some("default"));

        // Two local entries differing only by session are two runtimes.
        assert_eq!(remotes[2].name, "scratch");
        assert_eq!(remotes[2].session.as_deref(), Some("scratch"));
        assert_eq!(remotes[2].state, RemoteConnectionStateInfo::Local);

        // Disabled still wins over local: it is a config-level "off".
        let off = remote_infos_from_entries(&[local_entry("me", "default", false)]);
        assert_eq!(off[0].state, RemoteConnectionStateInfo::Disabled);
    }

    #[test]
    fn a_live_local_runtime_reports_the_local_state() {
        let statuses = vec![RemoteStatus {
            entry: local_entry("me", "default", true),
            kind: RemoteStatusKind::Local,
        }];
        let remotes = remote_infos_from_statuses(&statuses);
        assert_eq!(remotes[0].state, RemoteConnectionStateInfo::Local);
        assert_eq!(remotes[0].target, None);
        assert_eq!(remotes[0].attempt, None);
        assert_eq!(
            remotes[0].retry_in_ms, None,
            "a local runtime never retries"
        );
    }

    #[test]
    fn statuses_map_to_states_with_offline_details() {
        let statuses = vec![
            RemoteStatus {
                entry: entry("up", true),
                kind: RemoteStatusKind::Connected,
            },
            RemoteStatus {
                entry: entry("down", true),
                kind: RemoteStatusKind::Offline {
                    attempt: 3,
                    retry_in: Duration::from_millis(1500),
                    last_error: "connect failed: refused".to_string(),
                },
            },
            RemoteStatus {
                entry: entry("off", false),
                kind: RemoteStatusKind::Disabled,
            },
        ];
        let remotes = remote_infos_from_statuses(&statuses);
        assert_eq!(remotes.len(), 3);

        assert_eq!(remotes[0].index, 0);
        assert_eq!(remotes[0].state, RemoteConnectionStateInfo::Connected);
        assert_eq!(remotes[0].target.as_deref(), Some("can@up.example"));

        assert_eq!(remotes[1].state, RemoteConnectionStateInfo::Offline);
        assert_eq!(remotes[1].attempt, Some(3));
        assert_eq!(remotes[1].retry_in_ms, Some(1500));
        assert_eq!(
            remotes[1].last_error.as_deref(),
            Some("connect failed: refused")
        );

        assert_eq!(remotes[2].state, RemoteConnectionStateInfo::Disabled);
        assert!(!remotes[2].enabled);
    }

    #[test]
    fn incompatible_status_maps_to_its_own_state_with_the_message() {
        let statuses = vec![RemoteStatus {
            entry: entry("old", true),
            kind: RemoteStatusKind::Incompatible {
                message: "upgrade the remote herdr".to_string(),
            },
        }];
        let remotes = remote_infos_from_statuses(&statuses);
        assert_eq!(remotes[0].state, RemoteConnectionStateInfo::Incompatible);
        assert_eq!(
            remotes[0].last_error.as_deref(),
            Some("upgrade the remote herdr")
        );
    }

    #[test]
    fn config_only_listing_reports_unknown_for_enabled_entries() {
        let remotes = remote_infos_from_entries(&[entry("a", true), entry("b", false)]);
        assert_eq!(remotes[0].state, RemoteConnectionStateInfo::Unknown);
        assert_eq!(remotes[1].state, RemoteConnectionStateInfo::Disabled);
    }

    #[test]
    fn json_shape_of_remote_info_rows_is_stable() {
        let remotes =
            remote_infos_from_entries(&[entry("a", true), local_entry("me", "default", true)]);
        let json = serde_json::to_value(&remotes).unwrap();
        assert_eq!(
            json,
            serde_json::json!([
                {
                    "index": 0,
                    "name": "a",
                    "target": "can@a.example",
                    "session": "work",
                    "enabled": true,
                    "state": "unknown",
                },
                {
                    "index": 1,
                    "name": "me",
                    "session": "default",
                    "enabled": true,
                    "state": "local",
                },
            ])
        );
    }
}

//! Mapping from fleet state to the API's `RemoteInfo` rows.
//!
//! Shared by the server-side `remote.*` handlers and the CLI's
//! offline fallback, so both render the same shape. The local runtime is the
//! implicit remote `#0`.

use crate::api::schema::{RemoteConnectionStateInfo, RemoteInfo};

use super::config::{RemoteEntry, LOCAL_REMOTE_NAME};
use super::manager::{RemoteStatus, RemoteStatusKind};

/// The implicit local runtime as remote `#0`.
fn local_remote_info(local_session: Option<&str>) -> RemoteInfo {
    RemoteInfo {
        index: 0,
        name: LOCAL_REMOTE_NAME.to_string(),
        target: None,
        session: Some(
            local_session
                .unwrap_or(crate::session::DEFAULT_SESSION_NAME)
                .to_string(),
        ),
        enabled: true,
        state: RemoteConnectionStateInfo::Local,
        attempt: None,
        retry_in_ms: None,
        last_error: None,
    }
}

/// Maps live fleet statuses to API remote infos, local runtime first.
pub fn remote_infos_from_statuses(
    statuses: &[RemoteStatus],
    local_session: Option<&str>,
) -> Vec<RemoteInfo> {
    let mut remotes = vec![local_remote_info(local_session)];
    for (offset, status) in statuses.iter().enumerate() {
        let (state, attempt, retry_in_ms, last_error) = match &status.kind {
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
            index: offset + 1,
            name: status.entry.name.clone(),
            target: Some(status.entry.target.clone()),
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
/// `unknown` connection state, local runtime first. This keeps `remote list`
/// offline-capable: the config is readable even when nothing is connected.
pub fn remote_infos_from_entries(
    entries: &[RemoteEntry],
    local_session: Option<&str>,
) -> Vec<RemoteInfo> {
    let mut remotes = vec![local_remote_info(local_session)];
    for (offset, entry) in entries.iter().enumerate() {
        remotes.push(RemoteInfo {
            index: offset + 1,
            name: entry.name.clone(),
            target: Some(entry.target.clone()),
            session: Some(entry.session.clone()),
            enabled: entry.enabled,
            state: if entry.enabled {
                RemoteConnectionStateInfo::Unknown
            } else {
                RemoteConnectionStateInfo::Disabled
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
            target: format!("can@{name}.example"),
            session: "work".to_string(),
            enabled,
        }
    }

    #[test]
    fn local_runtime_is_the_implicit_first_remote() {
        let remotes = remote_infos_from_statuses(&[], None);
        assert_eq!(remotes.len(), 1);
        assert_eq!(remotes[0].index, 0);
        assert_eq!(remotes[0].name, "local");
        assert_eq!(remotes[0].state, RemoteConnectionStateInfo::Local);
        assert_eq!(remotes[0].session.as_deref(), Some("default"));

        let named = remote_infos_from_statuses(&[], Some("work"));
        assert_eq!(named[0].session.as_deref(), Some("work"));
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
        let remotes = remote_infos_from_statuses(&statuses, None);
        assert_eq!(remotes.len(), 4);

        assert_eq!(remotes[1].index, 1);
        assert_eq!(remotes[1].state, RemoteConnectionStateInfo::Connected);
        assert_eq!(remotes[1].target.as_deref(), Some("can@up.example"));

        assert_eq!(remotes[2].state, RemoteConnectionStateInfo::Offline);
        assert_eq!(remotes[2].attempt, Some(3));
        assert_eq!(remotes[2].retry_in_ms, Some(1500));
        assert_eq!(
            remotes[2].last_error.as_deref(),
            Some("connect failed: refused")
        );

        assert_eq!(remotes[3].state, RemoteConnectionStateInfo::Disabled);
        assert!(!remotes[3].enabled);
    }

    #[test]
    fn incompatible_status_maps_to_its_own_state_with_the_message() {
        let statuses = vec![RemoteStatus {
            entry: entry("old", true),
            kind: RemoteStatusKind::Incompatible {
                message: "upgrade the remote herdr".to_string(),
            },
        }];
        let remotes = remote_infos_from_statuses(&statuses, None);
        assert_eq!(remotes[1].state, RemoteConnectionStateInfo::Incompatible);
        assert_eq!(
            remotes[1].last_error.as_deref(),
            Some("upgrade the remote herdr")
        );
    }

    #[test]
    fn config_only_listing_reports_unknown_for_enabled_entries() {
        let remotes = remote_infos_from_entries(&[entry("a", true), entry("b", false)], None);
        assert_eq!(remotes[1].state, RemoteConnectionStateInfo::Unknown);
        assert_eq!(remotes[2].state, RemoteConnectionStateInfo::Disabled);
    }

    #[test]
    fn json_shape_of_remote_info_rows_is_stable() {
        let remotes = remote_infos_from_entries(&[entry("a", true)], None);
        let json = serde_json::to_value(&remotes).unwrap();
        assert_eq!(
            json,
            serde_json::json!([
                {
                    "index": 0,
                    "name": "local",
                    "session": "default",
                    "enabled": true,
                    "state": "local",
                },
                {
                    "index": 1,
                    "name": "a",
                    "target": "can@a.example",
                    "session": "work",
                    "enabled": true,
                    "state": "unknown",
                },
            ])
        );
    }
}

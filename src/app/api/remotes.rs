//! `remote.*` API handlers: fleet listing, manual reset, and explicit config
//! reload.

use super::responses;
use crate::api::schema::{RemoteInfo, RemoteTargetParams, ResponseResult};
use crate::app::App;
use crate::fleet::status::{remote_infos_from_entries, remote_infos_from_statuses};

impl App {
    fn current_remote_infos(&self) -> Vec<RemoteInfo> {
        match &self.fleet {
            Some(fleet) => remote_infos_from_statuses(&fleet.snapshot()),
            None => remote_infos_from_entries(&crate::fleet::config::load()),
        }
    }

    pub(crate) fn handle_remote_list(&mut self, id: String) -> String {
        responses::encode_success(
            id,
            ResponseResult::RemoteList {
                remotes: self.current_remote_infos(),
            },
        )
    }

    pub(crate) fn handle_remote_reset(&mut self, id: String, params: RemoteTargetParams) -> String {
        let Some(fleet) = self.fleet.as_mut() else {
            return responses::encode_error(
                id,
                "fleet_not_running",
                "no fleet connection manager is running in this process",
            );
        };
        if fleet.reset(&params.name) {
            responses::encode_success(id, ResponseResult::Ok {})
        } else {
            responses::encode_error(
                id,
                "remote_not_found",
                format!("no enabled remote named '{}'", params.name),
            )
        }
    }

    /// Starts a server on one remote. Separate from `remote.reset` because
    /// it writes a daemon to the far side: it is a user action, never
    /// something the reconnect loop reaches on its own.
    pub(crate) fn handle_remote_start(&mut self, id: String, params: RemoteTargetParams) -> String {
        let Some(fleet) = self.fleet.as_mut() else {
            return responses::encode_error(
                id,
                "fleet_not_running",
                "no fleet connection manager is running in this process",
            );
        };
        match fleet.start_remote(&params.name) {
            Ok(()) => responses::encode_success(id, ResponseResult::Ok {}),
            Err(err) => responses::encode_error(id, "remote_start_failed", err.to_string()),
        }
    }

    pub(crate) fn handle_remote_reload(&mut self, id: String) -> String {
        let entries = match crate::fleet::config::try_load() {
            Ok(entries) => entries,
            Err(err) => {
                return responses::encode_error(
                    id,
                    "remote_config_invalid",
                    format!("failed to load remotes.toml: {err}"),
                );
            }
        };
        // An explicit reload is strict: surface invalid or duplicate entries
        // as an error (keeping the running fleet unchanged) instead of
        // silently dropping them from the listing.
        if let Err(err) = crate::fleet::config::validate_entries(&entries) {
            return responses::encode_error(
                id,
                "remote_config_invalid",
                format!("invalid remotes.toml: {err}"),
            );
        }
        if let Some(fleet) = self.fleet.as_mut() {
            let changes = fleet.apply_config(entries);
            tracing::info!(changes = changes.len(), "fleet config reloaded");
        }
        responses::encode_success(
            id,
            ResponseResult::RemoteList {
                remotes: self.current_remote_infos(),
            },
        )
    }
}

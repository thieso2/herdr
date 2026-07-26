//! `herdr remote` — fleet listing, connection lifecycle, and upgrades.

use std::io::IsTerminal;

use crate::api::schema::{EmptyParams, Method, Request, SuccessResponse};

const UPGRADE_USAGE: &str =
    "usage: herdr remote upgrade <name-or-target> | --all [--yes] [--json]";

pub(super) fn run_remote_command(args: &[String]) -> std::io::Result<i32> {
    match args.first().map(|arg| arg.as_str()) {
        Some("list") => remote_list(&args[1..]),
        Some("reset") => remote_reset(&args[1..]),
        Some("reload") => remote_reload(&args[1..]),
        Some("upgrade") => remote_upgrade(&args[1..]),
        Some("help" | "--help" | "-h") => {
            print_remote_help();
            Ok(0)
        }
        _ => {
            print_remote_help();
            Ok(2)
        }
    }
}

fn remote_list(args: &[String]) -> std::io::Result<i32> {
    let json = match super::parse_session_json_only(args, "usage: herdr remote list [--json]") {
        Ok(json) => json,
        Err(code) => return Ok(code),
    };

    let request_id = "cli:remote:list";
    let response = match super::send_request(&Request {
        id: request_id.into(),
        method: Method::RemoteList(EmptyParams::default()),
    }) {
        Ok(response) => response,
        // Offline-capable: with no reachable server, list the fleet straight
        // from remotes.toml with an unknown connection state.
        Err(err) if super::plugin::is_connection_error(&err) => {
            offline_remote_list_response(request_id)?
        }
        Err(err) => return Err(err),
    };
    if json {
        return super::print_response(&response);
    }
    print_remote_table(&response)
}

fn offline_remote_list_response(id: &str) -> std::io::Result<serde_json::Value> {
    let entries = crate::fleet::config::load();
    let remotes = crate::fleet::status::remote_infos_from_entries(
        &entries,
        crate::session::active_name().as_deref(),
    );
    serde_json::to_value(&SuccessResponse {
        id: id.to_string(),
        result: crate::api::schema::ResponseResult::RemoteList { remotes },
    })
    .map_err(std::io::Error::other)
}

fn remote_reset(args: &[String]) -> std::io::Result<i32> {
    let (name, json) =
        match super::parse_session_name_and_json(args, "usage: herdr remote reset <name> [--json]")
        {
            Ok(parsed) => parsed,
            Err(code) => return Ok(code),
        };

    let response = super::send_request(&Request {
        id: "cli:remote:reset".into(),
        method: Method::RemoteReset(crate::api::schema::RemoteTargetParams { name: name.clone() }),
    })?;
    if json || response.get("error").is_some() {
        return super::print_response(&response);
    }
    println!("reset remote {name}; reconnecting now");
    Ok(0)
}

fn remote_reload(args: &[String]) -> std::io::Result<i32> {
    let json = match super::parse_session_json_only(args, "usage: herdr remote reload [--json]") {
        Ok(json) => json,
        Err(code) => return Ok(code),
    };

    let response = super::send_request(&Request {
        id: "cli:remote:reload".into(),
        method: Method::RemoteReload(EmptyParams::default()),
    })?;
    if json {
        return super::print_response(&response);
    }
    print_remote_table(&response)
}

/// Parsed `herdr remote upgrade` arguments.
#[derive(Debug, Default, PartialEq, Eq)]
struct RemoteUpgradeArgs {
    /// Roll the whole configured fleet forward, sequentially.
    all: bool,
    /// A configured remote name, or a bare ssh target.
    target: Option<String>,
    yes: bool,
    json: bool,
}

/// Pure argument parsing for `herdr remote upgrade`.
fn parse_remote_upgrade_args(args: &[String]) -> Result<RemoteUpgradeArgs, String> {
    let mut parsed = RemoteUpgradeArgs::default();
    for arg in args {
        match arg.as_str() {
            "--all" => parsed.all = true,
            "--yes" | "-y" => parsed.yes = true,
            "--json" => parsed.json = true,
            "--help" | "-h" => return Err(UPGRADE_USAGE.to_string()),
            other if other.starts_with('-') => {
                return Err(format!("unknown option: {other}\n{UPGRADE_USAGE}"));
            }
            other if parsed.target.is_some() => {
                return Err(format!("unexpected argument: {other}\n{UPGRADE_USAGE}"));
            }
            other => parsed.target = Some(other.to_string()),
        }
    }
    if parsed.all && parsed.target.is_some() {
        return Err(format!("--all takes no remote name\n{UPGRADE_USAGE}"));
    }
    if !parsed.all && parsed.target.is_none() {
        return Err(UPGRADE_USAGE.to_string());
    }
    Ok(parsed)
}

fn remote_upgrade(args: &[String]) -> std::io::Result<i32> {
    let parsed = match parse_remote_upgrade_args(args) {
        Ok(parsed) => parsed,
        Err(message) => {
            eprintln!("{message}");
            return Ok(2);
        }
    };
    // An upgrade stops remote servers and rewrites binaries: unattended runs
    // must say so explicitly, exactly like `herdr plugin install`.
    if !parsed.yes && !std::io::stdin().is_terminal() {
        eprintln!("herdr remote upgrade requires --yes when stdin is not interactive");
        return Ok(2);
    }
    #[cfg(unix)]
    {
        run_remote_upgrade(parsed)
    }
    #[cfg(windows)]
    {
        let _ = parsed;
        eprintln!("herdr remote upgrade is not supported on Windows yet");
        Ok(1)
    }
}

/// The remotes an upgrade run touches, as `(name, ssh target)` pairs. A name
/// that is not in the fleet config is used as a bare ssh target, so
/// `herdr remote upgrade user@host` works before the remote is saved.
#[cfg(unix)]
fn upgrade_targets(
    parsed: &RemoteUpgradeArgs,
    entries: &[crate::fleet::config::RemoteEntry],
) -> Vec<(String, String)> {
    if parsed.all {
        return entries
            .iter()
            .map(|entry| (entry.name.clone(), entry.target.clone()))
            .collect();
    }
    let Some(requested) = parsed.target.as_deref() else {
        return Vec::new();
    };
    match entries.iter().find(|entry| entry.name == requested) {
        Some(entry) => vec![(entry.name.clone(), entry.target.clone())],
        None => vec![(requested.to_string(), requested.to_string())],
    }
}

#[cfg(unix)]
fn run_remote_upgrade(parsed: RemoteUpgradeArgs) -> std::io::Result<i32> {
    let entries = crate::fleet::config::load();
    let targets = upgrade_targets(&parsed, &entries);
    if targets.is_empty() {
        eprintln!(
            "no remotes configured in {}",
            crate::fleet::config::remotes_path().display()
        );
        return Ok(1);
    }

    let target_version = crate::update::remote_upgrade_target_version();
    let options = crate::remote::RemoteUpgradeOptions { yes: parsed.yes };
    let mut rows = Vec::new();
    let mut failures = 0;
    // Rolling: one remote at a time, and a failure never stops the fleet.
    for (name, target) in targets {
        if !parsed.json {
            eprintln!("herdr: upgrading {name} ({target}) to {target_version}");
        }
        match crate::remote::upgrade_remote(&target, &target_version, options) {
            Ok(outcome) => {
                if !parsed.json {
                    println!("{}", outcome.summary(&name));
                }
                let mut row = serde_json::json!({
                    "name": name,
                    "target": target,
                    "action": outcome.action(),
                });
                if let crate::remote::RemoteUpgradeOutcome::Upgraded { from, to } = &outcome {
                    row["from"] = serde_json::json!(from);
                    row["to"] = serde_json::json!(to);
                }
                rows.push(row);
            }
            Err(err) => {
                failures += 1;
                if !parsed.json {
                    eprintln!("{name}: upgrade failed: {err}");
                    crate::remote::print_remote_error_hint(&err, &target);
                }
                rows.push(serde_json::json!({
                    "name": name,
                    "target": target,
                    "action": "failed",
                    "error": err.to_string(),
                }));
            }
        }
    }

    if parsed.json {
        let response = serde_json::json!({
            "id": "cli:remote:upgrade",
            "result": {
                "type": "remote_upgrade",
                "target_version": target_version,
                "remotes": rows,
            },
        });
        println!("{response}");
    }
    Ok(if failures > 0 { 1 } else { 0 })
}

fn print_remote_table(response: &serde_json::Value) -> std::io::Result<i32> {
    if response.get("error").is_some() {
        eprintln!("{response}");
        return Ok(1);
    }

    println!(
        "{:<4} {:<20} {:<12} {:<28} {:<16} {:<8} detail",
        "#", "name", "state", "target", "session", "enabled"
    );
    let empty = Vec::new();
    let remotes = response["result"]["remotes"].as_array().unwrap_or(&empty);
    for remote in remotes {
        let mut detail = String::new();
        if let Some(retry_in_ms) = remote["retry_in_ms"].as_u64() {
            detail.push_str(&format!("retry in {}s", retry_in_ms.div_ceil(1000)));
        }
        if let Some(last_error) = remote["last_error"].as_str() {
            if !detail.is_empty() {
                detail.push_str(": ");
            }
            detail.push_str(last_error);
        }
        println!(
            "{:<4} {:<20} {:<12} {:<28} {:<16} {:<8} {}",
            remote["index"].as_u64().unwrap_or(0),
            remote["name"].as_str().unwrap_or("?"),
            remote["state"].as_str().unwrap_or("?"),
            remote["target"].as_str().unwrap_or("-"),
            remote["session"].as_str().unwrap_or("-"),
            if remote["enabled"].as_bool().unwrap_or(false) {
                "yes"
            } else {
                "no"
            },
            detail
        );
    }
    Ok(0)
}

fn print_remote_help() {
    eprintln!("herdr remote commands:");
    eprintln!("  herdr remote list [--json]    list fleet remotes and their connection state");
    eprintln!("  herdr remote reset <name>     drop a remote connection and reconnect now");
    eprintln!("  herdr remote reload [--json]  apply hand edits made to remotes.toml");
    eprintln!(
        "  herdr remote upgrade <name-or-target> | --all [--yes] [--json]"
    );
    eprintln!(
        "                                roll remotes forward to the channel's herdr (never downgrades)"
    );
    eprintln!(
        "  remotes are configured in {}",
        crate::fleet::config::remotes_path().display()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn upgrade_args_take_a_name_or_all_but_never_both() {
        assert_eq!(
            parse_remote_upgrade_args(&args(&["gpu-1"])),
            Ok(RemoteUpgradeArgs {
                all: false,
                target: Some("gpu-1".into()),
                yes: false,
                json: false,
            })
        );
        assert_eq!(
            parse_remote_upgrade_args(&args(&["--all", "--yes", "--json"])),
            Ok(RemoteUpgradeArgs {
                all: true,
                target: None,
                yes: true,
                json: true,
            })
        );
        // A bare target is legal: upgrading precedes saving the remote.
        assert_eq!(
            parse_remote_upgrade_args(&args(&["can@buildbox.example", "-y"]))
                .map(|parsed| parsed.target),
            Ok(Some("can@buildbox.example".to_string()))
        );

        for bad in [
            vec!["--all", "gpu-1"],
            vec![],
            vec!["gpu-1", "gpu-2"],
            vec!["--nope"],
        ] {
            assert!(
                parse_remote_upgrade_args(&args(&bad)).is_err(),
                "expected {bad:?} to be rejected"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn upgrade_targets_resolve_names_from_the_fleet_config() {
        let entries = vec![
            crate::fleet::config::RemoteEntry {
                name: "gpu-1".into(),
                target: "can@gpu-1.example".into(),
                session: "default".into(),
                enabled: true,
            },
            crate::fleet::config::RemoteEntry {
                name: "buildbox".into(),
                target: "can@buildbox.example".into(),
                session: "default".into(),
                enabled: false,
            },
        ];

        let named = parse_remote_upgrade_args(&args(&["gpu-1"])).expect("parsed");
        assert_eq!(
            upgrade_targets(&named, &entries),
            vec![("gpu-1".to_string(), "can@gpu-1.example".to_string())]
        );

        // Unknown names are ssh targets, not errors.
        let bare = parse_remote_upgrade_args(&args(&["root@fresh.example"])).expect("parsed");
        assert_eq!(
            upgrade_targets(&bare, &entries),
            vec![(
                "root@fresh.example".to_string(),
                "root@fresh.example".to_string()
            )]
        );

        // --all rolls the whole config, disabled remotes included: a
        // disabled remote still needs a current binary when it comes back.
        let all = parse_remote_upgrade_args(&args(&["--all"])).expect("parsed");
        assert_eq!(upgrade_targets(&all, &entries).len(), 2);
    }
}

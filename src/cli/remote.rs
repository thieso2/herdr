//! `herdr remote` — fleet listing and connection lifecycle actions.

use crate::api::schema::{EmptyParams, Method, Request, SuccessResponse};

pub(super) fn run_remote_command(args: &[String]) -> std::io::Result<i32> {
    match args.first().map(|arg| arg.as_str()) {
        Some("list") => remote_list(&args[1..]),
        Some("reset") => remote_reset(&args[1..]),
        Some("reload") => remote_reload(&args[1..]),
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
        "  remotes are configured in {}",
        crate::fleet::config::remotes_path().display()
    );
}

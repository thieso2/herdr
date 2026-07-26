#[cfg(unix)]
mod unix;

#[cfg(unix)]
pub(crate) use unix::*;

/// What a rolling `herdr remote upgrade` should do with one remote, given the
/// version it currently runs and the version the fleet is rolling forward to.
///
/// Upgrades are forward-only: a remote that already runs the target (or
/// something newer, for example a host upgraded ahead of this client) is
/// never rewritten. Prerelease builds (`X.Y.Z-preview.<build>`) compare on
/// their release core; a release outranks every prerelease of the same core,
/// and two different prerelease builds of the same core cannot be ordered, so
/// they are skipped rather than risked as a downgrade. Only a version whose
/// release core does not parse at all (an unreadable or hand-built binary) is
/// rolled forward unconditionally rather than silently left behind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
// Remote attach and fleet upgrades are unix-only; the decision itself is
// platform-neutral so it stays testable everywhere.
#[cfg_attr(windows, allow(dead_code))]
pub(crate) enum RemoteUpgradeDecision {
    /// Install the target version (the remote is older, absent, or unknown).
    Install,
    /// The remote already runs the target version.
    AlreadyCurrent,
    /// The remote runs something newer; never downgrade.
    RemoteIsNewer,
    /// The remote runs a different build of the same release core (for
    /// example two preview builds); neither side is provably newer, so never
    /// rewrite it.
    Unordered,
}

/// A version split into its parseable release core and the raw prerelease
/// suffix, when present: `0.10.0-preview.42` -> (`0.10.0`, `preview.42`).
fn split_release_core(version: &str) -> (Option<crate::update::Version>, Option<&str>) {
    let (core, suffix) = match version.split_once('-') {
        Some((core, suffix)) => (core, Some(suffix)),
        None => (version, None),
    };
    (crate::update::Version::parse(core), suffix)
}

/// Pure forward-only guard for one remote.
// Remote attach and fleet upgrades are unix-only; the decision itself is
// platform-neutral so it stays testable everywhere.
#[cfg_attr(windows, allow(dead_code))]
pub(crate) fn remote_upgrade_decision(
    remote_version: Option<&str>,
    target_version: &str,
) -> RemoteUpgradeDecision {
    let Some(remote_version) = remote_version else {
        return RemoteUpgradeDecision::Install;
    };
    if remote_version == target_version {
        return RemoteUpgradeDecision::AlreadyCurrent;
    }
    let (remote_core, remote_suffix) = split_release_core(remote_version);
    let (target_core, target_suffix) = split_release_core(target_version);
    let (Some(remote_core), Some(target_core)) = (remote_core, target_core) else {
        // An unreadable or hand-built version is rolled forward rather than
        // silently left behind.
        return RemoteUpgradeDecision::Install;
    };
    if remote_core > target_core {
        return RemoteUpgradeDecision::RemoteIsNewer;
    }
    if remote_core < target_core {
        return RemoteUpgradeDecision::Install;
    }
    // Same release core: prereleases sort below their release, and two
    // distinct prerelease builds carry no order at all.
    match (remote_suffix, target_suffix) {
        (None, None) => RemoteUpgradeDecision::AlreadyCurrent,
        (None, Some(_)) => RemoteUpgradeDecision::RemoteIsNewer,
        (Some(_), None) => RemoteUpgradeDecision::Install,
        (Some(remote), Some(target)) if remote == target => RemoteUpgradeDecision::AlreadyCurrent,
        (Some(_), Some(_)) => RemoteUpgradeDecision::Unordered,
    }
}

/// A default fleet remote name derived from an ssh target, for the
/// save-this-target offer. Keeps the host part, drops the user and port, and
/// maps anything outside the remote-name charset to `-`.
// Remote attach and fleet upgrades are unix-only; the decision itself is
// platform-neutral so it stays testable everywhere.
#[cfg_attr(windows, allow(dead_code))]
pub(crate) fn remote_name_from_target(target: &str) -> Option<String> {
    let host = target.rsplit('@').next().unwrap_or(target);
    // `host:port` and bracketed IPv6 literals both reduce to the host.
    let host = host.trim_start_matches('[');
    let host = host.split(']').next().unwrap_or(host);
    let host = host.split(':').next().unwrap_or(host);
    let mut name = String::with_capacity(host.len());
    for ch in host.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
            name.push(ch);
        } else {
            name.push('-');
        }
    }
    let name = name.trim_matches('-').to_string();
    if name.is_empty() {
        return None;
    }
    // The derived name still has to be a legal remote name (length, reserved
    // `local`, and the rest); an illegal derivation means no default.
    crate::fleet::config::validate_remote_name(&name)
        .ok()
        .map(|()| name)
}

/// What `--remote` should do about the herdr binary on the target host.
///
/// Connecting never auto-installs and never auto-upgrades: only a host with
/// no herdr at all is bootstrapped. A host running a *different* version is
/// used as it is and left to the protocol window (`herdr remote upgrade`
/// rolls it forward when the user asks for it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
// Remote attach and fleet upgrades are unix-only; the decision itself is
// platform-neutral so it stays testable everywhere.
#[cfg_attr(windows, allow(dead_code))]
pub(crate) enum RemoteBootstrapAction {
    /// Use the herdr already installed over there, whatever version it is.
    UseInstalled,
    /// No herdr on the host: install one to bootstrap this launch.
    FreshInstall,
}

/// Pure fresh-install-only decision for a `--remote` launch.
// Remote attach and fleet upgrades are unix-only; the decision itself is
// platform-neutral so it stays testable everywhere.
#[cfg_attr(windows, allow(dead_code))]
pub(crate) fn remote_bootstrap_action(
    has_binary_override: bool,
    installed_version: Option<&str>,
) -> RemoteBootstrapAction {
    if has_binary_override {
        // HERDR_REMOTE_BINARY is an explicit "seed the remote from this
        // file" instruction, so it always installs.
        return RemoteBootstrapAction::FreshInstall;
    }
    match installed_version {
        Some(_) => RemoteBootstrapAction::UseInstalled,
        None => RemoteBootstrapAction::FreshInstall,
    }
}

#[cfg(test)]
mod shared_tests {
    use super::*;

    #[test]
    fn upgrades_are_forward_only() {
        assert_eq!(
            remote_upgrade_decision(Some("0.9.0"), "0.10.0"),
            RemoteUpgradeDecision::Install
        );
        assert_eq!(
            remote_upgrade_decision(Some("0.10.0"), "0.10.0"),
            RemoteUpgradeDecision::AlreadyCurrent
        );
        assert_eq!(
            remote_upgrade_decision(Some("0.11.0"), "0.10.0"),
            RemoteUpgradeDecision::RemoteIsNewer
        );
        // No herdr over there, or a version string we cannot read: install.
        assert_eq!(
            remote_upgrade_decision(None, "0.10.0"),
            RemoteUpgradeDecision::Install
        );
        assert_eq!(
            remote_upgrade_decision(Some("nightly-abc"), "0.10.0"),
            RemoteUpgradeDecision::Install
        );
    }

    #[test]
    fn preview_builds_compare_on_their_release_core_and_never_downgrade() {
        // A newer preview core is never downgraded by an older stable client.
        assert_eq!(
            remote_upgrade_decision(Some("0.8.0-preview.2026-07-21"), "0.7.5"),
            RemoteUpgradeDecision::RemoteIsNewer
        );
        // An older preview core still rolls forward.
        assert_eq!(
            remote_upgrade_decision(Some("0.7.0-preview.2026-06-01"), "0.7.5"),
            RemoteUpgradeDecision::Install
        );
        // A prerelease rolls forward to the release of its own core...
        assert_eq!(
            remote_upgrade_decision(Some("0.10.0-preview.40"), "0.10.0"),
            RemoteUpgradeDecision::Install
        );
        // ...and the release is never rewritten by a prerelease of that core.
        assert_eq!(
            remote_upgrade_decision(Some("0.10.0"), "0.10.0-preview.40"),
            RemoteUpgradeDecision::RemoteIsNewer
        );
        // Two different preview builds of the same core carry no order:
        // skipped, never reinstalled backwards.
        assert_eq!(
            remote_upgrade_decision(Some("0.10.0-preview.50"), "0.10.0-preview.40"),
            RemoteUpgradeDecision::Unordered
        );
        // Identical builds spelled differently are current, not unordered.
        assert_eq!(
            remote_upgrade_decision(Some("v0.10.0-preview.40"), "0.10.0-preview.40"),
            RemoteUpgradeDecision::AlreadyCurrent
        );
        assert_eq!(
            remote_upgrade_decision(Some("v0.10.0"), "0.10.0"),
            RemoteUpgradeDecision::AlreadyCurrent
        );
    }

    #[test]
    fn only_a_missing_remote_binary_bootstraps_an_install() {
        assert_eq!(
            remote_bootstrap_action(false, None),
            RemoteBootstrapAction::FreshInstall
        );
        // Skewed but present: connecting must not turn into an installer.
        assert_eq!(
            remote_bootstrap_action(false, Some("0.9.0")),
            RemoteBootstrapAction::UseInstalled
        );
        assert_eq!(
            remote_bootstrap_action(false, Some(crate::build_info::version().as_str())),
            RemoteBootstrapAction::UseInstalled
        );
        // An explicit binary override is a deliberate seeding instruction.
        assert_eq!(
            remote_bootstrap_action(true, Some("0.9.0")),
            RemoteBootstrapAction::FreshInstall
        );
    }

    #[test]
    fn derived_remote_names_are_valid_or_absent() {
        assert_eq!(
            remote_name_from_target("can@buildbox.example"),
            Some("buildbox.example".to_string())
        );
        assert_eq!(remote_name_from_target("gpu-1"), Some("gpu-1".to_string()));
        assert_eq!(
            remote_name_from_target("root@10.0.0.4:2222"),
            Some("10.0.0.4".to_string())
        );
        // `local` is reserved for the implicit local runtime.
        assert_eq!(remote_name_from_target("local"), None);
        assert_eq!(remote_name_from_target("@"), None);
    }
}

#[cfg(windows)]
pub(crate) const REATTACH_COMMAND_ENV_VAR: &str = "HERDR_REATTACH_COMMAND";
#[cfg(windows)]
pub(crate) const REMOTE_KEYBINDINGS_ENV_VAR: &str = "HERDR_REMOTE_KEYBINDINGS";

#[cfg(windows)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoteKeybindings {
    Local,
    Server,
}

#[cfg(windows)]
impl RemoteKeybindings {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "local" => Ok(Self::Local),
            "server" => Ok(Self::Server),
            _ => Err("--remote-keybindings must be 'local' or 'server'".to_string()),
        }
    }
}

#[cfg(windows)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteLaunch {
    pub(crate) target: String,
    pub(crate) keybindings: RemoteKeybindings,
    pub(crate) live_handoff: bool,
}

#[cfg(windows)]
pub(crate) fn extract_remote_args(
    args: &[String],
) -> Result<(Vec<String>, Option<RemoteLaunch>), String> {
    let mut cleaned = Vec::with_capacity(args.len());
    if let Some(program) = args.first() {
        cleaned.push(program.clone());
    }

    let mut remote_target = None;
    let mut keybindings = RemoteKeybindings::Local;
    let mut keybindings_seen = false;
    let mut live_handoff = false;
    let mut index = 1;
    while index < args.len() {
        let arg = &args[index];
        if arg == "--" {
            cleaned.extend_from_slice(&args[index..]);
            break;
        }
        if arg == "--handoff" {
            live_handoff = true;
            index += 1;
            continue;
        }
        if arg == "--remote" {
            if remote_target.is_some() {
                return Err("--remote can only be specified once".to_string());
            }
            let Some(value) = args.get(index + 1) else {
                return Err("missing value for --remote".to_string());
            };
            remote_target = Some(validate_remote_target(value)?.to_owned());
            index += 2;
            continue;
        }
        if let Some(value) = arg.strip_prefix("--remote=") {
            if remote_target.is_some() {
                return Err("--remote can only be specified once".to_string());
            }
            remote_target = Some(validate_remote_target(value)?.to_owned());
            index += 1;
            continue;
        }
        if arg == "--remote-keybindings" {
            if keybindings_seen {
                return Err("--remote-keybindings can only be specified once".to_string());
            }
            let Some(value) = args.get(index + 1) else {
                return Err("missing value for --remote-keybindings".to_string());
            };
            keybindings = RemoteKeybindings::parse(value)?;
            keybindings_seen = true;
            index += 2;
            continue;
        }
        if let Some(value) = arg.strip_prefix("--remote-keybindings=") {
            if keybindings_seen {
                return Err("--remote-keybindings can only be specified once".to_string());
            }
            keybindings = RemoteKeybindings::parse(value)?;
            keybindings_seen = true;
            index += 1;
            continue;
        }

        cleaned.push(arg.clone());
        index += 1;
    }

    let remote = remote_target.map(|target| RemoteLaunch {
        target,
        keybindings,
        live_handoff,
    });
    if remote.is_none() && keybindings_seen {
        return Err("--remote-keybindings requires --remote".to_string());
    }
    if remote.is_none() && live_handoff {
        cleaned.push("--handoff".to_string());
    }

    Ok((cleaned, remote))
}

#[cfg(windows)]
fn validate_remote_target(target: &str) -> Result<&str, String> {
    if target.is_empty() {
        return Err("missing value for --remote".to_string());
    }
    if target.starts_with('-') {
        return Err("--remote target must not start with '-'".to_string());
    }
    Ok(target)
}

#[cfg(windows)]
pub(crate) fn run_remote(_remote: RemoteLaunch) -> std::io::Result<()> {
    debug_assert!(!crate::platform::capabilities().remote_attach);
    Err(std::io::Error::other(
        "remote mode is not supported on Windows yet",
    ))
}

#[cfg(windows)]
pub(crate) fn run_remote_client_bridge() -> std::io::Result<()> {
    debug_assert!(!crate::platform::capabilities().remote_attach);
    Err(std::io::Error::other(
        "remote client bridge is not supported on Windows yet",
    ))
}

pub(crate) fn print_remote_error_hint(err: &std::io::Error, target: &str) {
    if is_remote_auth_error(err) {
        eprintln!(
            "hint: verify SSH access first with `{}`.",
            ssh_check_command(target)
        );
        eprintln!(
            "hint: if your SSH key has a passphrase, load it into ssh-agent with `ssh-add` before running `herdr --remote`."
        );
    }
}

fn is_remote_auth_error(err: &std::io::Error) -> bool {
    let message = err.to_string();
    message.contains("Permission denied")
        && (message.contains("(publickey")
            || message.contains("(keyboard-interactive")
            || message.contains("(password"))
}

fn ssh_check_command(target: &str) -> String {
    format!("ssh {}", shell_quote(target))
}

pub(crate) fn shell_quote(value: &str) -> String {
    if !value.is_empty()
        && value.chars().all(|ch| {
            ch.is_ascii_alphanumeric()
                || matches!(
                    ch,
                    '@' | '%' | '_' | '+' | '=' | ':' | ',' | '.' | '/' | '-'
                )
        })
    {
        return value.to_string();
    }

    format!("'{}'", value.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_auth_error_matches_ssh_auth_denied() {
        let err = std::io::Error::other(
            "remote platform detection failed: user@host: Permission denied (publickey).",
        );

        assert!(is_remote_auth_error(&err));
    }

    #[test]
    fn remote_auth_error_matches_keyboard_interactive_denied() {
        let err = std::io::Error::other(
            "remote server status failed: user@host: Permission denied (keyboard-interactive).",
        );

        assert!(is_remote_auth_error(&err));
    }

    #[test]
    fn remote_auth_error_ignores_non_auth_errors() {
        let err = std::io::Error::other("remote platform detection failed: unsupported platform");

        assert!(!is_remote_auth_error(&err));
    }

    #[test]
    fn ssh_check_command_quotes_remote_target() {
        assert_eq!(ssh_check_command("host name"), "ssh 'host name'");
    }
}

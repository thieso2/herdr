//! Persistent SSH stdio bridge child.
//!
//! One child per enabled remote: `ssh -T <target> "<resolve> [--session S]
//! bridge"`. The child's stdin/stdout carry the framed protocol directly —
//! no local socket hop and no install prompts (`BatchMode=yes` keeps ssh from
//! ever blocking on interactive auth).
//!
//! A bare binary name is *resolved* on the far side rather than execed
//! blindly: ssh runs the login shell non-interactively, so a managed install
//! under `~/.local/bin` is routinely off `PATH` (zsh reads only `.zshenv`
//! there). The resolver tries `PATH` first, then the managed install path,
//! and otherwise prints [`REMOTE_BINARY_MISSING_MARKER`] so the fleet manager
//! can tell "no herdr on this host" apart from every other bridge failure and
//! bootstrap one. An explicit path (from `--remote`, which already discovered
//! or installed it) is execed as given.
//!
//! The child's stderr is captured into a bounded tail so connection failures
//! surface ssh's own diagnosis (bad key, unknown host key, missing `herdr`)
//! in the remote's `last_error` instead of an opaque EOF.

use std::io::{self, Read};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Arc, Mutex};

use tracing::warn;

/// Upper bound on the retained ssh stderr tail.
const STDERR_TAIL_MAX: usize = 2048;

/// Where the resolver looks after `PATH`: the one directory herdr installs
/// into, so a managed install is always found even off a stripped `PATH`.
const MANAGED_INSTALL_DIR: &str = "$HOME/.local/bin";

/// Printed by the remote resolver when the host has no herdr at all.
///
/// This is a *contract*, not a heuristic: the string comes from the script
/// this module sends, never from ssh or a shell, so
/// [`diagnostics_report_missing_binary`] cannot confuse a missing binary with
/// an unrelated failure that happens to mention one.
pub const REMOTE_BINARY_MISSING_MARKER: &str = "herdr-bridge: remote binary not found";

/// Printed by the far-side bridge when herdr is installed but no server is
/// running for the requested session.
///
/// Like [`REMOTE_BINARY_MISSING_MARKER`] this is a contract, not a heuristic:
/// the far side emits exactly this string, so "nothing is running there" is
/// never confused with an unreachable host or a dead connection.
pub const REMOTE_SERVER_STOPPED_MARKER: &str = "herdr-bridge: no server running";

/// Whether a bridge failure's diagnostics say the far side has no herdr —
/// the only failure a bootstrap install can fix.
pub fn diagnostics_report_missing_binary(diagnostics: &str) -> bool {
    diagnostics.contains(REMOTE_BINARY_MISSING_MARKER)
}

/// Whether a bridge failure's diagnostics say the far side has herdr but no
/// running server — the only failure an explicit `remote start` can fix.
pub fn diagnostics_report_stopped_server(diagnostics: &str) -> bool {
    diagnostics.contains(REMOTE_SERVER_STOPPED_MARKER)
}

/// Pumps `reader` into `tail` until EOF, keeping only the newest
/// [`STDERR_TAIL_MAX`] bytes so a chatty ssh cannot grow memory unboundedly.
fn pump_stderr_tail(mut reader: impl Read, tail: &Arc<Mutex<String>>) {
    let mut buf = [0u8; 512];
    loop {
        let read = match reader.read(&mut buf) {
            Ok(0) | Err(_) => return,
            Ok(read) => read,
        };
        let chunk = String::from_utf8_lossy(&buf[..read]).into_owned();
        let Ok(mut tail) = tail.lock() else {
            return;
        };
        tail.push_str(&chunk);
        if tail.len() > STDERR_TAIL_MAX {
            let excess = tail.len() - STDERR_TAIL_MAX;
            // Trim on a char boundary at or after the excess.
            let cut = (excess..=tail.len())
                .find(|index| tail.is_char_boundary(*index))
                .unwrap_or(tail.len());
            tail.drain(..cut);
        }
    }
}

/// Builds the far-side command string executed by ssh.
///
/// A bare binary name (the saved-fleet case) is resolved on the far side;
/// anything path-shaped is a binary a `--remote` launch already located, so it
/// is execed verbatim.
pub fn remote_bridge_command_for(program: &str, session: &str) -> String {
    remote_bridge_command_with(program, session, false)
}

/// As [`remote_bridge_command_for`], but `start` permits the far side to
/// spawn a server if none is running. Only an explicit start passes true.
pub fn remote_bridge_command_with(program: &str, session: &str, start: bool) -> String {
    let args = bridge_args(session, start);
    if !is_bare_binary_name(program) {
        return format!("exec {program}{args}");
    }

    // Wrapped in `/bin/sh` so the resolver's syntax does not depend on which
    // login shell the remote account happens to use.
    format!(
        "exec /bin/sh -c {}",
        crate::remote::shell_quote(&resolve_script(program, &args))
    )
}

/// `--session S` (omitted for the default session) followed by the
/// subcommand: the tail every form of the bridge command ends with.
fn bridge_args(session: &str, start: bool) -> String {
    let mut args = String::new();
    if session != crate::session::DEFAULT_SESSION_NAME {
        args.push_str(" --session ");
        args.push_str(&crate::remote::shell_quote(session));
    }
    args.push_str(" bridge");
    if start {
        args.push_str(" --start");
    }
    args
}

/// Whether `program` is a plain command name to look up rather than a path to
/// exec. Deliberately narrow: only names that need no shell quoting at all can
/// be interpolated into the resolver script.
fn is_bare_binary_name(program: &str) -> bool {
    !program.is_empty()
        && program
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'))
}

/// The far-side lookup: `PATH`, then the managed install directory, then a
/// machine-readable "not installed here" report.
fn resolve_script(program: &str, args: &str) -> String {
    let managed = format!("{MANAGED_INSTALL_DIR}/{program}");
    let report = format!(
        "{REMOTE_BINARY_MISSING_MARKER}: {program} (searched PATH and {MANAGED_INSTALL_DIR})"
    );
    format!(
        "if command -v {program} >/dev/null 2>&1; then exec {program}{args}; fi\n\
         if [ -x \"{managed}\" ]; then exec \"{managed}\"{args}; fi\n\
         printf '%s\\n' {report} >&2\n\
         exit 127\n",
        report = crate::remote::shell_quote(&report)
    )
}

/// Starts a herdr server on `target` over one `bridge --start`, and waits
/// for it to come up.
///
/// The single implementation behind every explicit start: the fleet manager's
/// `remote.start` and the pure client's confirmation prompt both call it, so
/// they cannot drift apart on which binary they run or what counts as
/// success. `program` is the binary the caller already resolved for this
/// remote — a pinned path from `--remote`, else the fork's own name — because
/// starting a *different* binary than the one being connected is how a remote
/// ends up serving two versions.
///
/// Closing both pipe halves makes the far side pump nothing and exit as soon
/// as its daemon is up, so the child's exit status is the answer. Reading its
/// stdout instead would block forever: the framed protocol has the client
/// speak first, so a healthy bridge writes nothing on its own.
pub fn start_remote_server(target: &str, session: &str, program: &str) -> Result<(), String> {
    let (child, stdout, stdin) = BridgeChild::spawn_program_with(target, session, program, true)
        .map_err(|err| format!("ssh bridge spawn failed: {err}"))?;
    drop((stdout, stdin));
    let status = child.wait().map_err(|err| err.to_string())?;
    if status.success() {
        return Ok(());
    }
    let tail = child
        .stderr_tail()
        .lock()
        .map(|tail| tail.trim().replace('\n', "; "))
        .unwrap_or_default();
    Err(if tail.is_empty() {
        format!("the bridge exited with {status}")
    } else {
        tail
    })
}

/// A live SSH bridge child. Dropping it kills and reaps the child, which
/// closes both pipe halves.
pub struct BridgeChild {
    child: Arc<Mutex<Child>>,
    stderr_tail: Arc<Mutex<String>>,
}

/// Detached kill handle for deadline watchdogs: kills the bridge child
/// without owning the guard, unblocking any thread reading its stdout.
// Consumed only by the unix-only pure-client run path (#20/#23).
#[cfg_attr(windows, allow(dead_code))]
pub struct BridgeChildKiller {
    child: Arc<Mutex<Child>>,
}

impl BridgeChildKiller {
    /// Kills and reaps the child. Idempotent: killing an already-dead or
    /// already-reaped child is a no-op.
    // Consumed only by the unix-only pure-client run path (#20/#23).
    #[cfg_attr(windows, allow(dead_code))]
    pub fn kill(&self) {
        if let Ok(mut child) = self.child.lock() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl BridgeChild {
    /// Spawns the bridge child for `target`/`session` with piped stdio and a
    /// background thread capturing stderr into a bounded tail.
    pub fn spawn(target: &str, session: &str) -> io::Result<(Self, ChildStdout, ChildStdin)> {
        Self::spawn_with(target, session, false)
    }

    fn spawn_with(
        target: &str,
        session: &str,
        start: bool,
    ) -> io::Result<(Self, ChildStdout, ChildStdin)> {
        // The fleet bridge is a separate path from the `--remote` bootstrap:
        // it invokes the remote binary by bare name over ssh, so it has to ask
        // for the fork's name or it finds an unrelated upstream herdr (or, as
        // reported, nothing at all).
        Self::spawn_program_with(target, session, crate::identity::BRAND, start)
    }

    /// Spawns the bridge child running an explicit remote herdr path.
    pub fn spawn_program(
        target: &str,
        session: &str,
        program: &str,
    ) -> io::Result<(Self, ChildStdout, ChildStdin)> {
        Self::spawn_program_with(target, session, program, false)
    }

    fn spawn_program_with(
        target: &str,
        session: &str,
        program: &str,
        start: bool,
    ) -> io::Result<(Self, ChildStdout, ChildStdin)> {
        let mut command = Command::new("ssh");
        command
            .arg("-T")
            .arg("-o")
            .arg("BatchMode=yes")
            .arg(target)
            .arg(remote_bridge_command_with(program, session, start))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        crate::platform::configure_background_command(&mut command);

        let mut child = command.spawn().map_err(|err| {
            io::Error::new(err.kind(), format!("failed to start ssh bridge: {err}"))
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            io::Error::new(io::ErrorKind::BrokenPipe, "ssh bridge stdout missing")
        })?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "ssh bridge stdin missing"))?;

        let stderr_tail = Arc::new(Mutex::new(String::new()));
        if let Some(stderr) = child.stderr.take() {
            let tail = Arc::clone(&stderr_tail);
            let thread = std::thread::Builder::new()
                .name("fleet-bridge-stderr".to_string())
                .spawn(move || pump_stderr_tail(stderr, &tail));
            if thread.is_err() {
                // Diagnostics are best-effort; the bridge still works.
                warn!("failed to spawn ssh bridge stderr reader thread");
            }
        }
        Ok((
            Self {
                child: Arc::new(Mutex::new(child)),
                stderr_tail,
            },
            stdout,
            stdin,
        ))
    }

    /// Waits for the child to exit and returns its status.
    ///
    /// Used by the explicit start, whose whole job is the daemon the child
    /// leaves behind: with stdin closed the far side pumps nothing, sees
    /// EOF and exits, so its status *is* the answer. Reading its stdout
    /// would block forever instead - the framed protocol has the client
    /// speak first, so a healthy bridge writes nothing on its own.
    pub fn wait(&self) -> io::Result<std::process::ExitStatus> {
        let mut child = self
            .child
            .lock()
            .map_err(|_| io::Error::other("bridge child mutex is poisoned"))?;
        child.wait()
    }

    /// Shared handle to the bounded stderr tail for failure diagnostics.
    pub fn stderr_tail(&self) -> Arc<Mutex<String>> {
        Arc::clone(&self.stderr_tail)
    }

    /// A kill handle usable from another thread (handshake watchdogs).
    // Consumed only by the unix-only pure-client run path (#20/#23).
    #[cfg_attr(windows, allow(dead_code))]
    pub fn killer(&self) -> BridgeChildKiller {
        BridgeChildKiller {
            child: Arc::clone(&self.child),
        }
    }
}

impl Drop for BridgeChild {
    fn drop(&mut self) {
        if let Ok(mut child) = self.child.lock() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_bridge_program_is_the_fork_binary() {
        // Regression: the bridge used to exec bare `herdr`, which on a host
        // with no upstream install failed with "command not found: herdr".
        let brand = crate::identity::BRAND;
        let command = remote_bridge_command_for(brand, crate::session::DEFAULT_SESSION_NAME);
        assert!(
            command.contains(&format!("command -v {brand}")),
            "{command}"
        );
        assert!(!command.contains("herdr bridge") || brand.ends_with("herdr"));
        assert_ne!(brand, "herdr");
    }

    #[test]
    fn a_bare_name_is_resolved_through_path_then_the_managed_install_dir() {
        // Regression: ssh runs the login shell non-interactively, so
        // `~/.local/bin` is routinely off PATH and a bare `exec overherdr`
        // died with "command not found" on a host that *had* it installed.
        let command = remote_bridge_command_for("overherdr", "default");
        assert!(
            command.starts_with("exec /bin/sh -c "),
            "the resolver must not depend on the remote login shell: {command}"
        );
        assert!(command.contains("command -v overherdr"), "{command}");
        assert!(
            command.contains("$HOME/.local/bin/overherdr"),
            "the managed install path must be searched: {command}"
        );
        // Both branches exec, so the bridge process is the shell's own.
        assert_eq!(command.matches("exec").count(), 3, "{command}");
    }

    #[test]
    fn an_unresolvable_bare_name_reports_the_missing_binary_marker() {
        let command = remote_bridge_command_for("overherdr", "default");
        assert!(
            command.contains(REMOTE_BINARY_MISSING_MARKER),
            "the fleet manager keys its bootstrap install off this marker: {command}"
        );
        assert!(command.contains("exit 127"), "{command}");
        assert!(diagnostics_report_missing_binary(&format!(
            "bridge closed: unexpected end of stream (ssh: {REMOTE_BINARY_MISSING_MARKER}: overherdr)"
        )));
        // Every other bridge failure must stay untouched by the installer.
        assert!(!diagnostics_report_missing_binary(
            "bridge closed: unexpected end of stream (ssh: Permission denied (publickey))"
        ));
    }

    #[test]
    fn the_three_far_side_outcomes_are_told_apart_by_their_own_markers() {
        // The manager routes on these: a missing binary is installed, a
        // stopped server is offered a start, and everything else retries.
        // Each marker must therefore match only its own failure.
        let missing = format!("bridge closed (ssh: {REMOTE_BINARY_MISSING_MARKER}: overherdr)");
        let stopped =
            format!("bridge closed (ssh: {REMOTE_SERVER_STOPPED_MARKER} for session default)");
        let unreachable = "bridge closed (ssh: Permission denied (publickey))";

        assert!(diagnostics_report_missing_binary(&missing));
        assert!(!diagnostics_report_stopped_server(&missing));

        assert!(diagnostics_report_stopped_server(&stopped));
        assert!(
            !diagnostics_report_missing_binary(&stopped),
            "a stopped server must never trigger a bootstrap install"
        );

        assert!(!diagnostics_report_missing_binary(unreachable));
        assert!(!diagnostics_report_stopped_server(unreachable));
    }

    #[test]
    fn a_pinned_path_is_execed_verbatim() {
        // `--remote` already discovered or installed this exact binary;
        // re-resolving could pick a different one.
        assert_eq!(
            remote_bridge_command_for("/home/can/.local/bin/herdr", "default"),
            "exec /home/can/.local/bin/herdr bridge"
        );
        assert_eq!(
            remote_bridge_command_for("\"$HOME/.local/bin/herdr\"", "work"),
            "exec \"$HOME/.local/bin/herdr\" --session work bridge"
        );
    }

    #[test]
    fn stderr_tail_is_bounded_and_keeps_the_newest_output() {
        let tail = Arc::new(Mutex::new(String::new()));
        let noise = "x".repeat(STDERR_TAIL_MAX * 2);
        let input = format!("{noise}Permission denied (publickey)\n");
        pump_stderr_tail(io::Cursor::new(input.into_bytes()), &tail);
        let tail = tail.lock().unwrap();
        assert!(
            tail.len() <= STDERR_TAIL_MAX,
            "tail too long: {}",
            tail.len()
        );
        assert!(
            tail.ends_with("Permission denied (publickey)\n"),
            "newest output must be kept: {tail}"
        );
    }

    #[test]
    fn bridge_command_quotes_named_session() {
        assert_eq!(bridge_args("default", false), " bridge");
        assert_eq!(bridge_args("work", false), " --session work bridge");
        assert_eq!(
            bridge_args("with'quote", false),
            " --session 'with'\\''quote' bridge"
        );
        // Only an explicit start may spawn a server on the far side.
        assert_eq!(bridge_args("default", true), " bridge --start");
        assert_eq!(bridge_args("work", true), " --session work bridge --start");
        // Both resolver branches carry the same quoted session through.
        let script = resolve_script("overherdr", &bridge_args("with'quote", false));
        assert_eq!(
            script.matches(" --session 'with'\\''quote' bridge").count(),
            2,
            "{script}"
        );
    }

    #[test]
    fn only_plain_command_names_take_the_resolver_path() {
        for bare in ["overherdr", "herdr-dev", "herdr.test", "a_b"] {
            assert!(is_bare_binary_name(bare), "{bare}");
        }
        // Anything path-shaped or needing quoting is execed as given, so no
        // caller-supplied text is ever interpolated into the script.
        for path in [
            "/usr/bin/herdr",
            "./herdr",
            "",
            "\"$HOME/bin/herdr\"",
            "a b",
        ] {
            assert!(!is_bare_binary_name(path), "{path}");
        }
    }
}

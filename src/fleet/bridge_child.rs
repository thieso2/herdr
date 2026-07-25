//! Persistent SSH stdio bridge child.
//!
//! One child per enabled remote: `ssh -T <target> "exec herdr [--session S]
//! bridge"`. The child's stdin/stdout carry the framed protocol directly —
//! no local socket hop and no install prompts (`BatchMode=yes` keeps ssh from
//! ever blocking on interactive auth). If `herdr` is missing on the far side
//! the child exits and the remote shows as offline.
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
pub fn remote_bridge_command(session: &str) -> String {
    let mut command = String::from("exec herdr");
    if session != crate::session::DEFAULT_SESSION_NAME {
        command.push_str(" --session ");
        command.push_str(&crate::remote::shell_quote(session));
    }
    command.push_str(" bridge");
    command
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
        let mut command = Command::new("ssh");
        command
            .arg("-T")
            .arg("-o")
            .arg("BatchMode=yes")
            .arg(target)
            .arg(remote_bridge_command(session))
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
    fn bridge_command_omits_default_session() {
        assert_eq!(remote_bridge_command("default"), "exec herdr bridge");
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
        assert_eq!(
            remote_bridge_command("work"),
            "exec herdr --session work bridge"
        );
        assert_eq!(
            remote_bridge_command("with'quote"),
            "exec herdr --session 'with'\\''quote' bridge"
        );
    }
}

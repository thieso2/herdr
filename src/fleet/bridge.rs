//! Far side of an SSH stdio bridge: the `herdr bridge` subcommand.
//!
//! Invoked on a remote host as `ssh -T <target> "exec herdr [--session S]
//! bridge"`. It connects the server's API socket (where the framed protocol
//! is spoken) and pumps stdin/stdout to and from that socket. All protocol
//! negotiation happens end-to-end between the fleet client and the server;
//! the bridge itself is a byte pump.
//!
//! Connecting never starts a server. Spawning a daemon writes to the host and
//! outlives the connection, so it is not something a background reconnect
//! should do behind the user's back: with no server listening the bridge
//! reports [`crate::fleet::bridge_child::REMOTE_SERVER_STOPPED_MARKER`] and
//! exits, the remote parks in a dimmed `stopped` state, and `herdr remote
//! start` (or the TUI's confirmation) is what actually starts one — that is
//! `bridge --start`.

/// Exit code for "herdr is here, but no server is running". Distinct from
/// the resolver's 127 (no binary at all) so the two are never conflated.
pub const BRIDGE_NO_SERVER_EXIT: i32 = 3;

#[cfg(unix)]
pub fn run_bridge(start: bool) -> std::io::Result<()> {
    unix::run_bridge(start)
}

#[cfg(windows)]
pub fn run_bridge(_start: bool) -> std::io::Result<()> {
    debug_assert!(!crate::platform::capabilities().remote_attach);
    Err(std::io::Error::other(
        "the herdr bridge is not supported on Windows yet",
    ))
}

#[cfg(unix)]
mod unix {
    use std::io;
    use std::os::unix::net::UnixStream;
    use std::thread;
    use std::time::Duration;

    const SERVER_START_TIMEOUT: Duration = Duration::from_secs(5);

    pub(super) fn run_bridge(start: bool) -> io::Result<()> {
        if !ensure_server_running(start)? {
            // Not an error the caller should retry into: report the one fact
            // the fleet manager routes on, then exit with its own code.
            eprintln!(
                "{}: session {}",
                super::super::bridge_child::REMOTE_SERVER_STOPPED_MARKER,
                crate::session::active_name()
                    .unwrap_or_else(|| crate::session::DEFAULT_SESSION_NAME.to_string())
            );
            std::process::exit(super::BRIDGE_NO_SERVER_EXIT);
        }

        let socket_path = crate::api::socket_path();
        let stream = UnixStream::connect(&socket_path).map_err(|err| {
            io::Error::new(
                err.kind(),
                format!(
                    "failed to connect to herdr API socket {}: {err}",
                    socket_path.display()
                ),
            )
        })?;

        let mut stdout = io::stdout().lock();
        let mut socket_to_stdout = stream.try_clone()?;
        let mut stdin_to_socket = stream;

        let _upload = thread::spawn(move || {
            let mut stdin = io::stdin();
            let _ = crate::remote::copy_flush(&mut stdin, &mut stdin_to_socket);
            let _ = stdin_to_socket.shutdown(std::net::Shutdown::Write);
        });

        crate::remote::copy_flush(&mut socket_to_stdout, &mut stdout).map(|_| ())
    }

    /// Whether a server is available to pump against. Only `start` may bring
    /// one up; otherwise a missing server is reported, never fixed.
    fn ensure_server_running(start: bool) -> io::Result<bool> {
        if crate::server::autodetect::is_server_listening() {
            return Ok(true);
        }
        if !start {
            return Ok(false);
        }
        crate::server::autodetect::spawn_server_daemon()?;
        crate::server::autodetect::wait_for_server_socket(
            &crate::api::socket_path(),
            SERVER_START_TIMEOUT,
        )
        .map(|()| true)
    }
}

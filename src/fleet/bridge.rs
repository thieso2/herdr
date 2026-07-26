//! Far side of an SSH stdio bridge: the `herdr bridge` subcommand.
//!
//! Invoked on a remote host as `ssh -T <target> "exec herdr [--session S]
//! bridge"`. It makes sure a herdr server is running for the requested
//! session, connects the server's API socket (where the framed protocol is
//! spoken), and pumps stdin/stdout to and from that socket. All protocol
//! negotiation happens end-to-end between the fleet client and the server;
//! the bridge itself is a byte pump.

#[cfg(unix)]
pub fn run_bridge() -> std::io::Result<()> {
    unix::run_bridge()
}

#[cfg(windows)]
pub fn run_bridge() -> std::io::Result<()> {
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

    pub(super) fn run_bridge() -> io::Result<()> {
        ensure_server_running()?;

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

    fn ensure_server_running() -> io::Result<()> {
        if crate::server::autodetect::is_server_listening() {
            return Ok(());
        }
        crate::server::autodetect::spawn_server_daemon()?;
        crate::server::autodetect::wait_for_server_socket(
            &crate::api::socket_path(),
            SERVER_START_TIMEOUT,
        )
    }
}

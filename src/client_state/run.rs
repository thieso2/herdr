//! Pure-client run loop: the TUI as a framed-protocol client of the local
//! server (remote #0) plus one framed session per enabled fleet remote.
//!
//! Enabled by `[experimental] pure_client` (or `HERDR_PURE_CLIENT=1`). The
//! loop owns the keyed [`super::RemoteMirrors`]: per remote it negotiates a
//! framed session with the `catalog` capability (local over the API socket,
//! fleet remotes over an SSH stdio bridge child), resyncs the session
//! catalog from `session.snapshot`, applies `catalog.event` frames, opens
//! pane streams for the visible tab into
//! [`crate::terminal::replica::PaneReplica`]s, and renders by composing the
//! in-view mirrors plus chrome through the shared `compute_view` + `render`
//! pair. Input is interpreted client-side and dispatched to the remote
//! owning the focused space or pane; chip clicks mutate view membership
//! only — connections are never touched by selection. Notifications arrive
//! from every remote; the focused remote wins the window title.

#![cfg(unix)]

use std::collections::{HashMap, HashSet};
use std::io::{self, IsTerminal as _, Write as _};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use crossterm::event::{MouseEvent, MouseEventKind};
use interprocess::local_socket::traits::Stream as _;
use interprocess::TryClone as _;
use tracing::{debug, info, warn};

use std::collections::BTreeMap;

use crate::app::{AppState, Mode};
use crate::fleet::bridge_child::BridgeChild;
use crate::ipc::LocalStream;
use crate::protocol::framed::{
    control_error, pane_send_bytes_request, parse_session_snapshot, parse_session_welcome,
    parse_stream_opened, read_frame, session_hello_request_with_capabilities,
    session_snapshot_request, stream_close_request, stream_open_request, stream_resize_request,
    write_frame, Frame, FrameType, FramedCodecError, HelloRemedy, SessionWelcome, StreamMode,
    CAPABILITY_CATALOG, CAPABILITY_PANE_STREAM, CATALOG_EVENT, CATALOG_RESYNC_EVENT,
    CONTROL_STREAM_ID, FRAMED_MAGIC, PANE_WRITE_LOCKED_ERROR, SERVER_STOPPING_EVENT,
    STREAM_CLOSED_EVENT, STREAM_REVOKED_EVENT,
};
use crate::terminal::TerminalId;

use super::chrome::GlobalChrome;
use super::compose::{apply_client_config, compose_fleet_into, ComposeIds, MirrorPaneSource};
use super::fleet_view::{remote_descriptors, RemoteDescriptor};
use super::{RemoteMirrors, SessionCatalog, LOCAL_REMOTE_INDEX};

/// How long the pure client waits for the `session.hello` answer.
const HELLO_TIMEOUT: Duration = Duration::from_secs(5);

/// Idle poll tick of the run loop.
const LOOP_TICK: Duration = Duration::from_millis(30);

/// Release default of the pure-client mode when `[experimental]
/// pure_client` is not set explicitly.
///
/// FLIPPED: the pure client is the default run path. An explicit
/// `pure_client = true`/`false` in the user's config always wins over this
/// default (see [`pure_client_enabled`]), so opt-outs keep their behavior,
/// and the legacy path remains reachable with `pure_client = false` until
/// it is deleted. Windows keeps ignoring the flag entirely
/// (`run_client_with_mode` only consults it on unix), so this constant does
/// not change Windows behavior.
pub(crate) const PURE_CLIENT_DEFAULT: bool = true;

/// Whether the pure-client run path is enabled for this process.
///
/// Resolution order: `HERDR_PURE_CLIENT=1`/`0` (test/dev override,
/// following the `HERDR_RENDER_ENCODING` precedent) beats the explicit
/// `[experimental] pure_client` setting, which beats
/// [`PURE_CLIENT_DEFAULT`].
pub(crate) fn pure_client_enabled(config: &crate::config::Config) -> bool {
    match std::env::var("HERDR_PURE_CLIENT") {
        Ok(value) if value == "1" => true,
        Ok(value) if value == "0" => false,
        _ => config
            .experimental
            .pure_client
            .unwrap_or(PURE_CLIENT_DEFAULT),
    }
}

/// Double-click window for the remote chips, matching the sidebar's.
const CHIP_DOUBLE_CLICK_WINDOW: Duration = Duration::from_millis(350);
/// How long a chip status flash (refused toggle) stays visible.
const STATUS_FLASH_TTL: Duration = Duration::from_millis(2500);
/// Handshake deadline for a fleet remote's `session.hello` answer, matching
/// the fleet manager's tuning. A watchdog kills the bridge child at the
/// deadline so the connect thread's blocking read always returns.
const REMOTE_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Heartbeat ping cadence on established fleet remote sessions.
const REMOTE_PING_INTERVAL: Duration = Duration::from_secs(5);
/// A fleet remote with no inbound frame for this long is treated as dead:
/// a silently dropped transport must not keep a connected chip dot.
const REMOTE_PONG_TIMEOUT: Duration = Duration::from_secs(15);
/// Bound on encoded frames queued to one fleet remote's writer thread; a
/// stalled remote fails its writes instead of blocking the run loop.
const REMOTE_WRITE_QUEUE_FRAMES: usize = 1024;
/// One visual spinner step (`spinner_frame` divides the tick by 8).
const SPINNER_FRAME_INTERVAL: Duration = Duration::from_millis(120);

/// Events the pure-client loop multiplexes, tagged with the remote they
/// belong to.
enum LoopEvent {
    Stdin(Vec<u8>),
    Resize(u16, u16),
    /// A frame read by a remote's reader thread, tagged with the link
    /// generation it was read under so frames from a torn-down session
    /// never reach a successor's fresh mirror.
    Frame(usize, u64, Frame),
    /// A reader thread's transport died. Generation-tagged like frames so
    /// a stale reader cannot demote its successor's link.
    Disconnected(usize, u64),
    /// A fleet remote's connect thread finished its handshake. The
    /// generation drops results from threads whose link was reconciled
    /// away (or replaced) while they were connecting.
    Established(usize, u64, Box<RemoteEstablished>),
    /// An explicit `bridge --start` finished. Carries the failure so the
    /// prompt can show it; `Ok` means the daemon is up and the remote can
    /// reconnect.
    RemoteStarted(usize, Result<(), String>),
    /// Outcome of an explicit stop asked for from the remotes list.
    RemoteStopped(usize, Result<(), String>),
}

/// Outcome of a remote's connect-plus-handshake thread. On success the
/// thread keeps running as the session's frame reader; the writer half and
/// the child guard travel here to the loop. The local runtime connects the
/// same way, over the API socket instead of an SSH bridge child.
enum RemoteEstablished {
    /// The local API socket negotiated a session: the connect thread kept
    /// the read half and the ready session travels here.
    LocalConnected {
        welcome: SessionWelcome,
        session: Box<Session>,
    },
    Connected {
        welcome: SessionWelcome,
        writer: std::process::ChildStdin,
        guard: BridgeChild,
    },
    Incompatible {
        remedy: HelloRemedy,
        message: String,
    },
    /// The far side has herdr but no running server. Terminal until the user
    /// approves a start, so it is its own outcome and not a `Failed`.
    Stopped(String),
    Failed(String),
}

/// In-flight control requests awaiting their response frame.
enum Pending {
    Snapshot,
    /// Heartbeat ping; any inbound frame already proves liveness, so the
    /// pong itself needs no further handling.
    Ping,
    StreamOpen {
        pane_id: String,
        mode: StreamMode,
    },
    History {
        stream_id: u32,
    },
    Resize {
        stream_id: u32,
    },
    Api,
}

/// Where a session's encoded frames go.
enum SessionWriter {
    /// Blocking writes on the loop thread (the local socket).
    Direct(Box<dyn io::Write + Send>),
    /// Frames handed to a per-remote writer thread. A full queue fails the
    /// write instead of blocking the run loop on one stalled remote's pipe.
    Threaded(mpsc::SyncSender<Vec<u8>>),
}

/// One connected framed session (local socket or SSH bridge child stdio).
pub(super) struct Session {
    writer: SessionWriter,
    /// Keeps the SSH bridge child alive for the session's lifetime; dropped
    /// with the session, which kills the child and unblocks its reader.
    _guard: Option<BridgeChild>,
    /// The link generation this session and its reader thread belong to;
    /// events tagged with another generation are stale.
    generation: u64,
    /// When the last frame arrived, for the fleet heartbeat.
    last_inbound: Instant,
    /// When the next heartbeat ping is due (fleet remotes only).
    next_ping: Instant,
    next_request_id: u64,
    pending: HashMap<String, Pending>,
    /// Last size sent per stream id, to keep stream.resize idempotent.
    sent_sizes: HashMap<u32, (u16, u16)>,
    /// Streams opened read-only because another client holds the pane's
    /// write grant; their geometry belongs to that writer, so no
    /// stream.resize is sent on them.
    read_only: HashSet<u32>,
}

impl Session {
    fn request_id(&mut self, kind: &str) -> String {
        let id = self.next_request_id;
        self.next_request_id += 1;
        format!("pure:{kind}:{id}")
    }

    fn send_control(&mut self, value: &serde_json::Value) -> io::Result<()> {
        let payload = serde_json::to_vec(value)
            .map_err(|err| io::Error::other(format!("failed to encode control frame: {err}")))?;
        let map_codec = |err: FramedCodecError| match err {
            FramedCodecError::Io(err) => err,
            other => io::Error::new(io::ErrorKind::InvalidData, other.to_string()),
        };
        match &mut self.writer {
            SessionWriter::Direct(writer) => {
                write_frame(writer, FrameType::Control, CONTROL_STREAM_ID, &payload)
                    .map_err(map_codec)
            }
            SessionWriter::Threaded(tx) => {
                let mut buf = Vec::with_capacity(payload.len() + 16);
                write_frame(&mut buf, FrameType::Control, CONTROL_STREAM_ID, &payload)
                    .map_err(map_codec)?;
                tx.try_send(buf).map_err(|err| match err {
                    mpsc::TrySendError::Full(_) => io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "remote write queue full (stalled connection)",
                    ),
                    mpsc::TrySendError::Disconnected(_) => {
                        io::Error::new(io::ErrorKind::BrokenPipe, "remote writer thread ended")
                    }
                })
            }
        }
    }
}

/// Per-remote connection links, keyed like the mirrors.
pub(super) type Links = BTreeMap<usize, Link>;

/// The live session for a remote, when its link is up.
fn session_for(links: &mut Links, remote: usize) -> Option<&mut Session> {
    match links.get_mut(&remote) {
        Some(Link::Up(session)) => Some(session.as_mut()),
        _ => None,
    }
}

/// Capabilities every pure-client session asks for.
const SESSION_CAPABILITIES: &[&str] = &[
    CAPABILITY_PANE_STREAM,
    CAPABILITY_CATALOG,
    crate::protocol::framed::CAPABILITY_NOTIFICATION,
    crate::protocol::framed::CAPABILITY_WINDOW_TITLE,
    crate::protocol::framed::CAPABILITY_PASTE_IMAGE,
];

/// Outcome of one local connect attempt.
enum ConnectOutcome {
    Connected(Box<ConnectedLocal>),
    Incompatible {
        remedy: HelloRemedy,
        message: String,
    },
    Failed(String),
}

/// The connected-variant payload, boxed to keep the outcome enum small.
struct ConnectedLocal {
    session: Session,
    reader: LocalStream,
    welcome: SessionWelcome,
}

/// Why a `session.hello` did not produce a session.
enum HelloRejection {
    /// Terminal for this pairing: the peer is outside the supported window
    /// (or lacks a capability the client cannot run without). The remedy is
    /// the server's own, never guessed.
    Incompatible {
        remedy: HelloRemedy,
        message: String,
    },
    /// Retryable failure (transport, malformed answer, other rejections).
    Failed(String),
}

/// Interprets a `session.hello` answer.
fn interpret_hello_answer(response: &serde_json::Value) -> Result<SessionWelcome, HelloRejection> {
    if let Some(error) = control_error(response) {
        if error.code == crate::protocol::framed::PROTOCOL_OUT_OF_WINDOW_CODE {
            return Err(HelloRejection::Incompatible {
                // The server names which side must upgrade; a rejection
                // without a remedy is conservatively read as "this client is
                // too old", the only side we can act on locally.
                remedy: crate::protocol::framed::parse_hello_remedy(&error)
                    .unwrap_or(HelloRemedy::UpgradeClient),
                message: error.message,
            });
        }
        return Err(HelloRejection::Failed(format!(
            "session.hello rejected: {}",
            error.message
        )));
    }
    match parse_session_welcome(response) {
        Ok(welcome) => {
            match crate::protocol::framed::check_required_capabilities(
                &welcome,
                crate::protocol::framed::REQUIRED_CATALOG_CAPABILITIES,
            ) {
                Ok(()) => Ok(welcome),
                Err(crate::protocol::framed::HelloError::OutOfWindow { remedy, message }) => {
                    Err(HelloRejection::Incompatible { remedy, message })
                }
                Err(crate::protocol::framed::HelloError::InvalidWindow { message }) => {
                    Err(HelloRejection::Failed(message))
                }
            }
        }
        Err(err) => Err(HelloRejection::Failed(err)),
    }
}

fn fresh_session(writer: SessionWriter, guard: Option<BridgeChild>, generation: u64) -> Session {
    let now = Instant::now();
    Session {
        writer,
        _guard: guard,
        generation,
        last_inbound: now,
        next_ping: now + REMOTE_PING_INTERVAL,
        next_request_id: 1,
        pending: HashMap::new(),
        sent_sizes: HashMap::new(),
        read_only: HashSet::new(),
    }
}

/// Connects to the local API socket and negotiates a catalog session.
fn connect(session: &str) -> ConnectOutcome {
    // A local runtime names its session like any other fleet entry, so two
    // local entries differing only by session are two independent servers.
    // The default session still resolves through the environment overrides
    // `active_api_socket_path` honors.
    let socket_path = if session == crate::session::DEFAULT_SESSION_NAME {
        crate::api::socket_path()
    } else {
        crate::session::api_socket_path_for(Some(session))
    };
    let mut stream = match crate::ipc::connect_local_stream(&socket_path) {
        Ok(stream) => stream,
        Err(err) => return ConnectOutcome::Failed(format!("api socket unreachable: {err}")),
    };
    if let Err(err) = stream
        .set_nonblocking(false)
        .and_then(|()| stream.write_all(&FRAMED_MAGIC))
        .and_then(|()| stream.flush())
    {
        return ConnectOutcome::Failed(format!("framed handshake failed: {err}"));
    }

    let hello = session_hello_request_with_capabilities("pure:hello:0", SESSION_CAPABILITIES);
    let payload = match serde_json::to_vec(&hello) {
        Ok(payload) => payload,
        Err(err) => return ConnectOutcome::Failed(format!("hello encode failed: {err}")),
    };
    if let Err(err) = write_frame(&mut stream, FrameType::Control, CONTROL_STREAM_ID, &payload) {
        return ConnectOutcome::Failed(format!("session.hello send failed: {err}"));
    }
    let _ = stream.set_recv_timeout(Some(HELLO_TIMEOUT));
    let response = loop {
        match read_frame(&mut stream) {
            Ok(frame) if frame.frame_type == FrameType::Control => {
                match serde_json::from_slice::<serde_json::Value>(&frame.payload) {
                    Ok(value) => break value,
                    Err(err) => {
                        return ConnectOutcome::Failed(format!("invalid hello answer: {err}"))
                    }
                }
            }
            Ok(_) => continue,
            Err(err) => return ConnectOutcome::Failed(format!("session.hello failed: {err}")),
        }
    };
    let _ = stream.set_recv_timeout(None);

    match interpret_hello_answer(&response) {
        Ok(welcome) => {
            info!(
                protocol = welcome.protocol,
                server_version = %welcome.server_version,
                "pure client negotiated framed catalog session"
            );
            let reader = match stream.try_clone() {
                Ok(reader) => reader,
                Err(err) => return ConnectOutcome::Failed(format!("socket clone failed: {err}")),
            };
            ConnectOutcome::Connected(Box::new(ConnectedLocal {
                // Generation 0 placeholder; `establish_local` stamps the
                // real link generation before the reader thread starts.
                session: fresh_session(SessionWriter::Direct(Box::new(stream)), None, 0),
                reader,
                welcome,
            }))
        }
        Err(HelloRejection::Incompatible { remedy, message }) => {
            ConnectOutcome::Incompatible { remedy, message }
        }
        Err(HelloRejection::Failed(err)) => ConnectOutcome::Failed(err),
    }
}

/// Connect-plus-handshake for one fleet remote, run on its own thread: the
/// SSH bridge child's stdio carries the framed protocol directly. On
/// success the same thread becomes the session's frame reader, so the child
/// stdout never crosses threads.
fn remote_connect_and_read(
    descriptor: RemoteDescriptor,
    generation: u64,
    event_tx: mpsc::SyncSender<LoopEvent>,
    should_quit: Arc<AtomicBool>,
) {
    let remote = descriptor.index;
    let Some(target) = descriptor.target.as_deref() else {
        return;
    };
    let established: Result<_, String> = (|| {
        let (child, mut stdout, mut stdin) = BridgeChild::spawn_program(
            target,
            &descriptor.session,
            // Same reason the fleet transport asks for the fork by name: a
            // saved remote with no pinned path must not look up `herdr`.
            descriptor
                .program
                .as_deref()
                .unwrap_or(crate::identity::BRAND),
        )
        .map_err(|err| format!("ssh bridge spawn failed: {err}"))?;
        // Every failure below has to carry the child's stderr tail, because
        // that tail is where the far side's own report lives - including the
        // "no server running" marker. A path that drops it silently
        // downgrades a stopped remote into an ordinary retrying outage.
        let with_tail = |message: String, child: &BridgeChild| -> String {
            let tail = child
                .stderr_tail()
                .lock()
                .map(|tail| tail.trim().replace('\n', "; "))
                .unwrap_or_default();
            if tail.is_empty() {
                message
            } else {
                format!("{message} (ssh: {tail})")
            }
        };
        // Watchdog: the hello-answer read below is a blocking pipe read with
        // no native timeout, so a remote that accepts the connection but
        // never answers would wedge this thread and its link forever.
        // Killing the child at the deadline forces the read to fail into
        // the normal retry path.
        let handshake_started = Instant::now();
        let (handshake_done_tx, handshake_done_rx) = mpsc::channel::<()>();
        let killer = child.killer();
        std::thread::spawn(move || {
            if handshake_done_rx
                .recv_timeout(REMOTE_HANDSHAKE_TIMEOUT)
                .is_err()
            {
                killer.kill();
            }
        });
        stdin
            .write_all(&FRAMED_MAGIC)
            .and_then(|()| stdin.flush())
            .map_err(|err| with_tail(format!("framed handshake failed: {err}"), &child))?;
        let hello = session_hello_request_with_capabilities("pure:hello:0", SESSION_CAPABILITIES);
        let payload =
            serde_json::to_vec(&hello).map_err(|err| format!("hello encode failed: {err}"))?;
        write_frame(&mut stdin, FrameType::Control, CONTROL_STREAM_ID, &payload)
            .map_err(|err| with_tail(format!("session.hello send failed: {err}"), &child))?;
        let response = loop {
            let frame = read_frame(&mut stdout).map_err(|err| {
                if handshake_started.elapsed() >= REMOTE_HANDSHAKE_TIMEOUT {
                    return format!(
                        "session.hello timed out after {}s",
                        REMOTE_HANDSHAKE_TIMEOUT.as_secs()
                    );
                }
                with_tail(format!("session.hello failed: {err}"), &child)
            })?;
            if frame.frame_type == FrameType::Control {
                let response = serde_json::from_slice::<serde_json::Value>(&frame.payload)
                    .map_err(|err| format!("invalid hello answer: {err}"))?;
                // The handshake answered; stand the watchdog down.
                let _ = handshake_done_tx.send(());
                break response;
            }
        };
        Ok((child, stdout, stdin, response))
    })();

    let (child, stdout, stdin, response) = match established {
        Ok(parts) => parts,
        Err(message) => {
            // "no server running there" is not an outage to retry: only an
            // explicit start changes it, so it gets its own outcome.
            let outcome = if crate::fleet::bridge_child::diagnostics_report_stopped_server(
                message.as_str(),
            ) {
                RemoteEstablished::Stopped(crate::fleet::connection::stopped_status_line(
                    &descriptor.name,
                ))
            } else {
                RemoteEstablished::Failed(message)
            };
            let _ = event_tx.send(LoopEvent::Established(
                remote,
                generation,
                Box::new(outcome),
            ));
            return;
        }
    };
    match interpret_hello_answer(&response) {
        Ok(welcome) => {
            if event_tx
                .send(LoopEvent::Established(
                    remote,
                    generation,
                    Box::new(RemoteEstablished::Connected {
                        welcome,
                        writer: stdin,
                        guard: child,
                    }),
                ))
                .is_err()
            {
                return;
            }
        }
        Err(HelloRejection::Incompatible { remedy, message }) => {
            let _ = event_tx.send(LoopEvent::Established(
                remote,
                generation,
                Box::new(RemoteEstablished::Incompatible { remedy, message }),
            ));
            return;
        }
        Err(HelloRejection::Failed(message)) => {
            let _ = event_tx.send(LoopEvent::Established(
                remote,
                generation,
                Box::new(RemoteEstablished::Failed(message)),
            ));
            return;
        }
    }

    // Reader phase: pump frames until the transport dies. Dropping the
    // session on the loop side kills the child, which ends this read.
    let mut stdout = stdout;
    while !should_quit.load(Ordering::Acquire) {
        match read_frame(&mut stdout) {
            Ok(frame) => {
                if event_tx
                    .send(LoopEvent::Frame(remote, generation, frame))
                    .is_err()
                {
                    return;
                }
            }
            Err(err) => {
                debug!(remote, err = %err, "fleet remote session read ended");
                let _ = event_tx.send(LoopEvent::Disconnected(remote, generation));
                return;
            }
        }
    }
}

/// Runs the pure-client TUI until the user detaches. Never returns to the
/// legacy client path.
pub(crate) fn run_pure_client(config: &crate::config::Config) -> io::Result<()> {
    crate::logging::startup("client");
    info!("running pure client of the local server (remote #0)");

    // The fleet config defines the whole fleet - local runtimes included -
    // so every enabled entry gets a mirror and a connection regardless of
    // view membership. No entry is implicit, so none is skipped here.
    let mut mirrors = RemoteMirrors::new();
    let descriptors = remote_descriptors(&crate::fleet::config::load());
    for descriptor in &descriptors {
        mirrors.insert(super::RemoteMirror::new(
            descriptor.index,
            descriptor.name.clone(),
        ));
    }
    run_pure_client_over(config, descriptors, mirrors, FleetSource::Config).map(|_| ())
}

/// Runs the pure client as an *ephemeral fleet-of-one* against one ssh
/// target: a `--remote` launch that is not in `remotes.toml`, has no local
/// runtime in view, and lives only for this session. It rides the same
/// framed session path as a saved fleet remote, so it gets the n/n-1
/// version window instead of the legacy exact-protocol match.
///
/// Returns whether the session ever completed a handshake, which gates the
/// offer to save the target as a named remote.
pub(crate) fn run_pure_client_fleet_of_one(
    config: &crate::config::Config,
    name: &str,
    target: &str,
    session: &str,
    program: Option<String>,
) -> io::Result<bool> {
    crate::logging::startup("client");
    info!(
        target,
        session, "running pure client as an ephemeral fleet-of-one"
    );

    let descriptor = RemoteDescriptor::ephemeral(name, target, session, program);
    let mut mirrors = RemoteMirrors::new();
    // The ephemeral remote *is* remote #0: it is the whole fleet.
    mirrors.insert(super::RemoteMirror::new(
        descriptor.index,
        descriptor.name.clone(),
    ));
    run_pure_client_over(config, vec![descriptor], mirrors, FleetSource::Ephemeral)
}

/// Where the composed fleet came from. The config-backed fleet can be
/// edited from the client (a save rewrites `remotes.toml` and the running
/// fleet is reconciled against it); an ephemeral `--remote` fleet-of-one
/// cannot, because its live remote sits at the index a reconcile hands to
/// the local runtime. Saving that target is offered on the way out instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FleetSource {
    Config,
    Ephemeral,
}

/// The shared pure-client run: terminal setup, the loop, and teardown.
/// Returns whether any remote ever completed a handshake.
fn run_pure_client_over(
    config: &crate::config::Config,
    mut descriptors: Vec<RemoteDescriptor>,
    mut mirrors: RemoteMirrors,
    source: FleetSource,
) -> io::Result<bool> {
    // A TUI has nowhere to draw with stdout redirected, and `ratatui::init`
    // below answers that by panicking with a raw OS error. Say what is
    // actually wrong instead. Guarded here rather than in either entry point
    // so `--remote` gets the same answer as the plain client.
    if !io::stdout().is_terminal() {
        let brand = crate::identity::BRAND;
        eprintln!("{brand} client needs a terminal: stdout is not a TTY.");
        eprintln!("Run it from a terminal, or run `{brand} server` for a headless server.");
        std::process::exit(1);
    }

    // Terminal graphics respect the same [experimental] kitty_graphics gate
    // as the server render path: replicas ingest kitty APC data from the
    // pane DATA stream, and the pure client paints visible placements onto
    // the host terminal after each draw.
    crate::kitty_graphics::set_enabled(config.experimental.kitty_graphics);

    let mut chrome = GlobalChrome::new();
    let mut ids = ComposeIds::new();
    let mut app = AppState::empty();
    apply_client_config(&mut app, config);
    chrome.sidebar_collapsed = app.sidebar_collapsed;
    app.mode = Mode::Navigate;
    // Menu entries with no client-side effect (settings, reload config) are
    // omitted from the global menu, and detach exits the fleet client while
    // leaving every remote server running (same as prefix-d).
    app.pure_client = true;
    app.fleet_config_backed = source == FleetSource::Config;
    app.detach_exits = true;

    let should_quit = Arc::new(AtomicBool::new(false));
    let (event_tx, event_rx) = mpsc::sync_channel::<LoopEvent>(1024);

    // Host input and resize detection reuse the legacy client's readers.
    let (bridge_tx, mut bridge_rx) =
        tokio::sync::mpsc::channel::<crate::client::ClientLoopEvent>(256);
    let stdin_quit = Arc::clone(&should_quit);
    let stdin_tx = bridge_tx.clone();
    let host_mouse_capture_active = Arc::new(AtomicBool::new(true));
    let stdin_capture = Arc::clone(&host_mouse_capture_active);
    std::thread::spawn(move || {
        crate::client::input::stdin_reader_loop(stdin_tx, &stdin_quit, false, stdin_capture);
    });
    let (start_cols, start_rows) = crossterm::terminal::size().unwrap_or((80, 24));
    let resize_quit = Arc::clone(&should_quit);
    std::thread::spawn(move || {
        crate::client::resize_poll_loop(bridge_tx, start_cols, start_rows, false, &resize_quit);
    });
    let input_bridge = event_tx.clone();
    std::thread::spawn(move || {
        while let Some(event) = bridge_rx.blocking_recv() {
            let bridged = match event {
                crate::client::ClientLoopEvent::StdinInput(data) => LoopEvent::Stdin(data),
                crate::client::ClientLoopEvent::Resize(cols, rows, _, _) => {
                    LoopEvent::Resize(cols, rows)
                }
                _ => continue,
            };
            if input_bridge.send(bridged).is_err() {
                break;
            }
        }
    });

    let mut terminal = ratatui::init();
    if config.ui.mouse_capture {
        let _ = crossterm::execute!(io::stdout(), crossterm::event::EnableMouseCapture);
    }
    let _ = crossterm::execute!(io::stdout(), crossterm::event::EnableBracketedPaste);
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = crossterm::execute!(
            io::stdout(),
            crossterm::event::DisableBracketedPaste,
            crossterm::event::DisableMouseCapture
        );
        ratatui::restore();
        original_hook(info);
    }));

    let result = run_loop(
        config,
        &mut terminal,
        &mut descriptors,
        &mut mirrors,
        &mut chrome,
        &mut ids,
        &mut app,
        &event_tx,
        &event_rx,
        &should_quit,
    );

    should_quit.store(true, Ordering::Release);
    let _ = crossterm::execute!(
        io::stdout(),
        crossterm::event::DisableBracketedPaste,
        crossterm::event::DisableMouseCapture
    );
    ratatui::restore();
    result
}

/// Connection state the loop threads through reconnects.
pub(super) enum Link {
    /// Boxed: the session is much larger than the other variants.
    Up(Box<Session>),
    /// A fleet remote's connect thread is in flight.
    Pending {
        generation: u64,
    },
    Down {
        retry_at: Instant,
    },
    /// Reachable, but nothing running there. Terminal like `Incompatible`,
    /// and kept separate from it so the two dead ends stay tellable apart -
    /// one is fixed by starting a server, the other by upgrading one.
    Stopped,
    Incompatible,
}

/// Transient interaction bookkeeping owned by the run loop: click timing
/// and status flashes are neither server facts nor persistent chrome.
#[derive(Default)]
struct InteractionState {
    /// Last chip click, for eager double-click (second click solos).
    last_chip_click: Option<(usize, Instant)>,
    /// Short-lived status message (for example a refused chip toggle).
    status_flash: Option<(String, Instant)>,
    /// Last window title written to the host terminal.
    last_window_title: Option<String>,
    /// Generation source for connect threads and reader threads.
    next_generation: u64,
    /// When the connecting spinner last advanced one visual frame.
    last_spinner_step: Option<Instant>,
    /// Whether any remote ever completed a handshake this run. An ephemeral
    /// `--remote` launch only offers to save a target that actually worked.
    ever_connected: bool,
}

/// Everything one input or frame event may touch. Bundled so the event
/// handlers stay callable from both the blocking receive and the drain
/// loop without repeating ten arguments.
struct LoopCtx<'a> {
    config: &'a crate::config::Config,
    descriptors: &'a mut Vec<RemoteDescriptor>,
    links: &'a mut Links,
    mirrors: &'a mut RemoteMirrors,
    chrome: &'a mut GlobalChrome,
    ids: &'a mut ComposeIds,
    app: &'a mut AppState,
    ui: &'a mut InteractionState,
    event_tx: &'a mpsc::SyncSender<LoopEvent>,
    should_quit: &'a Arc<AtomicBool>,
    scrollback_limit: usize,
}

impl LoopCtx<'_> {
    fn handle_event(&mut self, event: LoopEvent, framer: &mut crate::raw_input::RawInputFramer) {
        match event {
            LoopEvent::Stdin(data) => {
                for raw in framer.push(&data) {
                    handle_raw_input(raw, self);
                }
            }
            LoopEvent::Resize(cols, rows) => {
                debug!(cols, rows, "host terminal resized");
            }
            LoopEvent::RemoteStarted(remote, result) => {
                handle_remote_started(remote, result, self);
            }
            LoopEvent::RemoteStopped(remote, result) => {
                handle_remote_stopped(remote, result, self);
            }
            LoopEvent::Frame(remote, generation, frame) => {
                if link_generation(self.links.get(&remote)) != Some(generation) {
                    debug!(remote, generation, "dropping frame from a stale reader");
                    return;
                }
                if let Some(Link::Up(session)) = self.links.get_mut(&remote) {
                    session.last_inbound = Instant::now();
                }
                handle_server_frame(remote, frame, self);
            }
            LoopEvent::Disconnected(remote, generation) => {
                if link_generation(self.links.get(&remote)) == Some(generation) {
                    drop_link(remote, self, "connection closed");
                } else {
                    debug!(
                        remote,
                        generation, "ignoring disconnect from a stale reader"
                    );
                }
            }
            LoopEvent::Established(remote, generation, outcome) => {
                handle_established(remote, generation, *outcome, self);
            }
        }
    }
}

#[allow(clippy::too_many_arguments)] // run-loop wiring: every argument is a distinct owned subsystem
fn run_loop(
    config: &crate::config::Config,
    terminal: &mut ratatui::DefaultTerminal,
    descriptors: &mut Vec<RemoteDescriptor>,
    mirrors: &mut RemoteMirrors,
    chrome: &mut GlobalChrome,
    ids: &mut ComposeIds,
    app: &mut AppState,
    event_tx: &mpsc::SyncSender<LoopEvent>,
    event_rx: &mpsc::Receiver<LoopEvent>,
    should_quit: &Arc<AtomicBool>,
) -> io::Result<bool> {
    let mut framer = crate::raw_input::RawInputFramer::for_host_input();
    let mut ui = InteractionState::default();
    let mut links: Links = BTreeMap::new();
    // Transport follows the descriptor, not the index: remote #0 is the
    // local socket in a normal run and the ssh bridge in an ephemeral
    // `--remote` fleet-of-one.
    for descriptor in descriptors.iter() {
        let link = establish_for(descriptor, mirrors, &mut ui, event_tx, should_quit);
        note_local_handshake(descriptor, descriptors, app, chrome);
        links.insert(descriptor.index, link);
    }
    let mut ctx = LoopCtx {
        config,
        descriptors,
        links: &mut links,
        mirrors,
        chrome,
        ids,
        app,
        ui: &mut ui,
        event_tx,
        should_quit,
        scrollback_limit: config.advanced.scrollback_limit_bytes,
    };
    let mut dirty = true;

    loop {
        if ctx.should_quit.load(Ordering::Acquire) || ctx.app.should_quit {
            return Ok(ctx.ui.ever_connected);
        }

        // Reconnect with backoff whenever a link is down. View membership
        // never gates this: filtered-out remotes keep reconnecting.
        let now = Instant::now();
        let due: Vec<usize> = ctx
            .links
            .iter()
            .filter_map(|(remote, link)| match link {
                Link::Down { retry_at }
                    if now >= *retry_at
                        && ctx
                            .mirrors
                            .get(*remote)
                            .is_some_and(|mirror| mirror.connection.may_retry()) =>
                {
                    Some(*remote)
                }
                _ => None,
            })
            .collect();
        for remote in due {
            let Some(descriptor) = ctx
                .descriptors
                .iter()
                .find(|descriptor| descriptor.index == remote)
                .cloned()
            else {
                continue;
            };
            let link = establish_for(
                &descriptor,
                ctx.mirrors,
                ctx.ui,
                ctx.event_tx,
                ctx.should_quit,
            );
            note_local_handshake(&descriptor, ctx.descriptors, ctx.app, ctx.chrome);
            ctx.links.insert(remote, link);
            dirty = true;
        }

        // Liveness and animation are time-driven: the recv timeout below
        // wakes this loop even when no events arrive.
        if service_remote_heartbeats(&mut ctx) {
            dirty = true;
        }
        if service_ui_ticks(&mut ctx) {
            dirty = true;
        }

        if dirty {
            compose_fleet_into(ctx.mirrors, ctx.descriptors, ctx.chrome, ctx.ids, ctx.app);
            // Copy mode cannot outlive its pane: a catalog update that
            // removed the pane must drop copy-mode state before rendering.
            if let Some(pane_id) = ctx
                .app
                .copy_mode
                .as_ref()
                .map(|copy_mode| copy_mode.pane_id)
            {
                let alive = ctx
                    .ids
                    .public_pane_id(pane_id)
                    .is_some_and(|(remote, public)| {
                        ctx.mirrors
                            .get(remote)
                            .is_some_and(|mirror| mirror.catalog.pane(public).is_some())
                    });
                if !alive {
                    ctx.app.clear_copy_mode_for_removed_panes([pane_id]);
                }
            }
            apply_status_flash(ctx.ui, ctx.app);
            sync_mode(ctx.app);
            let in_view = in_view_remotes(&ctx);
            let mut resize_requests = Vec::new();
            let mut painted_area = ratatui::layout::Rect::default();
            let dialog = ctx.chrome.remote_edit.clone();
            let start_prompt = ctx.chrome.remote_start.clone();
            let remote_list = ctx.chrome.remote_list.clone();
            terminal.draw(|frame| {
                let source = MirrorPaneSource::for_view(ctx.mirrors, &in_view);
                resize_requests =
                    crate::ui::compute_view_with_content(ctx.app, &source, frame.area());
                ctx.app.sync_copy_mode_search_geometry();
                crate::ui::render_with_content(ctx.app, &source, frame);
                // At most one fleet modal is ever open; the start prompt
                // wins if both somehow are, matching the key routing.
                if let Some(prompt) = &start_prompt {
                    crate::ui::render_remote_start_overlay(ctx.app, prompt, frame);
                } else if let Some(dialog) = &dialog {
                    // The field dialog is reached *from* the list, so it
                    // draws over it.
                    crate::ui::render_remote_edit_overlay(ctx.app, dialog, frame);
                } else if let Some(list) = &remote_list {
                    crate::ui::render_remote_list_overlay(ctx.app, list, frame);
                }
                painted_area = frame.area();
            })?;
            if crate::kitty_graphics::is_enabled() {
                let cell_size =
                    crate::kitty_graphics::HostCellSize::try_from_terminal(painted_area)
                        .unwrap_or_else(|| {
                            crate::kitty_graphics::HostCellSize::fallback_for_area(painted_area)
                        });
                let source = MirrorPaneSource::for_view(ctx.mirrors, &in_view);
                if let Err(err) =
                    crate::kitty_graphics::paint_local_pane_graphics(ctx.app, &source, cell_size)
                {
                    debug!(err = %err, "kitty graphics paint failed");
                }
            }
            sync_all_pane_streams(&mut ctx, &resize_requests);
            apply_window_title(&mut ctx);
            dirty = false;
        }

        let event = match event_rx.recv_timeout(LOOP_TICK) {
            Ok(event) => event,
            // A quiet tick is what tells the framer that a held `esc` was the
            // whole keystroke. This is the only place that decision gets made.
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if drain_idle_input(&mut framer, &mut ctx) {
                    dirty = true;
                }
                continue;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(ctx.ui.ever_connected),
        };
        ctx.handle_event(event, &mut framer);
        dirty = true;

        // Drain whatever queued behind the first event before redrawing.
        while let Ok(event) = event_rx.try_recv() {
            ctx.handle_event(event, &mut framer);
        }
    }
}

/// Surfaces a short-lived interaction flash (for example a refused chip
/// toggle) as the toast of the freshly composed frame.
fn apply_status_flash(ui: &mut InteractionState, app: &mut AppState) {
    let Some((message, at)) = &ui.status_flash else {
        return;
    };
    if at.elapsed() > STATUS_FLASH_TTL {
        ui.status_flash = None;
        return;
    }
    app.toast = Some(crate::app::state::ToastNotification {
        kind: crate::app::state::ToastKind::NeedsAttention,
        title: message.clone(),
        context: "remotes".to_owned(),
        position: None,
        target: None,
    });
}

/// The in-view remote indices, in descriptor order, for the pane-content
/// seam over the composed view.
fn in_view_remotes(ctx: &LoopCtx<'_>) -> Vec<usize> {
    ctx.chrome
        .selection
        .in_view(ctx.descriptors)
        .iter()
        .map(|descriptor| descriptor.index)
        .collect()
}

/// The focused remote wins the window title; write it only on change.
fn apply_window_title(ctx: &mut LoopCtx<'_>) {
    let focused = ctx
        .chrome
        .selection
        .effective_focused_remote(ctx.descriptors);
    let desired = super::fleet_view::select_window_title(&ctx.chrome.window_titles, focused)
        .map(str::to_owned);
    if desired != ctx.ui.last_window_title {
        crate::client::write_window_title(desired.as_deref());
        ctx.ui.last_window_title = desired;
    }
}

/// Says that the local handshake is in flight while it is the only thing
/// on screen. The local connect runs off-thread now, so the first frame is
/// drawn before the socket has answered: a fleet spins the local chip
/// through `Connecting`, but a single-remote client renders no strip at
/// all and would otherwise show a live-looking empty view - with the
/// "press prefix+shift+n" hint it cannot yet honour, since intents are
/// dropped until the link is up.
fn note_local_handshake(
    descriptor: &RemoteDescriptor,
    descriptors: &[RemoteDescriptor],
    app: &AppState,
    chrome: &mut GlobalChrome,
) {
    // Same gate as every other connection-state report: the chip dot says
    // it whenever the strip is on screen, and the status line only speaks
    // where no chip can. This used to test `no_chip_strip_is_composed`
    // alone, which silently stopped firing once a fleet of one composed a
    // strip - leaving a collapsed sidebar with nothing to say at all.
    if descriptor.target.is_none() && local_status_line_is_the_only_channel(app, descriptors) {
        chrome.connection_status = Some("connecting to the local server".to_owned());
    }
}

/// Connects one remote through the transport its descriptor names: the
/// local API socket when it has no ssh target, an ssh bridge child
/// otherwise. Every transport connects off the run-loop thread and reports
/// back through [`LoopEvent::Established`].
fn establish_for(
    descriptor: &RemoteDescriptor,
    mirrors: &mut RemoteMirrors,
    ui: &mut InteractionState,
    event_tx: &mpsc::SyncSender<LoopEvent>,
    should_quit: &Arc<AtomicBool>,
) -> Link {
    if descriptor.target.is_some() {
        establish_remote(descriptor, mirrors, ui, event_tx, should_quit)
    } else {
        establish_local(descriptor, mirrors, ui, event_tx, should_quit)
    }
}

/// Starts the local runtime's connect thread. The local API socket is one
/// fleet transport among others: it connects off the run-loop thread so
/// its chip can show (and spin through) `Connecting`, and so a local
/// server that accepts the socket but never answers `session.hello` cannot
/// block the loop - and with it every fleet remote - for the hello
/// timeout.
fn establish_local(
    descriptor: &RemoteDescriptor,
    mirrors: &mut RemoteMirrors,
    ui: &mut InteractionState,
    event_tx: &mpsc::SyncSender<LoopEvent>,
    should_quit: &Arc<AtomicBool>,
) -> Link {
    // Unreachable: every descriptor gets a mirror before any connect. A
    // retry-later link keeps this total without inventing a session.
    let Some(mirror) = mirrors.get_mut(descriptor.index) else {
        return Link::Down {
            retry_at: Instant::now() + Duration::from_secs(1),
        };
    };
    debug!(remote = mirror.remote_index, name = %mirror.name, "connecting");
    mirror.connection.connect_started();
    ui.next_generation += 1;
    let generation = ui.next_generation;
    let tx = event_tx.clone();
    let quit = Arc::clone(should_quit);
    // A local runtime is no longer pinned to remote #0: it sits wherever
    // config puts it, and carries its own session.
    let remote = descriptor.index;
    let session = descriptor.session.clone();
    std::thread::spawn(move || local_connect_and_read(remote, &session, generation, tx, quit));
    Link::Pending { generation }
}

/// Connect-plus-handshake for the local API socket, run on its own thread.
/// On success the same thread becomes the session's frame reader, so the
/// socket's read half never crosses threads.
fn local_connect_and_read(
    remote: usize,
    session_name: &str,
    generation: u64,
    event_tx: mpsc::SyncSender<LoopEvent>,
    should_quit: Arc<AtomicBool>,
) {
    let outcome = match connect(session_name) {
        ConnectOutcome::Connected(connected) => {
            let ConnectedLocal {
                mut session,
                reader,
                welcome,
            } = *connected;
            // Stamped before the session leaves this thread: frames the
            // reader loop tags below must match the session's generation.
            session.generation = generation;
            let established = LoopEvent::Established(
                remote,
                generation,
                Box::new(RemoteEstablished::LocalConnected {
                    welcome,
                    session: Box::new(session),
                }),
            );
            if event_tx.send(established).is_err() {
                return;
            }
            socket_reader_loop(remote, reader, generation, event_tx, &should_quit);
            return;
        }
        ConnectOutcome::Incompatible { remedy, message } => {
            RemoteEstablished::Incompatible { remedy, message }
        }
        ConnectOutcome::Failed(error) => RemoteEstablished::Failed(error),
    };
    let _ = event_tx.send(LoopEvent::Established(
        remote,
        generation,
        Box::new(outcome),
    ));
}

/// Starts a fleet remote's connect thread (SSH bridge child + handshake).
/// The thread reports back through [`LoopEvent::Established`] and then
/// serves as the session's frame reader.
fn establish_remote(
    descriptor: &RemoteDescriptor,
    mirrors: &mut RemoteMirrors,
    ui: &mut InteractionState,
    event_tx: &mpsc::SyncSender<LoopEvent>,
    should_quit: &Arc<AtomicBool>,
) -> Link {
    if let Some(mirror) = mirrors.get_mut(descriptor.index) {
        mirror.connection.connect_started();
    }
    ui.next_generation += 1;
    let generation = ui.next_generation;
    debug!(remote = descriptor.index, name = %descriptor.name, "connecting via ssh bridge");
    let descriptor = descriptor.clone();
    let tx = event_tx.clone();
    let quit = Arc::clone(should_quit);
    std::thread::spawn(move || remote_connect_and_read(descriptor, generation, tx, quit));
    Link::Pending { generation }
}

/// Applies a fleet remote connect thread's outcome to its link and mirror.
fn handle_established(
    remote: usize,
    generation: u64,
    outcome: RemoteEstablished,
    ctx: &mut LoopCtx<'_>,
) {
    // Only the thread the current pending link belongs to may report;
    // reconciled-away or superseded threads are dropped (their child guard
    // dies with the outcome).
    if !matches!(
        ctx.links.get(&remote),
        Some(Link::Pending { generation: pending }) if *pending == generation
    ) {
        debug!(remote, generation, "dropping stale remote connect outcome");
        return;
    }
    // Remote #0 is the local runtime in a normal run, but an ephemeral
    // `--remote` fleet-of-one puts an ssh target there: the remedy line
    // follows the transport, not the index.
    let is_local = ctx
        .descriptors
        .iter()
        .find(|descriptor| descriptor.index == remote)
        .is_none_or(|descriptor| descriptor.target.is_none());
    let Some(mirror) = ctx.mirrors.get_mut(remote) else {
        ctx.links.remove(&remote);
        return;
    };
    match outcome {
        RemoteEstablished::LocalConnected { welcome, session } => {
            info!(
                remote,
                name = %mirror.name,
                protocol = welcome.protocol,
                server_version = %welcome.server_version,
                "pure client negotiated framed catalog session"
            );
            // The mirror holds what the server actually negotiated, not
            // what this client asked for: capability gates (pane streams)
            // and any protocol downgrade must reflect the welcome.
            mirror
                .connection
                .connected(crate::protocol::framed::NegotiatedSession {
                    protocol: welcome.protocol,
                    capabilities: welcome.capabilities,
                });
            // Full resync: the fresh snapshot plus re-opened streams are
            // the only source of truth for this connection.
            mirror.begin_resync();
            ctx.ui.ever_connected = true;
            if local_status_line_is_the_only_channel(ctx.app, ctx.descriptors) {
                ctx.chrome.connection_status = None;
            }
            let mut session = *session;
            let id = session.request_id("snapshot");
            session.pending.insert(id.clone(), Pending::Snapshot);
            if let Err(err) = session.send_control(&session_snapshot_request(&id)) {
                drop(session);
                mirror.connection_lost(format!("snapshot request failed: {err}"));
                ctx.links.insert(
                    remote,
                    Link::Down {
                        retry_at: Instant::now() + Duration::from_secs(1),
                    },
                );
                return;
            }
            ctx.links.insert(remote, Link::Up(Box::new(session)));
        }
        RemoteEstablished::Connected {
            welcome,
            writer,
            guard,
        } => {
            info!(
                remote,
                name = %mirror.name,
                protocol = welcome.protocol,
                server_version = %welcome.server_version,
                "fleet remote negotiated framed catalog session"
            );
            mirror
                .connection
                .connected(crate::protocol::framed::NegotiatedSession {
                    protocol: welcome.protocol,
                    capabilities: welcome.capabilities,
                });
            mirror.begin_resync();
            ctx.ui.ever_connected = true;
            // Outbound frames go through a dedicated writer thread so a
            // stalled remote's full pipe can never block the run loop (and
            // with it every other remote and local pane). The thread ends
            // when the session drops (channel closes) or the write fails
            // (child killed).
            let (writer_tx, writer_rx) = mpsc::sync_channel::<Vec<u8>>(REMOTE_WRITE_QUEUE_FRAMES);
            let spawned = std::thread::Builder::new()
                .name("fleet-remote-writer".to_owned())
                .spawn(move || {
                    let mut stdin = writer;
                    while let Ok(buf) = writer_rx.recv() {
                        if stdin.write_all(&buf).and_then(|()| stdin.flush()).is_err() {
                            return;
                        }
                    }
                });
            if let Err(err) = spawned {
                mirror.connection_lost(format!("writer thread spawn failed: {err}"));
                ctx.links.insert(
                    remote,
                    Link::Down {
                        retry_at: Instant::now() + Duration::from_secs(1),
                    },
                );
                return;
            }
            let mut session =
                fresh_session(SessionWriter::Threaded(writer_tx), Some(guard), generation);
            let id = session.request_id("snapshot");
            session.pending.insert(id.clone(), Pending::Snapshot);
            if let Err(err) = session.send_control(&session_snapshot_request(&id)) {
                drop(session);
                mirror.connection_lost(format!("snapshot request failed: {err}"));
                ctx.links.insert(
                    remote,
                    Link::Down {
                        retry_at: Instant::now() + Duration::from_secs(1),
                    },
                );
                return;
            }
            ctx.links.insert(remote, Link::Up(Box::new(session)));
        }
        RemoteEstablished::Incompatible { remedy, message } => {
            let status = crate::fleet::connection::incompatible_status_line(
                &mirror.name,
                is_local,
                remedy,
                &message,
            );
            warn!(remote, status = %status, "fleet remote protocol incompatible");
            mirror.connection.incompatible(remedy, message);
            ctx.chrome.connection_status = Some(status);
            ctx.links.insert(remote, Link::Incompatible);
        }
        RemoteEstablished::Stopped(status) => {
            // Terminal like Incompatible: retrying cannot start a daemon on
            // that host. The chip dims and the dialog offers the one action
            // that can - starting it - so the user decides, not the loop.
            info!(remote, status = %status, "fleet remote has no server running");
            mirror.connection.stopped(status.clone());
            ctx.chrome.connection_status = Some(status.clone());
            ctx.chrome.remote_start = Some(super::remote_start::RemoteStartPrompt {
                remote,
                name: mirror.name.clone(),
                status,
                error: None,
                starting: false,
            });
            ctx.links.insert(remote, Link::Stopped);
        }
        RemoteEstablished::Failed(error) => {
            let attempt = match mirror.connection {
                super::ClientConnectionState::Connecting { attempt } => attempt,
                _ => 1,
            };
            debug!(remote, error = %error, "remote connect failed");
            if is_local && local_status_line_is_the_only_channel(ctx.app, ctx.descriptors) {
                ctx.chrome.connection_status =
                    Some(format!("local server unreachable; retrying: {error}"));
            }
            mirror.connection_lost(error);
            let delay = crate::fleet::connection::backoff_delay(
                attempt,
                crate::fleet::connection::BackoffTuning::default(),
                0.5,
            );
            ctx.links.insert(
                remote,
                Link::Down {
                    retry_at: Instant::now() + delay,
                },
            );
        }
    }
}

/// The generation of a link's current session or in-flight connect.
fn link_generation(link: Option<&Link>) -> Option<u64> {
    match link {
        Some(Link::Up(session)) => Some(session.generation),
        Some(Link::Pending { generation }) => Some(*generation),
        _ => None,
    }
}

/// Sends heartbeat pings on every established link and fails links whose
/// peer has been silent past the pong timeout, so a silently dead
/// transport cannot keep a connected chip dot. The local socket is no
/// exception: now that its chip carries connection state like any fleet
/// member's, a wedged local server (one that keeps the socket open and
/// answers nothing) must go hollow on the same clock as the ssh remotes
/// beside it - only a clean EOF is detected without a heartbeat. Every
/// framed session answers `ping`, whatever the transport. Returns true
/// when a link changed state.
fn service_remote_heartbeats(ctx: &mut LoopCtx<'_>) -> bool {
    let now = Instant::now();
    let mut dead: Vec<usize> = Vec::new();
    for (remote, link) in ctx.links.iter_mut() {
        let Link::Up(session) = link else {
            continue;
        };
        if now.duration_since(session.last_inbound) >= REMOTE_PONG_TIMEOUT {
            dead.push(*remote);
            continue;
        }
        if now >= session.next_ping {
            session.next_ping = now + REMOTE_PING_INTERVAL;
            let id = session.request_id("ping");
            session.pending.insert(id.clone(), Pending::Ping);
            if let Err(err) = session.send_control(&crate::protocol::framed::ping_request(&id)) {
                debug!(remote = *remote, err = %err, "heartbeat ping send failed");
                session.pending.remove(&id);
            }
        }
    }
    let changed = !dead.is_empty();
    for remote in dead {
        drop_link(remote, ctx, "heartbeat timed out");
    }
    changed
}

/// Expires the status flash and animates the connecting spinner. Both are
/// time-driven: without this, an idle loop would freeze the spinner on its
/// first frame and pin an expired toast to the screen until the next event.
fn service_ui_ticks(ctx: &mut LoopCtx<'_>) -> bool {
    let mut dirty = false;
    if ctx
        .ui
        .status_flash
        .as_ref()
        .is_some_and(|(_, at)| at.elapsed() > STATUS_FLASH_TTL)
    {
        ctx.ui.status_flash = None;
        dirty = true;
    }
    let connecting = ctx.mirrors.iter().any(|mirror| {
        matches!(
            mirror.connection,
            super::ClientConnectionState::Connecting { .. }
        )
    });
    if connecting {
        let due = ctx
            .ui
            .last_spinner_step
            .is_none_or(|at| at.elapsed() >= SPINNER_FRAME_INTERVAL);
        if due {
            // `spinner_frame` divides the tick by 8, so stepping by 8
            // advances exactly one visual frame per interval.
            ctx.app.spinner_tick = ctx.app.spinner_tick.wrapping_add(8);
            ctx.ui.last_spinner_step = Some(Instant::now());
            dirty = true;
        }
    } else {
        ctx.ui.last_spinner_step = None;
    }
    dirty
}

/// Whether a chip strip is composed at all. `compose_fleet_into` only
/// populates chips for a configured fleet, so a single-remote client can
/// never carry connection state on a dot, whatever the layout does.
fn no_chip_strip_is_composed(descriptors: &[RemoteDescriptor]) -> bool {
    // Only a fleet with nothing in it composes no strip at all.
    descriptors.is_empty()
}

/// Whether the local transport's state has nowhere to go but the status
/// line. A fleet shows every member's connection - the local runtime
/// included - on its chip dot, but only while that strip is on screen: it
/// is composed away for a single remote, and laid out away by a collapsed
/// sidebar, a sidebar too small to spare rows, or the mobile layout, which
/// never renders one. Whenever the dot is not there, the status line is
/// the only channel left.
fn local_status_line_is_the_only_channel(app: &AppState, descriptors: &[RemoteDescriptor]) -> bool {
    no_chip_strip_is_composed(descriptors) || app.view.remote_chip_strip_rect.height == 0
}

/// Drops a remote's link after its transport died and schedules the retry.
fn drop_link(remote: usize, ctx: &mut LoopCtx<'_>, why: &str) {
    if matches!(
        ctx.links.get(&remote),
        Some(Link::Incompatible) | Some(Link::Stopped) | None
    ) {
        return;
    }
    let Some(mirror) = ctx.mirrors.get_mut(remote) else {
        ctx.links.remove(&remote);
        return;
    };
    mirror.connection_lost(why);
    if remote == LOCAL_REMOTE_INDEX
        && local_status_line_is_the_only_channel(ctx.app, ctx.descriptors)
    {
        ctx.chrome.connection_status = Some(format!("{why}; reconnecting"));
    }
    ctx.links.insert(
        remote,
        Link::Down {
            retry_at: Instant::now() + Duration::from_millis(500),
        },
    );
}

/// The bindings navigate mode does not run leaderlessly. Legacy parity:
/// directional pane focus stays prefix-only there because the arrow keys
/// belong to the workspace selection.
fn navigate_mode_excludes(
    keybinds: &crate::config::Keybinds,
    key: crate::input::TerminalKey,
) -> bool {
    [
        &keybinds.focus_pane_left,
        &keybinds.focus_pane_down,
        &keybinds.focus_pane_up,
        &keybinds.focus_pane_right,
    ]
    .iter()
    .any(|binding| binding.matches_prefix_key(key))
}

/// Keeps app.mode consistent with the composed catalog without clobbering
/// client-side modal modes.
fn sync_mode(app: &mut AppState) {
    let has_focus = app.active.and_then(|idx| app.workspaces.get(idx)).is_some();
    match app.mode {
        Mode::Terminal if !has_focus => app.mode = Mode::Navigate,
        Mode::Navigate if has_focus => app.mode = Mode::Terminal,
        _ => {}
    }
}

/// Page keys scroll the replica locally when the pane is a plain screen
/// (no alternate screen, no app cursor, no mouse reporting), mirroring the
/// legacy attach's local-scrollback routing. Returns false to forward the
/// key to the pane instead.
fn scroll_focused_replica_page(code: crossterm::event::KeyCode, ctx: &mut LoopCtx<'_>) -> bool {
    let Some((remote, public)) = focused_public_pane(ctx.mirrors, ctx.ids, ctx.app) else {
        return false;
    };
    let Some(mirror) = ctx.mirrors.get_mut(remote) else {
        return false;
    };
    let Some(stream_id) = mirror.stream_for_pane(&public) else {
        return false;
    };
    let Some(replica) = mirror.replica_mut(stream_id) else {
        return false;
    };
    let plain = crate::pane::plain_terminal_input_state(replica.terminal())
        .is_some_and(|state| state.plain_page_keys_use_host_scrollback());
    if !plain {
        return false;
    }
    let rows = replica
        .scroll_metrics()
        .map(|metrics| metrics.viewport_rows.max(1) as isize)
        .unwrap_or(24);
    let delta = if code == crossterm::event::KeyCode::PageUp {
        -rows
    } else {
        rows
    };
    replica.scroll_delta(delta);
    request_backfill(
        ctx.links,
        ctx.mirrors,
        remote,
        stream_id,
        crate::terminal::replica::BackfillTrigger::Scroll,
    );
    true
}

/// Feeds a stream's replica and renumbers the client's absolute-row state by
/// however many older rows that prepended. Both ways a replica takes bytes -
/// a history page, and a tail that lets queued pages bake in - can prepend,
/// and history lands above everything already loaded, so a selection or
/// search hit left alone would cover different text than the user put it on.
///
/// `apply` returns the prepended row count, and owns its own error
/// reporting: a replica that failed to take the bytes prepended nothing.
///
/// Takes the context in pieces rather than `LoopCtx` because callers already
/// hold `mirror` as a mutable borrow out of `LoopCtx::mirrors`.
fn apply_and_rebase(
    mirror: &mut super::RemoteMirror,
    ids: &ComposeIds,
    app: &mut AppState,
    remote: usize,
    stream_id: u32,
    apply: impl FnOnce(&mut crate::terminal::replica::PaneReplica) -> usize,
) {
    let Some(rows) = mirror
        .replica_mut(stream_id)
        .map(apply)
        .filter(|rows| *rows > 0)
    else {
        return;
    };
    let Some(pane_id) = mirror
        .pane_for_stream(stream_id)
        .and_then(|public| ids.composed_pane_id(remote, public))
    else {
        return;
    };
    app.rebase_absolute_rows_after_prepend(pane_id, rows);
}

/// The remote and stream serving a composed pane, if that pane is streamed.
pub(super) fn stream_for_composed_pane(
    mirrors: &RemoteMirrors,
    ids: &ComposeIds,
    pane_id: crate::layout::PaneId,
) -> Option<(usize, u32)> {
    let (remote, public) = ids.public_pane_id(pane_id)?;
    let stream_id = mirrors.get(remote)?.stream_for_pane(public)?;
    Some((remote, stream_id))
}

/// Asks the replica's paging policy for a scrollback backfill and sends the
/// `stream.history` request it plans, if any. The response prepends through
/// the replica's rebuild path. The trigger says why the client is asking:
/// attach warms one page, scrolling pages lazily ahead of the loaded top, and
/// a jump to the top takes one large fetch instead of a page-by-page crawl.
pub(super) fn request_backfill(
    links: &mut Links,
    mirrors: &mut RemoteMirrors,
    remote: usize,
    stream_id: u32,
    trigger: crate::terminal::replica::BackfillTrigger,
) {
    let Some(session) = session_for(links, remote) else {
        return;
    };
    let Some(replica) = mirrors
        .get_mut(remote)
        .and_then(|mirror| mirror.replica_mut(stream_id))
    else {
        return;
    };
    let id = session.request_id("history");
    match replica.take_backfill_request(&id, trigger) {
        Ok(Some(request)) => {
            session.pending.insert(id, Pending::History { stream_id });
            if let Err(err) = session.send_control(&request) {
                warn!(err = %err, "stream.history send failed");
            }
        }
        Ok(None) => {}
        Err(err) => warn!(err = %err, "backfill planning failed"),
    }
}

/// Reads frames off the session socket into the loop channel.
fn socket_reader_loop(
    remote: usize,
    mut stream: LocalStream,
    generation: u64,
    event_tx: mpsc::SyncSender<LoopEvent>,
    should_quit: &Arc<AtomicBool>,
) {
    if stream.set_nonblocking(false).is_err() {
        let _ = event_tx.send(LoopEvent::Disconnected(remote, generation));
        return;
    }
    while !should_quit.load(Ordering::Acquire) {
        match read_frame(&mut stream) {
            Ok(frame) => {
                if event_tx
                    .send(LoopEvent::Frame(remote, generation, frame))
                    .is_err()
                {
                    return;
                }
            }
            Err(err) => {
                debug!(err = %err, "pure client session read ended");
                let _ = event_tx.send(LoopEvent::Disconnected(remote, generation));
                return;
            }
        }
    }
}

/// Applies one server frame to its remote's mirror.
fn handle_server_frame(remote: usize, frame: Frame, ctx: &mut LoopCtx<'_>) {
    let scrollback_limit = ctx.scrollback_limit;
    let Some(mirror) = ctx.mirrors.get_mut(remote) else {
        return;
    };
    match frame.frame_type {
        FrameType::Data => {
            // A tail that leaves the alternate screen lets queued history
            // pages bake in, which prepends rows just like a page landing.
            apply_and_rebase(
                mirror,
                ctx.ids,
                ctx.app,
                remote,
                frame.stream_id,
                |replica| {
                    replica.apply_tail(&frame.payload).unwrap_or_else(|err| {
                        warn!(err = %err, stream = frame.stream_id, "replica tail apply failed");
                        0
                    })
                },
            );
        }
        FrameType::Control => {
            let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&frame.payload) else {
                return;
            };
            if let Some(event) = payload.get("event").and_then(|value| value.as_str()) {
                match event {
                    CATALOG_EVENT => {
                        let seq = payload.get("seq").and_then(|value| value.as_u64());
                        let envelope = payload.get("data").cloned().and_then(|data| {
                            serde_json::from_value::<crate::api::schema::EventEnvelope>(data).ok()
                        });
                        if let (Some(seq), Some(envelope)) = (seq, envelope) {
                            mirror.catalog.apply(seq, &envelope);
                        }
                    }
                    CATALOG_RESYNC_EVENT => {
                        // The server's bounded event buffer overflowed past
                        // our cursor: catalog events were lost, so only a
                        // fresh snapshot can repair the mirror.
                        warn!(
                            remote,
                            "catalog events lost to server buffer overflow; resyncing"
                        );
                        if let Some(session) = session_for(ctx.links, remote) {
                            let id = session.request_id("snapshot");
                            session.pending.insert(id.clone(), Pending::Snapshot);
                            if let Err(err) = session.send_control(&session_snapshot_request(&id)) {
                                warn!(err = %err, "catalog resync snapshot request failed");
                                session.pending.remove(&id);
                            }
                        }
                    }
                    crate::protocol::framed::NOTIFICATION_POSTED_EVENT => {
                        // Notifications arrive from every remote; the
                        // delivery policy is shared with the legacy client,
                        // plus a remote label when a real fleet is
                        // configured.
                        let Some(data) = payload.get("data").cloned() else {
                            return;
                        };
                        let Ok(crate::api::schema::events::EventData::NotificationPosted {
                            kind,
                            message,
                            body,
                        }) = serde_json::from_value(data)
                        else {
                            return;
                        };
                        let name = ctx
                            .descriptors
                            .iter()
                            .find(|descriptor| descriptor.index == remote)
                            .map(|descriptor| descriptor.name.as_str())
                            .unwrap_or("remote");
                        let message = super::fleet_view::labeled_notification_message(
                            name,
                            ctx.descriptors.len(),
                            &message,
                        );
                        crate::client::deliver_notification(
                            kind,
                            &message,
                            body.as_deref(),
                            &ctx.config.ui.sound,
                        );
                    }
                    crate::protocol::framed::WINDOW_TITLE_CHANGED_EVENT => {
                        // Every remote's title is retained; the focused
                        // remote's wins the host terminal at draw time.
                        let title = payload
                            .get("data")
                            .and_then(|data| data.get("title"))
                            .and_then(|value| value.as_str())
                            .map(str::to_owned);
                        match title {
                            Some(title) => {
                                ctx.chrome.window_titles.insert(remote, title);
                            }
                            None => {
                                ctx.chrome.window_titles.remove(&remote);
                            }
                        }
                    }
                    STREAM_CLOSED_EVENT | STREAM_REVOKED_EVENT => {
                        if let Some(stream_id) = payload
                            .get("data")
                            .and_then(|data| data.get("stream_id"))
                            .and_then(|value| value.as_u64())
                        {
                            let stream_id = stream_id as u32;
                            if let Some(pane_id) = mirror.pane_for_stream(stream_id) {
                                debug!(pane = %pane_id, stream = stream_id, event, "pane stream ended");
                            }
                            mirror.stream_closed(stream_id);
                            if let Some(session) = session_for(ctx.links, remote) {
                                session.sent_sizes.remove(&stream_id);
                                session.read_only.remove(&stream_id);
                            }
                        }
                    }
                    SERVER_STOPPING_EVENT => {
                        // The server is going away on purpose. Go straight to
                        // Stopped rather than through the retry ladder: the
                        // ladder is for transports that might come back, and
                        // this one told us it will not.
                        //
                        // Setting `Link::Stopped` here also matters for the
                        // EOF that follows immediately after: `drop_link`
                        // early-returns on a stopped link, so the deliberate
                        // stop is not overwritten with `Offline`.
                        let reason = payload
                            .get("data")
                            .and_then(|data| data.get("reason"))
                            .and_then(|value| value.as_str())
                            .unwrap_or(crate::protocol::framed::SERVER_STOPPING_REASON_REQUESTED);
                        let status = crate::fleet::connection::announced_stop_status_line(
                            &mirror.name,
                            reason,
                        );
                        info!(remote, reason, "fleet remote server announced a stop");
                        mirror.connection.stopped(status.clone());
                        ctx.chrome.connection_status = Some(status.clone());
                        ctx.chrome.remote_start = Some(super::remote_start::RemoteStartPrompt {
                            remote,
                            name: mirror.name.clone(),
                            status,
                            error: None,
                            starting: false,
                        });
                        ctx.links.insert(remote, Link::Stopped);
                    }
                    _ => {}
                }
                return;
            }
            // Responses to our own control requests.
            let Some(id) = payload.get("id").and_then(|value| value.as_str()) else {
                return;
            };
            let Some(session) = session_for(ctx.links, remote) else {
                return;
            };
            match session.pending.remove(id) {
                Some(Pending::Snapshot) => match parse_session_snapshot(&payload) {
                    Ok((snapshot, sequence)) => {
                        match serde_json::from_value::<crate::api::schema::SessionSnapshot>(
                            snapshot,
                        ) {
                            Ok(snapshot) => {
                                let mut catalog = SessionCatalog::new();
                                catalog.resync(&snapshot, sequence);
                                mirror.catalog = catalog;
                                // A mid-session resync can reveal panes that
                                // closed while events were lost; their
                                // streams must not linger.
                                let stale: Vec<(String, u32)> = mirror
                                    .pane_streams
                                    .iter()
                                    .filter(|(pane_id, _)| mirror.catalog.pane(pane_id).is_none())
                                    .map(|(pane_id, stream_id)| (pane_id.clone(), *stream_id))
                                    .collect();
                                for (pane_id, stream_id) in stale {
                                    debug!(pane = %pane_id, stream = stream_id, "closing stream for pane gone after resync");
                                    let close_id = session.request_id("close");
                                    if let Err(err) = session
                                        .send_control(&stream_close_request(&close_id, stream_id))
                                    {
                                        warn!(err = %err, "stream.close send failed");
                                    }
                                    mirror.stream_closed(stream_id);
                                    session.sent_sizes.remove(&stream_id);
                                    session.read_only.remove(&stream_id);
                                }
                                if remote == LOCAL_REMOTE_INDEX
                                    && local_status_line_is_the_only_channel(
                                        ctx.app,
                                        ctx.descriptors,
                                    )
                                {
                                    ctx.chrome.connection_status = None;
                                }
                            }
                            Err(err) => warn!(err = %err, "session.snapshot did not deserialize"),
                        }
                    }
                    Err(err) => warn!(err = %err, "session.snapshot failed"),
                },
                Some(Pending::StreamOpen { pane_id, mode }) => {
                    match parse_stream_opened(&payload) {
                        Ok(opened) => {
                            if mirror.catalog.pane(&pane_id).is_none() {
                                // The pane vanished while the stream opened.
                                debug!(pane = %pane_id, "pane gone before its stream opened; closing");
                                let close_id = session.request_id("close");
                                if let Err(err) = session.send_control(&stream_close_request(
                                    &close_id,
                                    opened.stream_id,
                                )) {
                                    warn!(err = %err, "stream.close send failed");
                                }
                                return;
                            }
                            let history_cursor = (!opened.history_cursor.is_empty())
                                .then(|| opened.history_cursor.clone());
                            // Seed the replica at the size the snapshot was
                            // captured at; the next draw plans the real geometry.
                            let (cols, rows) = if opened.cols > 0 && opened.rows > 0 {
                                (opened.cols, opened.rows)
                            } else {
                                (80, 24)
                            };
                            match crate::terminal::replica::PaneReplica::open(
                                &opened.snapshot,
                                opened.sequence,
                                history_cursor,
                                cols,
                                rows,
                                scrollback_limit,
                            ) {
                                Ok(replica) => {
                                    if mode == StreamMode::Read {
                                        session.read_only.insert(opened.stream_id);
                                    }
                                    // Not `focused_public_pane`: that borrows
                                    // the mirrors this arm already holds
                                    // mutably, and its catalog check is
                                    // redundant here - the pane was vouched
                                    // for above - so focus is a pure id
                                    // comparison.
                                    let focused = ctx
                                        .app
                                        .active
                                        .and_then(|idx| ctx.app.workspaces.get(idx))
                                        .and_then(|ws| ws.focused_pane_id())
                                        .and_then(|local| ctx.ids.public_pane_id(local))
                                        .is_some_and(|(focus_remote, focus_public)| {
                                            focus_remote == remote && focus_public == pane_id
                                        });
                                    let stream_id = opened.stream_id;
                                    mirror.stream_opened(pane_id, stream_id, replica);
                                    // Warm one page for the pane the user is
                                    // looking at, so its first scroll tick
                                    // already has history behind it.
                                    request_backfill(
                                        ctx.links,
                                        ctx.mirrors,
                                        remote,
                                        stream_id,
                                        crate::terminal::replica::BackfillTrigger::Attach {
                                            focused,
                                        },
                                    );
                                }
                                Err(err) => warn!(err = %err, "replica open failed"),
                            }
                        }
                        Err(Some(error))
                            if error.code == PANE_WRITE_LOCKED_ERROR
                                && mode == StreamMode::Write =>
                        {
                            // Another client (direct attach) holds the write
                            // grant. Fall back to a read-only view whose
                            // geometry stays owned by that writer.
                            debug!(pane = %pane_id, "pane write-locked; reopening stream read-only");
                            let open_id = session.request_id("open");
                            session.pending.insert(
                                open_id.clone(),
                                Pending::StreamOpen {
                                    pane_id: pane_id.clone(),
                                    mode: StreamMode::Read,
                                },
                            );
                            let request = stream_open_request(
                                &open_id,
                                &pane_id,
                                StreamMode::Read,
                                false,
                                None,
                                None,
                            );
                            if let Err(err) = session.send_control(&request) {
                                warn!(err = %err, "read-only stream.open send failed");
                                session.pending.remove(&open_id);
                            }
                        }
                        Err(Some(error)) => {
                            debug!(code = %error.code, pane = %pane_id, "stream.open rejected")
                        }
                        Err(None) => warn!(pane = %pane_id, "stream.open answer malformed"),
                    }
                }
                Some(Pending::History { stream_id }) => {
                    apply_and_rebase(mirror, ctx.ids, ctx.app, remote, stream_id, |replica| {
                        match replica.apply_history_response(&payload) {
                            Ok(rows_prepended) => {
                                debug!(stream = stream_id, rows_prepended, "history page applied");
                                rows_prepended
                            }
                            Err(err) => {
                                warn!(err = %err, "history page apply failed");
                                0
                            }
                        }
                    });
                }
                Some(Pending::Resize { stream_id }) => {
                    if let Some(error) = control_error(&payload) {
                        // Forget the recorded size so the next draw retries;
                        // rejections here are transient grant races.
                        warn!(code = %error.code, stream = stream_id, "stream.resize rejected; retrying on next draw");
                        session.sent_sizes.remove(&stream_id);
                    }
                }
                Some(Pending::Ping) => {
                    // Liveness was already recorded when the frame arrived.
                }
                Some(Pending::Api) => {
                    if let Some(error) = control_error(&payload) {
                        debug!(code = %error.code, message = %error.message, "api.request rejected");
                    }
                }
                None => {
                    if let Some(error) = control_error(&payload) {
                        debug!(code = %error.code, message = %error.message, "control error");
                    }
                }
            }
        }
    }
}

/// Syncs pane streams on every connected remote: only the remote owning
/// the composed active workspace has visible panes; every other remote's
/// streams close. Selection filters the view, never the connections.
fn sync_all_pane_streams(
    ctx: &mut LoopCtx<'_>,
    resize_requests: &[crate::terminal::PaneResizeRequest],
) {
    let owner = ctx
        .app
        .active
        .and_then(|ws_idx| ctx.ids.workspace_owner(ws_idx))
        .map(|(remote, public)| (remote, public.to_owned()));
    let remotes: Vec<usize> = ctx.links.keys().copied().collect();
    for remote in remotes {
        let Some(session) = session_for(ctx.links, remote) else {
            continue;
        };
        let Some(mirror) = ctx.mirrors.get_mut(remote) else {
            continue;
        };
        let visible = match &owner {
            Some((owner_remote, workspace_public)) if *owner_remote == remote => {
                visible_panes_of_workspace(&mirror.catalog, workspace_public)
            }
            _ => Vec::new(),
        };
        sync_remote_pane_streams(session, mirror, remote, &visible, resize_requests);
    }
}

/// Opens streams for visible panes that lack one, closes streams whose
/// panes left visibility, and pushes stream.resize for panes whose planned
/// geometry changed.
fn sync_remote_pane_streams(
    session: &mut Session,
    mirror: &mut super::RemoteMirror,
    remote: usize,
    visible: &[String],
    resize_requests: &[crate::terminal::PaneResizeRequest],
) {
    let has_pane_streams = mirror
        .connection
        .negotiated()
        .is_some_and(|negotiated| negotiated.has_capability(CAPABILITY_PANE_STREAM));
    if !has_pane_streams {
        return;
    }

    // Streams for panes no longer visible are closed so resource use tracks
    // panes visible, not panes ever visited.
    for (pane_id, stream_id) in hidden_pane_streams(mirror, visible) {
        debug!(pane = %pane_id, stream = stream_id, "closing pane stream for hidden pane");
        let id = session.request_id("close");
        if let Err(err) = session.send_control(&stream_close_request(&id, stream_id)) {
            warn!(err = %err, "stream.close send failed");
        }
        mirror.stream_closed(stream_id);
        session.sent_sizes.remove(&stream_id);
        session.read_only.remove(&stream_id);
    }

    for pane_id in visible {
        if mirror.stream_for_pane(pane_id).is_some() {
            continue;
        }
        let already_opening = session
            .pending
            .values()
            .any(|pending| matches!(pending, Pending::StreamOpen { pane_id: opening, .. } if opening == pane_id));
        if already_opening {
            continue;
        }
        let id = session.request_id("open");
        session.pending.insert(
            id.clone(),
            Pending::StreamOpen {
                pane_id: pane_id.clone(),
                mode: StreamMode::Write,
            },
        );
        // Write mode: the pure client owns pane geometry (stream.resize
        // requires the write grant). No cols/rows on open — the replica
        // seeds at the server's snapshot size and the first planned resize
        // sets the real viewport. If another client holds the grant, the
        // response handler falls back to a read-only stream.
        let request = stream_open_request(&id, pane_id, StreamMode::Write, false, None, None);
        if let Err(err) = session.send_control(&request) {
            warn!(err = %err, "stream.open send failed");
            return;
        }
    }

    // Geometry: translate planned pane resizes into stream.resize. The
    // planner speaks composed (remote-scoped) terminal ids.
    let by_terminal: HashMap<TerminalId, String> = mirror
        .catalog
        .panes
        .iter()
        .map(|pane| {
            (
                super::compose::composed_terminal_id(remote, &pane.terminal_id),
                pane.pane_id.clone(),
            )
        })
        .collect();
    for request in resize_requests {
        let Some(pane_id) = by_terminal.get(&request.terminal_id) else {
            continue;
        };
        let Some(stream_id) = mirror.stream_for_pane(pane_id) else {
            continue;
        };
        if session.read_only.contains(&stream_id) {
            // The write grant holder owns this pane's geometry.
            continue;
        }
        if session.sent_sizes.get(&stream_id) == Some(&(request.cols, request.rows)) {
            continue;
        }
        let id = session.request_id("resize");
        let control = stream_resize_request(&id, stream_id, request.cols, request.rows, 0, 0);
        session
            .pending
            .insert(id.clone(), Pending::Resize { stream_id });
        if session.send_control(&control).is_ok() {
            session
                .sent_sizes
                .insert(stream_id, (request.cols, request.rows));
            if let Some(replica) = mirror.replica_mut(stream_id) {
                let _ = replica.resize(request.cols, request.rows, 1, 1);
            }
        } else {
            session.pending.remove(&id);
        }
    }
}

/// Open streams whose panes are not in the visible set.
fn hidden_pane_streams(mirror: &super::RemoteMirror, visible: &[String]) -> Vec<(String, u32)> {
    mirror
        .pane_streams
        .iter()
        .filter(|(pane_id, _)| !visible.iter().any(|visible_id| visible_id == *pane_id))
        .map(|(pane_id, stream_id)| (pane_id.clone(), *stream_id))
        .collect()
}

/// The panes of one workspace's active tab.
fn visible_panes_of_workspace(catalog: &SessionCatalog, workspace_id: &str) -> Vec<String> {
    let Some(workspace) = catalog.workspace(workspace_id) else {
        return Vec::new();
    };
    catalog
        .panes
        .iter()
        .filter(|pane| pane.tab_id == workspace.active_tab_id)
        .map(|pane| pane.pane_id.clone())
        .collect()
}

/// Interprets one host input event client-side, then drains the requests
/// the shared `AppState` layer can only ask for (it holds no client chrome).
fn handle_raw_input(raw: crate::raw_input::RawInputEvent, ctx: &mut LoopCtx<'_>) {
    interpret_raw_input(raw, ctx);
    drain_app_requests(ctx);
}

/// Releases input the framer is still holding once the host has gone quiet.
///
/// `RawInputFramer::push` cannot tell a lone `esc` from the first byte of an
/// escape sequence, so it buffers one and says nothing. Only a timeout flush
/// decides no continuation is coming. Nothing drove that flush here, so every
/// `esc` in the pure client sat in the buffer until the *next* keystroke
/// pushed it out - which is to say `esc` did nothing at all.
///
/// Returns whether anything was dispatched, so the caller can redraw.
fn drain_idle_input(framer: &mut crate::raw_input::RawInputFramer, ctx: &mut LoopCtx<'_>) -> bool {
    if !framer.has_pending_input() {
        return false;
    }
    let mut dispatched = false;
    for raw in framer.flush_timeout() {
        handle_raw_input(raw, ctx);
        dispatched = true;
    }
    dispatched
}

/// Turns pending `AppState` requests into client chrome. The global menu's
/// add-remote entry is the only way to add the first remote: with a single
/// runtime configured no chip strip is composed, so the strip's own add
/// affordance is not on screen.
fn drain_app_requests(ctx: &mut LoopCtx<'_>) {
    if ctx.app.request_add_remote {
        ctx.app.request_add_remote = false;
        // Saving reconciles the running fleet against `remotes.toml`,
        // which does not describe an ephemeral `--remote` fleet-of-one:
        // its live remote sits at the index a reconcile hands to the local
        // runtime. The menu entry is gated on the same fact, and the chip
        // strip's add affordance is not composed for a fleet of one; this
        // is the seam that keeps the dialog itself out of reach there.
        if ctx.chrome.remote_edit.is_none() && ctx.app.fleet_config_backed {
            debug!("opening the add-remote dialog from the global menu");
            ctx.chrome.remote_edit = Some(super::remote_edit::RemoteEditState::add());
        }
    }
    if ctx.app.request_manage_remotes {
        ctx.app.request_manage_remotes = false;
        // Same gate as adding, for the same reason: the modal's every
        // action is a write to `remotes.toml`.
        if ctx.chrome.remote_list.is_none() && ctx.app.fleet_config_backed {
            debug!("opening the remotes list");
            let rows = super::remote_list::remote_list_rows(
                &crate::fleet::config::load(),
                ctx.descriptors,
                ctx.mirrors,
            );
            ctx.chrome.remote_list = Some(super::remote_list::RemoteListState::new(rows));
        }
    }
}

/// Runs one remotes-list action as its own transaction, then re-renders the
/// modal from the list the write returned.
///
/// There is no draft: each action commits immediately and individually
/// through the transactional fleet-config update, so a stale baseline is
/// structurally impossible and closing never discards work. A refused write
/// leaves the file untouched, surfaces in the modal, and leaves it open.
fn commit_remote_list_change(
    ctx: &mut LoopCtx<'_>,
    mutation: impl FnOnce(&mut Vec<crate::fleet::config::RemoteEntry>),
) {
    let result = crate::fleet::config::update(mutation);
    let Some(list) = ctx.chrome.remote_list.as_mut() else {
        return;
    };
    match result {
        Ok(((), entries)) => {
            list.error = None;
            reconcile_fleet(&entries, ctx);
            // Re-read live status through the reconciled descriptors.
            refresh_remote_list(&entries, ctx);
        }
        Err(err) => list.error = Some(err.to_string()),
    }
}

/// Performs one action from the remotes list modal.
fn apply_remote_list_action(
    action: super::remote_list::RemoteListKeyResult,
    ctx: &mut LoopCtx<'_>,
) {
    use super::remote_list::RemoteListKeyResult as Action;
    match action {
        Action::Consumed | Action::Ignored => {}
        Action::Close => ctx.chrome.remote_list = None,
        Action::Reorder(name, direction) => {
            commit_remote_list_change(ctx, |remotes| {
                crate::fleet::config::move_in(remotes, &name, direction);
            });
        }
        Action::ToggleEnabled(name) => {
            // Flipped against the list loaded *inside* the lock, not against
            // the rows on screen: an external edit between the keystroke and
            // the write would otherwise turn the toggle into a silent no-op
            // that leaves the remote where it already was.
            commit_remote_list_change(ctx, |remotes| {
                let current = remotes
                    .iter()
                    .find(|remote| remote.name == name)
                    .map(|remote| remote.enabled);
                if let Some(current) = current {
                    crate::fleet::config::set_enabled_in(remotes, &name, !current);
                }
            });
        }
        Action::Remove(name) => {
            commit_remote_list_change(ctx, |remotes| {
                crate::fleet::config::remove_in(remotes, &name);
            });
        }
        Action::Edit(name) => {
            // Field editing delegates to the existing single-remote dialog
            // rather than being rebuilt here. The list stays open behind it.
            let entry = ctx.chrome.remote_list.as_ref().and_then(|list| {
                list.rows
                    .iter()
                    .find(|row| row.entry.name == name)
                    .map(|row| row.entry.clone())
            });
            if let Some(entry) = entry {
                ctx.chrome.remote_edit = Some(super::remote_edit::RemoteEditState::edit(&entry));
            }
        }
        Action::StartStop(name) => start_or_stop_listed_remote(&name, ctx),
    }
}

/// Drives lifecycle on one remote from the list.
///
/// Per-remote start and stop live here because dropping chip right-click
/// left them homeless. This issues them through the same paths the existing
/// start prompt uses; it does not implement start or stop itself.
fn start_or_stop_listed_remote(name: &str, ctx: &mut LoopCtx<'_>) {
    let Some(descriptor) = ctx
        .descriptors
        .iter()
        .find(|descriptor| descriptor.name == name)
        .cloned()
    else {
        if let Some(list) = ctx.chrome.remote_list.as_mut() {
            list.error = Some(format!("remote '{name}' is disabled"));
        }
        return;
    };
    let running = ctx
        .mirrors
        .get(descriptor.index)
        .is_some_and(|mirror| !mirror.connection.is_stopped());
    if running {
        stop_remote(descriptor.index, ctx);
    } else {
        // Reuses the confirmation the stopped chip already opens, so the
        // one action that can bring a remote back has a single path.
        ctx.chrome.remote_start = Some(super::remote_start::RemoteStartPrompt {
            remote: descriptor.index,
            name: descriptor.name.clone(),
            status: crate::fleet::connection::stopped_status_line(&descriptor.name),
            error: None,
            starting: false,
        });
    }
}

fn interpret_raw_input(raw: crate::raw_input::RawInputEvent, ctx: &mut LoopCtx<'_>) {
    match raw {
        crate::raw_input::RawInputEvent::Key(key) => handle_key(key, ctx),
        crate::raw_input::RawInputEvent::Paste(text) => {
            if ctx.chrome.remote_edit.is_some() {
                return;
            }
            if matches!(
                ctx.app.mode,
                Mode::RenameWorkspace | Mode::RenameTab | Mode::RenamePane
            ) {
                crate::app::insert_rename_input_text(ctx.app, &text);
                return;
            }
            if ctx.app.mode == Mode::Terminal {
                if try_paste_image(&text, ctx) {
                    return;
                }
                if let Some((remote, pane_id, bytes)) =
                    encode_paste(ctx.mirrors, ctx.ids, ctx.app, &text)
                {
                    send_pane_bytes(ctx.links, remote, &pane_id, &bytes);
                }
            }
        }
        crate::raw_input::RawInputEvent::Mouse(mouse) => handle_mouse(mouse, ctx),
        _ => {}
    }
}

/// Approves the open start prompt: starts the remote's server over its own
/// ssh bridge, off the run-loop thread.
///
/// Never blocking here is the point: an ssh spawn to a host that accepts the
/// connection but stalls would otherwise freeze the render and every other
/// remote with it. The prompt stays open showing progress until
/// [`LoopEvent::RemoteStarted`] arrives.
fn start_stopped_remote(ctx: &mut LoopCtx<'_>) {
    let Some(prompt) = ctx.chrome.remote_start.as_ref() else {
        return;
    };
    if prompt.starting {
        return;
    }
    let remote = prompt.remote;
    let Some(descriptor) = ctx
        .descriptors
        .iter()
        .find(|descriptor| descriptor.index == remote)
    else {
        ctx.chrome.remote_start = None;
        return;
    };
    // A local runtime has no remote daemon to start.
    let Some(target) = descriptor.target.clone() else {
        ctx.chrome.remote_start = None;
        return;
    };
    let session = descriptor.session.clone();
    // The same binary this remote connects with: starting a different one is
    // how a host ends up serving a version nobody asked for.
    let program = descriptor
        .program
        .clone()
        .unwrap_or_else(|| crate::identity::BRAND.to_owned());

    if let Some(prompt) = ctx.chrome.remote_start.as_mut() {
        prompt.starting = true;
        prompt.error = None;
    }
    let tx = ctx.event_tx.clone();
    std::thread::spawn(move || {
        let result = crate::fleet::bridge_child::start_remote_server(&target, &session, &program);
        let _ = tx.send(LoopEvent::RemoteStarted(remote, result));
    });
}

/// Asks a remote's server to stop, off-thread so the loop keeps rendering.
///
/// A local runtime is deliberately excluded: it has no ssh path, and the way
/// to stop it is to close its last space, which the server-side exit-on-empty
/// rule already handles.
fn stop_remote(remote: usize, ctx: &mut LoopCtx<'_>) {
    let Some(descriptor) = ctx
        .descriptors
        .iter()
        .find(|descriptor| descriptor.index == remote)
    else {
        return;
    };
    let Some(target) = descriptor.target.clone() else {
        if let Some(list) = ctx.chrome.remote_list.as_mut() {
            list.error = Some("a local runtime stops when its last space closes".to_owned());
        }
        return;
    };
    let session = descriptor.session.clone();
    let program = descriptor
        .program
        .clone()
        .unwrap_or_else(|| crate::identity::BRAND.to_owned());
    let tx = ctx.event_tx.clone();
    std::thread::spawn(move || {
        let result = crate::fleet::bridge_child::stop_remote_server(&target, &session, &program);
        let _ = tx.send(LoopEvent::RemoteStopped(remote, result));
    });
}

/// Applies the outcome of an explicit stop. Success needs no state change
/// here: the server announces its own shutdown on the control plane, which
/// is what parks the remote as stopped.
fn handle_remote_stopped(remote: usize, result: Result<(), String>, ctx: &mut LoopCtx<'_>) {
    if let Err(err) = result {
        warn!(remote, err = %err, "remote stop failed");
        if let Some(list) = ctx.chrome.remote_list.as_mut() {
            list.error = Some(err);
        }
    }
}

/// Applies the outcome of an explicit start.
fn handle_remote_started(remote: usize, result: Result<(), String>, ctx: &mut LoopCtx<'_>) {
    // The prompt may have been dismissed, or the fleet reconciled, while the
    // start was in flight; a stale result must not resurrect either.
    if ctx
        .chrome
        .remote_start
        .as_ref()
        .is_none_or(|prompt| prompt.remote != remote)
    {
        return;
    }
    match result {
        Ok(()) => {
            info!(remote, "started remote server; reconnecting");
            ctx.chrome.remote_start = None;
            ctx.chrome.connection_status = None;
            if let Some(mirror) = ctx.mirrors.get_mut(remote) {
                mirror.connection = super::ClientConnectionState::new();
            }
            // Leave the terminal link state so the loop reconnects at once.
            ctx.links.insert(
                remote,
                Link::Down {
                    retry_at: Instant::now(),
                },
            );
        }
        Err(err) => {
            warn!(remote, err = %err, "starting the remote server failed");
            if let Some(prompt) = ctx.chrome.remote_start.as_mut() {
                prompt.starting = false;
                prompt.error = Some(err);
            }
        }
    }
}

/// The focused pane's owning remote and public pane id, when the pane is
/// still present in that remote's catalog.
fn focused_public_pane(
    mirrors: &RemoteMirrors,
    ids: &ComposeIds,
    app: &AppState,
) -> Option<(usize, String)> {
    let ws = app.active.and_then(|idx| app.workspaces.get(idx))?;
    let pane_id = ws.focused_pane_id()?;
    let (remote, public) = ids.public_pane_id(pane_id)?;
    mirrors.get(remote)?.catalog.pane(public)?;
    Some((remote, public.to_owned()))
}

fn handle_key(key: crate::input::TerminalKey, ctx: &mut LoopCtx<'_>) {
    use crossterm::event::KeyCode;

    // The start-a-stopped-remote confirmation captures the keyboard while
    // open, ahead of the edit dialog: it is the more urgent of the two and
    // they are never open together.
    if ctx.chrome.remote_start.is_some() {
        if key.kind == crossterm::event::KeyEventKind::Release {
            return;
        }
        let decision = ctx
            .chrome
            .remote_start
            .as_ref()
            .map(|prompt| super::remote_start::remote_start_apply_key(prompt, key));
        match decision.unwrap_or(super::remote_start::RemoteStartKeyResult::Ignored) {
            super::remote_start::RemoteStartKeyResult::Start => start_stopped_remote(ctx),
            super::remote_start::RemoteStartKeyResult::Dismiss => {
                ctx.chrome.remote_start = None;
            }
            super::remote_start::RemoteStartKeyResult::Ignored => {}
        }
        return;
    }

    // The add/edit-remote dialog captures the keyboard while open.
    if ctx.chrome.remote_edit.is_some() {
        if key.kind == crossterm::event::KeyEventKind::Release {
            return;
        }
        let result = ctx
            .chrome
            .remote_edit
            .as_mut()
            .map(|dialog| super::remote_edit::remote_edit_apply_key(dialog, key));
        match result {
            Some(super::remote_edit::RemoteEditKeyResult::Submit) => submit_remote_edit(ctx),
            Some(super::remote_edit::RemoteEditKeyResult::Remove) => remove_edited_remote(ctx),
            Some(super::remote_edit::RemoteEditKeyResult::Cancel) => {
                ctx.chrome.remote_edit = None;
            }
            _ => {}
        }
        return;
    }

    // The remotes list captures the keyboard while open, behind the field
    // dialog it opens.
    if ctx.chrome.remote_list.is_some() {
        if key.kind == crossterm::event::KeyEventKind::Release {
            return;
        }
        let action = ctx
            .chrome
            .remote_list
            .as_mut()
            .map(|list| super::remote_list::remote_list_apply_key(list, key));
        if let Some(action) = action {
            apply_remote_list_action(action, ctx);
        }
        return;
    }

    if key.kind == crossterm::event::KeyEventKind::Release {
        // Legacy parity: only Terminal mode forwards key events to the
        // pane. Releases of keys typed into modals (rename, confirm-close,
        // context menu) or copy mode must not leak CSI-u release reports
        // into kitty-protocol panes behind the modal.
        if ctx.app.mode == Mode::Terminal {
            forward_key(key, ctx);
        }
        return;
    }

    match ctx.app.mode {
        Mode::Prefix => {
            ctx.app.mode = Mode::Terminal;
            match key.code {
                KeyCode::Char('d') => {
                    ctx.app.should_quit = true;
                }
                KeyCode::Esc => {}
                _ if ctx.app.keybinds.copy_mode.matches_prefix_key(key) => {
                    let in_view = in_view_remotes(ctx);
                    let source = MirrorPaneSource::for_view(ctx.mirrors, &in_view);
                    ctx.app.enter_copy_mode(&source);
                }
                _ => super::intent::dispatch_prefix_intent(
                    key,
                    ctx.links,
                    ctx.mirrors,
                    ctx.ids,
                    ctx.descriptors,
                    ctx.app,
                    ctx.chrome,
                ),
            }
        }
        Mode::Terminal => {
            if ctx.app.is_prefix_key(key) {
                ctx.app.mode = Mode::Prefix;
                return;
            }
            if matches!(key.code, KeyCode::PageUp | KeyCode::PageDown)
                && scroll_focused_replica_page(key.code, ctx)
            {
                return;
            }
            // Retained-selection copy (legacy try_copy_retained_selection):
            // with copy_on_select off, the copy chord copies a finalized
            // mouse selection out of the replica instead of reaching the
            // pane. An empty extraction falls through and forwards the key,
            // matching the legacy client.
            if !ctx.app.copy_on_select
                && crate::app::is_retained_selection_copy_key(key)
                && ctx
                    .app
                    .selection
                    .as_ref()
                    .is_some_and(crate::selection::Selection::is_finalized)
            {
                {
                    let in_view = in_view_remotes(ctx);
                    let source = MirrorPaneSource::for_view(ctx.mirrors, &in_view);
                    ctx.app.copy_selection(&source);
                }
                if let Some(content) = ctx.app.request_clipboard_write.take() {
                    crate::selection::write_osc52_bytes(&content);
                    return;
                }
            }
            forward_key(key, ctx);
        }
        Mode::Navigate => {
            // Nothing focused is still a live client: the empty screen's
            // own hint is "press prefix+shift+n to create one", so the
            // prefix key opens prefix mode here exactly as it does in
            // Terminal and Copy mode. Without this every binding is dead
            // while the composed view has no workspace (a fresh server,
            // the last space closed, or a solo'd remote with no spaces).
            if ctx.app.is_prefix_key(key) {
                ctx.app.mode = Mode::Prefix;
                return;
            }
            if key.code == KeyCode::Char('q') {
                ctx.app.should_quit = true;
                return;
            }
            // Navigate is the leaderless twin of prefix mode, exactly as
            // in the legacy client (`handle_navigate_key` resolves keys
            // through `BindingDispatch::Prefix`): every binding the
            // NAVIGATE bar advertises - new tab, splits, close, zoom - has
            // to act on a bare press, or the bar names keys that do
            // nothing. Pane-focus directions are the one exclusion the
            // legacy mode makes, because the arrows steer the workspace
            // selection there.
            if navigate_mode_excludes(&ctx.app.keybinds, key) {
                return;
            }
            super::intent::dispatch_prefix_intent(
                key,
                ctx.links,
                ctx.mirrors,
                ctx.ids,
                ctx.descriptors,
                ctx.app,
                ctx.chrome,
            );
        }
        Mode::Copy => {
            if ctx.app.is_prefix_key(key) {
                ctx.app.mode = Mode::Prefix;
                return;
            }
            let copy_pane = ctx
                .app
                .copy_mode
                .as_ref()
                .map(|copy_mode| copy_mode.pane_id);
            {
                // Copy mode runs entirely against the replica through the
                // pane-content seam: search, motions, selection, scrolling.
                let in_view = in_view_remotes(ctx);
                let source = MirrorPaneSource::for_view(ctx.mirrors, &in_view);
                ctx.app.handle_copy_mode_key(&source, key);
            }
            // OSC52: the pure client is the host terminal, so a pending
            // clipboard write goes straight to stdout.
            if let Some(content) = ctx.app.request_clipboard_write.take() {
                crate::selection::write_osc52_bytes(&content);
            }
            // Scrolling near the top of loaded history pages more in; a jump
            // to the top of history takes one large fetch instead of crawling
            // there a page per keypress.
            let trigger = if std::mem::take(&mut ctx.app.request_history_top_backfill) {
                crate::terminal::replica::BackfillTrigger::JumpToTop
            } else {
                crate::terminal::replica::BackfillTrigger::Scroll
            };
            if let Some((remote, stream_id)) = copy_pane
                .and_then(|pane_id| stream_for_composed_pane(ctx.mirrors, ctx.ids, pane_id))
            {
                request_backfill(ctx.links, ctx.mirrors, remote, stream_id, trigger);
            }
        }
        Mode::RenameWorkspace
        | Mode::RenameTab
        | Mode::RenamePane
        | Mode::ConfirmClose
        | Mode::ContextMenu => {
            let fallback_remote = ctx
                .chrome
                .selection
                .effective_focused_remote(ctx.descriptors);
            super::intent::dispatch_modal_key(
                key,
                ctx.links,
                ctx.mirrors,
                ctx.ids,
                ctx.app,
                fallback_remote,
            );
        }
        Mode::GlobalMenu => {
            // The global menu is a pure AppState modal: reuse the legacy
            // key handler (Esc closes, arrows move, Enter applies).
            crate::app::handle_global_menu_key(ctx.app, key.as_key_event());
        }
        Mode::KeybindHelp => {
            crate::app::handle_keybind_help_key(ctx.app, key);
        }
        _ => {
            // Residual modal modes (Settings, Navigator,
            // worktree dialogs) are unsupported under the flag: their
            // handlers live on App, coupled to config-file persistence and
            // live workspace runtimes (see the NOT CLOSED note in
            // intent::dispatch_mouse_intent). Esc or q folds back to the
            // base modes instead of trapping the user (or quitting the
            // whole client from inside a dead modal).
            if matches!(key.code, KeyCode::Esc | KeyCode::Char('q')) {
                ctx.app.context_menu = None;
                ctx.app.mode = Mode::Navigate;
            }
        }
    }
}

/// Validates and saves the dialog's remote, then reconciles the running
/// fleet against the freshly saved config.
fn submit_remote_edit(ctx: &mut LoopCtx<'_>) {
    let Some(dialog) = ctx.chrome.remote_edit.clone() else {
        return;
    };
    let entry = match dialog.entry() {
        Ok(entry) => entry,
        Err(err) => {
            if let Some(dialog) = ctx.chrome.remote_edit.as_mut() {
                dialog.error = Some(err);
            }
            return;
        }
    };
    let original = dialog.original_name.clone();
    let result = crate::fleet::config::update(move |remotes| {
        if let Some(original) = &original {
            if *original != entry.name {
                crate::fleet::config::remove_in(remotes, original);
            }
        }
        crate::fleet::config::upsert_in(remotes, entry);
    });
    match result {
        Ok(((), remotes)) => {
            ctx.chrome.remote_edit = None;
            reconcile_fleet(&remotes, ctx);
            refresh_remote_list(&remotes, ctx);
        }
        Err(err) => {
            if let Some(dialog) = ctx.chrome.remote_edit.as_mut() {
                dialog.error = Some(err.to_string());
            }
        }
    }
}

/// Re-renders the remotes list, if it is open behind the field dialog, from
/// the entries a write returned. Keeps the two surfaces from disagreeing
/// about the fleet after an edit made through the dialog.
fn refresh_remote_list(entries: &[crate::fleet::config::RemoteEntry], ctx: &mut LoopCtx<'_>) {
    if ctx.chrome.remote_list.is_none() {
        return;
    }
    let rows = super::remote_list::remote_list_rows(entries, ctx.descriptors, ctx.mirrors);
    if let Some(list) = ctx.chrome.remote_list.as_mut() {
        list.reload(rows);
    }
}

/// Removes the remote the dialog is editing, then reconciles.
fn remove_edited_remote(ctx: &mut LoopCtx<'_>) {
    let Some(name) = ctx
        .chrome
        .remote_edit
        .as_ref()
        .and_then(|dialog| dialog.original_name.clone())
    else {
        return;
    };
    match crate::fleet::config::remove_remote(&name) {
        Ok((_, remotes)) => {
            ctx.chrome.remote_edit = None;
            reconcile_fleet(&remotes, ctx);
            refresh_remote_list(&remotes, ctx);
        }
        Err(err) => {
            if let Some(dialog) = ctx.chrome.remote_edit.as_mut() {
                dialog.error = Some(err.to_string());
            }
        }
    }
}

/// Diffs the freshly saved config against the running fleet: identity
/// changes (or removals) tear the remote's link and mirror down, additions
/// get a fresh mirror and a link the reconnect scan picks up immediately.
fn reconcile_fleet(entries: &[crate::fleet::config::RemoteEntry], ctx: &mut LoopCtx<'_>) {
    let new_descriptors = remote_descriptors(entries);
    let old_descriptors = ctx.descriptors.clone();
    for old in ctx.descriptors.iter().skip(1) {
        let unchanged = new_descriptors.get(old.index).is_some_and(|new| {
            new.name == old.name && new.target == old.target && new.session == old.session
        });
        if !unchanged {
            debug!(remote = old.index, name = %old.name, "tearing down reconfigured remote");
            ctx.links.remove(&old.index);
            ctx.mirrors.remove(old.index);
            ctx.chrome.window_titles.remove(&old.index);
        }
    }
    for new in new_descriptors.iter().skip(1) {
        if ctx.mirrors.get(new.index).is_none() {
            ctx.mirrors
                .insert(super::RemoteMirror::new(new.index, new.name.clone()));
        }
        ctx.links.entry(new.index).or_insert(Link::Down {
            retry_at: Instant::now(),
        });
    }
    *ctx.descriptors = new_descriptors;
    ctx.chrome
        .selection
        .remap(&old_descriptors, ctx.descriptors);
    let valid: std::collections::BTreeSet<usize> = ctx
        .descriptors
        .iter()
        .map(|descriptor| descriptor.index)
        .collect();
    ctx.chrome
        .window_titles
        .retain(|remote, _| valid.contains(remote));
}

/// Encodes and forwards a key to the focused pane on its owning remote.
fn forward_key(key: crate::input::TerminalKey, ctx: &mut LoopCtx<'_>) {
    let Some((remote, pane_id)) = focused_public_pane(ctx.mirrors, ctx.ids, ctx.app) else {
        return;
    };
    let Some(mirror) = ctx.mirrors.get(remote) else {
        return;
    };
    let replica = mirror
        .stream_for_pane(&pane_id)
        .and_then(|stream_id| mirror.replicas.get(&stream_id))
        .map(|cell| cell.borrow());
    let protocol = replica
        .as_ref()
        .map(|replica| {
            crate::input::KeyboardProtocol::from_kitty_flags(
                replica.terminal().kitty_keyboard_flags().unwrap_or(0) as u16,
            )
        })
        .unwrap_or(crate::input::KeyboardProtocol::from_kitty_flags(0));
    // DECCKM: outside the kitty protocol, bare arrows on a pane in
    // application-cursor mode must be SS3 sequences, matching the legacy
    // encoder's terminal-mode awareness.
    if matches!(protocol, crate::input::KeyboardProtocol::Legacy)
        && key.kind != crossterm::event::KeyEventKind::Release
        && key.modifiers.is_empty()
        && matches!(
            key.code,
            crossterm::event::KeyCode::Up
                | crossterm::event::KeyCode::Down
                | crossterm::event::KeyCode::Left
                | crossterm::event::KeyCode::Right
        )
    {
        let application_cursor = replica
            .as_ref()
            .and_then(|replica| crate::pane::plain_terminal_input_state(replica.terminal()))
            .map(|state| state.application_cursor)
            .unwrap_or(false);
        if application_cursor {
            let bytes = crate::input::encode_cursor_key(key.code, true);
            if !bytes.is_empty() {
                send_pane_bytes(ctx.links, remote, &pane_id, &bytes);
            }
            return;
        }
    }
    let bytes = crate::input::encode_terminal_key(key, protocol);
    if bytes.is_empty() {
        return;
    }
    send_pane_bytes(ctx.links, remote, &pane_id, &bytes);
}

fn encode_paste(
    mirrors: &RemoteMirrors,
    ids: &ComposeIds,
    app: &AppState,
    text: &str,
) -> Option<(usize, String, Vec<u8>)> {
    let (remote, pane_id) = focused_public_pane(mirrors, ids, app)?;
    let mirror = mirrors.get(remote)?;
    let bracketed = mirror
        .stream_for_pane(&pane_id)
        .and_then(|stream_id| mirror.replicas.get(&stream_id))
        .and_then(|cell| crate::pane::plain_terminal_input_state(cell.borrow().terminal()))
        .map(|state| state.bracketed_paste)
        .unwrap_or(false);
    let mut bytes = Vec::new();
    if bracketed {
        bytes.extend_from_slice(b"\x1b[200~");
        bytes.extend_from_slice(text.as_bytes());
        bytes.extend_from_slice(b"\x1b[201~");
    } else {
        bytes.extend_from_slice(text.as_bytes());
    }
    Some((remote, pane_id, bytes))
}

/// Bridges an image paste into `pane.paste_image`: an empty bracketed
/// paste means the host clipboard holds an image instead of text, pasted
/// into the focused pane on whichever remote owns it. Pasted file paths
/// (terminal file drops) stay plain text — path-to-image bridging is a
/// remote-client concern the bridge does not attempt. Returns false to
/// fall through to a text paste.
fn try_paste_image(text: &str, ctx: &mut LoopCtx<'_>) -> bool {
    let Some((remote, pane_id)) = focused_public_pane(ctx.mirrors, ctx.ids, ctx.app) else {
        return false;
    };
    let negotiated_paste_image = ctx
        .mirrors
        .get(remote)
        .and_then(|mirror| mirror.connection.negotiated())
        .is_some_and(|negotiated| {
            negotiated.has_capability(crate::protocol::framed::CAPABILITY_PASTE_IMAGE)
        });
    if !negotiated_paste_image {
        return false;
    }
    if !text.is_empty() {
        return false;
    }
    let Some(image) = crate::platform::read_clipboard_image() else {
        return false;
    };
    if image.bytes.len() > crate::protocol::MAX_CLIPBOARD_IMAGE_PAYLOAD {
        warn!(
            bytes = image.bytes.len(),
            max = crate::protocol::MAX_CLIPBOARD_IMAGE_PAYLOAD,
            "clipboard image is too large to paste"
        );
        return true;
    }
    let Some(session) = session_for(ctx.links, remote) else {
        return false;
    };
    info!(
        bytes = image.bytes.len(),
        extension = image.extension,
        pane = %pane_id,
        "pasting image through pane.paste_image"
    );
    let id = session.request_id("paste-image");
    let request = crate::protocol::framed::pane_paste_image_request(
        &id,
        &pane_id,
        image.extension,
        &image.bytes,
    );
    session.pending.insert(id, Pending::Api);
    if let Err(err) = session.send_control(&request) {
        warn!(err = %err, "pane.paste_image send failed");
    }
    true
}

fn send_pane_bytes(links: &mut Links, remote: usize, pane_id: &str, bytes: &[u8]) {
    let Some(session) = session_for(links, remote) else {
        return;
    };
    let id = session.request_id("input");
    if let Err(err) = session.send_control(&pane_send_bytes_request(&id, pane_id, bytes)) {
        warn!(err = %err, "pane.send_bytes failed");
    }
}

/// Mouse: wheel scrolls the replica locally (or forwards to reporting
/// panes); buttons and drags inside reporting panes are encoded and
/// forwarded; remaining clicks resolve against the computed view into
/// focus intents.
fn handle_mouse(mouse: MouseEvent, ctx: &mut LoopCtx<'_>) {
    // Herdr is mouse-first: both fleet modals swallow the mouse while open
    // and only their own buttons act. The start prompt is checked first, in
    // the same order as the key routing.
    if ctx.chrome.remote_start.is_some() {
        handle_start_prompt_click(mouse, ctx);
        return;
    }
    // The add/edit-remote dialog swallows the mouse while open; only its
    // buttons act.
    if ctx.chrome.remote_edit.is_some() {
        handle_dialog_click(mouse, ctx);
        return;
    }
    // The remotes list likewise swallows the mouse: its rows select, and
    // `[done]` closes.
    if ctx.chrome.remote_list.is_some() {
        handle_remote_list_click(mouse, ctx);
        return;
    }
    // Chip strip first: chips are pure client chrome, hit-tested against the
    // computed view. The strip's header is not handled here - it is a
    // section menu now, opened through the shared intent path like the other
    // two headers.
    if let MouseEventKind::Down(button) = mouse.kind {
        if let Some(chip_idx) = crate::ui::remote_chip_at(ctx.app, mouse.column, mouse.row) {
            handle_chip_click(chip_idx, button, ctx);
            return;
        }
    }
    match mouse.kind {
        MouseEventKind::ScrollUp | MouseEventKind::ScrollDown => {
            let pane_hit = ctx
                .app
                .view
                .pane_infos
                .iter()
                .find(|info| {
                    info.inner_rect
                        .contains(ratatui::layout::Position::new(mouse.column, mouse.row))
                })
                .map(|info| (info.id, info.inner_rect));
            let Some((pane_id, inner_rect)) = pane_hit else {
                return;
            };
            let Some((remote, public)) = ctx
                .ids
                .public_pane_id(pane_id)
                .map(|(remote, public)| (remote, public.to_owned()))
            else {
                return;
            };
            let Some(mirror) = ctx.mirrors.get_mut(remote) else {
                return;
            };
            let Some(stream_id) = mirror.stream_for_pane(&public) else {
                return;
            };
            let Some(replica) = mirror.replica_mut(stream_id) else {
                return;
            };
            let input_state = crate::pane::plain_terminal_input_state(replica.terminal());
            let reporting = input_state.is_some_and(|state| state.mouse_reporting_enabled());
            if reporting {
                if let Some(state) = input_state {
                    // Mouse reports are pane-local coordinates.
                    if let Some(bytes) = crate::input::encode_mouse_scroll(
                        mouse.kind,
                        mouse.column.saturating_sub(inner_rect.x),
                        mouse.row.saturating_sub(inner_rect.y),
                        mouse.modifiers,
                        state.mouse_protocol_encoding,
                    ) {
                        send_pane_bytes(ctx.links, remote, &public, &bytes);
                    }
                }
                return;
            }
            let lines = ctx.app.mouse_scroll_lines as isize;
            let delta = if mouse.kind == MouseEventKind::ScrollUp {
                -lines
            } else {
                lines
            };
            replica.scroll_delta(delta);
            request_backfill(
                ctx.links,
                ctx.mirrors,
                remote,
                stream_id,
                crate::terminal::replica::BackfillTrigger::Scroll,
            );
        }
        MouseEventKind::Down(_) | MouseEventKind::Up(_) | MouseEventKind::Drag(_)
            if forward_reported_mouse_button(mouse, ctx) => {}
        _ => {
            if handle_pane_double_click(mouse, ctx) {
                return;
            }
            super::intent::dispatch_mouse_intent(
                mouse,
                ctx.links,
                ctx.mirrors,
                ctx.ids,
                ctx.descriptors,
                ctx.app,
                ctx.chrome,
            );
        }
    }
}

/// Double-click word selection against the replica, mirroring the legacy
/// `App::handle_pane_double_click` gesture: two adjacent left-clicks in the
/// same pane cell within the double-click window select the token under
/// the cursor, and copy_on_select sends it out as OSC52. Mouse-reporting
/// panes never reach this (their buttons forward upstream). Returns true
/// when the double-click was consumed.
fn handle_pane_double_click(mouse: MouseEvent, ctx: &mut LoopCtx<'_>) -> bool {
    use crossterm::event::MouseButton;

    // A pane press stops being a double-click candidate once it becomes a
    // drag or completes as a real text selection (legacy parity).
    match mouse.kind {
        MouseEventKind::Drag(MouseButton::Left) => {
            ctx.chrome.last_pane_click = None;
            return false;
        }
        MouseEventKind::Up(MouseButton::Left)
            if ctx
                .app
                .selection
                .as_ref()
                .is_some_and(crate::selection::Selection::is_visible) =>
        {
            ctx.chrome.last_pane_click = None;
            return false;
        }
        MouseEventKind::Down(MouseButton::Left) => {}
        _ => return false,
    }

    if !mouse.modifiers.is_empty() || ctx.app.mode != Mode::Terminal {
        ctx.chrome.last_pane_click = None;
        return false;
    }
    let Some(info) = ctx
        .app
        .view
        .pane_infos
        .iter()
        .find(|info| {
            info.inner_rect
                .contains(ratatui::layout::Position::new(mouse.column, mouse.row))
        })
        .cloned()
    else {
        ctx.chrome.last_pane_click = None;
        return false;
    };
    let viewport_row = mouse.row.saturating_sub(info.inner_rect.y);
    let col = mouse.column.saturating_sub(info.inner_rect.x);
    let click = crate::app::PaneClickState::new(info.id, viewport_row, col);
    if !ctx
        .chrome
        .last_pane_click
        .take()
        .is_some_and(|last| last.is_double_click_for(click))
    {
        ctx.chrome.last_pane_click = Some(click);
        return false;
    }

    let selected = {
        let in_view = in_view_remotes(ctx);
        let source = MirrorPaneSource::for_view(ctx.mirrors, &in_view);
        ctx.app
            .select_word_at_pane_cell(&source, info.id, viewport_row, col)
    };
    // copy_on_select word copies go straight out as OSC52, like every
    // other pure-client copy.
    if let Some(content) = ctx.app.request_clipboard_write.take() {
        crate::selection::write_osc52_bytes(&content);
    }
    selected
}

/// A left click toggles the chip's view membership; a second click within
/// the double-click window solos it; a right click opens the edit dialog.
/// Selection never touches connections: filtered-out remotes stay
/// connected and syncing.
fn handle_chip_click(
    chip_idx: usize,
    button: crossterm::event::MouseButton,
    ctx: &mut LoopCtx<'_>,
) {
    let Some(descriptor) = ctx.descriptors.get(chip_idx).cloned() else {
        return;
    };
    match button {
        crossterm::event::MouseButton::Left => {
            // A dimmed, stopped chip re-offers the start. Without this,
            // declining the prompt once would leave that remote dimmed for
            // the rest of the session with nothing to click.
            if matches!(ctx.links.get(&descriptor.index), Some(Link::Stopped)) {
                if let Some(mirror) = ctx.mirrors.get(descriptor.index) {
                    let status = match &mirror.connection {
                        super::ClientConnectionState::Stopped { message } => message.clone(),
                        _ => crate::fleet::connection::stopped_status_line(&descriptor.name),
                    };
                    ctx.chrome.remote_start = Some(super::remote_start::RemoteStartPrompt {
                        remote: descriptor.index,
                        name: descriptor.name.clone(),
                        status,
                        error: None,
                        starting: false,
                    });
                    return;
                }
            }
            let now = Instant::now();
            let double = ctx.ui.last_chip_click.take().is_some_and(|(previous, at)| {
                previous == chip_idx
                    && now.saturating_duration_since(at) <= CHIP_DOUBLE_CLICK_WINDOW
            });
            if double {
                ctx.chrome.selection.solo(descriptor.index, ctx.descriptors);
                return;
            }
            ctx.ui.last_chip_click = Some((chip_idx, now));
            if let Err(refusal) = ctx
                .chrome
                .selection
                .toggle(descriptor.index, ctx.descriptors)
            {
                ctx.ui.status_flash = Some((refusal.to_owned(), now));
            }
        }
        // Right-click on a chip does nothing. It used to jump straight into
        // the edit dialog - the one place in herdr where right-click skipped
        // a menu - and editing now lives in the remotes list modal, reached
        // from the remotes section menu.
        crossterm::event::MouseButton::Right | crossterm::event::MouseButton::Middle => {}
    }
}

/// Routes a click inside the open remotes list: a row selects, `[done]`
/// closes, and a click outside the modal is swallowed rather than reaching
/// the view behind it.
fn handle_remote_list_click(mouse: MouseEvent, ctx: &mut LoopCtx<'_>) {
    if !matches!(
        mouse.kind,
        MouseEventKind::Down(crossterm::event::MouseButton::Left)
    ) {
        return;
    }
    let Ok((cols, rows)) = crossterm::terminal::size() else {
        return;
    };
    let area = ratatui::layout::Rect::new(0, 0, cols, rows);
    let count = ctx
        .chrome
        .remote_list
        .as_ref()
        .map(|list| list.rows.len())
        .unwrap_or(0);
    let Some(inner) = crate::ui::remote_list_inner_rect(area, count) else {
        return;
    };
    let position = ratatui::layout::Position::new(mouse.column, mouse.row);
    if crate::ui::remote_list_done_rect(inner).contains(position) {
        ctx.chrome.remote_list = None;
        return;
    }
    let row = crate::ui::remote_list_row_rects(inner, count)
        .into_iter()
        .position(|rect| rect.contains(position));
    if let (Some(row), Some(list)) = (row, ctx.chrome.remote_list.as_mut()) {
        list.error = None;
        list.selected = row;
    }
}

/// Routes a click inside the open start-remote confirmation to its buttons.
fn handle_start_prompt_click(mouse: MouseEvent, ctx: &mut LoopCtx<'_>) {
    if !matches!(
        mouse.kind,
        MouseEventKind::Down(crossterm::event::MouseButton::Left)
    ) {
        return;
    }
    let (cols, rows) = crossterm::terminal::size().unwrap_or((80, 24));
    let area = ratatui::layout::Rect::new(0, 0, cols, rows);
    let Some(inner) = crate::ui::remote_start_inner_rect(area) else {
        return;
    };
    let (start, cancel) = crate::ui::remote_start_button_rects(inner);
    let position = ratatui::layout::Position::new(mouse.column, mouse.row);
    if start.contains(position) {
        start_stopped_remote(ctx);
    } else if cancel.contains(position) {
        ctx.chrome.remote_start = None;
    }
}

/// Routes a click inside the open add/edit-remote dialog to its buttons.
fn handle_dialog_click(mouse: MouseEvent, ctx: &mut LoopCtx<'_>) {
    if !matches!(
        mouse.kind,
        MouseEventKind::Down(crossterm::event::MouseButton::Left)
    ) {
        return;
    }
    let (cols, rows) = crossterm::terminal::size().unwrap_or((80, 24));
    let area = ratatui::layout::Rect::new(0, 0, cols, rows);
    let Some(inner) = crate::ui::remote_edit_inner_rect(area) else {
        return;
    };
    let (save, cancel) = crate::ui::remote_edit_button_rects(inner);
    let position = ratatui::layout::Position::new(mouse.column, mouse.row);
    if save.contains(position) {
        submit_remote_edit(ctx);
    } else if cancel.contains(position) {
        ctx.chrome.remote_edit = None;
    }
}

/// Encodes a button/drag event for a mouse-reporting pane and forwards it as
/// pane bytes, focusing the pane first when it was not focused. Returns
/// false when the event is not over a reporting pane, so it falls through to
/// chrome intent dispatch.
fn forward_reported_mouse_button(mouse: MouseEvent, ctx: &mut LoopCtx<'_>) -> bool {
    let app = &mut *ctx.app;
    if app.mode != Mode::Terminal {
        return false;
    }
    let pane_hit = app
        .view
        .pane_infos
        .iter()
        .find(|info| {
            info.inner_rect
                .contains(ratatui::layout::Position::new(mouse.column, mouse.row))
        })
        .map(|info| (info.id, info.inner_rect));
    let Some((pane_id, inner_rect)) = pane_hit else {
        return false;
    };
    let Some((remote, public)) = ctx
        .ids
        .public_pane_id(pane_id)
        .map(|(remote, public)| (remote, public.to_owned()))
    else {
        return false;
    };
    let Some(mirror) = ctx.mirrors.get(remote) else {
        return false;
    };
    let Some(replica) = mirror
        .stream_for_pane(&public)
        .and_then(|stream_id| mirror.replicas.get(&stream_id))
    else {
        return false;
    };
    let Some(state) = crate::pane::plain_terminal_input_state(replica.borrow().terminal()) else {
        return false;
    };
    if !state.mouse_reporting_enabled() {
        return false;
    }
    let Some(bytes) = crate::input::encode_mouse_button(
        mouse.kind,
        mouse.column.saturating_sub(inner_rect.x),
        mouse.row.saturating_sub(inner_rect.y),
        mouse.modifiers,
        state.mouse_protocol_encoding,
    ) else {
        return false;
    };
    // Clicking an unfocused reporting pane focuses it and delivers the
    // report, matching the legacy pane-first routing.
    if matches!(mouse.kind, MouseEventKind::Down(_)) {
        let focused = app
            .active
            .and_then(|idx| app.workspaces.get(idx))
            .and_then(|ws| ws.focused_pane_id());
        if focused != Some(pane_id) {
            send_api_request(
                ctx.links,
                remote,
                crate::api::schema::Method::PaneFocus(crate::api::schema::PaneTarget {
                    pane_id: public.clone(),
                }),
            );
        }
    }
    send_pane_bytes(ctx.links, remote, &public, &bytes);
    true
}

/// Sends a JSON API request to one remote's framed control plane.
/// Fire-and-forget: the response only surfaces errors, and the resulting
/// catalog events update that remote's mirror.
pub(super) fn send_api_request(
    links: &mut Links,
    remote: usize,
    method: crate::api::schema::Method,
) {
    let Some(session) = session_for(links, remote) else {
        return;
    };
    let id = session.request_id("api");
    let request = crate::api::schema::Request {
        id: id.clone(),
        method,
    };
    let Ok(request_value) = serde_json::to_value(&request) else {
        return;
    };
    let control = serde_json::json!({
        "id": id.clone(),
        "method": crate::protocol::framed::API_REQUEST_METHOD,
        "params": { "request": request_value },
    });
    session.pending.insert(id, Pending::Api);
    if let Err(err) = session.send_control(&control) {
        warn!(err = %err, "api.request failed");
    }
}

#[cfg(test)]
mod tests {
    use super::super::compose::compose_into;
    use super::*;
    use crossterm::event::{KeyCode, KeyModifiers};

    fn key(code: KeyCode) -> crate::input::TerminalKey {
        crate::input::TerminalKey::new(code, KeyModifiers::empty())
    }

    /// Builds a plain-data loop context (no sockets, no threads) and hands
    /// it to the test body.
    fn with_test_ctx(descriptors: Vec<RemoteDescriptor>, f: impl FnOnce(&mut LoopCtx<'_>)) {
        let config = crate::config::Config::default();
        let mut descriptors = descriptors;
        let mut links: Links = BTreeMap::new();
        let mut mirrors = RemoteMirrors::with_local();
        for descriptor in descriptors.iter().skip(1) {
            mirrors.insert(super::super::RemoteMirror::new(
                descriptor.index,
                descriptor.name.clone(),
            ));
        }
        let mut chrome = GlobalChrome::new();
        let mut ids = ComposeIds::new();
        let mut app = AppState::test_new();
        app.keybinds = crate::config::Config::default().keybinds();
        let mut ui = InteractionState::default();
        let (event_tx, _event_rx) = mpsc::sync_channel::<LoopEvent>(64);
        let should_quit = Arc::new(AtomicBool::new(false));
        let mut ctx = LoopCtx {
            config: &config,
            descriptors: &mut descriptors,
            links: &mut links,
            mirrors: &mut mirrors,
            chrome: &mut chrome,
            ids: &mut ids,
            app: &mut app,
            ui: &mut ui,
            event_tx: &event_tx,
            should_quit: &should_quit,
            scrollback_limit: 64 * 1024,
        };
        f(&mut ctx);
    }

    fn three_descriptors() -> Vec<RemoteDescriptor> {
        remote_descriptors(&[
            // Remote #0 is a local runtime - an ordinary target-less entry
            // now that no runtime is implicit. `with_test_ctx` seeds its
            // mirror, so index 0 still means "the local one" in these tests.
            crate::fleet::config::RemoteEntry {
                name: "local".into(),
                target: None,
                session: "default".into(),
                enabled: true,
                hue: None,
            },
            crate::fleet::config::RemoteEntry {
                name: "buildbox".into(),
                target: Some("can@buildbox.example".into()),
                session: "default".into(),
                enabled: true,
                hue: None,
            },
            crate::fleet::config::RemoteEntry {
                name: "gpu-01".into(),
                target: Some("can@gpu-01.example".into()),
                session: "default".into(),
                enabled: true,
                hue: None,
            },
        ])
    }

    /// With nothing focused - a fresh server, the last space closed, or a
    /// solo'd remote that has no spaces yet - the composed view is empty
    /// and `sync_mode` parks the client in `Mode::Navigate`. That screen's
    /// own hint is "press prefix+shift+n to create one", so the prefix key
    /// has to open prefix mode there exactly as it does in Terminal and
    /// Copy mode; otherwise every binding is dead.
    #[tokio::test]
    async fn the_prefix_key_opens_prefix_mode_with_no_workspace_in_view() {
        with_test_ctx(vec![RemoteDescriptor::local()], |ctx| {
            compose_into(ctx.mirrors.local(), ctx.chrome, ctx.ids, ctx.app);
            sync_mode(ctx.app);
            assert_eq!(
                ctx.app.mode,
                Mode::Navigate,
                "an empty catalog focuses nothing"
            );

            let (session, mut server) = session_pair();
            ctx.links
                .insert(LOCAL_REMOTE_INDEX, Link::Up(Box::new(session)));

            // The default prefix is ctrl+b.
            handle_key(
                crate::input::TerminalKey::new(KeyCode::Char('b'), KeyModifiers::CONTROL),
                ctx,
            );
            assert_eq!(
                ctx.app.mode,
                Mode::Prefix,
                "the empty screen tells the user to press prefix+shift+n"
            );

            // ...and the whole user-visible path runs: the following key
            // reaches intent dispatch and its method leaves the link. The
            // mode alone proves nothing - the prefix arm consumes the mode
            // before it matches any binding.
            handle_key(
                crate::input::TerminalKey::new(KeyCode::Char('N'), KeyModifiers::SHIFT),
                ctx,
            );
            let frame = try_read_control(&mut server).expect("prefix+shift+n reaches the wire");
            assert_eq!(frame["method"], "api.request");
            assert_eq!(
                frame["params"]["request"]["method"], "workspace.create",
                "{frame}"
            );
        });
    }

    /// Navigate is the leaderless twin of prefix mode, like the legacy
    /// client's: the NAVIGATE bar on the empty screen names bare keys, so a
    /// bare press has to run the same intent the prefix chord runs, or the
    /// bar advertises keys that do nothing. Directional pane focus is the
    /// one exclusion legacy makes.
    #[tokio::test]
    async fn navigate_mode_runs_prefix_intents_without_the_prefix() {
        with_test_ctx(vec![RemoteDescriptor::local()], |ctx| {
            compose_into(ctx.mirrors.local(), ctx.chrome, ctx.ids, ctx.app);
            sync_mode(ctx.app);
            assert_eq!(ctx.app.mode, Mode::Navigate);
            let (session, mut server) = session_pair();
            ctx.links
                .insert(LOCAL_REMOTE_INDEX, Link::Up(Box::new(session)));

            // Bare shift+N is the default new-workspace binding's RHS.
            handle_key(
                crate::input::TerminalKey::new(KeyCode::Char('N'), KeyModifiers::SHIFT),
                ctx,
            );
            let frame =
                try_read_control(&mut server).expect("a bare binding acts in navigate mode");
            assert_eq!(
                frame["params"]["request"]["method"], "workspace.create",
                "{frame}"
            );

            // ...but the arrows and their vi twins steer the selection in
            // navigate mode, so directional pane focus stays prefix-only.
            handle_key(key(KeyCode::Char('h')), ctx);
            assert!(
                try_read_control(&mut server).is_none(),
                "directional pane focus is prefix-only in navigate mode"
            );
        });
    }

    #[tokio::test]
    async fn global_menu_and_keybind_help_keys_are_handled_client_side() {
        with_test_ctx(vec![RemoteDescriptor::local()], |ctx| {
            ctx.app.pure_client = true;
            ctx.app.detach_exits = true;

            // Open the menu; q inside the menu must not quit the client.
            ctx.app.mode = Mode::GlobalMenu;
            ctx.app.global_menu = crate::app::state::MenuListState::new(0);
            handle_key(key(KeyCode::Char('q')), ctx);
            assert!(!ctx.app.should_quit, "q inside the menu must not quit");
            assert_eq!(ctx.app.mode, Mode::GlobalMenu);

            // Enter on the first entry (keybinds) opens the help overlay,
            // and Esc walks back out to a base mode.
            handle_key(key(KeyCode::Enter), ctx);
            assert_eq!(ctx.app.mode, Mode::KeybindHelp);
            handle_key(key(KeyCode::Esc), ctx);
            assert_eq!(ctx.app.mode, Mode::Navigate);

            // Esc closes the menu itself.
            ctx.app.mode = Mode::GlobalMenu;
            handle_key(key(KeyCode::Esc), ctx);
            assert_eq!(ctx.app.mode, Mode::Navigate);
            assert!(!ctx.app.should_quit);

            // Enter on detach (last entry) exits the fleet client.
            let detach_row = ctx.app.global_menu_labels().len().saturating_sub(1);
            ctx.app.mode = Mode::GlobalMenu;
            ctx.app.global_menu = crate::app::state::MenuListState::new(detach_row);
            handle_key(key(KeyCode::Enter), ctx);
            assert!(ctx.app.should_quit, "detach exits the pure client");
        });
    }

    /// The reported bootstrap hole, closed at the source: a one-remote
    /// fleet used to compose no strip, so there was nowhere to add a remote
    /// from and a launcher-menu entry had to stand in. The strip is always
    /// composed now, and its header carries the menu that owns adding.
    #[tokio::test]
    async fn the_first_remote_can_be_added_from_a_one_remote_strip() {
        use crossterm::event::MouseButton;
        with_test_ctx(vec![RemoteDescriptor::local()], |ctx| {
            ctx.app.pure_client = true;
            ctx.app.fleet_config_backed = true;
            render_chip_strip(ctx, 106, 30);
            assert_eq!(
                ctx.app.remote_chips.len(),
                1,
                "a one-remote fleet still composes its strip"
            );
            assert!(
                ctx.app.view.sidebar_remotes_header_rect.width > 0,
                "and so its header, which owns adding, is on screen"
            );

            let click = |column: u16, row: u16| {
                crate::raw_input::RawInputEvent::Mouse(MouseEvent {
                    kind: MouseEventKind::Down(MouseButton::Left),
                    column,
                    row,
                    modifiers: KeyModifiers::empty(),
                })
            };
            let launcher = ctx.app.global_launcher_rect();
            handle_raw_input(click(launcher.x, launcher.y), ctx);
            assert_eq!(ctx.app.mode, Mode::GlobalMenu);

            let row = ctx
                .app
                .global_menu_labels()
                .iter()
                .position(|label| *label == "add remote")
                .expect("the pure client offers an add-remote entry");
            let menu = ctx.app.global_menu_rect();
            handle_raw_input(click(menu.x + 2, menu.y + 1 + row as u16), ctx);

            assert_eq!(
                ctx.chrome.remote_edit,
                Some(super::super::remote_edit::RemoteEditState::add()),
                "the menu opens the same add dialog the chip strip opens"
            );
            assert!(!ctx.app.request_add_remote, "the request is drained once");
            assert!(!ctx.app.should_quit);
        });
    }

    /// `esc` dismissed nothing in the pure client. The framer holds a lone
    /// escape back because it cannot yet tell it from the start of a
    /// sequence, and only an idle flush resolves that - which the run loop
    /// never performed, so the keystroke was stuck in the buffer until the
    /// next one arrived. Every `esc` in the client rode on this: closing a
    /// dialog, cancelling prefix, leaving copy mode.
    #[tokio::test]
    async fn an_idle_tick_releases_a_held_escape_and_closes_a_dialog() {
        with_test_ctx(vec![RemoteDescriptor::local()], |ctx| {
            ctx.app.pure_client = true;
            ctx.app.fleet_config_backed = true;
            ctx.chrome.remote_edit = Some(super::super::remote_edit::RemoteEditState::add());

            let mut framer = crate::raw_input::RawInputFramer::for_host_input();

            // The escape byte alone yields no event: the framer is waiting to
            // see whether a sequence follows.
            assert!(
                framer.push(b"\x1b").is_empty(),
                "a lone escape is held, not dispatched"
            );
            assert!(framer.has_pending_input(), "and it is still buffered");
            assert!(
                ctx.chrome.remote_edit.is_some(),
                "so the dialog is still open at this point"
            );

            // The quiet tick is what settles it.
            assert!(
                drain_idle_input(&mut framer, ctx),
                "the idle flush dispatches"
            );
            assert!(
                ctx.chrome.remote_edit.is_none(),
                "esc closed the dialog once the tick released it"
            );
            assert!(!framer.has_pending_input(), "nothing is left held");
        });
    }

    /// The bootstrap hole one step further in than a fleet of one: a fresh
    /// install has no `remotes.toml` at all, so the fleet is empty, and a
    /// strip composed only when it has a chip to show left that user with no
    /// remotes section and nowhere to add their first remote.
    #[tokio::test]
    async fn an_empty_fleet_still_composes_the_remotes_strip() {
        with_test_ctx(vec![], |ctx| {
            ctx.app.pure_client = true;
            ctx.app.fleet_config_backed = true;
            render_chip_strip(ctx, 106, 30);

            assert!(
                ctx.app.remote_chips.is_empty(),
                "nothing is configured, so there is no chip"
            );
            assert!(
                ctx.app.view.remote_chip_strip_rect.height > 0,
                "the strip is still on screen"
            );
            assert!(
                ctx.app.view.sidebar_remotes_header_rect.width > 0,
                "and its header, which owns adding, is reachable"
            );
            assert!(
                ctx.app.global_menu_labels().contains(&"add remote"),
                "a fleet with nothing in it is exactly where adding must work"
            );
        });
    }

    #[tokio::test]
    async fn the_menu_adds_a_remote_from_the_keyboard_too() {
        with_test_ctx(vec![RemoteDescriptor::local()], |ctx| {
            ctx.app.pure_client = true;
            ctx.app.fleet_config_backed = true;
            render_chip_strip(ctx, 106, 30);

            let row = ctx
                .app
                .global_menu_labels()
                .iter()
                .position(|label| *label == "add remote")
                .expect("the pure client offers an add-remote entry");
            ctx.app.mode = Mode::GlobalMenu;
            ctx.app.global_menu = crate::app::state::MenuListState::new(row);
            handle_raw_input(
                crate::raw_input::RawInputEvent::Key(key(KeyCode::Enter)),
                ctx,
            );

            assert_eq!(
                ctx.chrome.remote_edit,
                Some(super::super::remote_edit::RemoteEditState::add())
            );
            assert!(!ctx.app.request_add_remote);
        });
    }

    /// An ephemeral `--remote` fleet-of-one is not the config-backed fleet:
    /// its live ssh remote *is* remote #0, the index `reconcile_fleet`
    /// hands to the local runtime. Saving a remote there would leave the
    /// ssh link and mirror at 0 under a descriptor describing the local
    /// machine - the chip would read "local" and the next reconnect would
    /// dial the local socket instead of the host. So the entry is not
    /// offered, and the dialog stays out of reach even if the request flag
    /// is set some other way.
    #[tokio::test]
    async fn an_ephemeral_fleet_of_one_cannot_add_remotes() {
        let ephemeral = RemoteDescriptor::ephemeral("can@gpu1", "can@gpu1", "default", None);
        with_test_ctx(vec![ephemeral.clone()], |ctx| {
            ctx.app.pure_client = true;
            render_chip_strip(ctx, 106, 30);
            assert_eq!(
                ctx.app.remote_chips.len(),
                1,
                "the strip is composed even for a fleet of one"
            );

            assert!(
                !ctx.app.global_menu_labels().contains(&"add remote"),
                "the menu offers no entry whose save would rewrite remote #0"
            );

            ctx.app.request_add_remote = true;
            drain_app_requests(ctx);

            assert!(
                ctx.chrome.remote_edit.is_none(),
                "the add-remote dialog never opens in a fleet of one"
            );
            assert!(!ctx.app.request_add_remote, "the request is still drained");
            assert_eq!(
                ctx.descriptors.first(),
                Some(&ephemeral),
                "the ephemeral remote keeps its identity"
            );
        });
    }

    #[tokio::test]
    async fn residual_modal_modes_never_quit_on_q() {
        with_test_ctx(vec![RemoteDescriptor::local()], |ctx| {
            // Context menus are interpreted client-side now: q does nothing
            // and never quits; Esc closes the menu back to a base mode.
            ctx.app.mode = Mode::ContextMenu;
            handle_key(key(KeyCode::Char('q')), ctx);
            assert!(!ctx.app.should_quit, "q inside a modal must not quit");
            assert_eq!(ctx.app.mode, Mode::ContextMenu);
            handle_key(key(KeyCode::Esc), ctx);
            assert!(!ctx.app.should_quit);
            assert_eq!(ctx.app.mode, Mode::Navigate);

            // Confirm-close cancels on Esc.
            ctx.app.mode = Mode::ConfirmClose;
            handle_key(key(KeyCode::Esc), ctx);
            assert!(!ctx.app.should_quit);
            assert_eq!(ctx.app.mode, Mode::Navigate);

            // Still-unsupported modal modes fold back to Navigate on Esc/q.
            ctx.app.mode = Mode::Settings;
            handle_key(key(KeyCode::Esc), ctx);
            assert!(!ctx.app.should_quit);
            assert_eq!(ctx.app.mode, Mode::Navigate);

            // Navigate itself still quits on q.
            handle_key(key(KeyCode::Char('q')), ctx);
            assert!(ctx.app.should_quit);
        });
    }

    #[tokio::test]
    async fn chip_clicks_toggle_and_solo_without_touching_links() {
        with_test_ctx(three_descriptors(), |ctx| {
            // A click on chip 1 filters buildbox out of view.
            handle_chip_click(1, crossterm::event::MouseButton::Left, ctx);
            assert!(!ctx.chrome.selection.is_in_view(1));
            assert!(
                ctx.links.is_empty() && ctx.mirrors.get(1).is_some(),
                "selection must not create, drop, or disconnect links"
            );

            // A second click within the window solos buildbox (eager
            // double-click: the intermediate toggle is superseded).
            handle_chip_click(1, crossterm::event::MouseButton::Left, ctx);
            handle_chip_click(1, crossterm::event::MouseButton::Left, ctx);
            assert!(ctx.chrome.selection.is_in_view(1));
            assert!(!ctx.chrome.selection.is_in_view(0));
            assert!(!ctx.chrome.selection.is_in_view(2));
            assert_eq!(ctx.chrome.selection.focused_remote, 1);

            // Filtering the last in-view remote is refused with a flash.
            ctx.ui.last_chip_click = None;
            handle_chip_click(1, crossterm::event::MouseButton::Left, ctx);
            assert!(ctx.chrome.selection.is_in_view(1));
            assert!(
                ctx.ui
                    .status_flash
                    .as_ref()
                    .is_some_and(|(message, _)| message.contains("stays in view")),
                "refusal surfaces as a status flash"
            );
        });
    }

    #[tokio::test]
    async fn the_remotes_menu_opens_the_list_and_the_list_shows_disabled_remotes() {
        with_test_ctx(three_descriptors(), |ctx| {
            ctx.app.pure_client = true;
            ctx.app.fleet_config_backed = true;

            // The menu's `edit remotes...` sets the request flag; the run
            // loop drains it into the modal.
            ctx.app.request_manage_remotes = true;
            drain_app_requests(ctx);
            assert!(
                ctx.chrome.remote_list.is_some(),
                "the remotes menu opens the list"
            );
            assert!(!ctx.app.request_manage_remotes, "drained once");

            // A disabled remote has no descriptor and no mirror, so it must
            // come from the config rather than the descriptor list - or the
            // user could never find it again to re-enable it.
            let rows = super::super::remote_list::remote_list_rows(
                &[
                    crate::fleet::config::RemoteEntry {
                        name: "buildbox".into(),
                        target: Some("can@buildbox".into()),
                        session: "default".into(),
                        enabled: true,
                        hue: Some(0),
                    },
                    crate::fleet::config::RemoteEntry {
                        name: "dark".into(),
                        target: Some("can@dark".into()),
                        session: "default".into(),
                        enabled: false,
                        hue: Some(1),
                    },
                ],
                ctx.descriptors,
                ctx.mirrors,
            );
            assert_eq!(rows.len(), 2, "disabled entries stay listed");
            assert_eq!(
                rows[1].status,
                super::super::remote_list::RemoteListStatus::Disabled
            );
        });
    }

    #[tokio::test]
    async fn an_ephemeral_fleet_never_opens_the_remotes_list() {
        // Every action in the modal is a write to `remotes.toml`, which a
        // `--remote` fleet-of-one does not have.
        with_test_ctx(three_descriptors(), |ctx| {
            ctx.app.pure_client = true;
            ctx.app.fleet_config_backed = false;

            ctx.app.request_manage_remotes = true;
            drain_app_requests(ctx);

            assert!(ctx.chrome.remote_list.is_none());
        });
    }

    #[tokio::test]
    async fn the_list_captures_the_keyboard_and_escape_closes_it() {
        with_test_ctx(three_descriptors(), |ctx| {
            ctx.chrome.remote_list = Some(super::super::remote_list::RemoteListState::new(
                super::super::remote_list::remote_list_rows(
                    &[crate::fleet::config::RemoteEntry {
                        name: "buildbox".into(),
                        target: Some("can@buildbox".into()),
                        session: "default".into(),
                        enabled: true,
                        hue: Some(0),
                    }],
                    ctx.descriptors,
                    ctx.mirrors,
                ),
            ));

            // Keys go to the modal, not the session behind it.
            handle_key(key(KeyCode::Down), ctx);
            assert!(ctx.chrome.remote_list.is_some());
            assert!(!ctx.app.should_quit);

            // Enter hands the selected entry to the existing field dialog
            // rather than rebuilding field editing in the list.
            handle_key(key(KeyCode::Enter), ctx);
            let dialog = ctx.chrome.remote_edit.as_ref().expect("field dialog");
            assert_eq!(dialog.original_name.as_deref(), Some("buildbox"));
            ctx.chrome.remote_edit = None;

            handle_key(key(KeyCode::Esc), ctx);
            assert!(ctx.chrome.remote_list.is_none(), "escape closes it");
            assert!(!ctx.app.should_quit, "and does not quit the client");
        });
    }

    #[tokio::test]
    async fn right_clicking_a_chip_does_nothing() {
        with_test_ctx(three_descriptors(), |ctx| {
            // Right-click used to jump straight into the edit dialog - the
            // one place in herdr where it skipped a menu. Editing lives in
            // the remotes list modal now, so right-click means "open a menu"
            // everywhere without exception, and a chip has none.
            let before = ctx.chrome.selection.clone();

            handle_chip_click(0, crossterm::event::MouseButton::Right, ctx);
            handle_chip_click(2, crossterm::event::MouseButton::Right, ctx);

            assert!(ctx.chrome.remote_edit.is_none(), "no dialog is opened");
            assert!(ctx.chrome.remote_start.is_none());
            assert_eq!(ctx.chrome.selection, before, "and nothing is filtered");
            assert!(!ctx.app.should_quit);
        });
    }

    #[tokio::test]
    async fn left_clicking_a_chip_still_filters_and_solos() {
        with_test_ctx(three_descriptors(), |ctx| {
            // The menus change nothing about how the fleet is filtered.
            handle_chip_click(2, crossterm::event::MouseButton::Left, ctx);
            assert!(
                !ctx.chrome.selection.is_in_view(2),
                "left-click still toggles a remote out of view"
            );

            handle_chip_click(2, crossterm::event::MouseButton::Left, ctx);
            assert!(ctx.chrome.selection.is_in_view(2), "and back in");
        });
    }

    #[tokio::test]
    async fn reconcile_replaces_identity_changes_and_drops_removed_remotes() {
        with_test_ctx(three_descriptors(), |ctx| {
            ctx.links.insert(1, Link::Incompatible);
            ctx.links.insert(2, Link::Incompatible);
            ctx.chrome.window_titles.insert(2, "gpu title".into());
            ctx.chrome.selection.solo(2, ctx.descriptors);

            // gpu-01 is removed; buildbox changes target (identity change).
            let entries = vec![
                crate::fleet::config::RemoteEntry {
                    name: "local".into(),
                    target: None,
                    session: "default".into(),
                    enabled: true,
                    hue: None,
                },
                crate::fleet::config::RemoteEntry {
                    name: "buildbox".into(),
                    target: Some("can@buildbox2.example".into()),
                    session: "default".into(),
                    enabled: true,
                    hue: None,
                },
            ];
            reconcile_fleet(&entries, ctx);

            assert_eq!(ctx.descriptors.len(), 2);
            assert!(ctx.mirrors.get(2).is_none(), "removed remote is gone");
            assert!(!ctx.chrome.window_titles.contains_key(&2));
            assert!(
                matches!(ctx.links.get(&1), Some(Link::Down { .. })),
                "identity change reconnects from scratch"
            );
            assert!(
                ctx.chrome.selection.is_in_view(0) || ctx.chrome.selection.is_in_view(1),
                "the view never ends up empty after a reconcile"
            );
        });
    }

    #[test]
    fn hello_rejection_keeps_the_servers_own_remedy() {
        // An older *server* must never be reported as "upgrade the client".
        let rejection = serde_json::json!({
            "id": "pure:hello:0",
            "error": {
                "code": crate::protocol::framed::PROTOCOL_OUT_OF_WINDOW_CODE,
                "message": "client minimum protocol 2 is newer than this server's protocol 1",
                "data": { "remedy": "upgrade_server" },
            },
        });
        match interpret_hello_answer(&rejection) {
            Err(HelloRejection::Incompatible { remedy, message }) => {
                assert_eq!(remedy, HelloRemedy::UpgradeServer);
                assert!(message.contains("protocol 1"), "{message}");
            }
            _ => panic!("expected an incompatible rejection"),
        }

        // A welcome without the catalog capability is equally terminal, and
        // equally an upgrade-the-server situation.
        let welcome = serde_json::json!({
            "id": "pure:hello:0",
            "result": {
                "type": "session.welcome",
                "protocol": 1,
                "min_protocol": 1,
                "capabilities": ["pane-stream"],
                "server_version": "0.9.0",
            },
        });
        match interpret_hello_answer(&welcome) {
            Err(HelloRejection::Incompatible { remedy, message }) => {
                assert_eq!(remedy, HelloRemedy::UpgradeServer);
                assert!(message.contains("catalog"), "{message}");
            }
            _ => panic!("expected an incompatible rejection"),
        }
    }

    #[tokio::test]
    async fn a_stopped_remote_dims_and_opens_the_start_prompt_instead_of_retrying() {
        with_test_ctx(three_descriptors(), |ctx| {
            ctx.links.insert(1, Link::Pending { generation: 1 });
            handle_established(
                1,
                1,
                RemoteEstablished::Stopped(crate::fleet::connection::stopped_status_line(
                    "buildbox",
                )),
                ctx,
            );

            // Terminal link: the retry ladder must not run, because no
            // number of reconnects can start a daemon on that host. Its own
            // variant, not Incompatible's: the two dead ends have different
            // fixes and must stay tellable apart.
            assert!(
                matches!(ctx.links.get(&1), Some(Link::Stopped)),
                "a stopped remote must not be scheduled for retry"
            );
            let connection = ctx
                .mirrors
                .get(1)
                .map(|mirror| mirror.connection.clone())
                .expect("mirror");
            assert!(connection.is_stopped(), "{connection:?}");
            assert!(!connection.may_retry(), "stopped is terminal");

            // The prompt names the remote and carries the actionable line.
            let prompt = ctx.chrome.remote_start.as_ref().expect("start prompt");
            assert_eq!(prompt.remote, 1);
            assert_eq!(prompt.name, "buildbox");
            assert!(
                prompt.status.contains("remote start buildbox"),
                "{prompt:?}"
            );
            assert_eq!(prompt.error, None);

            // Declining leaves it stopped rather than starting anything.
            ctx.chrome.remote_start = None;
            assert!(ctx
                .mirrors
                .get(1)
                .is_some_and(|mirror| mirror.connection.is_stopped()));
            assert!(matches!(ctx.links.get(&1), Some(Link::Stopped)));

            // ...but the dimmed chip re-offers it, so declining once does not
            // strand the remote for the rest of the session.
            handle_chip_click(1, crossterm::event::MouseButton::Left, ctx);
            let reoffered = ctx.chrome.remote_start.as_ref().expect("re-offered");
            assert_eq!(reoffered.remote, 1);
            assert!(!reoffered.starting);
            assert!(reoffered.status.contains("remote start buildbox"));
        });
    }

    fn server_stopping_frame(reason: &str) -> Frame {
        let payload = serde_json::to_vec(&serde_json::json!({
            "event": SERVER_STOPPING_EVENT,
            "data": { "reason": reason },
        }))
        .expect("encode");
        Frame {
            frame_type: FrameType::Control,
            stream_id: CONTROL_STREAM_ID,
            payload,
        }
    }

    #[tokio::test]
    async fn an_announced_server_stop_parks_the_remote_without_running_the_ladder() {
        with_test_ctx(three_descriptors(), |ctx| {
            ctx.links.insert(1, Link::Pending { generation: 1 });

            handle_server_frame(
                1,
                server_stopping_frame(crate::protocol::framed::SERVER_STOPPING_REASON_EMPTY),
                ctx,
            );

            // Straight to Stopped: a server that said it is going away is not
            // a transport that might come back.
            assert!(
                matches!(ctx.links.get(&1), Some(Link::Stopped)),
                "an announced stop must not be scheduled for retry"
            );
            let connection = ctx
                .mirrors
                .get(1)
                .map(|mirror| mirror.connection.clone())
                .expect("mirror");
            assert!(connection.is_stopped(), "{connection:?}");
            assert!(!connection.may_retry());

            // The status line is written from the reason, not from
            // connect-failure text.
            let prompt = ctx.chrome.remote_start.as_ref().expect("start prompt");
            assert_eq!(prompt.name, "buildbox");
            assert!(prompt.status.contains("no panes left"), "{prompt:?}");

            // The EOF that follows the announcement must not downgrade the
            // deliberate stop back into the offline retry ladder.
            drop_link(1, ctx, "connection closed");
            assert!(
                matches!(ctx.links.get(&1), Some(Link::Stopped)),
                "the EOF after an announced stop must not overwrite it"
            );
            assert!(ctx
                .mirrors
                .get(1)
                .is_some_and(|mirror| mirror.connection.is_stopped()));
        });
    }

    #[tokio::test]
    async fn an_unrecognised_stop_reason_is_treated_as_stopped_by_request() {
        // Version skew must degrade gracefully: a newer server can add a
        // reason without stranding this client retrying a server that is gone.
        with_test_ctx(three_descriptors(), |ctx| {
            ctx.links.insert(1, Link::Pending { generation: 1 });

            handle_server_frame(1, server_stopping_frame("hibernated"), ctx);

            assert!(matches!(ctx.links.get(&1), Some(Link::Stopped)));
            let prompt = ctx.chrome.remote_start.as_ref().expect("start prompt");
            assert!(prompt.status.contains("stopped by request"), "{prompt:?}");
        });
    }

    #[tokio::test]
    async fn a_plain_disconnect_still_walks_the_retry_ladder() {
        // The contrast: an unannounced drop is exactly what the ladder is
        // for, so the new event must not make every disconnect terminal.
        with_test_ctx(three_descriptors(), |ctx| {
            ctx.links.insert(1, Link::Pending { generation: 1 });

            drop_link(1, ctx, "connection closed");

            assert!(
                matches!(ctx.links.get(&1), Some(Link::Down { .. })),
                "an unannounced disconnect keeps retrying"
            );
            assert!(ctx
                .mirrors
                .get(1)
                .is_some_and(|mirror| mirror.connection.may_retry()));
        });
    }

    #[tokio::test]
    async fn the_start_prompt_captures_input_ahead_of_the_edit_dialog() {
        with_test_ctx(three_descriptors(), |ctx| {
            ctx.chrome.remote_start = Some(super::super::remote_start::RemoteStartPrompt {
                remote: 1,
                name: "buildbox".into(),
                status: "no server".into(),
                error: None,
                starting: false,
            });
            // A chip right-click would normally open the edit dialog; while
            // the prompt is open `handle_mouse` routes to the prompt alone,
            // so the click never reaches the chip strip.
            handle_mouse(
                MouseEvent {
                    kind: MouseEventKind::Down(crossterm::event::MouseButton::Right),
                    column: 0,
                    row: 0,
                    modifiers: KeyModifiers::empty(),
                },
                ctx,
            );
            assert!(
                ctx.chrome.remote_edit.is_none(),
                "the start prompt swallows the mouse while open"
            );
            assert!(ctx.chrome.remote_start.is_some(), "and stays open");

            // Keys reach the prompt ahead of the edit dialog too: an open
            // edit dialog must not steal the confirmation's Esc.
            ctx.chrome.remote_edit = Some(super::super::remote_edit::RemoteEditState::add());
            handle_key(key(KeyCode::Esc), ctx);
            assert!(
                ctx.chrome.remote_start.is_none(),
                "Esc dismissed the prompt"
            );
            assert!(
                ctx.chrome.remote_edit.is_some(),
                "and left the edit dialog untouched"
            );
        });
    }

    #[tokio::test]
    async fn established_outcomes_from_stale_generations_are_dropped() {
        with_test_ctx(three_descriptors(), |ctx| {
            ctx.links.insert(1, Link::Pending { generation: 2 });
            handle_established(1, 1, RemoteEstablished::Failed("stale thread".into()), ctx);
            assert!(
                matches!(ctx.links.get(&1), Some(Link::Pending { generation: 2 })),
                "a superseded connect thread must not disturb the live link"
            );

            handle_established(
                1,
                2,
                RemoteEstablished::Incompatible {
                    remedy: HelloRemedy::UpgradeServer,
                    message: "windows do not overlap".into(),
                },
                ctx,
            );
            assert!(matches!(ctx.links.get(&1), Some(Link::Incompatible)));
            assert!(matches!(
                ctx.mirrors.get(1).map(|mirror| &mirror.connection),
                Some(super::super::ClientConnectionState::Incompatible {
                    remedy: HelloRemedy::UpgradeServer,
                    ..
                })
            ));
            // The greyed-out remote names itself and the exact fix.
            let status = ctx
                .chrome
                .connection_status
                .clone()
                .expect("an incompatible remote reports a remedy");
            assert!(status.contains("buildbox"), "{status}");
            assert!(status.contains("herdr remote upgrade buildbox"), "{status}");
        });
    }

    /// A test session over a threaded writer whose receive half is kept
    /// alive by the returned receiver.
    fn threaded_session(generation: u64, bound: usize) -> (Session, mpsc::Receiver<Vec<u8>>) {
        let (tx, rx) = mpsc::sync_channel::<Vec<u8>>(bound);
        (
            fresh_session(SessionWriter::Threaded(tx), None, generation),
            rx,
        )
    }

    /// The local runtime is one fleet member among others: its connect
    /// must run off the run-loop thread like every ssh remote's, so its
    /// chip can show (and spin through) `Connecting`, and so a local
    /// server that accepts the socket but never answers `session.hello`
    /// cannot block the loop - and with it every other remote - for the
    /// hello timeout.
    #[tokio::test]
    async fn local_connects_off_thread_so_its_chip_can_show_connecting() {
        // The connect thread must never reach a real dev server.
        std::env::set_var(
            crate::api::SOCKET_PATH_ENV_VAR,
            "/nonexistent/herdr-pure-client-test.sock",
        );
        with_test_ctx(three_descriptors(), |ctx| {
            let descriptor = ctx.descriptors[0].clone();
            assert_eq!(descriptor.target, None, "remote #0 is the local runtime");

            let link = establish_for(
                &descriptor,
                ctx.mirrors,
                ctx.ui,
                ctx.event_tx,
                ctx.should_quit,
            );
            assert!(
                matches!(link, Link::Pending { .. }),
                "local connects off the run-loop thread, like any remote"
            );
            let chips = super::super::fleet_view::remote_chip_states(
                ctx.mirrors,
                ctx.descriptors,
                &ctx.chrome.selection,
            );
            assert_eq!(
                chips[0].connection,
                crate::app::state::RemoteChipConnection::Connecting,
                "the local chip reports connecting like any remote"
            );
        });
    }

    /// The local session lands through the same `Established` path as any
    /// remote's: link up, mirror connected and resyncing, snapshot
    /// requested.
    #[tokio::test]
    async fn a_local_session_opens_its_link_and_resyncs_like_a_remote() {
        with_test_ctx(three_descriptors(), |ctx| {
            ctx.links
                .insert(LOCAL_REMOTE_INDEX, Link::Pending { generation: 4 });
            let (session, rx) = threaded_session(4, 8);
            let welcome = SessionWelcome {
                protocol: crate::protocol::PROTOCOL_VERSION,
                min_protocol: 1,
                capabilities: vec![CAPABILITY_CATALOG.to_owned()],
                server_version: env!("CARGO_PKG_VERSION").to_owned(),
            };

            handle_established(
                LOCAL_REMOTE_INDEX,
                4,
                RemoteEstablished::LocalConnected {
                    welcome,
                    session: Box::new(session),
                },
                ctx,
            );

            assert!(matches!(
                ctx.links.get(&LOCAL_REMOTE_INDEX),
                Some(Link::Up(_))
            ));
            let chips = super::super::fleet_view::remote_chip_states(
                ctx.mirrors,
                ctx.descriptors,
                &ctx.chrome.selection,
            );
            assert_eq!(
                chips[0].connection,
                crate::app::state::RemoteChipConnection::Connected
            );
            assert!(rx.try_recv().is_ok(), "the snapshot request went out");
        });
    }

    /// Composes the fleet and lays out a desktop frame, so the chip strip
    /// geometry the status-line fallback reads is the real one.
    fn render_chip_strip(ctx: &mut LoopCtx<'_>, width: u16, height: u16) {
        super::super::compose::compose_fleet_into(
            ctx.mirrors,
            ctx.descriptors,
            ctx.chrome,
            ctx.ids,
            ctx.app,
        );
        crate::ui::compute_view(ctx.app, ratatui::layout::Rect::new(0, 0, width, height));
    }

    /// A fleet reads every member's transport state off its chip dot. The
    /// local runtime is no exception: dropping its link must not raise a
    /// status line no other remote gets.
    #[tokio::test]
    async fn a_fleet_reports_local_transport_loss_in_the_chip_not_a_toast() {
        with_test_ctx(three_descriptors(), |ctx| {
            let (session, _rx) = threaded_session(1, 8);
            ctx.links
                .insert(LOCAL_REMOTE_INDEX, Link::Up(Box::new(session)));
            render_chip_strip(ctx, 106, 30);
            assert!(
                ctx.app.view.remote_chip_strip_rect.height > 0,
                "the strip is on screen to carry the dot"
            );

            drop_link(LOCAL_REMOTE_INDEX, ctx, "connection closed");

            assert!(matches!(
                ctx.links.get(&LOCAL_REMOTE_INDEX),
                Some(Link::Down { .. })
            ));
            let chips = super::super::fleet_view::remote_chip_states(
                ctx.mirrors,
                ctx.descriptors,
                &ctx.chrome.selection,
            );
            assert_eq!(
                chips[0].connection,
                crate::app::state::RemoteChipConnection::Offline,
                "the local chip goes hollow like any remote's"
            );
            assert_eq!(
                ctx.chrome.connection_status, None,
                "in a fleet the chip carries connection state, not a toast"
            );
        });
    }

    /// The strip is composed even for a fleet of one, so its dot carries
    /// the connection state and the status line stays quiet - but a strip
    /// laid out away (collapsed sidebar, mobile, too few rows) leaves the
    /// status line as the only channel, and it must still speak there.
    #[tokio::test]
    async fn a_single_remote_reports_transport_loss_wherever_it_can_be_seen() {
        with_test_ctx(vec![RemoteDescriptor::local()], |ctx| {
            let (session, _rx) = threaded_session(1, 8);
            ctx.links
                .insert(LOCAL_REMOTE_INDEX, Link::Up(Box::new(session)));
            render_chip_strip(ctx, 106, 30);
            assert!(
                ctx.app.view.remote_chip_strip_rect.height > 0,
                "a fleet of one still composes its strip"
            );

            drop_link(LOCAL_REMOTE_INDEX, ctx, "connection closed");
            assert_eq!(
                ctx.chrome.connection_status, None,
                "with the strip on screen the chip dot is the channel"
            );

            // Lay the strip away: now the status line is all that is left.
            ctx.app.view.remote_chip_strip_rect = ratatui::layout::Rect::default();
            ctx.links.insert(
                LOCAL_REMOTE_INDEX,
                Link::Up(Box::new(threaded_session(1, 8).0)),
            );
            drop_link(LOCAL_REMOTE_INDEX, ctx, "connection closed");
            assert_eq!(
                ctx.chrome.connection_status.as_deref(),
                Some("connection closed; reconnecting"),
                "with no strip on screen the status line is the only channel"
            );
        });
    }

    /// A configured fleet is not the same fact as a visible chip strip: a
    /// collapsed sidebar and the mobile layout both lay the strip away, and
    /// with it the only place local's transport state was being reported.
    /// The status line has to come back whenever the dot is not on screen.
    #[tokio::test]
    async fn a_hidden_chip_strip_sends_local_transport_loss_back_to_the_status_line() {
        // Sidebar collapsed: chips are composed, the strip is not laid out.
        with_test_ctx(three_descriptors(), |ctx| {
            let (session, _rx) = threaded_session(1, 8);
            ctx.links
                .insert(LOCAL_REMOTE_INDEX, Link::Up(Box::new(session)));
            ctx.chrome.sidebar_collapsed = true;
            render_chip_strip(ctx, 106, 30);
            assert!(
                !ctx.app.remote_chips.is_empty(),
                "the fleet still has chips"
            );
            assert_eq!(ctx.app.view.remote_chip_strip_rect.height, 0);

            drop_link(LOCAL_REMOTE_INDEX, ctx, "connection closed");

            assert_eq!(
                ctx.chrome.connection_status.as_deref(),
                Some("connection closed; reconnecting"),
                "a collapsed sidebar hides the dot, so the toast is the only channel"
            );
        });

        // Mobile: no strip is rendered at any width.
        with_test_ctx(three_descriptors(), |ctx| {
            let (session, _rx) = threaded_session(1, 8);
            ctx.links
                .insert(LOCAL_REMOTE_INDEX, Link::Up(Box::new(session)));
            render_chip_strip(ctx, 40, 24);
            assert_eq!(ctx.app.view.layout, crate::app::state::ViewLayout::Mobile);
            assert_eq!(ctx.app.view.remote_chip_strip_rect.height, 0);

            drop_link(LOCAL_REMOTE_INDEX, ctx, "connection closed");

            assert_eq!(
                ctx.chrome.connection_status.as_deref(),
                Some("connection closed; reconnecting"),
                "the mobile layout renders no chips, so the toast is the only channel"
            );
        });
    }

    /// The local connect runs off-thread now, so the first frame is drawn
    /// before the socket answers. Wherever a chip is on screen it spins and
    /// says so; where no strip is laid out - the mobile layout, a collapsed
    /// sidebar - the client must say it in the status line instead, or the
    /// empty view looks live while every intent is dropped.
    #[tokio::test]
    async fn the_client_says_the_local_handshake_is_in_flight_when_no_chip_can() {
        with_test_ctx(vec![RemoteDescriptor::local()], |ctx| {
            // No strip laid out: the status line is the only channel.
            ctx.app.view.remote_chip_strip_rect = ratatui::layout::Rect::default();
            note_local_handshake(
                &ctx.descriptors[0].clone(),
                ctx.descriptors,
                ctx.app,
                ctx.chrome,
            );
            assert_eq!(
                ctx.chrome.connection_status.as_deref(),
                Some("connecting to the local server")
            );
        });
        with_test_ctx(vec![RemoteDescriptor::local()], |ctx| {
            // Strip on screen: its dot spins, so the status line stays quiet.
            render_chip_strip(ctx, 106, 30);
            assert!(ctx.app.view.remote_chip_strip_rect.height > 0);
            note_local_handshake(
                &ctx.descriptors[0].clone(),
                ctx.descriptors,
                ctx.app,
                ctx.chrome,
            );
            assert_eq!(ctx.chrome.connection_status, None);
        });
        with_test_ctx(three_descriptors(), |ctx| {
            render_chip_strip(ctx, 106, 30);
            note_local_handshake(
                &ctx.descriptors[0].clone(),
                ctx.descriptors,
                ctx.app,
                ctx.chrome,
            );
            assert_eq!(
                ctx.chrome.connection_status, None,
                "a fleet spins the local chip instead"
            );
        });
    }

    #[tokio::test]
    async fn stale_reader_events_cannot_touch_a_successor_link() {
        with_test_ctx(three_descriptors(), |ctx| {
            let (mut session, _rx) = threaded_session(7, 8);
            let backdated = Instant::now() - Duration::from_secs(1);
            session.last_inbound = backdated;
            ctx.links.insert(1, Link::Up(Box::new(session)));
            let mut framer = crate::raw_input::RawInputFramer::for_host_input();
            let last_inbound = |links: &Links| match links.get(&1) {
                Some(Link::Up(session)) => Some(session.last_inbound),
                _ => None,
            };

            // A disconnect from a torn-down reader (older generation) must
            // not demote the successor's link.
            ctx.handle_event(LoopEvent::Disconnected(1, 6), &mut framer);
            assert!(matches!(ctx.links.get(&1), Some(Link::Up(_))));

            // A stale reader's frame never reaches the successor's session.
            let frame = Frame {
                frame_type: FrameType::Data,
                stream_id: 9,
                payload: b"x".to_vec(),
            };
            ctx.handle_event(LoopEvent::Frame(1, 6, frame), &mut framer);
            assert_eq!(last_inbound(ctx.links), Some(backdated));

            // The current generation's frame counts as liveness.
            let frame = Frame {
                frame_type: FrameType::Data,
                stream_id: 9,
                payload: b"x".to_vec(),
            };
            ctx.handle_event(LoopEvent::Frame(1, 7, frame), &mut framer);
            assert!(last_inbound(ctx.links).expect("link is up") > backdated);

            // The current generation's disconnect still tears the link down.
            ctx.handle_event(LoopEvent::Disconnected(1, 7), &mut framer);
            assert!(matches!(ctx.links.get(&1), Some(Link::Down { .. })));
        });
    }

    #[tokio::test]
    async fn silent_remotes_fail_the_heartbeat_and_pings_keep_live_ones_honest() {
        with_test_ctx(three_descriptors(), |ctx| {
            // Remote 1: silent past the pong timeout — the link must drop.
            let (mut session, _rx1) = threaded_session(1, 8);
            session.last_inbound = Instant::now() - REMOTE_PONG_TIMEOUT;
            ctx.links.insert(1, Link::Up(Box::new(session)));

            // Remote 2: alive but idle with a due ping — a ping must go out.
            let (mut session, rx2) = threaded_session(2, 8);
            session.next_ping = Instant::now();
            ctx.links.insert(2, Link::Up(Box::new(session)));

            assert!(service_remote_heartbeats(ctx), "a dead link changed state");
            assert!(matches!(ctx.links.get(&1), Some(Link::Down { .. })));
            assert!(matches!(ctx.links.get(&2), Some(Link::Up(_))));
            assert!(rx2.try_recv().is_ok(), "heartbeat ping was written");
        });
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn the_ephemeral_fleet_of_one_at_index_zero_gets_heartbeats() {
        // `--remote` puts an ssh transport at index 0: the local-socket
        // heartbeat exemption must follow the descriptor, not the index.
        let descriptor =
            RemoteDescriptor::ephemeral("can@gpu-1.example", "can@gpu-1.example", "default", None);
        with_test_ctx(vec![descriptor], |ctx| {
            let (mut session, _rx) = threaded_session(1, 8);
            session.last_inbound = Instant::now() - REMOTE_PONG_TIMEOUT;
            ctx.links
                .insert(LOCAL_REMOTE_INDEX, Link::Up(Box::new(session)));

            assert!(
                service_remote_heartbeats(ctx),
                "a dead ephemeral transport must drop"
            );
            assert!(matches!(
                ctx.links.get(&LOCAL_REMOTE_INDEX),
                Some(Link::Down { .. })
            ));
        });
    }

    /// A wedged local server - socket open, nothing answered - has to go
    /// hollow on the same clock as the ssh remotes beside it, now that its
    /// chip carries connection state like theirs. Without a heartbeat only
    /// a clean EOF is ever detected, so a stopped-but-not-closed server
    /// would keep a filled dot forever while its keystrokes vanish.
    #[tokio::test]
    async fn a_silent_local_socket_fails_the_heartbeat_like_any_remote() {
        with_test_ctx(three_descriptors(), |ctx| {
            let (mut session, rx) = threaded_session(1, 8);
            session.next_ping = Instant::now();
            ctx.links
                .insert(LOCAL_REMOTE_INDEX, Link::Up(Box::new(session)));

            // Alive but idle: local is pinged like every other member.
            assert!(!service_remote_heartbeats(ctx));
            assert!(rx.try_recv().is_ok(), "the local link is pinged too");

            // Silent past the pong timeout: the link drops and the chip
            // goes hollow.
            if let Some(Link::Up(session)) = ctx.links.get_mut(&LOCAL_REMOTE_INDEX) {
                session.last_inbound = Instant::now() - REMOTE_PONG_TIMEOUT;
            }
            assert!(service_remote_heartbeats(ctx), "a dead link changed state");
            assert!(matches!(
                ctx.links.get(&LOCAL_REMOTE_INDEX),
                Some(Link::Down { .. })
            ));
            let chips = super::super::fleet_view::remote_chip_states(
                ctx.mirrors,
                ctx.descriptors,
                &ctx.chrome.selection,
            );
            assert_eq!(
                chips[0].connection,
                crate::app::state::RemoteChipConnection::Offline
            );
        });
    }

    #[test]
    fn a_full_remote_write_queue_fails_instead_of_blocking_the_loop() {
        let (mut session, _rx) = threaded_session(1, 1);
        assert!(session
            .send_control(&serde_json::json!({"id": "a"}))
            .is_ok());
        let err = session
            .send_control(&serde_json::json!({"id": "b"}))
            .expect_err("second frame overflows the bound-1 queue");
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    }

    #[tokio::test]
    async fn ui_ticks_expire_the_status_flash_and_animate_the_spinner() {
        with_test_ctx(three_descriptors(), |ctx| {
            ctx.ui.status_flash = Some((
                "at least one remote stays in view".to_owned(),
                Instant::now() - STATUS_FLASH_TTL - Duration::from_millis(50),
            ));
            assert!(service_ui_ticks(ctx), "expiry forces a recompose");
            assert!(ctx.ui.status_flash.is_none());

            // No remote connecting: the spinner does not tick.
            assert!(!service_ui_ticks(ctx));

            // A connecting remote advances the spinner one visual frame.
            if let Some(mirror) = ctx.mirrors.get_mut(1) {
                mirror.connection.connect_started();
            }
            let before = ctx.app.spinner_tick;
            assert!(service_ui_ticks(ctx), "spinner tick forces a redraw");
            assert_ne!(ctx.app.spinner_tick, before);
        });
    }

    #[test]
    fn pure_client_default_resolution_honors_explicit_setting() {
        // nextest runs each test in its own process, so env mutation here
        // cannot race other tests.
        std::env::remove_var("HERDR_PURE_CLIENT");
        let mut config = crate::config::Config::default();
        assert_eq!(
            config.experimental.pure_client, None,
            "unset by default so the release default applies"
        );
        assert_eq!(pure_client_enabled(&config), PURE_CLIENT_DEFAULT);
        assert!(
            pure_client_enabled(&config),
            "the pure client is the default run path"
        );

        // An explicit user setting always wins over the release default —
        // in both directions, so opt-ins and opt-outs survive the flip.
        config.experimental.pure_client = Some(true);
        assert!(pure_client_enabled(&config));
        config.experimental.pure_client = Some(false);
        assert!(!pure_client_enabled(&config));

        // The env override beats even explicit settings (test/dev hook).
        std::env::set_var("HERDR_PURE_CLIENT", "1");
        assert!(pure_client_enabled(&config));
        config.experimental.pure_client = Some(true);
        std::env::set_var("HERDR_PURE_CLIENT", "0");
        assert!(!pure_client_enabled(&config));
        std::env::remove_var("HERDR_PURE_CLIENT");
    }

    /// A mirror whose catalog holds one focused pane (`p_1_1`) served by a
    /// replica seeded with `screen`.
    fn mirror_with_replica(screen: &str) -> super::super::RemoteMirror {
        let mut mirror = mirror_with_catalog();
        let replica =
            crate::terminal::replica::PaneReplica::open(screen, 10, None, 80, 24, 64 * 1024)
                .expect("replica opens");
        mirror.stream_opened("p_1_1", 3, replica);
        mirror
    }

    /// Same, but the replica knows there is older history behind an opaque
    /// cursor and has room in its budget to fetch it.
    fn mirror_with_history(screen: &str, cursor: &str) -> super::super::RemoteMirror {
        let mut mirror = mirror_with_catalog();
        let replica = crate::terminal::replica::PaneReplica::open(
            screen,
            10,
            Some(cursor.to_owned()),
            80,
            24,
            1024 * 1024,
        )
        .expect("replica opens");
        mirror.stream_opened("p_1_1", 3, replica);
        mirror
    }

    /// A mirror holding the same catalog with no pane stream open yet.
    fn mirror_with_catalog() -> super::super::RemoteMirror {
        mirror_with_panes(1)
    }

    /// The same catalog split across `panes` panes in one tab, the first of
    /// which is the focused one.
    fn mirror_with_panes(panes: usize) -> super::super::RemoteMirror {
        let mut mirror = super::super::RemoteMirror::test_new();
        let snapshot: crate::api::schema::session::SessionSnapshot =
            serde_json::from_value(serde_json::json!({
                "version": "test",
                "protocol": 3,
                "focused_workspace_id": "ws_1",
                "focused_tab_id": "t_1_1",
                "focused_pane_id": "p_1_1",
                "workspaces": [{
                    "workspace_id": "ws_1", "number": 1, "label": "repo",
                    "focused": true, "pane_count": panes, "tab_count": 1,
                    "active_tab_id": "t_1_1", "agent_status": "idle"
                }],
                "tabs": [{
                    "tab_id": "t_1_1", "workspace_id": "ws_1", "number": 1,
                    "label": "shell", "focused": true, "pane_count": panes,
                    "agent_status": "idle"
                }],
                "panes": (1..=panes).map(|number| serde_json::json!({
                    "pane_id": format!("p_1_{number}"),
                    "terminal_id": format!("term_{number}"),
                    "workspace_id": "ws_1", "tab_id": "t_1_1",
                    // The catalog's pane flags are focus authority.
                    "focused": number == 1,
                    "agent_status": "idle", "revision": 1
                })).collect::<Vec<_>>(),
                "layouts": [],
                "agents": []
            }))
            .expect("snapshot deserializes");
        mirror.catalog.resync(&snapshot, 1);
        mirror
    }

    /// More rows than a 24-row replica can show, so its pane has scrollback
    /// to page through and renders a scrollbar.
    fn tall_screen(rows: usize) -> String {
        (0..rows).map(|row| format!("line {row}\r\n")).collect()
    }

    /// The pure client's view of a mirror with one pane, laid out.
    fn compose_and_lay_out(ctx: &mut LoopCtx<'_>) {
        compose_into(ctx.mirrors.local(), ctx.chrome, ctx.ids, ctx.app);
        ctx.app.mode = Mode::Terminal;
        let source = MirrorPaneSource::new(ctx.mirrors.local());
        let _requests = crate::ui::compute_view_with_content(
            ctx.app,
            &source,
            ratatui::layout::Rect::new(0, 0, 106, 26),
        );
    }

    fn mouse_at(
        kind: crossterm::event::MouseEventKind,
        column: u16,
        row: u16,
    ) -> crossterm::event::MouseEvent {
        crossterm::event::MouseEvent {
            kind,
            column,
            row,
            modifiers: KeyModifiers::empty(),
        }
    }

    /// The server's `stream.opened` answer to our own open request.
    fn stream_opened_frame(request_id: &str, pane_id: &str, stream_id: u32) -> Frame {
        let payload = serde_json::json!({
            "id": request_id,
            "result": {
                "type": "pane_stream_opened",
                "stream": {
                    "pane_id": pane_id,
                    "stream_id": stream_id,
                    "sequence": 0,
                    "snapshot": "hello",
                    "history_cursor": "cursor-1",
                    "cols": 80,
                    "rows": 24,
                },
            },
        });
        Frame {
            frame_type: FrameType::Control,
            stream_id: 0,
            payload: serde_json::to_vec(&payload).expect("payload serializes"),
        }
    }

    /// A connected [`Session`] whose peer end the test reads frames from.
    fn session_pair() -> (Session, crate::ipc::LocalStream) {
        use interprocess::local_socket::traits::Listener as _;
        static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "herdr-pure-run-test-{}-{}.sock",
            std::process::id(),
            COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        ));
        let _ = std::fs::remove_file(&path);
        let listener = crate::ipc::bind_local_listener(&path).expect("bind test socket");
        let client = crate::ipc::connect_local_stream(&path).expect("connect test socket");
        let server = listener.accept().expect("accept test socket");
        let _ = std::fs::remove_file(&path);
        (
            fresh_session(SessionWriter::Direct(Box::new(client)), None, 0),
            server,
        )
    }

    /// Reads one control frame off the test peer, or None on timeout.
    fn try_read_control(server: &mut crate::ipc::LocalStream) -> Option<serde_json::Value> {
        let _ = server.set_recv_timeout(Some(Duration::from_millis(200)));
        match read_frame(server) {
            Ok(frame) => serde_json::from_slice(&frame.payload).ok(),
            Err(_) => None,
        }
    }

    #[tokio::test]
    async fn attaching_warms_one_history_page_for_the_focused_pane_only() {
        with_test_ctx(vec![RemoteDescriptor::local()], |ctx| {
            // Two panes in one tab, p_1_1 focused. Both stream; only the
            // focused one may pay for history at attach.
            *ctx.mirrors.local_mut() = mirror_with_panes(2);
            compose_into(ctx.mirrors.local(), ctx.chrome, ctx.ids, ctx.app);
            assert_eq!(
                ctx.app.workspaces[0]
                    .focused_pane_id()
                    .and_then(|pane| ctx.ids.public_pane_id(pane))
                    .map(|(_, public)| public),
                Some("p_1_1"),
                "the composed view focuses the pane the catalog flagged"
            );

            let (mut session, mut server) = session_pair();
            for (id, pane_id) in [("o1", "p_1_2"), ("o2", "p_1_1")] {
                session.pending.insert(
                    id.to_owned(),
                    Pending::StreamOpen {
                        pane_id: pane_id.to_owned(),
                        mode: StreamMode::Write,
                    },
                );
            }
            ctx.links
                .insert(LOCAL_REMOTE_INDEX, Link::Up(Box::new(session)));

            // The unfocused pane attaches first and pays nothing.
            handle_server_frame(
                LOCAL_REMOTE_INDEX,
                stream_opened_frame("o1", "p_1_2", 4),
                ctx,
            );
            assert!(
                try_read_control(&mut server).is_none(),
                "a pane nobody is looking at must not fetch history at attach"
            );

            // The focused pane gets one eager page, so its first scroll tick
            // already has history behind it.
            handle_server_frame(
                LOCAL_REMOTE_INDEX,
                stream_opened_frame("o2", "p_1_1", 3),
                ctx,
            );
            let request = try_read_control(&mut server).expect("attach warms the focused pane");
            assert_eq!(
                request["method"],
                crate::protocol::framed::STREAM_HISTORY_METHOD
            );
            assert_eq!(request["params"]["cursor"], "cursor-1");
        });
    }

    #[tokio::test]
    async fn clicking_the_pane_scrollbar_track_pages_in_more_history() {
        use crossterm::event::{MouseButton, MouseEventKind};

        with_test_ctx(vec![RemoteDescriptor::local()], |ctx| {
            *ctx.mirrors.local_mut() = mirror_with_history(&tall_screen(60), "cursor-1");
            compose_and_lay_out(ctx);
            let track = ctx
                .app
                .view
                .pane_infos
                .first()
                .and_then(crate::ui::pane_scrollbar_rect)
                .expect("a pane with scrollback shows a scrollbar");

            let (session, mut server) = session_pair();
            ctx.links
                .insert(LOCAL_REMOTE_INDEX, Link::Up(Box::new(session)));

            // Clicking the top of the track jumps to the top of what is
            // loaded, which is exactly where older history is wanted.
            handle_mouse(
                mouse_at(MouseEventKind::Down(MouseButton::Left), track.x, track.y),
                ctx,
            );

            let request = try_read_control(&mut server).expect("the click pages history in");
            assert_eq!(
                request["method"],
                crate::protocol::framed::STREAM_HISTORY_METHOD
            );
            assert_eq!(request["params"]["cursor"], "cursor-1");
        });
    }

    #[tokio::test]
    async fn dragging_the_pane_scrollbar_thumb_pages_in_more_history() {
        use crossterm::event::{MouseButton, MouseEventKind};

        with_test_ctx(vec![RemoteDescriptor::local()], |ctx| {
            *ctx.mirrors.local_mut() = mirror_with_history(&tall_screen(60), "cursor-1");
            compose_and_lay_out(ctx);
            let track = ctx
                .app
                .view
                .pane_infos
                .first()
                .and_then(crate::ui::pane_scrollbar_rect)
                .expect("a pane with scrollback shows a scrollbar");

            let (session, mut server) = session_pair();
            ctx.links
                .insert(LOCAL_REMOTE_INDEX, Link::Up(Box::new(session)));

            // The pane sits at the bottom of its history, so the thumb sits
            // at the bottom of the track. Grab it and drag to the top.
            let thumb_row = track.y + track.height.saturating_sub(1);
            handle_mouse(
                mouse_at(MouseEventKind::Down(MouseButton::Left), track.x, thumb_row),
                ctx,
            );
            assert!(
                try_read_control(&mut server).is_none(),
                "grabbing the thumb scrolls nothing yet"
            );

            handle_mouse(
                mouse_at(MouseEventKind::Drag(MouseButton::Left), track.x, track.y),
                ctx,
            );

            let request = try_read_control(&mut server).expect("the drag pages history in");
            assert_eq!(
                request["method"],
                crate::protocol::framed::STREAM_HISTORY_METHOD
            );
            assert_eq!(request["params"]["cursor"], "cursor-1");
            assert!(
                ctx.app.request_history_backfill_pane.is_none(),
                "the request is drained once"
            );
        });
    }

    #[tokio::test]
    async fn wheel_scrolling_toward_the_top_pages_in_more_history() {
        use crossterm::event::MouseEventKind;

        with_test_ctx(vec![RemoteDescriptor::local()], |ctx| {
            *ctx.mirrors.local_mut() = mirror_with_history(&tall_screen(60), "cursor-1");
            compose_and_lay_out(ctx);
            let inner = ctx
                .app
                .view
                .pane_infos
                .first()
                .expect("pane info")
                .inner_rect;

            let (session, mut server) = session_pair();
            ctx.links
                .insert(LOCAL_REMOTE_INDEX, Link::Up(Box::new(session)));

            handle_mouse(mouse_at(MouseEventKind::ScrollUp, inner.x, inner.y), ctx);

            let request = try_read_control(&mut server).expect("the wheel pages history in");
            assert_eq!(
                request["method"],
                crate::protocol::framed::STREAM_HISTORY_METHOD
            );
            // The lazy scroll plan, not the whole budget.
            assert_eq!(
                request["params"]["max_bytes"],
                crate::protocol::framed::HISTORY_PAGE_DEFAULT_BYTES
            );
        });
    }

    #[tokio::test]
    async fn page_up_on_a_plain_pane_pages_in_more_history() {
        with_test_ctx(vec![RemoteDescriptor::local()], |ctx| {
            *ctx.mirrors.local_mut() = mirror_with_history(&tall_screen(60), "cursor-1");
            compose_and_lay_out(ctx);

            let (session, mut server) = session_pair();
            ctx.links
                .insert(LOCAL_REMOTE_INDEX, Link::Up(Box::new(session)));

            handle_key(key(KeyCode::PageUp), ctx);

            let request = try_read_control(&mut server).expect("page up pages history in");
            assert_eq!(
                request["method"],
                crate::protocol::framed::STREAM_HISTORY_METHOD
            );
        });
    }

    /// The server's `stream.history` answer carrying `content` as the page.
    fn stream_history_frame(request_id: &str, stream_id: u32, content: &str) -> Frame {
        let payload = serde_json::json!({
            "id": request_id,
            "result": {
                "type": "stream_history",
                "stream_id": stream_id,
                "content": content,
                "next_cursor": "cursor-2",
                "at_top": false,
                "end_cut_mid_line": false,
            },
        });
        Frame {
            frame_type: FrameType::Control,
            stream_id: 0,
            payload: serde_json::to_vec(&payload).expect("payload serializes"),
        }
    }

    #[tokio::test]
    async fn copy_mode_scrolling_pages_history_in_lazily_not_all_at_once() {
        with_test_ctx(vec![RemoteDescriptor::local()], |ctx| {
            *ctx.mirrors.local_mut() = mirror_with_history(&tall_screen(60), "cursor-1");
            compose_and_lay_out(ctx);
            {
                let source = MirrorPaneSource::new(ctx.mirrors.local());
                ctx.app.enter_copy_mode(&source);
            }
            assert_eq!(ctx.app.mode, Mode::Copy);

            let (session, mut server) = session_pair();
            ctx.links
                .insert(LOCAL_REMOTE_INDEX, Link::Up(Box::new(session)));

            // Ctrl-U scrolls the copy-mode viewport up half a screen. (Not
            // ctrl-b, which is the prefix key and never reaches copy mode.)
            handle_key(
                crate::input::TerminalKey::new(KeyCode::Char('u'), KeyModifiers::CONTROL),
                ctx,
            );

            let request = try_read_control(&mut server).expect("the motion pages history in");
            assert_eq!(
                request["method"],
                crate::protocol::framed::STREAM_HISTORY_METHOD
            );
            // A motion is a scroll, not a jump: one lazy page, not the budget.
            assert_eq!(
                request["params"]["max_bytes"],
                crate::protocol::framed::HISTORY_PAGE_DEFAULT_BYTES
            );
        });
    }

    #[tokio::test]
    async fn a_landed_history_page_renumbers_the_selection_it_pushed_down() {
        with_test_ctx(vec![RemoteDescriptor::local()], |ctx| {
            *ctx.mirrors.local_mut() = mirror_with_history("alpha bravo charlie\r\n", "cursor-1");
            compose_and_lay_out(ctx);
            let pane_id = ctx.app.workspaces[0]
                .focused_pane_id()
                .expect("composed focused pane");

            // Select "alpha" on the first row of the loaded screen.
            let metrics = {
                let source = MirrorPaneSource::new(ctx.mirrors.local());
                ctx.app.pane_scroll_metrics(&source, pane_id)
            };
            let mut selection = crate::selection::Selection::range(pane_id, 0, 0, 4, metrics);
            assert!(selection.finish());
            let before = selection.ordered_cells();
            ctx.app.selection = Some(selection);

            let (mut session, _server) = session_pair();
            session
                .pending
                .insert("h1".to_owned(), Pending::History { stream_id: 3 });
            ctx.links
                .insert(LOCAL_REMOTE_INDEX, Link::Up(Box::new(session)));

            let loaded_rows = |ctx: &LoopCtx<'_>| {
                ctx.mirrors
                    .local()
                    .replicas
                    .get(&3)
                    .and_then(|replica| replica.borrow().scroll_metrics().ok())
                    .map(|metrics| metrics.max_offset_from_bottom)
                    .expect("scroll metrics")
            };
            let rows_before = loaded_rows(ctx);

            // A page of older history lands above the loaded screen.
            let page = tall_screen(30);
            handle_server_frame(
                LOCAL_REMOTE_INDEX,
                stream_history_frame("h1", 3, &page),
                ctx,
            );

            // The growth in scrollback is the prepend count, whatever the
            // fixture started with.
            let rows_prepended = loaded_rows(ctx) - rows_before;
            assert!(rows_prepended > 0, "the page really did prepend rows");

            let after = ctx
                .app
                .selection
                .as_ref()
                .expect("selection survives the page")
                .ordered_cells();
            assert_eq!(
                (after.0 .0 - before.0 .0, after.1 .0 - before.1 .0),
                (rows_prepended as u32, rows_prepended as u32),
                "the selection moved down by exactly the rows that landed above it"
            );
            assert_eq!(
                (after.0 .1, after.1 .1),
                (before.0 .1, before.1 .1),
                "columns are untouched"
            );
        });
    }

    #[tokio::test]
    async fn a_landed_history_page_leaves_other_panes_selections_alone() {
        with_test_ctx(vec![RemoteDescriptor::local()], |ctx| {
            // Two panes, each with its own stream. p_1_2's stream receives
            // the page; the selection lives on p_1_1.
            let mut mirror = mirror_with_panes(2);
            for (public, stream_id) in [("p_1_1", 3u32), ("p_1_2", 4)] {
                let replica = crate::terminal::replica::PaneReplica::open(
                    "alpha bravo charlie\r\n",
                    10,
                    Some("cursor-1".to_owned()),
                    80,
                    24,
                    1024 * 1024,
                )
                .expect("replica opens");
                mirror.stream_opened(public, stream_id, replica);
            }
            *ctx.mirrors.local_mut() = mirror;
            compose_and_lay_out(ctx);

            let untouched_pane = ctx
                .ids
                .composed_pane_id(LOCAL_REMOTE_INDEX, "p_1_1")
                .expect("p_1_1 is composed");
            let mut selection = crate::selection::Selection::range(untouched_pane, 0, 0, 4, None);
            assert!(selection.finish());
            let before = selection.ordered_cells();
            ctx.app.selection = Some(selection);

            let (mut session, _server) = session_pair();
            session
                .pending
                .insert("h1".to_owned(), Pending::History { stream_id: 4 });
            ctx.links
                .insert(LOCAL_REMOTE_INDEX, Link::Up(Box::new(session)));

            handle_server_frame(
                LOCAL_REMOTE_INDEX,
                stream_history_frame("h1", 4, &tall_screen(30)),
                ctx,
            );

            assert_eq!(
                ctx.app
                    .selection
                    .as_ref()
                    .expect("selection")
                    .ordered_cells(),
                before,
                "a page on one pane must not renumber another pane's selection"
            );
        });
    }

    #[tokio::test]
    async fn a_page_that_bakes_when_the_alternate_screen_ends_renumbers_too() {
        with_test_ctx(vec![RemoteDescriptor::local()], |ctx| {
            *ctx.mirrors.local_mut() = mirror_with_history("alpha bravo charlie\r\n", "cursor-1");
            compose_and_lay_out(ctx);
            let pane_id = ctx.app.workspaces[0]
                .focused_pane_id()
                .expect("composed focused pane");

            let (mut session, _server) = session_pair();
            session
                .pending
                .insert("h1".to_owned(), Pending::History { stream_id: 3 });
            ctx.links
                .insert(LOCAL_REMOTE_INDEX, Link::Up(Box::new(session)));

            // The pane enters the alternate screen, so the page that arrives
            // next can only queue - replaying primary history there would
            // corrupt the alternate buffer.
            handle_server_frame(
                LOCAL_REMOTE_INDEX,
                Frame {
                    frame_type: FrameType::Data,
                    stream_id: 3,
                    payload: b"\x1b[?1049h\x1b[HALTSCREEN".to_vec(),
                },
                ctx,
            );
            let metrics = {
                let source = MirrorPaneSource::new(ctx.mirrors.local());
                ctx.app.pane_scroll_metrics(&source, pane_id)
            };
            let mut selection = crate::selection::Selection::range(pane_id, 0, 0, 4, metrics);
            assert!(selection.finish());
            let before = selection.ordered_cells();
            ctx.app.selection = Some(selection);

            handle_server_frame(
                LOCAL_REMOTE_INDEX,
                stream_history_frame("h1", 3, &tall_screen(30)),
                ctx,
            );
            assert_eq!(
                ctx.app
                    .selection
                    .as_ref()
                    .expect("selection")
                    .ordered_cells(),
                before,
                "a queued page has not landed yet, so nothing moves"
            );

            // Leaving the alternate screen bakes the queued page in.
            handle_server_frame(
                LOCAL_REMOTE_INDEX,
                Frame {
                    frame_type: FrameType::Data,
                    stream_id: 3,
                    payload: b"\x1b[?1049l".to_vec(),
                },
                ctx,
            );

            let after = ctx
                .app
                .selection
                .as_ref()
                .expect("selection")
                .ordered_cells();
            assert!(
                after.0 .0 > before.0 .0,
                "the deferred bake renumbered the selection: {before:?} -> {after:?}"
            );
            assert_eq!(
                after.0 .0 - before.0 .0,
                after.1 .0 - before.1 .0,
                "both ends moved by the same amount"
            );
        });
    }

    #[tokio::test]
    async fn copy_mode_jump_to_top_fetches_one_large_history_page() {
        with_test_ctx(vec![RemoteDescriptor::local()], |ctx| {
            *ctx.mirrors.local_mut() = mirror_with_history("alpha\r\n", "cursor-1");
            compose_into(ctx.mirrors.local(), ctx.chrome, ctx.ids, ctx.app);
            ctx.app.mode = Mode::Terminal;
            {
                let source = MirrorPaneSource::new(ctx.mirrors.local());
                let _requests = crate::ui::compute_view_with_content(
                    ctx.app,
                    &source,
                    ratatui::layout::Rect::new(0, 0, 106, 26),
                );
                ctx.app.enter_copy_mode(&source);
            }
            assert_eq!(ctx.app.mode, Mode::Copy);

            let (session, mut server) = session_pair();
            ctx.links
                .insert(LOCAL_REMOTE_INDEX, Link::Up(Box::new(session)));

            handle_key(key(KeyCode::Char('g')), ctx);

            let request = try_read_control(&mut server).expect("the jump fetches history");
            assert_eq!(
                request["method"],
                crate::protocol::framed::STREAM_HISTORY_METHOD
            );
            // One large fetch, not the page-by-page scroll plan.
            assert_eq!(request["params"]["max_bytes"], 1024 * 1024);
            assert!(
                !ctx.app.request_history_top_backfill,
                "the request is drained once"
            );
        });
    }

    #[tokio::test]
    async fn copy_mode_selects_and_copies_replica_content_through_the_seam() {
        let mirror = mirror_with_replica("alpha bravo charlie\r\n");

        let chrome = GlobalChrome::new();
        let mut ids = ComposeIds::new();
        let mut app = AppState::test_new();
        compose_into(&mirror, &chrome, &mut ids, &mut app);
        app.keybinds = crate::config::Config::default().keybinds();
        app.mode = Mode::Terminal;
        {
            let source = MirrorPaneSource::new(&mirror);
            let _requests = crate::ui::compute_view_with_content(
                &mut app,
                &source,
                ratatui::layout::Rect::new(0, 0, 106, 26),
            );
            app.enter_copy_mode(&source);
        }
        assert_eq!(app.mode, Mode::Copy);

        // Search finds replica text and jumps the cursor to it.
        {
            let source = MirrorPaneSource::new(&mirror);
            app.handle_copy_mode_key(
                &source,
                crate::input::TerminalKey::new(KeyCode::Char('/'), KeyModifiers::empty()),
            );
            for ch in "bravo".chars() {
                app.handle_copy_mode_key(
                    &source,
                    crate::input::TerminalKey::new(KeyCode::Char(ch), KeyModifiers::empty()),
                );
            }
            app.handle_copy_mode_key(
                &source,
                crate::input::TerminalKey::new(KeyCode::Enter, KeyModifiers::empty()),
            );
        }
        let copy_mode = app.copy_mode.as_ref().expect("copy mode active");
        assert_eq!(copy_mode.search.matches.len(), 1);
        assert_eq!(copy_mode.cursor_col, 6, "cursor jumped to the match");

        // Line-select and copy: the extracted text is the replica's row.
        let source = MirrorPaneSource::new(&mirror);
        app.handle_copy_mode_key(
            &source,
            crate::input::TerminalKey::new(KeyCode::Char('v'), KeyModifiers::SHIFT),
        );
        app.handle_copy_mode_key(
            &source,
            crate::input::TerminalKey::new(KeyCode::Char('y'), KeyModifiers::empty()),
        );
        let copied = app.request_clipboard_write.take().expect("copied bytes");
        assert_eq!(
            String::from_utf8_lossy(&copied).trim_end(),
            "alpha bravo charlie"
        );
        assert_eq!(app.mode, Mode::Terminal, "copy exits copy mode");
    }

    #[tokio::test]
    async fn modal_keys_and_releases_never_reach_the_pane() {
        with_test_ctx(vec![RemoteDescriptor::local()], |ctx| {
            *ctx.mirrors.local_mut() = mirror_with_replica("hello\r\n");
            compose_into(ctx.mirrors.local(), ctx.chrome, ctx.ids, ctx.app);

            let (session, mut server) = session_pair();
            ctx.links
                .insert(LOCAL_REMOTE_INDEX, Link::Up(Box::new(session)));

            // Positive control: a Terminal-mode press reaches the pane.
            ctx.app.mode = Mode::Terminal;
            handle_key(key(KeyCode::Char('a')), ctx);
            let frame = try_read_control(&mut server).expect("terminal press forwards to the pane");
            assert_eq!(frame["method"], "pane.send_bytes");

            // A press typed into a rename modal edits the modal, not the
            // pane.
            ctx.app.mode = Mode::RenameWorkspace;
            ctx.app.name_input.clear();
            ctx.app.name_input_replace_on_type = false;
            handle_key(key(KeyCode::Char('b')), ctx);
            assert_eq!(ctx.app.name_input, "b");
            assert!(
                try_read_control(&mut server).is_none(),
                "modal press must not reach the pane"
            );

            // The matching Release must not leak into the pane either
            // (legacy parity: key events are only forwarded from Terminal
            // mode).
            handle_key(
                crate::input::TerminalKey::new(KeyCode::Char('b'), KeyModifiers::empty())
                    .with_kind(crossterm::event::KeyEventKind::Release),
                ctx,
            );
            assert!(
                try_read_control(&mut server).is_none(),
                "modal key release must not reach the pane"
            );
        });
    }

    #[tokio::test]
    async fn retained_selection_copy_key_copies_replica_text_instead_of_forwarding() {
        with_test_ctx(vec![RemoteDescriptor::local()], |ctx| {
            *ctx.mirrors.local_mut() = mirror_with_replica("alpha bravo charlie\r\n");
            compose_into(ctx.mirrors.local(), ctx.chrome, ctx.ids, ctx.app);
            ctx.app.mode = Mode::Terminal;
            ctx.app.copy_on_select = false;

            let pane_id = ctx.app.workspaces[0]
                .focused_pane_id()
                .expect("composed focused pane");
            let metrics = {
                let source = MirrorPaneSource::new(ctx.mirrors.local());
                ctx.app.pane_scroll_metrics(&source, pane_id)
            };
            let mut selection = crate::selection::Selection::range(pane_id, 0, 0, 4, metrics);
            assert!(selection.finish());
            ctx.app.selection = Some(selection);

            let (session, mut server) = session_pair();
            ctx.links
                .insert(LOCAL_REMOTE_INDEX, Link::Up(Box::new(session)));

            // Ctrl-C copies the retained selection out of the replica and
            // is not forwarded to the pane.
            handle_key(
                crate::input::TerminalKey::new(KeyCode::Char('c'), KeyModifiers::CONTROL),
                ctx,
            );
            assert!(ctx.app.selection.is_none(), "copy consumes the selection");
            assert!(
                try_read_control(&mut server).is_none(),
                "the copy chord must not reach the pane"
            );

            // With copy_on_select the chord forwards to the pane instead.
            ctx.app.copy_on_select = true;
            handle_key(
                crate::input::TerminalKey::new(KeyCode::Char('c'), KeyModifiers::CONTROL),
                ctx,
            );
            let frame = try_read_control(&mut server).expect("ctrl-c forwards with copy_on_select");
            assert_eq!(frame["method"], "pane.send_bytes");
        });
    }

    #[tokio::test]
    async fn mouse_drag_selection_copies_replica_text_through_the_seam() {
        use crossterm::event::{MouseButton, MouseEventKind};

        let mirror = mirror_with_replica("alpha bravo charlie\r\n");
        let chrome = GlobalChrome::new();
        let mut ids = ComposeIds::new();
        let mut app = AppState::test_new();
        compose_into(&mirror, &chrome, &mut ids, &mut app);
        app.keybinds = crate::config::Config::default().keybinds();
        app.mode = Mode::Terminal;
        app.copy_on_select = true;

        let source = MirrorPaneSource::new(&mirror);
        let _requests = crate::ui::compute_view_with_content(
            &mut app,
            &source,
            ratatui::layout::Rect::new(0, 0, 106, 26),
        );
        let inner = app.view.pane_infos.first().expect("pane info").inner_rect;
        let empty = crate::terminal::TerminalRuntimeRegistry::new();
        let mouse = |kind: MouseEventKind, column: u16, row: u16| MouseEvent {
            kind,
            column,
            row,
            modifiers: KeyModifiers::empty(),
        };

        // Drag-select "alpha" (columns 0..=4 of the replica's first row)
        // and release: copy_on_select extracts the text from the replica
        // through the pane-content seam.
        app.handle_mouse_with_content(
            &empty,
            &source,
            mouse(MouseEventKind::Down(MouseButton::Left), inner.x, inner.y),
        );
        app.handle_mouse_with_content(
            &empty,
            &source,
            mouse(
                MouseEventKind::Drag(MouseButton::Left),
                inner.x + 4,
                inner.y,
            ),
        );
        app.handle_mouse_with_content(
            &empty,
            &source,
            mouse(MouseEventKind::Up(MouseButton::Left), inner.x + 4, inner.y),
        );

        let copied = app
            .request_clipboard_write
            .take()
            .expect("mouse selection copied replica text");
        assert_eq!(String::from_utf8_lossy(&copied), "alpha");
    }

    #[tokio::test]
    async fn double_click_selects_the_replica_word_under_the_cursor() {
        use crossterm::event::{MouseButton, MouseEventKind};

        with_test_ctx(vec![RemoteDescriptor::local()], |ctx| {
            *ctx.mirrors.local_mut() = mirror_with_replica("alpha bravo charlie\r\n");
            compose_into(ctx.mirrors.local(), ctx.chrome, ctx.ids, ctx.app);
            ctx.app.mode = Mode::Terminal;
            ctx.app.copy_on_select = false;

            {
                let source = MirrorPaneSource::new(ctx.mirrors.local());
                let _requests = crate::ui::compute_view_with_content(
                    ctx.app,
                    &source,
                    ratatui::layout::Rect::new(0, 0, 106, 26),
                );
            }
            let inner = ctx
                .app
                .view
                .pane_infos
                .first()
                .expect("pane info")
                .inner_rect;
            // Two clicks on the same cell inside "bravo".
            let click = MouseEvent {
                kind: MouseEventKind::Down(MouseButton::Left),
                column: inner.x + 7,
                row: inner.y,
                modifiers: KeyModifiers::empty(),
            };
            assert!(
                !handle_pane_double_click(click, ctx),
                "first click only arms the candidate"
            );
            assert!(
                handle_pane_double_click(click, ctx),
                "second click selects the word"
            );
            let selection = ctx.app.selection.clone().expect("word selection");
            assert!(selection.is_finalized());
            let source = MirrorPaneSource::new(ctx.mirrors.local());
            ctx.app.copy_selection(&source);
            let copied = ctx.app.request_clipboard_write.take().expect("copied word");
            assert_eq!(String::from_utf8_lossy(&copied), "bravo");
        });
    }

    #[test]
    fn hidden_pane_streams_are_selected_for_closing() {
        let mut mirror = super::super::RemoteMirror::test_with_adversarial_catalog();
        let visible_replica =
            crate::terminal::replica::PaneReplica::open("a", 1, None, 80, 24, 64 * 1024)
                .expect("replica opens");
        mirror.stream_opened("p_2_1", 3, visible_replica);
        let hidden_replica =
            crate::terminal::replica::PaneReplica::open("b", 1, None, 80, 24, 64 * 1024)
                .expect("replica opens");
        mirror.stream_opened("p_10_1", 4, hidden_replica);

        let visible = vec!["p_2_1".to_owned(), "p_2_10".to_owned()];
        let hidden = hidden_pane_streams(&mirror, &visible);
        assert_eq!(hidden, vec![("p_10_1".to_owned(), 4)]);

        mirror.stream_closed(4);
        assert!(hidden_pane_streams(&mirror, &visible).is_empty());
        mirror.assert_invariants_for_test();
    }
}

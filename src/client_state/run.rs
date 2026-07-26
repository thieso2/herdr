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
use std::io::{self, Write as _};
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
    write_frame, Frame, FrameType, FramedCodecError, SessionWelcome, StreamMode,
    CAPABILITY_CATALOG, CAPABILITY_PANE_STREAM, CATALOG_EVENT, CATALOG_RESYNC_EVENT,
    CONTROL_STREAM_ID, FRAMED_MAGIC, PANE_WRITE_LOCKED_ERROR, STREAM_CLOSED_EVENT,
    STREAM_REVOKED_EVENT,
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
/// FLIP PROCEDURE (release-gated): after the pure client has soaked on at
/// least two preview releases, flip this constant to `true` in its own
/// commit ("feat: default the tui to the pure client"). Nothing else
/// changes: an explicit `pure_client = true`/`false` in the user's config
/// always wins over this default (see [`pure_client_enabled`]), so early
/// adopters and opt-outs keep their behavior, and the legacy path remains
/// reachable with `pure_client = false` until it is deleted. Windows keeps
/// ignoring the flag entirely (`run_client_with_mode` only consults it on
/// unix) — flipping this constant does not change Windows behavior.
pub(crate) const PURE_CLIENT_DEFAULT: bool = false;

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
}

/// Outcome of a fleet remote's connect-plus-handshake thread. On success
/// the thread keeps running as the session's frame reader; the writer half
/// and the child guard travel here to the loop.
enum RemoteEstablished {
    Connected {
        welcome: SessionWelcome,
        writer: std::process::ChildStdin,
        guard: BridgeChild,
    },
    Incompatible {
        message: String,
    },
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
    Incompatible { message: String },
    Failed(String),
}

/// The connected-variant payload, boxed to keep the outcome enum small.
struct ConnectedLocal {
    session: Session,
    reader: LocalStream,
    welcome: SessionWelcome,
}

/// Interprets a `session.hello` answer. `Err(None)` means malformed.
fn interpret_hello_answer(
    response: &serde_json::Value,
) -> Result<SessionWelcome, Result<String, String>> {
    if let Some(error) = control_error(response) {
        if error.code == "protocol_out_of_window" {
            // Err(Ok(_)) = incompatible with this message.
            return Err(Ok(error.message));
        }
        return Err(Err(format!("session.hello rejected: {}", error.message)));
    }
    match parse_session_welcome(response) {
        Ok(welcome) => {
            if !welcome.capabilities.iter().any(|c| c == CAPABILITY_CATALOG) {
                return Err(Ok(format!(
                    "server {} does not offer the catalog capability; upgrade that herdr server",
                    welcome.server_version
                )));
            }
            Ok(welcome)
        }
        Err(err) => Err(Err(err)),
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
fn connect() -> ConnectOutcome {
    let socket_path = crate::api::socket_path();
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
        Err(Ok(message)) => ConnectOutcome::Incompatible { message },
        Err(Err(err)) => ConnectOutcome::Failed(err),
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
    let established = (|| {
        let (child, mut stdout, mut stdin) = BridgeChild::spawn(target, &descriptor.session)
            .map_err(|err| format!("ssh bridge spawn failed: {err}"))?;
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
            .map_err(|err| format!("framed handshake failed: {err}"))?;
        let hello = session_hello_request_with_capabilities("pure:hello:0", SESSION_CAPABILITIES);
        let payload =
            serde_json::to_vec(&hello).map_err(|err| format!("hello encode failed: {err}"))?;
        write_frame(&mut stdin, FrameType::Control, CONTROL_STREAM_ID, &payload)
            .map_err(|err| format!("session.hello send failed: {err}"))?;
        let response = loop {
            let frame = read_frame(&mut stdout).map_err(|err| {
                if handshake_started.elapsed() >= REMOTE_HANDSHAKE_TIMEOUT {
                    return format!(
                        "session.hello timed out after {}s",
                        REMOTE_HANDSHAKE_TIMEOUT.as_secs()
                    );
                }
                let tail = child
                    .stderr_tail()
                    .lock()
                    .map(|tail| tail.trim().replace('\n', "; "))
                    .unwrap_or_default();
                if tail.is_empty() {
                    format!("session.hello failed: {err}")
                } else {
                    format!("session.hello failed: {err} (ssh: {tail})")
                }
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
            let _ = event_tx.send(LoopEvent::Established(
                remote,
                generation,
                Box::new(RemoteEstablished::Failed(message)),
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
        Err(Ok(message)) => {
            let _ = event_tx.send(LoopEvent::Established(
                remote,
                generation,
                Box::new(RemoteEstablished::Incompatible { message }),
            ));
            return;
        }
        Err(Err(message)) => {
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

    // Terminal graphics respect the same [experimental] kitty_graphics gate
    // as the server render path: replicas ingest kitty APC data from the
    // pane DATA stream, and the pure client paints visible placements onto
    // the host terminal after each draw.
    crate::kitty_graphics::set_enabled(config.experimental.kitty_graphics);

    let mut mirrors = RemoteMirrors::with_local();
    // The fleet config defines the remotes; every enabled remote gets a
    // mirror and a connection regardless of view membership.
    let mut descriptors = remote_descriptors(&crate::fleet::config::load());
    for descriptor in descriptors.iter().skip(1) {
        mirrors.insert(super::RemoteMirror::new(
            descriptor.index,
            descriptor.name.clone(),
        ));
    }
    let mut chrome = GlobalChrome::new();
    let mut ids = ComposeIds::new();
    let mut app = AppState::empty();
    apply_client_config(&mut app, config);
    chrome.sidebar_collapsed = app.sidebar_collapsed;
    app.mode = Mode::Navigate;

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
) -> io::Result<()> {
    let mut framer = crate::raw_input::RawInputFramer::for_host_input();
    let mut ui = InteractionState::default();
    let mut links: Links = BTreeMap::new();
    links.insert(
        LOCAL_REMOTE_INDEX,
        establish_local(mirrors, chrome, &mut ui, event_tx, should_quit),
    );
    for descriptor in descriptors.iter().skip(1) {
        let link = establish_remote(descriptor, mirrors, &mut ui, event_tx, should_quit);
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
            return Ok(());
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
            let link = if remote == LOCAL_REMOTE_INDEX {
                establish_local(
                    ctx.mirrors,
                    ctx.chrome,
                    ctx.ui,
                    ctx.event_tx,
                    ctx.should_quit,
                )
            } else if let Some(descriptor) = ctx
                .descriptors
                .iter()
                .find(|descriptor| descriptor.index == remote)
            {
                establish_remote(
                    descriptor,
                    ctx.mirrors,
                    ctx.ui,
                    ctx.event_tx,
                    ctx.should_quit,
                )
            } else {
                continue;
            };
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
            terminal.draw(|frame| {
                let source = MirrorPaneSource::for_view(ctx.mirrors, &in_view);
                resize_requests =
                    crate::ui::compute_view_with_content(ctx.app, &source, frame.area());
                ctx.app.sync_copy_mode_search_geometry();
                crate::ui::render_with_content(ctx.app, &source, frame);
                if let Some(dialog) = &dialog {
                    crate::ui::render_remote_edit_overlay(ctx.app, dialog, frame);
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
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
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

/// Attempts the local connection and, on success, kicks off the catalog
/// resync and the socket reader thread.
fn establish_local(
    mirrors: &mut RemoteMirrors,
    chrome: &mut GlobalChrome,
    ui: &mut InteractionState,
    event_tx: &mpsc::SyncSender<LoopEvent>,
    should_quit: &Arc<AtomicBool>,
) -> Link {
    let mirror = mirrors.local_mut();
    debug!(remote = mirror.remote_index, name = %mirror.name, "connecting");
    mirror.connection.connect_started();
    ui.next_generation += 1;
    let generation = ui.next_generation;
    match connect() {
        ConnectOutcome::Connected(connected) => {
            let ConnectedLocal {
                mut session,
                reader,
                welcome,
            } = *connected;
            session.generation = generation;
            // The mirror holds what the server actually negotiated, not what
            // this client asked for: capability gates (pane streams) and any
            // protocol downgrade must reflect the welcome.
            mirror
                .connection
                .connected(crate::protocol::framed::NegotiatedSession {
                    protocol: welcome.protocol,
                    capabilities: welcome.capabilities,
                });
            // Full resync: the fresh snapshot plus re-opened streams are the
            // only source of truth for this connection.
            mirror.begin_resync();
            chrome.connection_status = None;

            let id = session.request_id("snapshot");
            session.pending.insert(id.clone(), Pending::Snapshot);
            if let Err(err) = session.send_control(&session_snapshot_request(&id)) {
                drop(session);
                mirror.connection_lost(format!("snapshot request failed: {err}"));
                chrome.connection_status = Some("local server unreachable; retrying".to_owned());
                return Link::Down {
                    retry_at: Instant::now() + Duration::from_secs(1),
                };
            }

            let frame_tx = event_tx.clone();
            let reader_quit = Arc::clone(should_quit);
            std::thread::spawn(move || {
                socket_reader_loop(reader, generation, frame_tx, &reader_quit)
            });
            Link::Up(Box::new(session))
        }
        ConnectOutcome::Incompatible { message } => {
            mirror.connection.incompatible(
                crate::protocol::framed::HelloRemedy::UpgradeClient,
                message.clone(),
            );
            chrome.connection_status = Some(message);
            Link::Incompatible
        }
        ConnectOutcome::Failed(error) => {
            let attempt = match mirror.connection {
                super::ClientConnectionState::Connecting { attempt } => attempt,
                _ => 1,
            };
            mirror.connection_lost(error.clone());
            chrome.connection_status = Some(format!("local server unreachable; retrying: {error}"));
            let delay = crate::fleet::connection::backoff_delay(
                attempt,
                crate::fleet::connection::BackoffTuning::default(),
                0.5,
            );
            Link::Down {
                retry_at: Instant::now() + delay,
            }
        }
    }
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
    let Some(mirror) = ctx.mirrors.get_mut(remote) else {
        ctx.links.remove(&remote);
        return;
    };
    match outcome {
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
        RemoteEstablished::Incompatible { message } => {
            warn!(remote, message = %message, "fleet remote protocol incompatible");
            mirror
                .connection
                .incompatible(crate::protocol::framed::HelloRemedy::UpgradeClient, message);
            ctx.links.insert(remote, Link::Incompatible);
        }
        RemoteEstablished::Failed(error) => {
            let attempt = match mirror.connection {
                super::ClientConnectionState::Connecting { attempt } => attempt,
                _ => 1,
            };
            debug!(remote, error = %error, "fleet remote connect failed");
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

/// Sends heartbeat pings on established fleet links and fails links whose
/// remote has been silent past the pong timeout, so a silently dead
/// transport cannot keep a connected chip dot. The local socket is exempt,
/// matching the legacy client. Returns true when a link changed state.
fn service_remote_heartbeats(ctx: &mut LoopCtx<'_>) -> bool {
    let now = Instant::now();
    let mut dead: Vec<usize> = Vec::new();
    for (remote, link) in ctx.links.iter_mut() {
        if *remote == LOCAL_REMOTE_INDEX {
            continue;
        }
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

/// Drops a remote's link after its transport died and schedules the retry.
fn drop_link(remote: usize, ctx: &mut LoopCtx<'_>, why: &str) {
    if matches!(ctx.links.get(&remote), Some(Link::Incompatible) | None) {
        return;
    }
    let Some(mirror) = ctx.mirrors.get_mut(remote) else {
        ctx.links.remove(&remote);
        return;
    };
    mirror.connection_lost(why);
    if remote == LOCAL_REMOTE_INDEX {
        ctx.chrome.connection_status = Some(format!("{why}; reconnecting"));
    }
    ctx.links.insert(
        remote,
        Link::Down {
            retry_at: Instant::now() + Duration::from_millis(500),
        },
    );
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
    request_backfill_if_needed(ctx.links, ctx.mirrors, remote, stream_id);
    true
}

/// Issues a lazy scrollback backfill for a stream whose viewport approaches
/// the top of loaded history. The response prepends through the replica's
/// rebuild path.
fn request_backfill_if_needed(
    links: &mut Links,
    mirrors: &mut RemoteMirrors,
    remote: usize,
    stream_id: u32,
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
    match replica.take_backfill_request(&id, crate::terminal::replica::BackfillTrigger::Scroll) {
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
    mut stream: LocalStream,
    generation: u64,
    event_tx: mpsc::SyncSender<LoopEvent>,
    should_quit: &Arc<AtomicBool>,
) {
    if stream.set_nonblocking(false).is_err() {
        let _ = event_tx.send(LoopEvent::Disconnected(LOCAL_REMOTE_INDEX, generation));
        return;
    }
    while !should_quit.load(Ordering::Acquire) {
        match read_frame(&mut stream) {
            Ok(frame) => {
                if event_tx
                    .send(LoopEvent::Frame(LOCAL_REMOTE_INDEX, generation, frame))
                    .is_err()
                {
                    return;
                }
            }
            Err(err) => {
                debug!(err = %err, "pure client session read ended");
                let _ = event_tx.send(LoopEvent::Disconnected(LOCAL_REMOTE_INDEX, generation));
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
            if let Some(replica) = mirror.replica_mut(frame.stream_id) {
                if let Err(err) = replica.apply_tail(&frame.payload) {
                    warn!(err = %err, stream = frame.stream_id, "replica tail apply failed");
                }
            }
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
                            remote,
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
                                if remote == LOCAL_REMOTE_INDEX {
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
                                    mirror.stream_opened(pane_id, opened.stream_id, replica);
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
                    if let Some(replica) = mirror.replica_mut(stream_id) {
                        match replica.apply_history_response(&payload) {
                            Ok(rows_prepended) => {
                                debug!(stream = stream_id, rows_prepended, "history page applied")
                            }
                            Err(err) => warn!(err = %err, "history page apply failed"),
                        }
                    }
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

/// Interprets one host input event client-side.
fn handle_raw_input(raw: crate::raw_input::RawInputEvent, ctx: &mut LoopCtx<'_>) {
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
            if key.code == KeyCode::Char('q') {
                ctx.app.should_quit = true;
            }
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
            // Scrolling near the top of loaded history pages more in.
            if let Some((remote, stream_id)) = copy_pane
                .and_then(|pane_id| ctx.ids.public_pane_id(pane_id))
                .and_then(|(remote, public)| {
                    ctx.mirrors
                        .get(remote)
                        .and_then(|mirror| mirror.stream_for_pane(public))
                        .map(|stream_id| (remote, stream_id))
                })
            {
                request_backfill_if_needed(ctx.links, ctx.mirrors, remote, stream_id);
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
        _ => {
            // Residual modal modes (Settings, Navigator, GlobalMenu,
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
        }
        Err(err) => {
            if let Some(dialog) = ctx.chrome.remote_edit.as_mut() {
                dialog.error = Some(err.to_string());
            }
        }
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
    // The add/edit-remote dialog swallows the mouse while open; only its
    // buttons act.
    if ctx.chrome.remote_edit.is_some() {
        handle_dialog_click(mouse, ctx);
        return;
    }
    // Chip strip first: chips and the add affordance are pure client
    // chrome, hit-tested against the computed view.
    if let MouseEventKind::Down(button) = mouse.kind {
        if button == crossterm::event::MouseButton::Left
            && ctx.app.view.remote_add_hit_area.width > 0
            && ctx
                .app
                .view
                .remote_add_hit_area
                .contains(ratatui::layout::Position::new(mouse.column, mouse.row))
        {
            ctx.chrome.remote_edit = Some(super::remote_edit::RemoteEditState::add());
            return;
        }
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
            request_backfill_if_needed(ctx.links, ctx.mirrors, remote, stream_id);
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
        crossterm::event::MouseButton::Right => {
            // Edit the remote behind the chip; the implicit local runtime
            // is not configurable.
            if descriptor.index == LOCAL_REMOTE_INDEX {
                return;
            }
            let Some(target) = descriptor.target.clone() else {
                return;
            };
            ctx.chrome.remote_edit = Some(super::remote_edit::RemoteEditState::edit(
                &crate::fleet::config::RemoteEntry {
                    name: descriptor.name.clone(),
                    target,
                    session: descriptor.session.clone(),
                    enabled: true,
                },
            ));
        }
        crossterm::event::MouseButton::Middle => {}
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
            crate::fleet::config::RemoteEntry {
                name: "buildbox".into(),
                target: "can@buildbox.example".into(),
                session: "default".into(),
                enabled: true,
            },
            crate::fleet::config::RemoteEntry {
                name: "gpu-01".into(),
                target: "can@gpu-01.example".into(),
                session: "default".into(),
                enabled: true,
            },
        ])
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
    async fn right_click_opens_the_edit_dialog_for_fleet_remotes_only() {
        with_test_ctx(three_descriptors(), |ctx| {
            handle_chip_click(0, crossterm::event::MouseButton::Right, ctx);
            assert!(
                ctx.chrome.remote_edit.is_none(),
                "the implicit local runtime is not configurable"
            );

            handle_chip_click(2, crossterm::event::MouseButton::Right, ctx);
            let dialog = ctx.chrome.remote_edit.as_ref().expect("edit dialog");
            assert_eq!(dialog.original_name.as_deref(), Some("gpu-01"));
            assert_eq!(dialog.target, "can@gpu-01.example");

            // While the dialog is open, keys go to it, not the session.
            handle_key(key(KeyCode::Esc), ctx);
            assert!(ctx.chrome.remote_edit.is_none());
            assert!(!ctx.app.should_quit);
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
            let entries = vec![crate::fleet::config::RemoteEntry {
                name: "buildbox".into(),
                target: "can@buildbox2.example".into(),
                session: "default".into(),
                enabled: true,
            }];
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
                    message: "windows do not overlap".into(),
                },
                ctx,
            );
            assert!(matches!(ctx.links.get(&1), Some(Link::Incompatible)));
            assert!(matches!(
                ctx.mirrors.get(1).map(|mirror| &mirror.connection),
                Some(super::super::ClientConnectionState::Incompatible { .. })
            ));
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
            !pure_client_enabled(&config),
            "release-gated: the default stays legacy until the documented flip"
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
                    "focused": true, "pane_count": 1, "tab_count": 1,
                    "active_tab_id": "t_1_1", "agent_status": "idle"
                }],
                "tabs": [{
                    "tab_id": "t_1_1", "workspace_id": "ws_1", "number": 1,
                    "label": "shell", "focused": true, "pane_count": 1,
                    "agent_status": "idle"
                }],
                "panes": [{
                    "pane_id": "p_1_1", "terminal_id": "term_1",
                    "workspace_id": "ws_1", "tab_id": "t_1_1", "focused": true,
                    "agent_status": "idle", "revision": 1
                }],
                "layouts": [],
                "agents": []
            }))
            .expect("snapshot deserializes");
        mirror.catalog.resync(&snapshot, 1);
        let replica =
            crate::terminal::replica::PaneReplica::open(screen, 10, None, 80, 24, 64 * 1024)
                .expect("replica opens");
        mirror.stream_opened("p_1_1", 3, replica);
        mirror
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

//! Pure-client run loop: the TUI as a framed-protocol client of the local
//! server (remote #0).
//!
//! Enabled by `[experimental] pure_client` (or `HERDR_PURE_CLIENT=1`). The
//! loop owns a [`super::RemoteMirror`]: it negotiates a framed session with
//! the `catalog` capability, resyncs the session catalog from
//! `session.snapshot`, applies `catalog.event` frames, opens pane streams
//! for the visible tab into [`crate::terminal::replica::PaneReplica`]s, and
//! renders by composing mirror plus chrome through the shared
//! `compute_view` + `render` pair. Input is interpreted client-side: keys
//! encode against the replica's terminal modes and travel as
//! `pane.send_bytes`, geometry changes travel as `stream.resize`, and
//! scrollback stays fully local against the replica.

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

use crate::app::{AppState, Mode};
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
use super::compose::{apply_client_config, compose_into, ComposeIds, MirrorPaneSource};
use super::{RemoteMirrors, SessionCatalog};

/// How long the pure client waits for the `session.hello` answer.
const HELLO_TIMEOUT: Duration = Duration::from_secs(5);

/// Idle poll tick of the run loop.
const LOOP_TICK: Duration = Duration::from_millis(30);

/// Whether the pure-client run path is enabled for this process.
///
/// `HERDR_PURE_CLIENT=1`/`0` overrides the `[experimental] pure_client`
/// config flag, following the `HERDR_RENDER_ENCODING` override precedent.
pub(crate) fn pure_client_enabled(config: &crate::config::Config) -> bool {
    match std::env::var("HERDR_PURE_CLIENT") {
        Ok(value) if value == "1" => true,
        Ok(value) if value == "0" => false,
        _ => config.experimental.pure_client,
    }
}

/// Events the pure-client loop multiplexes.
enum LoopEvent {
    Stdin(Vec<u8>),
    Resize(u16, u16),
    Frame(Frame),
    Disconnected,
}

/// In-flight control requests awaiting their response frame.
enum Pending {
    Snapshot,
    StreamOpen { pane_id: String, mode: StreamMode },
    History { stream_id: u32 },
    Resize { stream_id: u32 },
    Api,
}

/// One connected framed session.
pub(super) struct Session {
    stream: LocalStream,
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
        write_frame(
            &mut self.stream,
            FrameType::Control,
            CONTROL_STREAM_ID,
            &payload,
        )
        .map_err(|err| match err {
            FramedCodecError::Io(err) => err,
            other => io::Error::new(io::ErrorKind::InvalidData, other.to_string()),
        })
    }
}

/// Outcome of one connect attempt.
enum ConnectOutcome {
    Connected {
        session: Session,
        welcome: SessionWelcome,
    },
    Incompatible {
        message: String,
    },
    Failed(String),
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

    let mut session = Session {
        stream,
        next_request_id: 1,
        pending: HashMap::new(),
        sent_sizes: HashMap::new(),
        read_only: HashSet::new(),
    };
    let id = session.request_id("hello");
    let hello = session_hello_request_with_capabilities(
        &id,
        &[
            CAPABILITY_PANE_STREAM,
            CAPABILITY_CATALOG,
            crate::protocol::framed::CAPABILITY_NOTIFICATION,
            crate::protocol::framed::CAPABILITY_WINDOW_TITLE,
        ],
    );
    if let Err(err) = session.send_control(&hello) {
        return ConnectOutcome::Failed(format!("session.hello send failed: {err}"));
    }
    let _ = session.stream.set_recv_timeout(Some(HELLO_TIMEOUT));
    let response = loop {
        match read_frame(&mut session.stream) {
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
    let _ = session.stream.set_recv_timeout(None);

    if let Some(error) = control_error(&response) {
        if error.code == "protocol_out_of_window" {
            return ConnectOutcome::Incompatible {
                message: error.message,
            };
        }
        return ConnectOutcome::Failed(format!("session.hello rejected: {}", error.message));
    }
    match parse_session_welcome(&response) {
        Ok(welcome) => {
            if !welcome.capabilities.iter().any(|c| c == CAPABILITY_CATALOG) {
                return ConnectOutcome::Incompatible {
                    message: format!(
                        "server {} does not offer the catalog capability; upgrade this herdr server",
                        welcome.server_version
                    ),
                };
            }
            info!(
                protocol = welcome.protocol,
                server_version = %welcome.server_version,
                "pure client negotiated framed catalog session"
            );
            ConnectOutcome::Connected { session, welcome }
        }
        Err(err) => ConnectOutcome::Failed(err),
    }
}

/// Runs the pure-client TUI until the user detaches. Never returns to the
/// legacy client path.
pub(crate) fn run_pure_client(config: &crate::config::Config) -> io::Result<()> {
    crate::logging::startup("client");
    info!("running pure client of the local server (remote #0)");

    let mut mirrors = RemoteMirrors::with_local();
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
    Up(Session),
    Down { retry_at: Instant },
    Incompatible,
}

#[allow(clippy::too_many_arguments)] // run-loop wiring: every argument is a distinct owned subsystem
fn run_loop(
    config: &crate::config::Config,
    terminal: &mut ratatui::DefaultTerminal,
    mirrors: &mut RemoteMirrors,
    chrome: &mut GlobalChrome,
    ids: &mut ComposeIds,
    app: &mut AppState,
    event_tx: &mpsc::SyncSender<LoopEvent>,
    event_rx: &mpsc::Receiver<LoopEvent>,
    should_quit: &Arc<AtomicBool>,
) -> io::Result<()> {
    let scrollback_limit = config.advanced.scrollback_limit_bytes;
    let mut framer = crate::raw_input::RawInputFramer::for_host_input();
    let mut link = establish(mirrors, chrome, event_tx, should_quit);
    let mut dirty = true;

    loop {
        if should_quit.load(Ordering::Acquire) || app.should_quit {
            return Ok(());
        }

        // Reconnect with backoff whenever the link is down.
        if let Link::Down { retry_at } = &link {
            if mirrors.local().connection.may_retry() && Instant::now() >= *retry_at {
                link = establish(mirrors, chrome, event_tx, should_quit);
                dirty = true;
            }
        }

        if dirty {
            compose_into(mirrors.local(), chrome, ids, app);
            sync_mode(app);
            let mut resize_requests = Vec::new();
            terminal.draw(|frame| {
                let source = MirrorPaneSource::new(mirrors.local());
                resize_requests = crate::ui::compute_view_with_content(app, &source, frame.area());
                crate::ui::render_with_content(app, &source, frame);
            })?;
            if let Link::Up(session) = &mut link {
                sync_pane_streams(session, mirrors, &resize_requests, scrollback_limit);
            }
            dirty = false;
        }

        let event = match event_rx.recv_timeout(LOOP_TICK) {
            Ok(event) => event,
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
        };
        match event {
            LoopEvent::Stdin(data) => {
                for raw in framer.push(&data) {
                    handle_raw_input(raw, &mut link, mirrors, chrome, ids, app);
                }
                dirty = true;
            }
            LoopEvent::Resize(cols, rows) => {
                debug!(cols, rows, "host terminal resized");
                dirty = true;
            }
            LoopEvent::Frame(frame) => {
                handle_server_frame(frame, &mut link, mirrors, chrome, scrollback_limit);
                dirty = true;
            }
            LoopEvent::Disconnected => {
                drop_link(&mut link, mirrors, chrome, "server connection closed");
                dirty = true;
            }
        }

        // Drain whatever queued behind the first event before redrawing.
        while let Ok(event) = event_rx.try_recv() {
            match event {
                LoopEvent::Stdin(data) => {
                    for raw in framer.push(&data) {
                        handle_raw_input(raw, &mut link, mirrors, chrome, ids, app);
                    }
                }
                LoopEvent::Resize(_, _) => {}
                LoopEvent::Frame(frame) => {
                    handle_server_frame(frame, &mut link, mirrors, chrome, scrollback_limit)
                }
                LoopEvent::Disconnected => {
                    drop_link(&mut link, mirrors, chrome, "server connection closed")
                }
            }
        }
    }
}

/// Attempts a connection and, on success, kicks off the catalog resync and
/// the socket reader thread.
fn establish(
    mirrors: &mut RemoteMirrors,
    chrome: &mut GlobalChrome,
    event_tx: &mpsc::SyncSender<LoopEvent>,
    should_quit: &Arc<AtomicBool>,
) -> Link {
    let mirror = mirrors.local_mut();
    debug!(remote = mirror.remote_index, name = %mirror.name, "connecting");
    mirror.connection.connect_started();
    match connect() {
        ConnectOutcome::Connected {
            mut session,
            welcome,
        } => {
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

            let read_stream = match session.stream.try_clone() {
                Ok(stream) => stream,
                Err(err) => {
                    mirror.connection_lost(format!("socket clone failed: {err}"));
                    chrome.connection_status =
                        Some("local server unreachable; retrying".to_owned());
                    return Link::Down {
                        retry_at: Instant::now() + Duration::from_secs(1),
                    };
                }
            };
            let frame_tx = event_tx.clone();
            let reader_quit = Arc::clone(should_quit);
            std::thread::spawn(move || socket_reader_loop(read_stream, frame_tx, &reader_quit));
            Link::Up(session)
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

fn drop_link(link: &mut Link, mirrors: &mut RemoteMirrors, chrome: &mut GlobalChrome, why: &str) {
    if matches!(link, Link::Incompatible) {
        return;
    }
    mirrors.local_mut().connection_lost(why);
    chrome.connection_status = Some(format!("{why}; reconnecting"));
    *link = Link::Down {
        retry_at: Instant::now() + Duration::from_millis(500),
    };
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
fn scroll_focused_replica_page(
    code: crossterm::event::KeyCode,
    link: &mut Link,
    mirrors: &mut RemoteMirrors,
    ids: &mut ComposeIds,
    app: &mut AppState,
) -> bool {
    let Some(public) = focused_public_pane(mirrors, ids, app) else {
        return false;
    };
    let mirror = mirrors.local_mut();
    let Some(stream_id) = mirror.stream_for_pane(&public) else {
        return false;
    };
    let Some(replica) = mirror.replicas.get_mut(&stream_id) else {
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
    request_backfill_if_needed(link, stream_id, mirrors);
    true
}

/// Issues a lazy scrollback backfill for a stream whose viewport approaches
/// the top of loaded history. The response prepends through the replica's
/// rebuild path.
fn request_backfill_if_needed(link: &mut Link, stream_id: u32, mirrors: &mut RemoteMirrors) {
    let Link::Up(session) = link else {
        return;
    };
    let Some(replica) = mirrors.local_mut().replicas.get_mut(&stream_id) else {
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
    event_tx: mpsc::SyncSender<LoopEvent>,
    should_quit: &Arc<AtomicBool>,
) {
    if stream.set_nonblocking(false).is_err() {
        let _ = event_tx.send(LoopEvent::Disconnected);
        return;
    }
    while !should_quit.load(Ordering::Acquire) {
        match read_frame(&mut stream) {
            Ok(frame) => {
                if event_tx.send(LoopEvent::Frame(frame)).is_err() {
                    return;
                }
            }
            Err(err) => {
                debug!(err = %err, "pure client session read ended");
                let _ = event_tx.send(LoopEvent::Disconnected);
                return;
            }
        }
    }
}

/// Applies one server frame to the mirror.
fn handle_server_frame(
    frame: Frame,
    link: &mut Link,
    mirrors: &mut RemoteMirrors,
    chrome: &mut GlobalChrome,
    scrollback_limit: usize,
) {
    let mirror = mirrors.local_mut();
    match frame.frame_type {
        FrameType::Data => {
            if let Some(replica) = mirror.replicas.get_mut(&frame.stream_id) {
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
                        warn!("catalog events lost to server buffer overflow; resyncing");
                        if let Link::Up(session) = link {
                            let id = session.request_id("snapshot");
                            session.pending.insert(id.clone(), Pending::Snapshot);
                            if let Err(err) = session.send_control(&session_snapshot_request(&id)) {
                                warn!(err = %err, "catalog resync snapshot request failed");
                                session.pending.remove(&id);
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
                            if let Link::Up(session) = link {
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
            let Link::Up(session) = link else {
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
                                chrome.connection_status = None;
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
                    if let Some(replica) = mirror.replicas.get_mut(&stream_id) {
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

/// Opens streams for visible panes that lack one, closes streams whose
/// panes left visibility, and pushes stream.resize for panes whose planned
/// geometry changed.
fn sync_pane_streams(
    session: &mut Session,
    mirrors: &mut RemoteMirrors,
    resize_requests: &[crate::terminal::PaneResizeRequest],
    _scrollback_limit: usize,
) {
    let mirror = mirrors.local_mut();
    let has_pane_streams = mirror
        .connection
        .negotiated()
        .is_some_and(|negotiated| negotiated.has_capability(CAPABILITY_PANE_STREAM));
    if !has_pane_streams {
        return;
    }

    // Visible panes: every pane of the focused workspace's active tab.
    let visible: Vec<String> = visible_panes(&mirror.catalog);

    // Streams for panes no longer visible are closed so resource use tracks
    // panes visible, not panes ever visited.
    for (pane_id, stream_id) in hidden_pane_streams(mirror, &visible) {
        debug!(pane = %pane_id, stream = stream_id, "closing pane stream for hidden pane");
        let id = session.request_id("close");
        if let Err(err) = session.send_control(&stream_close_request(&id, stream_id)) {
            warn!(err = %err, "stream.close send failed");
        }
        mirror.stream_closed(stream_id);
        session.sent_sizes.remove(&stream_id);
        session.read_only.remove(&stream_id);
    }

    for pane_id in &visible {
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

    // Geometry: translate planned pane resizes into stream.resize.
    let by_terminal: HashMap<TerminalId, String> = mirror
        .catalog
        .panes
        .iter()
        .map(|pane| {
            (
                TerminalId::from_server(&pane.terminal_id),
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
            if let Some(replica) = mirror.replicas.get_mut(&stream_id) {
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

/// The panes of the focused workspace's active tab.
fn visible_panes(catalog: &SessionCatalog) -> Vec<String> {
    let Some(workspace_id) = catalog.focused_workspace_id.as_deref().or_else(|| {
        catalog
            .workspaces
            .first()
            .map(|workspace| workspace.workspace_id.as_str())
    }) else {
        return Vec::new();
    };
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
fn handle_raw_input(
    raw: crate::raw_input::RawInputEvent,
    link: &mut Link,
    mirrors: &mut RemoteMirrors,
    chrome: &mut GlobalChrome,
    ids: &mut ComposeIds,
    app: &mut AppState,
) {
    match raw {
        crate::raw_input::RawInputEvent::Key(key) => handle_key(key, link, mirrors, ids, app),
        crate::raw_input::RawInputEvent::Paste(text) => {
            if app.mode == Mode::Terminal {
                if let Some((pane_id, bytes)) = encode_paste(mirrors, ids, app, &text) {
                    send_pane_bytes(link, &pane_id, &bytes);
                }
            }
        }
        crate::raw_input::RawInputEvent::Mouse(mouse) => {
            handle_mouse(mouse, link, mirrors, ids, app, chrome)
        }
        _ => {}
    }
}

fn focused_public_pane(
    mirrors: &RemoteMirrors,
    ids: &ComposeIds,
    app: &AppState,
) -> Option<String> {
    let ws = app.active.and_then(|idx| app.workspaces.get(idx))?;
    let pane_id = ws.focused_pane_id()?;
    let public = ids.public_pane_id(pane_id)?;
    mirrors.local().catalog.pane(public)?;
    Some(public.to_owned())
}

fn handle_key(
    key: crate::input::TerminalKey,
    link: &mut Link,
    mirrors: &mut RemoteMirrors,
    ids: &mut ComposeIds,
    app: &mut AppState,
) {
    use crossterm::event::KeyCode;

    if key.kind == crossterm::event::KeyEventKind::Release {
        forward_key(key, link, mirrors, ids, app);
        return;
    }

    match app.mode {
        Mode::Prefix => {
            app.mode = Mode::Terminal;
            match key.code {
                KeyCode::Char('d') => {
                    app.should_quit = true;
                }
                KeyCode::Esc => {}
                _ => super::intent::dispatch_prefix_intent(key, link, mirrors, ids, app),
            }
        }
        Mode::Terminal => {
            if app.is_prefix_key(key) {
                app.mode = Mode::Prefix;
                return;
            }
            if matches!(key.code, KeyCode::PageUp | KeyCode::PageDown)
                && scroll_focused_replica_page(key.code, link, mirrors, ids, app)
            {
                return;
            }
            forward_key(key, link, mirrors, ids, app);
        }
        Mode::Navigate => {
            if key.code == KeyCode::Char('q') {
                app.should_quit = true;
            }
        }
        _ => {
            // Residual modal modes are unsupported under the flag; Esc or q
            // folds back to the base modes instead of trapping the user (or
            // quitting the whole client from inside a dead modal).
            if matches!(key.code, KeyCode::Esc | KeyCode::Char('q')) {
                app.context_menu = None;
                app.mode = Mode::Navigate;
            }
        }
    }
}

/// Encodes and forwards a key to the focused pane.
fn forward_key(
    key: crate::input::TerminalKey,
    link: &mut Link,
    mirrors: &mut RemoteMirrors,
    ids: &mut ComposeIds,
    app: &mut AppState,
) {
    let Some(pane_id) = focused_public_pane(mirrors, ids, app) else {
        return;
    };
    let mirror = mirrors.local();
    let replica = mirror
        .stream_for_pane(&pane_id)
        .and_then(|stream_id| mirror.replicas.get(&stream_id));
    let protocol = replica
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
            .and_then(|replica| crate::pane::plain_terminal_input_state(replica.terminal()))
            .map(|state| state.application_cursor)
            .unwrap_or(false);
        if application_cursor {
            let bytes = crate::input::encode_cursor_key(key.code, true);
            if !bytes.is_empty() {
                send_pane_bytes(link, &pane_id, &bytes);
            }
            return;
        }
    }
    let bytes = crate::input::encode_terminal_key(key, protocol);
    if bytes.is_empty() {
        return;
    }
    send_pane_bytes(link, &pane_id, &bytes);
}

fn encode_paste(
    mirrors: &RemoteMirrors,
    ids: &ComposeIds,
    app: &AppState,
    text: &str,
) -> Option<(String, Vec<u8>)> {
    let pane_id = focused_public_pane(mirrors, ids, app)?;
    let mirror = mirrors.local();
    let bracketed = mirror
        .stream_for_pane(&pane_id)
        .and_then(|stream_id| mirror.replicas.get(&stream_id))
        .and_then(|replica| crate::pane::plain_terminal_input_state(replica.terminal()))
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
    Some((pane_id, bytes))
}

fn send_pane_bytes(link: &mut Link, pane_id: &str, bytes: &[u8]) {
    let Link::Up(session) = link else {
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
fn handle_mouse(
    mouse: MouseEvent,
    link: &mut Link,
    mirrors: &mut RemoteMirrors,
    ids: &mut ComposeIds,
    app: &mut AppState,
    _chrome: &mut GlobalChrome,
) {
    match mouse.kind {
        MouseEventKind::ScrollUp | MouseEventKind::ScrollDown => {
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
                return;
            };
            let Some(public) = ids.public_pane_id(pane_id).map(str::to_owned) else {
                return;
            };
            let mirror = mirrors.local_mut();
            let Some(stream_id) = mirror.stream_for_pane(&public) else {
                return;
            };
            let Some(replica) = mirror.replicas.get_mut(&stream_id) else {
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
                        send_pane_bytes(link, &public, &bytes);
                    }
                }
                return;
            }
            let lines = app.mouse_scroll_lines as isize;
            let delta = if mouse.kind == MouseEventKind::ScrollUp {
                -lines
            } else {
                lines
            };
            replica.scroll_delta(delta);
            request_backfill_if_needed(link, stream_id, mirrors);
        }
        MouseEventKind::Down(_) | MouseEventKind::Up(_) | MouseEventKind::Drag(_)
            if forward_reported_mouse_button(mouse, link, mirrors, ids, app) => {}
        _ => {
            super::intent::dispatch_mouse_intent(mouse, link, mirrors, ids, app);
        }
    }
}

/// Encodes a button/drag event for a mouse-reporting pane and forwards it as
/// pane bytes, focusing the pane first when it was not focused. Returns
/// false when the event is not over a reporting pane, so it falls through to
/// chrome intent dispatch.
fn forward_reported_mouse_button(
    mouse: MouseEvent,
    link: &mut Link,
    mirrors: &mut RemoteMirrors,
    ids: &mut ComposeIds,
    app: &mut AppState,
) -> bool {
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
    let Some(public) = ids.public_pane_id(pane_id).map(str::to_owned) else {
        return false;
    };
    let mirror = mirrors.local();
    let Some(replica) = mirror
        .stream_for_pane(&public)
        .and_then(|stream_id| mirror.replicas.get(&stream_id))
    else {
        return false;
    };
    let Some(state) = crate::pane::plain_terminal_input_state(replica.terminal()) else {
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
                link,
                crate::api::schema::Method::PaneFocus(crate::api::schema::PaneTarget {
                    pane_id: public.clone(),
                }),
            );
        }
    }
    send_pane_bytes(link, &public, &bytes);
    true
}

/// Sends a JSON API request over the framed control plane. Fire-and-forget:
/// the response only surfaces errors, and the resulting catalog events
/// update the mirror.
pub(super) fn send_api_request(link: &mut Link, method: crate::api::schema::Method) {
    let Link::Up(session) = link else {
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

pub(super) use Link as SessionLink;

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyModifiers};

    fn key(code: KeyCode) -> crate::input::TerminalKey {
        crate::input::TerminalKey::new(code, KeyModifiers::empty())
    }

    #[tokio::test]
    async fn residual_modal_modes_never_quit_on_q() {
        let mut link = Link::Incompatible;
        let mut mirrors = RemoteMirrors::with_local();
        let mut ids = ComposeIds::new();
        let mut app = AppState::test_new();

        app.mode = Mode::ContextMenu;
        handle_key(
            key(KeyCode::Char('q')),
            &mut link,
            &mut mirrors,
            &mut ids,
            &mut app,
        );
        assert!(
            !app.should_quit,
            "q inside a dead modal must not quit the client"
        );
        assert_eq!(app.mode, Mode::Navigate);

        app.mode = Mode::ConfirmClose;
        handle_key(
            key(KeyCode::Esc),
            &mut link,
            &mut mirrors,
            &mut ids,
            &mut app,
        );
        assert!(!app.should_quit);
        assert_eq!(app.mode, Mode::Navigate);

        // Navigate itself still quits on q.
        handle_key(
            key(KeyCode::Char('q')),
            &mut link,
            &mut mirrors,
            &mut ids,
            &mut app,
        );
        assert!(app.should_quit);
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

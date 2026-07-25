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
            crate::protocol::framed::CAPABILITY_PASTE_IMAGE,
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

    // Terminal graphics respect the same [experimental] kitty_graphics gate
    // as the server render path: replicas ingest kitty APC data from the
    // pane DATA stream, and the pure client paints visible placements onto
    // the host terminal after each draw.
    crate::kitty_graphics::set_enabled(config.experimental.kitty_graphics);

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
            // Copy mode cannot outlive its pane: a catalog update that
            // removed the pane must drop copy-mode state before rendering.
            if let Some(pane_id) = app.copy_mode.as_ref().map(|copy_mode| copy_mode.pane_id) {
                let alive = ids
                    .public_pane_id(pane_id)
                    .is_some_and(|public| mirrors.local().catalog.pane(public).is_some());
                if !alive {
                    app.clear_copy_mode_for_removed_panes([pane_id]);
                }
            }
            sync_mode(app);
            let mut resize_requests = Vec::new();
            let mut painted_area = ratatui::layout::Rect::default();
            terminal.draw(|frame| {
                let source = MirrorPaneSource::new(mirrors.local());
                resize_requests = crate::ui::compute_view_with_content(app, &source, frame.area());
                app.sync_copy_mode_search_geometry();
                crate::ui::render_with_content(app, &source, frame);
                painted_area = frame.area();
            })?;
            if crate::kitty_graphics::is_enabled() {
                let cell_size =
                    crate::kitty_graphics::HostCellSize::try_from_terminal(painted_area)
                        .unwrap_or_else(|| {
                            crate::kitty_graphics::HostCellSize::fallback_for_area(painted_area)
                        });
                let source = MirrorPaneSource::new(mirrors.local());
                if let Err(err) =
                    crate::kitty_graphics::paint_local_pane_graphics(app, &source, cell_size)
                {
                    debug!(err = %err, "kitty graphics paint failed");
                }
            }
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
                handle_server_frame(
                    frame,
                    &mut link,
                    mirrors,
                    chrome,
                    &config.ui.sound,
                    scrollback_limit,
                );
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
                LoopEvent::Frame(frame) => handle_server_frame(
                    frame,
                    &mut link,
                    mirrors,
                    chrome,
                    &config.ui.sound,
                    scrollback_limit,
                ),
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
    let Some(replica) = mirrors.local_mut().replica_mut(stream_id) else {
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
    sound: &crate::config::SoundConfig,
    scrollback_limit: usize,
) {
    let mirror = mirrors.local_mut();
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
                    crate::protocol::framed::NOTIFICATION_POSTED_EVENT => {
                        // Client-side policy: the server states the fact, this
                        // client decides sound/terminal-toast/system-toast per
                        // its own sound config and notifiers.
                        let Some(data) = payload.get("data").cloned() else {
                            return;
                        };
                        if let Ok(crate::api::schema::events::EventData::NotificationPosted {
                            kind,
                            message,
                            body,
                        }) = serde_json::from_value(data)
                        {
                            crate::client::apply_notification_event(
                                kind,
                                &message,
                                body.as_deref(),
                                sound,
                            );
                        }
                    }
                    crate::protocol::framed::WINDOW_TITLE_CHANGED_EVENT => {
                        let Some(data) = payload.get("data").cloned() else {
                            return;
                        };
                        if let Ok(crate::api::schema::events::EventData::WindowTitleChanged {
                            title,
                        }) = serde_json::from_value(data)
                        {
                            apply_remote_window_title(chrome, mirror.remote_index, title);
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

/// Applies a `window_title.changed` fact from a remote to the host
/// terminal. Focused-remote-wins: with a single remote today the local
/// remote is always focused, so the title applies directly; when
/// multi-remote lands, only the focused remote's title is written and this
/// selector gains the focus check.
fn apply_remote_window_title(
    chrome: &mut GlobalChrome,
    _remote_index: usize,
    title: Option<String>,
) {
    if chrome.window_title == title {
        return;
    }
    crate::client::write_window_title(title.as_deref());
    chrome.window_title = title;
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
            if matches!(
                app.mode,
                Mode::RenameWorkspace | Mode::RenameTab | Mode::RenamePane
            ) {
                crate::app::insert_rename_input_text(app, &text);
                return;
            }
            if app.mode == Mode::Terminal {
                if try_paste_image(&text, link, mirrors, ids, app) {
                    return;
                }
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
        // Legacy parity: only Terminal mode forwards key events to the
        // pane. Releases of keys typed into modals (rename, confirm-close,
        // context menu) or copy mode must not leak CSI-u release reports
        // into kitty-protocol panes behind the modal.
        if app.mode == Mode::Terminal {
            forward_key(key, link, mirrors, ids, app);
        }
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
                _ if app.keybinds.copy_mode.matches_prefix_key(key) => {
                    let source = MirrorPaneSource::new(mirrors.local());
                    app.enter_copy_mode(&source);
                }
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
            // Retained-selection copy (legacy try_copy_retained_selection):
            // with copy_on_select off, the copy chord copies a finalized
            // mouse selection out of the replica instead of reaching the
            // pane. An empty extraction falls through and forwards the key,
            // matching the legacy client.
            if !app.copy_on_select
                && crate::app::is_retained_selection_copy_key(key)
                && app
                    .selection
                    .as_ref()
                    .is_some_and(crate::selection::Selection::is_finalized)
            {
                {
                    let source = MirrorPaneSource::new(mirrors.local());
                    app.copy_selection(&source);
                }
                if let Some(content) = app.request_clipboard_write.take() {
                    crate::selection::write_osc52_bytes(&content);
                    return;
                }
            }
            forward_key(key, link, mirrors, ids, app);
        }
        Mode::Navigate => {
            if key.code == KeyCode::Char('q') {
                app.should_quit = true;
            }
        }
        Mode::Copy => {
            if app.is_prefix_key(key) {
                app.mode = Mode::Prefix;
                return;
            }
            let copy_pane = app.copy_mode.as_ref().map(|copy_mode| copy_mode.pane_id);
            {
                // Copy mode runs entirely against the replica through the
                // pane-content seam: search, motions, selection, scrolling.
                let source = MirrorPaneSource::new(mirrors.local());
                app.handle_copy_mode_key(&source, key);
            }
            // OSC52: the pure client is the host terminal, so a pending
            // clipboard write goes straight to stdout.
            if let Some(content) = app.request_clipboard_write.take() {
                crate::selection::write_osc52_bytes(&content);
            }
            // Scrolling near the top of loaded history pages more in.
            if let Some(stream_id) = copy_pane
                .and_then(|pane_id| ids.public_pane_id(pane_id))
                .and_then(|public| mirrors.local().stream_for_pane(public))
            {
                request_backfill_if_needed(link, stream_id, mirrors);
            }
        }
        Mode::RenameWorkspace
        | Mode::RenameTab
        | Mode::RenamePane
        | Mode::ConfirmClose
        | Mode::ContextMenu => {
            super::intent::dispatch_modal_key(key, link, mirrors, ids, app);
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
    Some((pane_id, bytes))
}

/// Bridges an image paste into `pane.paste_image`: an empty bracketed
/// paste means the host clipboard holds an image instead of text. Pasted
/// file paths (terminal file drops) stay plain text — the pure client is
/// local, so the server reads the same filesystem and the legacy local
/// client pastes the path bytes unchanged; path-to-image bridging is a
/// remote-client concern. Returns false to fall through to a text paste.
fn try_paste_image(
    text: &str,
    link: &mut Link,
    mirrors: &RemoteMirrors,
    ids: &ComposeIds,
    app: &AppState,
) -> bool {
    let Some(pane_id) = focused_public_pane(mirrors, ids, app) else {
        return false;
    };
    let negotiated_paste_image =
        mirrors
            .local()
            .connection
            .negotiated()
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
    let Link::Up(session) = link else {
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
    chrome: &mut GlobalChrome,
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
            if handle_pane_double_click(mouse, mirrors, ids, app, chrome) {
                return;
            }
            super::intent::dispatch_mouse_intent(mouse, link, mirrors, ids, app);
        }
    }
}

/// Double-click word selection against the replica, mirroring the legacy
/// `App::handle_pane_double_click` gesture: two adjacent left-clicks in the
/// same pane cell within the double-click window select the token under
/// the cursor, and copy_on_select sends it out as OSC52. Mouse-reporting
/// panes never reach this (their buttons forward upstream). Returns true
/// when the double-click was consumed.
fn handle_pane_double_click(
    mouse: MouseEvent,
    mirrors: &RemoteMirrors,
    _ids: &ComposeIds,
    app: &mut AppState,
    chrome: &mut GlobalChrome,
) -> bool {
    use crossterm::event::MouseButton;

    // A pane press stops being a double-click candidate once it becomes a
    // drag or completes as a real text selection (legacy parity).
    match mouse.kind {
        MouseEventKind::Drag(MouseButton::Left) => {
            chrome.last_pane_click = None;
            return false;
        }
        MouseEventKind::Up(MouseButton::Left)
            if app
                .selection
                .as_ref()
                .is_some_and(crate::selection::Selection::is_visible) =>
        {
            chrome.last_pane_click = None;
            return false;
        }
        MouseEventKind::Down(MouseButton::Left) => {}
        _ => return false,
    }

    if !mouse.modifiers.is_empty() || app.mode != Mode::Terminal {
        chrome.last_pane_click = None;
        return false;
    }
    let Some(info) = app
        .view
        .pane_infos
        .iter()
        .find(|info| {
            info.inner_rect
                .contains(ratatui::layout::Position::new(mouse.column, mouse.row))
        })
        .cloned()
    else {
        chrome.last_pane_click = None;
        return false;
    };
    let viewport_row = mouse.row.saturating_sub(info.inner_rect.y);
    let col = mouse.column.saturating_sub(info.inner_rect.x);
    let click = crate::app::PaneClickState::new(info.id, viewport_row, col);
    if !chrome
        .last_pane_click
        .take()
        .is_some_and(|last| last.is_double_click_for(click))
    {
        chrome.last_pane_click = Some(click);
        return false;
    }

    let selected = {
        let source = MirrorPaneSource::new(mirrors.local());
        app.select_word_at_pane_cell(&source, info.id, viewport_row, col)
    };
    // copy_on_select word copies go straight out as OSC52, like every
    // other pure-client copy.
    if let Some(content) = app.request_clipboard_write.take() {
        crate::selection::write_osc52_bytes(&content);
    }
    selected
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

        // Context menus are interpreted client-side now: q does nothing and
        // never quits; Esc closes the menu back to a base mode.
        app.mode = Mode::ContextMenu;
        handle_key(
            key(KeyCode::Char('q')),
            &mut link,
            &mut mirrors,
            &mut ids,
            &mut app,
        );
        assert!(!app.should_quit, "q inside a modal must not quit");
        assert_eq!(app.mode, Mode::ContextMenu);
        handle_key(
            key(KeyCode::Esc),
            &mut link,
            &mut mirrors,
            &mut ids,
            &mut app,
        );
        assert!(!app.should_quit);
        assert_eq!(app.mode, Mode::Navigate);

        // Confirm-close cancels on Esc.
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

        // Still-unsupported modal modes fold back to Navigate on Esc/q.
        app.mode = Mode::Settings;
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
        let session = Session {
            stream: client,
            next_request_id: 1,
            pending: HashMap::new(),
            sent_sizes: HashMap::new(),
            read_only: HashSet::new(),
        };
        (session, server)
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
        let mut mirrors = RemoteMirrors::with_local();
        *mirrors.local_mut() = mirror_with_replica("hello\r\n");
        let chrome = GlobalChrome::new();
        let mut ids = ComposeIds::new();
        let mut app = AppState::test_new();
        compose_into(mirrors.local(), &chrome, &mut ids, &mut app);
        app.keybinds = crate::config::Config::default().keybinds();

        let (session, mut server) = session_pair();
        let mut link = Link::Up(session);

        // Positive control: a Terminal-mode press reaches the pane.
        app.mode = Mode::Terminal;
        handle_key(
            key(KeyCode::Char('a')),
            &mut link,
            &mut mirrors,
            &mut ids,
            &mut app,
        );
        let frame = try_read_control(&mut server).expect("terminal press forwards to the pane");
        assert_eq!(frame["method"], "pane.send_bytes");

        // A press typed into a rename modal edits the modal, not the pane.
        app.mode = Mode::RenameWorkspace;
        app.name_input.clear();
        app.name_input_replace_on_type = false;
        handle_key(
            key(KeyCode::Char('b')),
            &mut link,
            &mut mirrors,
            &mut ids,
            &mut app,
        );
        assert_eq!(app.name_input, "b");
        assert!(
            try_read_control(&mut server).is_none(),
            "modal press must not reach the pane"
        );

        // The matching Release must not leak into the pane either (legacy
        // parity: key events are only forwarded from Terminal mode).
        handle_key(
            crate::input::TerminalKey::new(KeyCode::Char('b'), KeyModifiers::empty())
                .with_kind(crossterm::event::KeyEventKind::Release),
            &mut link,
            &mut mirrors,
            &mut ids,
            &mut app,
        );
        assert!(
            try_read_control(&mut server).is_none(),
            "modal key release must not reach the pane"
        );
    }

    #[tokio::test]
    async fn retained_selection_copy_key_copies_replica_text_instead_of_forwarding() {
        let mut mirrors = RemoteMirrors::with_local();
        *mirrors.local_mut() = mirror_with_replica("alpha bravo charlie\r\n");
        let chrome = GlobalChrome::new();
        let mut ids = ComposeIds::new();
        let mut app = AppState::test_new();
        compose_into(mirrors.local(), &chrome, &mut ids, &mut app);
        app.keybinds = crate::config::Config::default().keybinds();
        app.mode = Mode::Terminal;
        app.copy_on_select = false;

        let pane_id = app.workspaces[0]
            .focused_pane_id()
            .expect("composed focused pane");
        let metrics = {
            let source = MirrorPaneSource::new(mirrors.local());
            app.pane_scroll_metrics(&source, pane_id)
        };
        let mut selection = crate::selection::Selection::range(pane_id, 0, 0, 4, metrics);
        assert!(selection.finish());
        app.selection = Some(selection);

        let (session, mut server) = session_pair();
        let mut link = Link::Up(session);

        // Ctrl-C copies the retained selection out of the replica and is
        // not forwarded to the pane.
        handle_key(
            crate::input::TerminalKey::new(KeyCode::Char('c'), KeyModifiers::CONTROL),
            &mut link,
            &mut mirrors,
            &mut ids,
            &mut app,
        );
        assert!(app.selection.is_none(), "copy consumes the selection");
        assert!(
            try_read_control(&mut server).is_none(),
            "the copy chord must not reach the pane"
        );

        // With copy_on_select the chord forwards to the pane instead.
        app.copy_on_select = true;
        handle_key(
            crate::input::TerminalKey::new(KeyCode::Char('c'), KeyModifiers::CONTROL),
            &mut link,
            &mut mirrors,
            &mut ids,
            &mut app,
        );
        let frame = try_read_control(&mut server).expect("ctrl-c forwards with copy_on_select");
        assert_eq!(frame["method"], "pane.send_bytes");
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

        let mut mirrors = RemoteMirrors::with_local();
        *mirrors.local_mut() = mirror_with_replica("alpha bravo charlie\r\n");
        let mut chrome = GlobalChrome::new();
        let mut ids = ComposeIds::new();
        let mut app = AppState::test_new();
        compose_into(mirrors.local(), &chrome, &mut ids, &mut app);
        app.keybinds = crate::config::Config::default().keybinds();
        app.mode = Mode::Terminal;
        app.copy_on_select = false;

        {
            let source = MirrorPaneSource::new(mirrors.local());
            let _requests = crate::ui::compute_view_with_content(
                &mut app,
                &source,
                ratatui::layout::Rect::new(0, 0, 106, 26),
            );
        }
        let inner = app.view.pane_infos.first().expect("pane info").inner_rect;
        // Two clicks on the same cell inside "bravo".
        let click = MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: inner.x + 7,
            row: inner.y,
            modifiers: KeyModifiers::empty(),
        };
        assert!(
            !handle_pane_double_click(click, &mirrors, &ids, &mut app, &mut chrome),
            "first click only arms the candidate"
        );
        assert!(
            handle_pane_double_click(click, &mirrors, &ids, &mut app, &mut chrome),
            "second click selects the word"
        );
        let selection = app.selection.clone().expect("word selection");
        assert!(selection.is_finalized());
        let source = MirrorPaneSource::new(mirrors.local());
        app.copy_selection(&source);
        let copied = app.request_clipboard_write.take().expect("copied word");
        assert_eq!(String::from_utf8_lossy(&copied), "bravo");
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

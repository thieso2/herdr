//! Framed pane-stream attach client.
//!
//! This is the unified-protocol replacement for direct terminal attach. It
//! speaks the framed protocol on the API socket instead of the private TUI
//! client socket: `HRDR` magic, `session.hello` with the `pane-stream`
//! capability, then `stream.open` in write mode. From there the pane's raw PTY
//! output arrives as DATA frames that go straight to stdout, and everything
//! flowing the other way — input, resize, scroll, close — is a control-plane
//! method on the same connection.
//!
//! Exclusivity is a write grant keyed on the stream id, not a client-keyed
//! lock: a second writer without takeover is refused with a structured
//! `pane_write_locked` error while its connection stays up, and a takeover
//! revokes the previous holder through a `stream.revoked` event on that
//! holder's own stream.
//!
//! Servers that do not advertise `pane-stream` report
//! [`FramedAttachOutcome::Unsupported`] so the caller can fall back to the
//! legacy client path for one release window.

use std::io::{self, BufRead as _, Write as _};
#[cfg(unix)]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(unix)]
use std::sync::{mpsc, Arc};
use std::time::Duration;

use base64::Engine as _;
use interprocess::local_socket::traits::Stream as _;
use interprocess::TryClone as _;
use tracing::{debug, info, warn};

use crate::ipc::LocalStream;
#[cfg(unix)]
use crate::protocol::framed::Frame;
use crate::protocol::framed::{
    control_error, pane_send_bytes_request, parse_session_welcome, parse_stream_opened, read_frame,
    session_hello_request, stream_close_request, stream_open_request, stream_resize_request,
    stream_scroll_request, write_frame, ControlError, FrameType, FramedCodecError, StreamMode,
    StreamOpened, StreamScrollDirection, StreamScrollParams, StreamScrollSource,
    CAPABILITY_PANE_STREAM, CONTROL_STREAM_ID, FRAMED_MAGIC, STREAM_CLOSED_EVENT,
    STREAM_REVOKED_EVENT,
};

/// How long a framed client waits for the `session.hello` answer.
const HELLO_TIMEOUT: Duration = Duration::from_secs(5);

/// Bound on the interactive attach event channel; see `run_framed_attach`.
#[cfg(unix)]
const FRAME_CHANNEL_BOUND: usize = 1024;

/// How the framed attach attempt ended.
pub(super) enum FramedAttachOutcome {
    /// The framed client ran to completion; nothing else to do.
    Finished,
    /// This server does not speak framed pane streams. The caller falls back
    /// to the legacy client path.
    Unsupported,
}

/// A negotiated framed session on the API socket.
struct FramedClient {
    stream: LocalStream,
    next_request_id: u64,
}

impl FramedClient {
    /// Connects and negotiates a pane-stream session, or reports that this
    /// server cannot serve one.
    ///
    /// Every handshake failure maps to `Ok(None)`: a pre-framed server never
    /// answers the binary hello (its JSON-line reader waits for a newline the
    /// framed frames do not carry, so the bounded recv times out with an io
    /// error), and a server that answers with anything but a framed welcome is
    /// equally unable to serve pane streams. Both must fall back to the legacy
    /// client path instead of failing the attach.
    fn connect() -> io::Result<Option<Self>> {
        let socket_path = crate::api::socket_path();
        let stream = match crate::ipc::connect_local_stream(&socket_path) {
            Ok(stream) => stream,
            Err(err) => {
                debug!(path = %socket_path.display(), err = %err, "framed attach could not reach the api socket");
                return Ok(None);
            }
        };
        match Self::negotiate(stream) {
            Ok(client) => Ok(Some(client)),
            Err(err) => {
                debug!(err = %err, "framed handshake failed; falling back to the legacy client");
                Ok(None)
            }
        }
    }

    /// Runs the framed handshake on a fresh connection: `HRDR` magic, then
    /// `session.hello`, expecting a welcome carrying the `pane-stream`
    /// capability.
    fn negotiate(mut stream: LocalStream) -> io::Result<Self> {
        stream.set_nonblocking(false)?;
        stream.write_all(&FRAMED_MAGIC)?;
        stream.flush()?;

        let mut client = Self {
            stream,
            next_request_id: 1,
        };
        let id = client.request_id("hello");
        client.send_control(&session_hello_request(&id))?;

        // Bound the handshake so a wedged or pre-framed server cannot hang
        // the attach; the timeout surfaces as an io error and the caller
        // falls back to the legacy client.
        let _ = client.stream.set_recv_timeout(Some(HELLO_TIMEOUT));
        let response = client.read_control()?;
        let _ = client.stream.set_recv_timeout(None);

        let welcome = parse_session_welcome(&response).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("framed session rejected: {err}"),
            )
        })?;
        if !welcome
            .capabilities
            .iter()
            .any(|capability| capability == CAPABILITY_PANE_STREAM)
        {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "server {} does not advertise pane streams",
                    welcome.server_version
                ),
            ));
        }
        crate::logging::startup("client");
        info!(
            protocol = welcome.protocol,
            server_version = %welcome.server_version,
            "framed pane-stream session negotiated"
        );
        Ok(client)
    }

    fn request_id(&mut self, kind: &str) -> String {
        let id = self.next_request_id;
        self.next_request_id += 1;
        format!("attach:{kind}:{id}")
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
        .map_err(codec_error_to_io)
    }

    /// Reads frames until the next control frame. Used before any stream is
    /// open, so no DATA frames can be lost here.
    fn read_control(&mut self) -> io::Result<serde_json::Value> {
        loop {
            let frame = read_frame(&mut self.stream).map_err(codec_error_to_io)?;
            if frame.frame_type == FrameType::Control {
                return serde_json::from_slice(&frame.payload).map_err(|err| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid control frame payload: {err}"),
                    )
                });
            }
        }
    }

    /// Opens a pane stream on `target`, which may be a pane id, a terminal id,
    /// or an agent name.
    fn open_stream(
        &mut self,
        target: &str,
        mode: StreamMode,
        takeover: bool,
        cols: Option<u16>,
        rows: Option<u16>,
    ) -> io::Result<Result<StreamOpened, ControlError>> {
        let id = self.request_id("open");
        self.send_control(&stream_open_request(
            &id, target, mode, takeover, cols, rows,
        ))?;
        let response = self.read_control()?;
        match parse_stream_opened(&response) {
            Ok(opened) => Ok(Ok(opened)),
            Err(Some(error)) => Ok(Err(error)),
            Err(None) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "stream.open answer was not a pane stream",
            )),
        }
    }
}

fn codec_error_to_io(err: FramedCodecError) -> io::Error {
    match err {
        FramedCodecError::Io(err) => err,
        FramedCodecError::UnexpectedEof => {
            io::Error::new(io::ErrorKind::UnexpectedEof, "framed connection closed")
        }
        other => io::Error::new(io::ErrorKind::InvalidData, other.to_string()),
    }
}

/// Prints a `stream.open` rejection and exits, mirroring the legacy attach
/// error path. A refused write grant reports `pane_write_locked` with the
/// holder in its message instead of tearing the connection down.
fn exit_with_open_error(context: &str, error: &ControlError) -> ! {
    debug!(code = %error.code, "framed stream.open rejected");
    eprintln!("herdr: {context} failed: {}", error.message);
    std::process::exit(1);
}

// ---------------------------------------------------------------------------
// Interactive attach (raw terminal)
// ---------------------------------------------------------------------------

/// Why the interactive attach loop stopped.
#[cfg(unix)]
enum AttachExit {
    /// The user pressed the detach key sequence, or Ctrl+C.
    Detached,
    /// The pane behind the stream went away.
    PaneClosed,
    /// Another client took the pane's write grant over.
    TakenOver,
    /// The server connection ended under us (shutdown, handoff, crash).
    Disconnected,
}

/// Events the attach loop multiplexes.
#[cfg(unix)]
enum AttachEvent {
    Stdin(Vec<u8>),
    Resize(u16, u16),
    Frame(Frame),
    Disconnected,
}

/// Runs an interactive framed attach against `target`.
#[cfg(unix)]
pub(super) fn run_framed_attach(target: &str, takeover: bool) -> io::Result<FramedAttachOutcome> {
    let Some(mut client) = FramedClient::connect()? else {
        return Ok(FramedAttachOutcome::Unsupported);
    };

    let loaded_config = crate::config::Config::load();
    let mouse_scroll_lines = loaded_config.config.ui.mouse_scroll_lines();

    // Geometry is read before raw mode, exactly like the legacy client.
    let (cols, rows) = crossterm::terminal::size().unwrap_or((80, 24));
    let opened =
        match client.open_stream(target, StreamMode::Write, takeover, Some(cols), Some(rows))? {
            Ok(opened) => opened,
            Err(error) => exit_with_open_error("terminal attach", &error),
        };
    info!(
        pane = %opened.pane_id,
        stream = opened.stream_id,
        "framed terminal attach opened"
    );

    // The terminal is taken over only after the stream is open, so a refused
    // attach never leaves the host terminal in raw mode.
    let terminal_guard = super::setup_direct_attach_terminal().inspect_err(|err| {
        eprintln!("herdr: failed to set up terminal: {err}");
    })?;
    let panic_resets_modify_other_keys = terminal_guard.reset_modify_other_keys;
    let panic_resets_host_color_scheme_reports = terminal_guard.reset_host_color_scheme_reports;
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        super::restore_terminal_state(
            panic_resets_modify_other_keys,
            panic_resets_host_color_scheme_reports,
        );
        original_hook(info);
    }));

    seed_screen_with_snapshot(&opened.snapshot);

    let should_quit = Arc::new(AtomicBool::new(false));
    let ctrlc_quit = Arc::clone(&should_quit);
    let _ = ctrlc::set_handler(move || {
        ctrlc_quit.store(true, Ordering::Release);
    });

    // Bounded: when host stdout blocks (for example XOFF), the socket reader
    // stops draining once the channel fills, which lets the server's bounded
    // pane-output buffer apply its overload protection instead of this
    // client buffering the pane's output without limit.
    let (event_tx, event_rx) = mpsc::sync_channel::<AttachEvent>(FRAME_CHANNEL_BOUND);
    let read_stream = client.stream.try_clone()?;
    let reader_quit = Arc::clone(&should_quit);
    let frame_tx = event_tx.clone();
    std::thread::spawn(move || socket_reader_loop(read_stream, frame_tx, &reader_quit));
    spawn_host_input_threads(&event_tx, cols, rows, &should_quit);

    let mut escape = super::AttachEscapeState::default();
    let mut viewport_rows = rows;
    let exit = loop {
        if should_quit.load(Ordering::Acquire) {
            break AttachExit::Detached;
        }
        let event = match event_rx.recv_timeout(Duration::from_millis(100)) {
            Ok(event) => event,
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => break AttachExit::Disconnected,
        };

        match event {
            AttachEvent::Stdin(data) => {
                match escape.filter_input(data, viewport_rows, mouse_scroll_lines) {
                    super::AttachInputAction::Forward(bytes) => {
                        let id = client.request_id("input");
                        if let Err(err) = client.send_control(&pane_send_bytes_request(
                            &id,
                            &opened.pane_id,
                            &bytes,
                        )) {
                            warn!(err = %err, "framed attach input failed");
                            break AttachExit::Disconnected;
                        }
                    }
                    super::AttachInputAction::Scroll {
                        source,
                        direction,
                        lines,
                        column,
                        row,
                        modifiers,
                    } => {
                        let params = StreamScrollParams {
                            stream_id: opened.stream_id,
                            direction: match direction {
                                crate::protocol::AttachScrollDirection::Up => {
                                    StreamScrollDirection::Up
                                }
                                crate::protocol::AttachScrollDirection::Down => {
                                    StreamScrollDirection::Down
                                }
                            },
                            lines,
                            source: match source {
                                crate::protocol::AttachScrollSource::Wheel => {
                                    StreamScrollSource::Wheel
                                }
                                crate::protocol::AttachScrollSource::PageKey { .. } => {
                                    StreamScrollSource::PageKey
                                }
                            },
                            column,
                            row,
                            modifiers,
                        };
                        let id = client.request_id("scroll");
                        if let Err(err) = client.send_control(&stream_scroll_request(&id, params)) {
                            warn!(err = %err, "framed attach scroll failed");
                            break AttachExit::Disconnected;
                        }
                    }
                    super::AttachInputAction::Detach => {
                        let id = client.request_id("close");
                        let _ = client.send_control(&stream_close_request(&id, opened.stream_id));
                        break AttachExit::Detached;
                    }
                    super::AttachInputAction::None => {}
                }
            }
            AttachEvent::Resize(new_cols, new_rows) => {
                viewport_rows = new_rows;
                let id = client.request_id("resize");
                if let Err(err) = client.send_control(&stream_resize_request(
                    &id,
                    opened.stream_id,
                    new_cols,
                    new_rows,
                    0,
                    0,
                )) {
                    warn!(err = %err, "framed attach resize failed");
                    break AttachExit::Disconnected;
                }
            }
            AttachEvent::Frame(frame) => match frame.frame_type {
                FrameType::Data if frame.stream_id == opened.stream_id => {
                    let mut stdout = io::stdout();
                    let _ = stdout.write_all(&frame.payload);
                    let _ = stdout.flush();
                }
                FrameType::Data => {}
                FrameType::Control => {
                    if let Some(exit) = control_frame_exit(&frame, opened.stream_id) {
                        break exit;
                    }
                }
            },
            AttachEvent::Disconnected => break AttachExit::Disconnected,
        }
    };

    should_quit.store(true, Ordering::Release);
    // Restore the terminal before printing anything.
    drop(terminal_guard);

    match exit {
        AttachExit::Detached => Ok(FramedAttachOutcome::Finished),
        AttachExit::PaneClosed => {
            eprintln!("herdr: pane closed");
            Ok(FramedAttachOutcome::Finished)
        }
        AttachExit::TakenOver => {
            eprintln!("herdr: terminal attach taken over");
            std::process::exit(1);
        }
        AttachExit::Disconnected => {
            eprintln!("herdr: server connection closed; reattach once the server is back");
            std::process::exit(1);
        }
    }
}

/// Interprets a control frame arriving during an interactive attach.
#[cfg(unix)]
fn control_frame_exit(frame: &Frame, stream_id: u32) -> Option<AttachExit> {
    let payload: serde_json::Value = serde_json::from_slice(&frame.payload).ok()?;
    let event = payload.get("event").and_then(|value| value.as_str());
    let event_stream_id = payload
        .get("data")
        .and_then(|data| data.get("stream_id"))
        .and_then(|value| value.as_u64());
    if event_stream_id.is_some_and(|id| id != stream_id as u64) {
        return None;
    }
    match event {
        Some(STREAM_CLOSED_EVENT) => Some(AttachExit::PaneClosed),
        Some(STREAM_REVOKED_EVENT) => Some(AttachExit::TakenOver),
        _ => {
            if let Some(error) = control_error(&payload) {
                warn!(code = %error.code, message = %error.message, "framed attach control error");
                if error.code == "stream_overloaded" {
                    return Some(AttachExit::Disconnected);
                }
            }
            None
        }
    }
}

/// Clears the host screen and paints the pane snapshot the stream opened with,
/// so the attach starts from the pane's current screen instead of a blank one.
#[cfg(unix)]
fn seed_screen_with_snapshot(snapshot: &str) {
    let mut stdout = io::stdout();
    let _ = stdout.write_all(b"\x1b[2J\x1b[H");
    let _ = stdout.write_all(snapshot.as_bytes());
    let _ = stdout.flush();
}

/// Bridges the shared stdin reader and resize poller onto the attach event
/// channel, so the framed client reuses the same input framing and resize
/// detection as the legacy client.
#[cfg(unix)]
fn spawn_host_input_threads(
    event_tx: &mpsc::SyncSender<AttachEvent>,
    cols: u16,
    rows: u16,
    should_quit: &Arc<AtomicBool>,
) {
    let (loop_tx, mut loop_rx) = tokio::sync::mpsc::channel::<super::ClientLoopEvent>(256);

    let stdin_quit = Arc::clone(should_quit);
    let stdin_tx = loop_tx.clone();
    // Direct attach captures the mouse and never queries the host theme.
    let host_mouse_capture_active = Arc::new(AtomicBool::new(true));
    std::thread::spawn(move || {
        super::input::stdin_reader_loop(stdin_tx, &stdin_quit, false, host_mouse_capture_active);
    });

    let resize_quit = Arc::clone(should_quit);
    std::thread::spawn(move || {
        super::resize_poll_loop(loop_tx, cols, rows, false, &resize_quit);
    });

    let bridge_tx = event_tx.clone();
    std::thread::spawn(move || {
        while let Some(event) = loop_rx.blocking_recv() {
            let bridged = match event {
                super::ClientLoopEvent::StdinInput(data) => AttachEvent::Stdin(data),
                super::ClientLoopEvent::Resize(cols, rows, _, _) => AttachEvent::Resize(cols, rows),
                _ => continue,
            };
            if bridge_tx.send(bridged).is_err() {
                break;
            }
        }
    });
}

/// Reads frames off the session socket into the attach event channel.
#[cfg(unix)]
fn socket_reader_loop(
    mut stream: LocalStream,
    event_tx: mpsc::SyncSender<AttachEvent>,
    should_quit: &Arc<AtomicBool>,
) {
    if stream.set_nonblocking(false).is_err() {
        let _ = event_tx.send(AttachEvent::Disconnected);
        return;
    }
    while !should_quit.load(Ordering::Acquire) {
        match read_frame(&mut stream) {
            Ok(frame) => {
                if event_tx.send(AttachEvent::Frame(frame)).is_err() {
                    return;
                }
            }
            Err(err) => {
                debug!(err = %err, "framed attach session read ended");
                let _ = event_tx.send(AttachEvent::Disconnected);
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Headless terminal session clients
// ---------------------------------------------------------------------------

/// Runs a read-only framed terminal session observer, printing newline
/// delimited JSON records.
pub(super) fn run_framed_session_observe(
    target: &str,
    cols: u16,
    rows: u16,
) -> io::Result<FramedAttachOutcome> {
    let Some(mut client) = FramedClient::connect()? else {
        return Ok(FramedAttachOutcome::Unsupported);
    };
    let opened =
        match client.open_stream(target, StreamMode::Read, false, Some(cols), Some(rows))? {
            Ok(opened) => opened,
            Err(error) => exit_with_open_error("terminal session observe", &error),
        };
    write_session_snapshot_record(&opened)?;
    pump_session_records(client.stream, opened.stream_id, opened.sequence)?;
    Ok(FramedAttachOutcome::Finished)
}

/// Runs a writable framed terminal session controller: JSON commands on
/// stdin, newline delimited JSON output records on stdout.
pub(super) fn run_framed_session_control(
    target: &str,
    takeover: bool,
    cols: u16,
    rows: u16,
) -> io::Result<FramedAttachOutcome> {
    let Some(mut client) = FramedClient::connect()? else {
        return Ok(FramedAttachOutcome::Unsupported);
    };
    let opened =
        match client.open_stream(target, StreamMode::Write, takeover, Some(cols), Some(rows))? {
            Ok(opened) => opened,
            Err(error) => exit_with_open_error("terminal session control", &error),
        };
    write_session_snapshot_record(&opened)?;

    let mut command_stream = client.stream.try_clone()?;
    let pane_id = opened.pane_id.clone();
    let stream_id = opened.stream_id;
    let _input_thread = std::thread::spawn(move || {
        let stdin = io::stdin();
        let mut request_id = 0_u64;
        for line in stdin.lock().lines() {
            let Ok(line) = line else {
                break;
            };
            if line.trim().is_empty() {
                continue;
            }
            request_id += 1;
            let id = format!("session:{request_id}");
            match framed_session_control_request(&id, &pane_id, stream_id, &line) {
                Ok(request) => {
                    let release = request.is_none();
                    let request = request.unwrap_or_else(|| stream_close_request(&id, stream_id));
                    if send_control_on(&mut command_stream, &request).is_err() {
                        return;
                    }
                    if release {
                        return;
                    }
                }
                Err(err) => eprintln!("herdr: terminal session control input ignored: {err}"),
            }
        }
        let id = format!("session:{}", request_id + 1);
        let _ = send_control_on(&mut command_stream, &stream_close_request(&id, stream_id));
    });

    pump_session_records(client.stream, opened.stream_id, opened.sequence)?;
    Ok(FramedAttachOutcome::Finished)
}

fn send_control_on(stream: &mut LocalStream, value: &serde_json::Value) -> io::Result<()> {
    let payload = serde_json::to_vec(value)
        .map_err(|err| io::Error::other(format!("failed to encode control frame: {err}")))?;
    write_frame(stream, FrameType::Control, CONTROL_STREAM_ID, &payload).map_err(codec_error_to_io)
}

/// Translates one line of the terminal session control JSON vocabulary into a
/// framed control request. `Ok(None)` means "release the stream".
fn framed_session_control_request(
    id: &str,
    pane_id: &str,
    stream_id: u32,
    raw: &str,
) -> Result<Option<serde_json::Value>, String> {
    match super::terminal_control_command_from_json_value(raw)? {
        super::TerminalSessionCommand::Input { data } => {
            Ok(Some(pane_send_bytes_request(id, pane_id, &data)))
        }
        super::TerminalSessionCommand::Resize {
            cols,
            rows,
            cell_width_px,
            cell_height_px,
        } => Ok(Some(stream_resize_request(
            id,
            stream_id,
            cols,
            rows,
            cell_width_px,
            cell_height_px,
        ))),
        super::TerminalSessionCommand::Scroll {
            direction,
            lines,
            source,
            column,
            row,
            modifiers,
        } => Ok(Some(stream_scroll_request(
            id,
            StreamScrollParams {
                stream_id,
                direction: match direction {
                    super::TerminalSessionScrollDirection::Up => StreamScrollDirection::Up,
                    super::TerminalSessionScrollDirection::Down => StreamScrollDirection::Down,
                },
                lines,
                source: match source {
                    super::TerminalSessionScrollSource::Wheel => StreamScrollSource::Wheel,
                    super::TerminalSessionScrollSource::PageKey => StreamScrollSource::PageKey,
                },
                column,
                row,
                modifiers,
            },
        ))),
        super::TerminalSessionCommand::Release => Ok(None),
    }
}

/// Prints the opening snapshot record of a headless terminal session.
fn write_session_snapshot_record(opened: &StreamOpened) -> io::Result<()> {
    write_session_record(&serde_json::json!({
        "type": "terminal.frame",
        "seq": opened.sequence,
        "encoding": "ansi",
        "full": true,
        "pane_id": opened.pane_id,
        "bytes": base64::engine::general_purpose::STANDARD.encode(opened.snapshot.as_bytes()),
    }))
}

/// Streams the pane output tail as newline delimited JSON records until the
/// stream or the connection ends. `sequence` starts at the pane byte offset
/// the snapshot record reported, so `seq` stays monotonic across the whole
/// session instead of restarting after the opening snapshot.
fn pump_session_records(
    mut stream: LocalStream,
    stream_id: u32,
    start_sequence: u64,
) -> io::Result<()> {
    let mut sequence = start_sequence;
    loop {
        let frame = match read_frame(&mut stream) {
            Ok(frame) => frame,
            Err(FramedCodecError::UnexpectedEof) => {
                return write_session_record(&serde_json::json!({
                    "type": "terminal.closed",
                    "reason": "server connection closed",
                }));
            }
            Err(err) => return Err(codec_error_to_io(err)),
        };
        match frame.frame_type {
            FrameType::Data if frame.stream_id == stream_id => {
                sequence = sequence.saturating_add(frame.payload.len() as u64);
                write_session_record(&serde_json::json!({
                    "type": "terminal.frame",
                    "seq": sequence,
                    "encoding": "ansi",
                    "full": false,
                    "bytes": base64::engine::general_purpose::STANDARD.encode(&frame.payload),
                }))?;
            }
            FrameType::Data => {}
            FrameType::Control => {
                let payload: serde_json::Value = match serde_json::from_slice(&frame.payload) {
                    Ok(payload) => payload,
                    Err(err) => {
                        warn!(err = %err, "ignoring malformed control frame");
                        continue;
                    }
                };
                let event = payload.get("event").and_then(|value| value.as_str());
                let reason = match event {
                    Some(STREAM_CLOSED_EVENT) => Some("pane closed".to_owned()),
                    Some(STREAM_REVOKED_EVENT) => Some("taken over".to_owned()),
                    // The answer to our own stream.close ends the session.
                    None if closed_our_stream(&payload, stream_id) => Some("released".to_owned()),
                    _ => None,
                };
                if let Some(reason) = reason {
                    return write_session_record(&serde_json::json!({
                        "type": "terminal.closed",
                        "reason": reason,
                    }));
                }
                // Errors answer a request; they do not end the session.
                if let Some(error) = control_error(&payload) {
                    warn!(code = %error.code, message = %error.message, "terminal session control error");
                }
            }
        }
    }
}

/// True when the control response is the acknowledgement of our own
/// `stream.close`.
fn closed_our_stream(payload: &serde_json::Value, stream_id: u32) -> bool {
    let Some(result) = payload.get("result") else {
        return false;
    };
    result.get("type").and_then(|value| value.as_str()) == Some("stream_closed")
        && result.get("stream_id").and_then(|value| value.as_u64()) == Some(stream_id as u64)
}

fn write_session_record(value: &serde_json::Value) -> io::Result<()> {
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, value)?;
    stdout.write_all(b"\n")?;
    stdout.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::framed::{
        StreamResizeParams, PANE_SEND_BYTES_METHOD, STREAM_CLOSE_METHOD, STREAM_RESIZE_METHOD,
        STREAM_SCROLL_METHOD,
    };

    #[test]
    fn session_control_input_becomes_a_pane_send_bytes_request() {
        let request = framed_session_control_request(
            "s1",
            "p_1_1",
            7,
            r#"{"type":"terminal.input","text":"ls"}"#,
        )
        .unwrap()
        .expect("input request");
        assert_eq!(request["method"], PANE_SEND_BYTES_METHOD);
        assert_eq!(request["params"]["pane_id"], "p_1_1");
        assert_eq!(request["params"]["data_base64"], "bHM=");
    }

    #[test]
    fn session_control_resize_and_scroll_address_the_stream() {
        let resize = framed_session_control_request(
            "s2",
            "p_1_1",
            7,
            r#"{"type":"terminal.resize","cols":100,"rows":30}"#,
        )
        .unwrap()
        .expect("resize request");
        assert_eq!(resize["method"], STREAM_RESIZE_METHOD);
        let params: StreamResizeParams = serde_json::from_value(resize["params"].clone()).unwrap();
        assert_eq!(params.stream_id, 7);
        assert_eq!((params.cols, params.rows), (100, 30));

        let scroll = framed_session_control_request(
            "s3",
            "p_1_1",
            7,
            r#"{"type":"terminal.scroll","direction":"up","lines":4,"source":"page_key"}"#,
        )
        .unwrap()
        .expect("scroll request");
        assert_eq!(scroll["method"], STREAM_SCROLL_METHOD);
        let params: StreamScrollParams = serde_json::from_value(scroll["params"].clone()).unwrap();
        assert_eq!(params.stream_id, 7);
        assert_eq!(params.direction, StreamScrollDirection::Up);
        assert_eq!(params.source, StreamScrollSource::PageKey);
        assert_eq!(params.lines, 4);
    }

    #[test]
    fn session_control_release_closes_the_stream() {
        let release =
            framed_session_control_request("s4", "p_1_1", 7, r#"{"type":"terminal.release"}"#)
                .unwrap();
        assert!(release.is_none(), "release maps to a stream close");
        let close = stream_close_request("s4", 7);
        assert_eq!(close["method"], STREAM_CLOSE_METHOD);
    }

    #[test]
    fn session_control_rejects_invalid_commands() {
        assert!(framed_session_control_request("s5", "p_1_1", 7, "not json").is_err());
        assert!(framed_session_control_request(
            "s6",
            "p_1_1",
            7,
            r#"{"type":"terminal.resize","cols":0,"rows":30}"#
        )
        .is_err());
    }
}

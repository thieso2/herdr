//! Framed unified-protocol session handler on the API socket.
//!
//! `handle_connection` routes here after consuming the `HRDR` magic. The
//! session must open with a `session.hello` control frame; once negotiated it
//! serves control-plane requests (heartbeat `ping`, `stream.open`,
//! `stream.close`, `pane.send_bytes`, `pane.paste_image`) until the client
//! disconnects or the server stops. Open pane streams deliver the raw PTY
//! output tail as DATA frames, and negotiated capabilities gate
//! `notification.posted` / `window_title.changed` event broadcasts.
//!
//! All frames are written from the single session thread, so per-connection
//! ordering is total FIFO. There is no flow control: a client that falls more
//! than the bounded output buffer behind is disconnected and reseeds by
//! reconnecting and calling `stream.open` again.

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::debug;

use crate::api::server::{
    api_response_outcome, dispatch_to_app_with_timeout, APP_RESPONSE_TIMEOUT,
    CONNECTION_POLL_INTERVAL,
};
use crate::api::{ApiRequestSender, EventHub};
use crate::ipc::{
    is_connection_closed_error, poll_local_stream_read_len, set_local_stream_polling, LocalStream,
    LocalStreamReadLen,
};
use crate::pane::output_tap::{
    cancel_pending_stream, claim_pending_stream, register_pending_stream, PaneOutputSubscription,
};
use crate::protocol::framed::{
    decode_frame_header, negotiate_session_hello, write_frame, Frame, FrameType, FramedCodecError,
    HelloError, HelloRemedy, NegotiatedSession, PanePasteImageControlParams,
    PaneSendBytesControlParams, SessionHelloParams, StreamCloseParams, StreamOpenParams,
    CAPABILITY_NOTIFICATION, CAPABILITY_PANE_STREAM, CAPABILITY_PASTE_IMAGE,
    CAPABILITY_WINDOW_TITLE, CONTROL_STREAM_ID, FRAMED_PROTOCOL_MIN_SUPPORTED,
    FRAMED_PROTOCOL_VERSION, FRAME_HEADER_BYTES, NOTIFICATION_POSTED_EVENT,
    PANE_PASTE_IMAGE_METHOD, PANE_SEND_BYTES_METHOD, PING_METHOD, SESSION_HELLO_METHOD,
    STREAM_CLOSED_EVENT, STREAM_CLOSE_METHOD, STREAM_OPEN_METHOD, WINDOW_TITLE_CHANGED_EVENT,
};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const READ_CHUNK_BYTES: usize = 64 * 1024;

/// Poll interval while at least one pane stream is open. Live output tails
/// need much lower latency than the idle control-plane poll.
const STREAM_POLL_INTERVAL: Duration = Duration::from_millis(2);

/// Allocator for server-owned stream ids. Process-global and monotonic, so a
/// stream id is never reused — not within a connection and not across
/// connections. Tracked as a u64 internally so the counter cannot wrap back
/// into the 32-bit wire id space; allocation fails once the wire space is
/// exhausted instead of reusing earlier ids. Never yields
/// `CONTROL_STREAM_ID` (the counter starts at 1).
static NEXT_STREAM_ID: AtomicU64 = AtomicU64::new(1);

fn allocate_stream_id() -> Option<u32> {
    u32::try_from(NEXT_STREAM_ID.fetch_add(1, Ordering::Relaxed)).ok()
}

/// Control request envelope: the JSON API request vocabulary carried in a
/// CONTROL frame.
#[derive(Debug, serde::Deserialize)]
struct ControlRequest {
    #[serde(default)]
    id: String,
    method: String,
    #[serde(default)]
    params: serde_json::Value,
}

enum SessionEnd {
    PeerClosed,
    ServerStopped,
    ProtocolError,
    Overloaded,
}

enum PumpOutcome {
    Continue,
    PeerClosed,
    /// A subscriber overran the bounded output buffer; the stream id names it.
    Overloaded(u32),
}

/// Serves one framed session. The `HRDR` magic has already been consumed.
pub(super) fn serve(
    mut stream: LocalStream,
    api_tx: &ApiRequestSender,
    event_hub: &EventHub,
    running: &Arc<AtomicBool>,
) -> io::Result<()> {
    let mut reader = FrameReader::new();

    // Handshake: the first frame must be a session.hello control request.
    let deadline = Instant::now() + HANDSHAKE_TIMEOUT;
    let frame = loop {
        match reader.poll_frame(&mut stream)? {
            PollFrame::Frame(frame) => break frame,
            PollFrame::Closed => return Ok(()),
            PollFrame::Pending => {
                if !running.load(Ordering::Relaxed) {
                    return Ok(());
                }
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "timed out waiting for session.hello",
                    ));
                }
                std::thread::sleep(CONNECTION_POLL_INTERVAL);
            }
        }
    };

    let session = match negotiate_handshake(&mut stream, frame)? {
        Some(session) => session,
        None => return Ok(()),
    };
    debug!(
        protocol = session.protocol,
        capabilities = ?session.capabilities,
        "framed session negotiated"
    );

    // Open pane output streams, keyed by server-allocated stream id. Dropping
    // a subscription detaches it from the pane tap.
    let mut streams: HashMap<u32, PaneOutputSubscription> = HashMap::new();
    // Events published before the handshake are history, not session traffic.
    let mut event_cursor = event_hub.current_sequence();

    // Negotiated session loop: control-plane requests, output pumping, and
    // event broadcasts until disconnect.
    let end = loop {
        match reader.poll_frame(&mut stream)? {
            PollFrame::Closed => break SessionEnd::PeerClosed,
            PollFrame::Pending => {
                if !running.load(Ordering::Relaxed) {
                    break SessionEnd::ServerStopped;
                }
                match pump_session_output(
                    &mut stream,
                    &session,
                    &mut streams,
                    event_hub,
                    &mut event_cursor,
                )? {
                    PumpOutcome::Continue => {}
                    PumpOutcome::PeerClosed => break SessionEnd::PeerClosed,
                    PumpOutcome::Overloaded(stream_id) => {
                        let _ = write_control_allow_disconnect(
                            &mut stream,
                            &error_response(
                                "",
                                "stream_overloaded",
                                &format!(
                                    "stream {stream_id} overran the bounded output buffer; \
                                     reconnect and reopen the stream to reseed"
                                ),
                                Some(serde_json::json!({ "stream_id": stream_id })),
                            ),
                        )?;
                        break SessionEnd::Overloaded;
                    }
                }
                std::thread::sleep(if streams.is_empty() {
                    CONNECTION_POLL_INTERVAL
                } else {
                    STREAM_POLL_INTERVAL
                });
            }
            PollFrame::Frame(frame) => match frame.frame_type {
                FrameType::Data => {
                    // DATA frames are server-to-client in protocol v1; pane
                    // input is a control-plane method. A client DATA frame
                    // indicates a desynchronized client.
                    let sent = write_control_allow_disconnect(
                        &mut stream,
                        &error_response(
                            "",
                            "unknown_stream",
                            &format!(
                                "data frames are server-to-client; no client-writable stream \
                                 with id {}",
                                frame.stream_id
                            ),
                            None,
                        ),
                    )?;
                    if !sent {
                        break SessionEnd::PeerClosed;
                    }
                    break SessionEnd::ProtocolError;
                }
                FrameType::Control => {
                    if !handle_control_request(&mut stream, &session, &frame, api_tx, &mut streams)?
                    {
                        break SessionEnd::PeerClosed;
                    }
                }
            },
        }
    };

    match end {
        SessionEnd::PeerClosed => debug!("framed session closed by client"),
        SessionEnd::ServerStopped => debug!("framed session closed on server stop"),
        SessionEnd::ProtocolError => debug!("framed session closed after protocol error"),
        SessionEnd::Overloaded => debug!("framed session closed after output overload"),
    }
    Ok(())
}

/// Drains open stream subscriptions into DATA frames and broadcasts
/// capability-gated events from the hub. Runs on every idle poll tick.
fn pump_session_output(
    stream: &mut LocalStream,
    session: &NegotiatedSession,
    streams: &mut HashMap<u32, PaneOutputSubscription>,
    event_hub: &EventHub,
    event_cursor: &mut u64,
) -> io::Result<PumpOutcome> {
    let mut closed_streams = Vec::new();
    for (&stream_id, subscription) in streams.iter() {
        let drain = subscription.drain();
        if drain.overloaded {
            return Ok(PumpOutcome::Overloaded(stream_id));
        }
        if !drain.bytes.is_empty()
            && !write_frame_allow_disconnect(stream, FrameType::Data, stream_id, &drain.bytes)?
        {
            return Ok(PumpOutcome::PeerClosed);
        }
        if drain.closed {
            closed_streams.push(stream_id);
        }
    }
    for stream_id in closed_streams {
        streams.remove(&stream_id);
        let sent = write_control_allow_disconnect(
            stream,
            &serde_json::json!({
                "event": STREAM_CLOSED_EVENT,
                "data": { "stream_id": stream_id, "reason": "pane_closed" },
            }),
        )?;
        if !sent {
            return Ok(PumpOutcome::PeerClosed);
        }
    }

    let wants_notifications = session.has_capability(CAPABILITY_NOTIFICATION);
    let wants_window_title = session.has_capability(CAPABILITY_WINDOW_TITLE);
    if wants_notifications || wants_window_title {
        for (sequence, envelope) in event_hub.events_after(*event_cursor) {
            *event_cursor = sequence;
            let event_name = match envelope.event {
                crate::api::schema::EventKind::NotificationPosted if wants_notifications => {
                    NOTIFICATION_POSTED_EVENT
                }
                crate::api::schema::EventKind::WindowTitleChanged if wants_window_title => {
                    WINDOW_TITLE_CHANGED_EVENT
                }
                _ => continue,
            };
            let sent = write_control_allow_disconnect(
                stream,
                &serde_json::json!({
                    "event": event_name,
                    "seq": sequence,
                    "data": envelope.data,
                }),
            )?;
            if !sent {
                return Ok(PumpOutcome::PeerClosed);
            }
        }
    } else {
        // Keep the cursor moving so a later capability change (future
        // protocol versions) never replays stale history.
        *event_cursor = event_hub.current_sequence().max(*event_cursor);
    }

    Ok(PumpOutcome::Continue)
}

/// Validates and answers the opening frame. Returns the negotiated session,
/// or `None` when the connection should close (rejection sent or peer gone).
fn negotiate_handshake(
    stream: &mut LocalStream,
    frame: Frame,
) -> io::Result<Option<NegotiatedSession>> {
    if frame.frame_type != FrameType::Control {
        write_control_allow_disconnect(
            stream,
            &error_response(
                "",
                "invalid_request",
                "expected a session.hello control frame",
                None,
            ),
        )?;
        return Ok(None);
    }

    let request = match serde_json::from_slice::<ControlRequest>(&frame.payload) {
        Ok(request) => request,
        Err(err) => {
            write_control_allow_disconnect(
                stream,
                &error_response(
                    "",
                    "invalid_request",
                    &format!("invalid control request: {err}"),
                    None,
                ),
            )?;
            return Ok(None);
        }
    };

    if request.method != SESSION_HELLO_METHOD {
        write_control_allow_disconnect(
            stream,
            &error_response(
                &request.id,
                "invalid_request",
                &format!(
                    "expected {SESSION_HELLO_METHOD} as the first control request, got {}",
                    request.method
                ),
                None,
            ),
        )?;
        return Ok(None);
    }

    let params = match serde_json::from_value::<SessionHelloParams>(request.params) {
        Ok(params) => params,
        Err(err) => {
            write_control_allow_disconnect(
                stream,
                &error_response(
                    &request.id,
                    "invalid_request",
                    &format!("invalid session.hello params: {err}"),
                    None,
                ),
            )?;
            return Ok(None);
        }
    };

    match negotiate_session_hello(&params) {
        Ok(session) => {
            let welcome = serde_json::json!({
                "id": request.id,
                "result": {
                    "type": "session.welcome",
                    "protocol": session.protocol,
                    "min_protocol": FRAMED_PROTOCOL_MIN_SUPPORTED,
                    "capabilities": session.capabilities,
                    "server_version": crate::build_info::version(),
                },
            });
            if !write_control_allow_disconnect(stream, &welcome)? {
                return Ok(None);
            }
            Ok(Some(session))
        }
        Err(HelloError::InvalidWindow { message }) => {
            write_control_allow_disconnect(
                stream,
                &error_response(&request.id, "invalid_request", &message, None),
            )?;
            Ok(None)
        }
        Err(HelloError::OutOfWindow { remedy, message }) => {
            let data = serde_json::json!({
                "remedy": match remedy {
                    HelloRemedy::UpgradeClient => "upgrade_client",
                    HelloRemedy::UpgradeServer => "upgrade_server",
                },
                "server_protocol": FRAMED_PROTOCOL_VERSION,
                "server_min_protocol": FRAMED_PROTOCOL_MIN_SUPPORTED,
                "client_protocol": params.protocol,
                "client_min_protocol": params.min_protocol.unwrap_or(params.protocol),
            });
            write_control_allow_disconnect(
                stream,
                &error_response(&request.id, "protocol_out_of_window", &message, Some(data)),
            )?;
            Ok(None)
        }
    }
}

/// Handles one post-handshake control frame. Returns false when the peer is
/// gone and the session should end.
fn handle_control_request(
    stream: &mut LocalStream,
    session: &NegotiatedSession,
    frame: &Frame,
    api_tx: &ApiRequestSender,
    streams: &mut HashMap<u32, PaneOutputSubscription>,
) -> io::Result<bool> {
    let request = match serde_json::from_slice::<ControlRequest>(&frame.payload) {
        Ok(request) => request,
        Err(err) => {
            return write_control_allow_disconnect(
                stream,
                &error_response(
                    "",
                    "invalid_request",
                    &format!("invalid control request: {err}"),
                    None,
                ),
            );
        }
    };

    match request.method.as_str() {
        PING_METHOD => write_control_allow_disconnect(
            stream,
            &serde_json::json!({
                "id": request.id,
                "result": {
                    "type": "pong",
                    "version": crate::build_info::version(),
                    "protocol": session.protocol,
                },
            }),
        ),
        STREAM_OPEN_METHOD => handle_stream_open(stream, session, request, api_tx, streams),
        STREAM_CLOSE_METHOD => handle_stream_close(stream, session, request, streams),
        PANE_SEND_BYTES_METHOD => {
            if let Some(rejection) = capability_rejection(session, CAPABILITY_PANE_STREAM, &request)
            {
                return write_control_allow_disconnect(stream, &rejection);
            }
            let params = match serde_json::from_value::<PaneSendBytesControlParams>(
                request.params.clone(),
            ) {
                Ok(params) => params,
                Err(err) => {
                    return write_control_allow_disconnect(
                        stream,
                        &error_response(
                            &request.id,
                            "invalid_params",
                            &format!("invalid {PANE_SEND_BYTES_METHOD} params: {err}"),
                            None,
                        ),
                    );
                }
            };
            let response = dispatch_to_app_with_timeout(
                crate::api::schema::Request {
                    id: request.id,
                    method: crate::api::schema::Method::PaneSendBytes(
                        crate::api::schema::PaneSendBytesParams {
                            pane_id: params.pane_id,
                            data_base64: params.data_base64,
                        },
                    ),
                },
                api_tx,
                Some(APP_RESPONSE_TIMEOUT),
            );
            write_control_raw_allow_disconnect(stream, &response)
        }
        PANE_PASTE_IMAGE_METHOD => {
            if let Some(rejection) = capability_rejection(session, CAPABILITY_PASTE_IMAGE, &request)
            {
                return write_control_allow_disconnect(stream, &rejection);
            }
            let params =
                match serde_json::from_value::<PanePasteImageControlParams>(request.params.clone())
                {
                    Ok(params) => params,
                    Err(err) => {
                        return write_control_allow_disconnect(
                            stream,
                            &error_response(
                                &request.id,
                                "invalid_params",
                                &format!("invalid {PANE_PASTE_IMAGE_METHOD} params: {err}"),
                                None,
                            ),
                        );
                    }
                };
            let response = dispatch_to_app_with_timeout(
                crate::api::schema::Request {
                    id: request.id,
                    method: crate::api::schema::Method::PanePasteImage(
                        crate::api::schema::PanePasteImageParams {
                            pane_id: params.pane_id,
                            extension: params.extension,
                            data_base64: params.data_base64,
                        },
                    ),
                },
                api_tx,
                Some(APP_RESPONSE_TIMEOUT),
            );
            write_control_raw_allow_disconnect(stream, &response)
        }
        SESSION_HELLO_METHOD => write_control_allow_disconnect(
            stream,
            &error_response(
                &request.id,
                "invalid_request",
                "session.hello was already negotiated on this connection",
                None,
            ),
        ),
        method => write_control_allow_disconnect(
            stream,
            &error_response(
                &request.id,
                "unknown_method",
                &format!("unknown control method {method}"),
                None,
            ),
        ),
    }
}

/// Opens a pane output stream: allocates the stream id, registers the pending
/// handoff slot, dispatches to the app thread, and claims the subscription.
fn handle_stream_open(
    stream: &mut LocalStream,
    session: &NegotiatedSession,
    request: ControlRequest,
    api_tx: &ApiRequestSender,
    streams: &mut HashMap<u32, PaneOutputSubscription>,
) -> io::Result<bool> {
    if let Some(rejection) = capability_rejection(session, CAPABILITY_PANE_STREAM, &request) {
        return write_control_allow_disconnect(stream, &rejection);
    }
    let params = match serde_json::from_value::<StreamOpenParams>(request.params) {
        Ok(params) => params,
        Err(err) => {
            return write_control_allow_disconnect(
                stream,
                &error_response(
                    &request.id,
                    "invalid_params",
                    &format!("invalid {STREAM_OPEN_METHOD} params: {err}"),
                    None,
                ),
            );
        }
    };

    let Some(stream_id) = allocate_stream_id() else {
        return write_control_allow_disconnect(
            stream,
            &error_response(
                &request.id,
                "stream_ids_exhausted",
                "this server process has exhausted its 32-bit stream id space; \
                 restart the server to open new streams",
                None,
            ),
        );
    };
    register_pending_stream(stream_id);
    let response = dispatch_to_app_with_timeout(
        crate::api::schema::Request {
            id: request.id.clone(),
            method: crate::api::schema::Method::PaneStreamOpen(
                crate::api::schema::PaneStreamOpenParams {
                    pane_id: params.pane_id,
                    stream_id,
                },
            ),
        },
        api_tx,
        Some(APP_RESPONSE_TIMEOUT),
    );
    if api_response_outcome(&response) != "ok" {
        cancel_pending_stream(stream_id);
        return write_control_raw_allow_disconnect(stream, &response);
    }

    match claim_pending_stream(stream_id) {
        Some(subscription) => {
            streams.insert(stream_id, subscription);
            write_control_raw_allow_disconnect(stream, &response)
        }
        None => {
            cancel_pending_stream(stream_id);
            write_control_allow_disconnect(
                stream,
                &error_response(
                    &request.id,
                    "internal_error",
                    "pane stream subscription was not attached",
                    None,
                ),
            )
        }
    }
}

/// Closes an open stream. Session-local: dropping the subscription detaches
/// it from the pane output tap.
fn handle_stream_close(
    stream: &mut LocalStream,
    session: &NegotiatedSession,
    request: ControlRequest,
    streams: &mut HashMap<u32, PaneOutputSubscription>,
) -> io::Result<bool> {
    if let Some(rejection) = capability_rejection(session, CAPABILITY_PANE_STREAM, &request) {
        return write_control_allow_disconnect(stream, &rejection);
    }
    let params = match serde_json::from_value::<StreamCloseParams>(request.params) {
        Ok(params) => params,
        Err(err) => {
            return write_control_allow_disconnect(
                stream,
                &error_response(
                    &request.id,
                    "invalid_params",
                    &format!("invalid {STREAM_CLOSE_METHOD} params: {err}"),
                    None,
                ),
            );
        }
    };

    if streams.remove(&params.stream_id).is_none() {
        return write_control_allow_disconnect(
            stream,
            &error_response(
                &request.id,
                "unknown_stream",
                &format!("no open stream with id {}", params.stream_id),
                None,
            ),
        );
    }

    write_control_allow_disconnect(
        stream,
        &serde_json::json!({
            "id": request.id,
            "result": {
                "type": "stream_closed",
                "stream_id": params.stream_id,
            },
        }),
    )
}

/// Builds the rejection response for a method whose gating capability was not
/// negotiated at hello, or `None` when the capability is active.
fn capability_rejection(
    session: &NegotiatedSession,
    capability: &str,
    request: &ControlRequest,
) -> Option<serde_json::Value> {
    if session.has_capability(capability) {
        return None;
    }
    Some(error_response(
        &request.id,
        "capability_not_negotiated",
        &format!(
            "{} requires the {capability} capability, which was not negotiated at session.hello",
            request.method
        ),
        Some(serde_json::json!({ "capability": capability })),
    ))
}

fn error_response(
    id: &str,
    code: &str,
    message: &str,
    data: Option<serde_json::Value>,
) -> serde_json::Value {
    let mut error = serde_json::json!({
        "code": code,
        "message": message,
    });
    if let Some(data) = data {
        error["data"] = data;
    }
    serde_json::json!({
        "id": id,
        "error": error,
    })
}

/// Writes one frame in blocking mode, returning false when the peer has
/// disconnected. The stream is returned to polling mode afterwards so the
/// session read loop keeps working.
fn write_frame_allow_disconnect(
    stream: &mut LocalStream,
    frame_type: FrameType,
    stream_id: u32,
    payload: &[u8],
) -> io::Result<bool> {
    set_local_stream_polling(stream, false)?;
    let result = write_frame(stream, frame_type, stream_id, payload);
    set_local_stream_polling(stream, true)?;

    match result {
        Ok(()) => Ok(true),
        Err(FramedCodecError::Io(err)) if is_connection_closed_error(&err) => Ok(false),
        Err(FramedCodecError::Io(err)) => Err(err),
        Err(err) => Err(io::Error::other(err.to_string())),
    }
}

fn write_control_allow_disconnect(
    stream: &mut LocalStream,
    value: &serde_json::Value,
) -> io::Result<bool> {
    let payload = serde_json::to_vec(value)
        .map_err(|err| io::Error::other(format!("failed to encode control frame: {err}")))?;
    write_frame_allow_disconnect(stream, FrameType::Control, CONTROL_STREAM_ID, &payload)
}

/// Writes an already-encoded JSON API response string as a control frame.
fn write_control_raw_allow_disconnect(
    stream: &mut LocalStream,
    response: &str,
) -> io::Result<bool> {
    write_frame_allow_disconnect(
        stream,
        FrameType::Control,
        CONTROL_STREAM_ID,
        response.as_bytes(),
    )
}

enum PollFrame {
    Frame(Frame),
    Pending,
    Closed,
}

/// Incremental frame reader over a polled (non-blocking) stream. Buffers
/// partial reads and yields complete frames.
struct FrameReader {
    buf: Vec<u8>,
}

impl FrameReader {
    fn new() -> Self {
        Self { buf: Vec::new() }
    }

    fn poll_frame(&mut self, stream: &mut LocalStream) -> io::Result<PollFrame> {
        set_local_stream_polling(stream, true)?;
        loop {
            if let Some(frame) = self.take_buffered_frame()? {
                return Ok(PollFrame::Frame(frame));
            }

            let mut chunk = [0u8; READ_CHUNK_BYTES];
            match poll_local_stream_read_len(stream, &mut chunk)? {
                LocalStreamReadLen::Closed => return Ok(PollFrame::Closed),
                LocalStreamReadLen::Pending => return Ok(PollFrame::Pending),
                LocalStreamReadLen::Data(read) => self.buf.extend_from_slice(&chunk[..read]),
            }
        }
    }

    fn take_buffered_frame(&mut self) -> io::Result<Option<Frame>> {
        if self.buf.len() < FRAME_HEADER_BYTES {
            return Ok(None);
        }
        let mut header_bytes = [0u8; FRAME_HEADER_BYTES];
        header_bytes.copy_from_slice(&self.buf[..FRAME_HEADER_BYTES]);
        let header = decode_frame_header(&header_bytes)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;

        let total = FRAME_HEADER_BYTES + header.payload_len as usize;
        if self.buf.len() < total {
            return Ok(None);
        }

        let payload = self.buf[FRAME_HEADER_BYTES..total].to_vec();
        self.buf.drain(..total);
        Ok(Some(Frame {
            frame_type: header.frame_type,
            stream_id: header.stream_id,
            payload,
        }))
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::api::schema::{Method, ResponseResult, SuccessResponse};
    use crate::api::{ApiRequestMessage, EventHub};
    use crate::ipc::{bind_local_listener, connect_local_stream};
    use crate::pane::output_tap::{fulfill_pending_stream, PaneOutputTap};
    use crate::protocol::framed::{encode_history_cursor, read_frame, FRAMED_MAGIC};
    use interprocess::local_socket::traits::Listener as _;
    use std::io::{BufRead, BufReader, Write as _};
    use std::path::PathBuf;
    use std::sync::mpsc::{self, Receiver};

    fn local_stream_pair(name: &str) -> (LocalStream, LocalStream, PathBuf) {
        static PAIR_ID: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let path = std::env::temp_dir().join(format!(
            "herdr-framed-{name}-{}-{}.sock",
            std::process::id(),
            PAIR_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_file(&path);
        let listener = bind_local_listener(&path).unwrap();
        let client = connect_local_stream(&path).unwrap();
        let server = listener.accept().unwrap();
        (client, server, path)
    }

    fn spawn_connection(
        server: LocalStream,
    ) -> (Receiver<io::Result<()>>, std::thread::JoinHandle<()>) {
        let (api_tx, _api_rx) = tokio::sync::mpsc::unbounded_channel();
        spawn_connection_with(server, api_tx, EventHub::default())
    }

    fn spawn_connection_with(
        server: LocalStream,
        api_tx: crate::api::ApiRequestSender,
        event_hub: EventHub,
    ) -> (Receiver<io::Result<()>>, std::thread::JoinHandle<()>) {
        let (done_tx, done_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let result = super::super::handle_connection(
                server,
                &api_tx,
                &event_hub,
                &Arc::new(AtomicBool::new(true)),
                None,
            );
            done_tx.send(result).unwrap();
        });
        (done_rx, thread)
    }

    /// Fake app thread answering the pane-stream methods the way the real app
    /// handler does: subscribe to the tap, fulfill the pending slot, respond.
    /// Holds the tap weakly so tests can drop the pane's tap to simulate the
    /// pane going away.
    fn spawn_pane_stream_responder(
        tap: std::sync::Weak<PaneOutputTap>,
        requests: mpsc::Sender<Method>,
    ) -> (crate::api::ApiRequestSender, std::thread::JoinHandle<()>) {
        let (api_tx, mut api_rx) = tokio::sync::mpsc::unbounded_channel::<ApiRequestMessage>();
        let responder = std::thread::spawn(move || {
            while let Some(msg) = api_rx.blocking_recv() {
                let _ = requests.send(msg.request.method.clone());
                let response = match msg.request.method {
                    Method::PaneStreamOpen(params) => {
                        let tap = tap.upgrade().expect("tap must be alive during stream.open");
                        let (subscription, snapshot) =
                            tap.subscribe_with_snapshot(|| "SNAPSHOT".to_owned());
                        let sequence = subscription.sequence();
                        assert!(fulfill_pending_stream(params.stream_id, subscription));
                        serde_json::to_string(&SuccessResponse {
                            id: msg.request.id,
                            result: ResponseResult::PaneStreamOpened {
                                stream: crate::api::schema::PaneStreamOpenInfo {
                                    pane_id: params.pane_id,
                                    workspace_id: "ws_1".into(),
                                    stream_id: params.stream_id,
                                    sequence,
                                    snapshot,
                                    history_cursor: encode_history_cursor("pane_1", sequence),
                                },
                            },
                        })
                        .unwrap()
                    }
                    Method::PaneSendBytes(_) | Method::PanePasteImage(_) => {
                        serde_json::to_string(&SuccessResponse {
                            id: msg.request.id,
                            result: ResponseResult::Ok {},
                        })
                        .unwrap()
                    }
                    other => panic!("unexpected request: {other:?}"),
                };
                msg.respond_to.send(response).unwrap();
            }
        });
        (api_tx, responder)
    }

    fn send_control(stream: &mut LocalStream, value: serde_json::Value) {
        let payload = serde_json::to_vec(&value).unwrap();
        write_frame(stream, FrameType::Control, CONTROL_STREAM_ID, &payload).unwrap();
    }

    fn read_control(stream: &mut LocalStream) -> serde_json::Value {
        let frame = read_frame(stream).unwrap();
        assert_eq!(frame.frame_type, FrameType::Control);
        assert_eq!(frame.stream_id, CONTROL_STREAM_ID);
        serde_json::from_slice(&frame.payload).unwrap()
    }

    fn hello(client: &mut LocalStream, id: &str, capabilities: &[&str]) -> serde_json::Value {
        client.write_all(&FRAMED_MAGIC).unwrap();
        send_control(
            client,
            serde_json::json!({
                "id": id,
                "method": "session.hello",
                "params": {
                    "protocol": FRAMED_PROTOCOL_VERSION,
                    "min_protocol": FRAMED_PROTOCOL_MIN_SUPPORTED,
                    "capabilities": capabilities,
                },
            }),
        );
        read_control(client)
    }

    fn finish(
        client: LocalStream,
        done_rx: Receiver<io::Result<()>>,
        thread: std::thread::JoinHandle<()>,
        path: PathBuf,
    ) {
        drop(client);
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("connection handler must finish")
            .expect("connection handler must succeed");
        thread.join().unwrap();
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn framed_hello_negotiates_and_serves_heartbeat_ping() {
        let (mut client, server, path) = local_stream_pair("hello-ping");
        let (done_rx, thread) = spawn_connection(server);

        let welcome = hello(&mut client, "h1", &["pane-stream"]);
        assert_eq!(welcome["id"], "h1");
        assert_eq!(welcome["result"]["type"], "session.welcome");
        assert_eq!(welcome["result"]["protocol"], FRAMED_PROTOCOL_VERSION);
        assert_eq!(
            welcome["result"]["min_protocol"],
            FRAMED_PROTOCOL_MIN_SUPPORTED
        );
        assert_eq!(
            welcome["result"]["capabilities"],
            serde_json::json!(["pane-stream"])
        );
        assert_eq!(
            welcome["result"]["server_version"],
            env!("CARGO_PKG_VERSION")
        );

        send_control(
            &mut client,
            serde_json::json!({"id": "p1", "method": "ping", "params": {}}),
        );
        let pong = read_control(&mut client);
        assert_eq!(pong["id"], "p1");
        assert_eq!(pong["result"]["type"], "pong");
        assert_eq!(pong["result"]["protocol"], FRAMED_PROTOCOL_VERSION);
        assert_eq!(pong["result"]["version"], env!("CARGO_PKG_VERSION"));

        // A second ping keeps working: the session stays open between beats.
        send_control(
            &mut client,
            serde_json::json!({"id": "p2", "method": "ping", "params": {}}),
        );
        assert_eq!(read_control(&mut client)["id"], "p2");

        finish(client, done_rx, thread, path);
    }

    #[test]
    fn framed_out_of_window_hello_is_rejected_with_exact_remedy() {
        let (mut client, server, path) = local_stream_pair("out-of-window");
        let (done_rx, thread) = spawn_connection(server);

        client.write_all(&FRAMED_MAGIC).unwrap();
        send_control(
            &mut client,
            serde_json::json!({
                "id": "h2",
                "method": "session.hello",
                "params": {"protocol": 99, "min_protocol": 99},
            }),
        );

        let rejection = read_control(&mut client);
        assert_eq!(rejection["id"], "h2");
        assert_eq!(rejection["error"]["code"], "protocol_out_of_window");
        assert!(rejection["error"]["message"]
            .as_str()
            .unwrap()
            .contains("upgrade this herdr server"));
        assert_eq!(rejection["error"]["data"]["remedy"], "upgrade_server");
        assert_eq!(
            rejection["error"]["data"]["server_protocol"],
            FRAMED_PROTOCOL_VERSION
        );
        assert_eq!(
            rejection["error"]["data"]["server_min_protocol"],
            FRAMED_PROTOCOL_MIN_SUPPORTED
        );
        assert_eq!(rejection["error"]["data"]["client_protocol"], 99);
        assert_eq!(rejection["error"]["data"]["client_min_protocol"], 99);

        // The server closes the connection after an out-of-window hello.
        match read_frame(&mut client) {
            Err(FramedCodecError::UnexpectedEof) => {}
            other => panic!("expected closed connection, got {other:?}"),
        }

        finish(client, done_rx, thread, path);
    }

    #[test]
    fn framed_first_control_request_must_be_session_hello() {
        let (mut client, server, path) = local_stream_pair("no-hello");
        let (done_rx, thread) = spawn_connection(server);

        client.write_all(&FRAMED_MAGIC).unwrap();
        send_control(
            &mut client,
            serde_json::json!({"id": "p0", "method": "ping", "params": {}}),
        );

        let rejection = read_control(&mut client);
        assert_eq!(rejection["id"], "p0");
        assert_eq!(rejection["error"]["code"], "invalid_request");
        assert!(rejection["error"]["message"]
            .as_str()
            .unwrap()
            .contains("session.hello"));

        finish(client, done_rx, thread, path);
    }

    #[test]
    fn framed_data_frame_from_client_ends_session() {
        let (mut client, server, path) = local_stream_pair("stray-data");
        let (done_rx, thread) = spawn_connection(server);

        let welcome = hello(&mut client, "h3", &[]);
        assert_eq!(welcome["result"]["type"], "session.welcome");

        write_frame(&mut client, FrameType::Data, 7, b"stray").unwrap();
        let error = read_control(&mut client);
        assert_eq!(error["error"]["code"], "unknown_stream");

        match read_frame(&mut client) {
            Err(FramedCodecError::UnexpectedEof) => {}
            other => panic!("expected closed connection, got {other:?}"),
        }

        finish(client, done_rx, thread, path);
    }

    #[test]
    fn ndjson_request_on_shared_socket_is_unaffected_by_demux() {
        let (mut client, server, path) = local_stream_pair("ndjson");
        let (done_rx, thread) = spawn_connection(server);

        client
            .write_all(b"{\"id\":\"legacy\",\"method\":\"ping\",\"params\":{}}\n")
            .unwrap();
        client.flush().unwrap();

        let mut response = String::new();
        BufReader::new(&mut client)
            .read_line(&mut response)
            .unwrap();
        let response: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert_eq!(response["id"], "legacy");
        assert_eq!(response["result"]["type"], "pong");

        finish(client, done_rx, thread, path);
    }

    #[test]
    fn framed_unknown_control_method_keeps_session_alive() {
        let (mut client, server, path) = local_stream_pair("unknown-method");
        let (done_rx, thread) = spawn_connection(server);

        let welcome = hello(&mut client, "h4", &[]);
        assert_eq!(welcome["result"]["type"], "session.welcome");

        send_control(
            &mut client,
            serde_json::json!({"id": "m1", "method": "no.such.method", "params": {}}),
        );
        let error = read_control(&mut client);
        assert_eq!(error["id"], "m1");
        assert_eq!(error["error"]["code"], "unknown_method");

        send_control(
            &mut client,
            serde_json::json!({"id": "p9", "method": "ping", "params": {}}),
        );
        assert_eq!(read_control(&mut client)["result"]["type"], "pong");

        finish(client, done_rx, thread, path);
    }

    #[test]
    fn framed_stream_open_returns_snapshot_and_streams_data_tail() {
        let tap = Arc::new(PaneOutputTap::default());
        tap.publish_with(b"pre-open bytes", || ());
        let (request_tx, request_rx) = mpsc::channel();
        let (api_tx, responder) = spawn_pane_stream_responder(Arc::downgrade(&tap), request_tx);

        let (mut client, server, path) = local_stream_pair("stream-open");
        let (done_rx, thread) = spawn_connection_with(server, api_tx.clone(), EventHub::default());

        hello(&mut client, "h5", &["pane-stream"]);

        send_control(
            &mut client,
            serde_json::json!({"id": "s1", "method": "stream.open", "params": {"pane_id": "pane_1"}}),
        );
        let opened = read_control(&mut client);
        assert_eq!(opened["id"], "s1");
        assert_eq!(opened["result"]["type"], "pane_stream_opened");
        let stream_id = opened["result"]["stream"]["stream_id"].as_u64().unwrap() as u32;
        assert_ne!(stream_id, CONTROL_STREAM_ID);
        assert_eq!(opened["result"]["stream"]["sequence"], 14);
        assert_eq!(opened["result"]["stream"]["snapshot"], "SNAPSHOT");
        let cursor = opened["result"]["stream"]["history_cursor"]
            .as_str()
            .unwrap();
        assert_eq!(
            crate::protocol::framed::decode_history_cursor(cursor),
            Some(("pane_1".to_owned(), 14))
        );
        assert!(matches!(
            request_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Method::PaneStreamOpen(_)
        ));

        // The live tail arrives as DATA frames on the allocated stream id,
        // in publish order.
        tap.publish_with(b"first ", || ());
        let frame = read_frame(&mut client).unwrap();
        assert_eq!(frame.frame_type, FrameType::Data);
        assert_eq!(frame.stream_id, stream_id);
        assert_eq!(frame.payload, b"first ");

        tap.publish_with(b"second", || ());
        let frame = read_frame(&mut client).unwrap();
        assert_eq!(frame.frame_type, FrameType::Data);
        assert_eq!(frame.stream_id, stream_id);
        assert_eq!(frame.payload, b"second");

        // A second open on the same connection allocates a fresh, never
        // reused stream id.
        send_control(
            &mut client,
            serde_json::json!({"id": "s2", "method": "stream.open", "params": {"pane_id": "pane_1"}}),
        );
        let second = read_control(&mut client);
        let second_id = second["result"]["stream"]["stream_id"].as_u64().unwrap() as u32;
        assert!(second_id > stream_id, "stream ids must never be reused");

        // stream.close detaches: published bytes stop flowing, the session
        // stays alive.
        send_control(
            &mut client,
            serde_json::json!({"id": "c1", "method": "stream.close", "params": {"stream_id": stream_id}}),
        );
        let closed = read_control(&mut client);
        assert_eq!(closed["result"]["type"], "stream_closed");
        send_control(
            &mut client,
            serde_json::json!({"id": "c2", "method": "stream.close", "params": {"stream_id": second_id}}),
        );
        assert_eq!(read_control(&mut client)["result"]["type"], "stream_closed");

        tap.publish_with(b"after close", || ());
        send_control(
            &mut client,
            serde_json::json!({"id": "p1", "method": "ping", "params": {}}),
        );
        let frame = read_frame(&mut client).unwrap();
        assert_eq!(
            frame.frame_type,
            FrameType::Control,
            "no data may flow after stream.close"
        );
        let pong: serde_json::Value = serde_json::from_slice(&frame.payload).unwrap();
        assert_eq!(pong["result"]["type"], "pong");

        finish(client, done_rx, thread, path);
        drop(api_tx);
        responder.join().unwrap();
    }

    #[test]
    fn framed_stream_open_requires_pane_stream_capability() {
        let (mut client, server, path) = local_stream_pair("stream-open-gated");
        let (done_rx, thread) = spawn_connection(server);

        hello(&mut client, "h6", &[]);

        send_control(
            &mut client,
            serde_json::json!({"id": "s1", "method": "stream.open", "params": {"pane_id": "pane_1"}}),
        );
        let rejection = read_control(&mut client);
        assert_eq!(rejection["id"], "s1");
        assert_eq!(rejection["error"]["code"], "capability_not_negotiated");
        assert_eq!(rejection["error"]["data"]["capability"], "pane-stream");

        send_control(
            &mut client,
            serde_json::json!({"id": "b1", "method": "pane.send_bytes", "params": {"pane_id": "pane_1", "data_base64": "aGk="}}),
        );
        assert_eq!(
            read_control(&mut client)["error"]["code"],
            "capability_not_negotiated"
        );

        send_control(
            &mut client,
            serde_json::json!({"id": "i1", "method": "pane.paste_image", "params": {"pane_id": "pane_1", "extension": "png", "data_base64": "aGk="}}),
        );
        let paste = read_control(&mut client);
        assert_eq!(paste["error"]["code"], "capability_not_negotiated");
        assert_eq!(paste["error"]["data"]["capability"], "paste-image");

        // The session survives capability rejections.
        send_control(
            &mut client,
            serde_json::json!({"id": "p1", "method": "ping", "params": {}}),
        );
        assert_eq!(read_control(&mut client)["result"]["type"], "pong");

        finish(client, done_rx, thread, path);
    }

    #[test]
    fn framed_pane_send_bytes_dispatches_to_app() {
        let tap = Arc::new(PaneOutputTap::default());
        let (request_tx, request_rx) = mpsc::channel();
        let (api_tx, responder) = spawn_pane_stream_responder(Arc::downgrade(&tap), request_tx);

        let (mut client, server, path) = local_stream_pair("send-bytes");
        let (done_rx, thread) = spawn_connection_with(server, api_tx.clone(), EventHub::default());

        hello(&mut client, "h7", &["pane-stream", "paste-image"]);

        send_control(
            &mut client,
            serde_json::json!({"id": "b1", "method": "pane.send_bytes", "params": {"pane_id": "pane_1", "data_base64": "aGVsbG8="}}),
        );
        let response = read_control(&mut client);
        assert_eq!(response["id"], "b1");
        assert_eq!(response["result"]["type"], "ok");
        match request_rx.recv_timeout(Duration::from_secs(1)).unwrap() {
            Method::PaneSendBytes(params) => {
                assert_eq!(params.pane_id, "pane_1");
                assert_eq!(params.data_base64, "aGVsbG8=");
            }
            other => panic!("unexpected request: {other:?}"),
        }

        send_control(
            &mut client,
            serde_json::json!({"id": "i1", "method": "pane.paste_image", "params": {"pane_id": "pane_1", "extension": "png", "data_base64": "aGVsbG8="}}),
        );
        let response = read_control(&mut client);
        assert_eq!(response["id"], "i1");
        assert_eq!(response["result"]["type"], "ok");
        match request_rx.recv_timeout(Duration::from_secs(1)).unwrap() {
            Method::PanePasteImage(params) => {
                assert_eq!(params.pane_id, "pane_1");
                assert_eq!(params.extension, "png");
            }
            other => panic!("unexpected request: {other:?}"),
        }

        finish(client, done_rx, thread, path);
        drop(api_tx);
        responder.join().unwrap();
    }

    #[test]
    fn framed_overloaded_stream_disconnects_client() {
        let tap = Arc::new(PaneOutputTap::default());
        let (request_tx, request_rx) = mpsc::channel();
        let (api_tx, responder) = spawn_pane_stream_responder(Arc::downgrade(&tap), request_tx);

        let (mut client, server, path) = local_stream_pair("overload");
        let (done_rx, thread) = spawn_connection_with(server, api_tx.clone(), EventHub::default());

        hello(&mut client, "h8", &["pane-stream"]);
        send_control(
            &mut client,
            serde_json::json!({"id": "s1", "method": "stream.open", "params": {"pane_id": "pane_1"}}),
        );
        let opened = read_control(&mut client);
        let stream_id = opened["result"]["stream"]["stream_id"].as_u64().unwrap();
        assert!(matches!(
            request_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Method::PaneStreamOpen(_)
        ));

        // A single publish larger than the bounded buffer overloads the
        // subscriber immediately.
        let oversized = vec![0_u8; crate::pane::output_tap::PANE_OUTPUT_BUFFER_LIMIT_BYTES + 1];
        tap.publish_with(&oversized, || ());

        let error = read_control(&mut client);
        assert_eq!(error["error"]["code"], "stream_overloaded");
        assert_eq!(error["error"]["data"]["stream_id"], stream_id);

        // The server then disconnects; the client reseeds by reconnecting.
        match read_frame(&mut client) {
            Err(FramedCodecError::UnexpectedEof) => {}
            other => panic!("expected closed connection, got {other:?}"),
        }

        finish(client, done_rx, thread, path);
        drop(api_tx);
        responder.join().unwrap();
    }

    #[test]
    fn framed_notification_and_window_title_events_broadcast_when_negotiated() {
        let event_hub = EventHub::default();
        // Pre-handshake history must not replay into the session.
        event_hub.push(crate::api::schema::EventEnvelope {
            event: crate::api::schema::EventKind::NotificationPosted,
            data: crate::api::schema::EventData::NotificationPosted {
                kind: crate::api::schema::NotificationEventKind::Toast,
                message: "stale".into(),
                body: None,
            },
        });

        let (mut client, server, path) = local_stream_pair("events");
        let (api_tx, _api_rx) = tokio::sync::mpsc::unbounded_channel();
        let (done_rx, thread) = spawn_connection_with(server, api_tx, event_hub.clone());

        hello(&mut client, "h9", &["notification", "window-title"]);

        event_hub.push(crate::api::schema::EventEnvelope {
            event: crate::api::schema::EventKind::NotificationPosted,
            data: crate::api::schema::EventData::NotificationPosted {
                kind: crate::api::schema::NotificationEventKind::Toast,
                message: "pi finished".into(),
                body: Some("workspace 1".into()),
            },
        });
        let event = read_control(&mut client);
        assert_eq!(event["event"], "notification.posted");
        assert_eq!(event["data"]["type"], "notification_posted");
        assert_eq!(event["data"]["kind"], "toast");
        assert_eq!(event["data"]["message"], "pi finished");
        assert_eq!(event["data"]["body"], "workspace 1");

        event_hub.push(crate::api::schema::EventEnvelope {
            event: crate::api::schema::EventKind::WindowTitleChanged,
            data: crate::api::schema::EventData::WindowTitleChanged {
                title: Some("herdr — work".into()),
            },
        });
        let event = read_control(&mut client);
        assert_eq!(event["event"], "window_title.changed");
        assert_eq!(event["data"]["title"], "herdr — work");

        // Unrelated hub events are not broadcast.
        event_hub.push(crate::api::schema::EventEnvelope {
            event: crate::api::schema::EventKind::PaneClosed,
            data: crate::api::schema::EventData::PaneClosed {
                pane_id: "p_1".into(),
                workspace_id: "ws_1".into(),
            },
        });
        send_control(
            &mut client,
            serde_json::json!({"id": "p1", "method": "ping", "params": {}}),
        );
        assert_eq!(read_control(&mut client)["result"]["type"], "pong");

        finish(client, done_rx, thread, path);
    }

    #[test]
    fn framed_events_are_not_broadcast_without_capability() {
        let event_hub = EventHub::default();
        let (mut client, server, path) = local_stream_pair("events-gated");
        let (api_tx, _api_rx) = tokio::sync::mpsc::unbounded_channel();
        let (done_rx, thread) = spawn_connection_with(server, api_tx, event_hub.clone());

        hello(&mut client, "h10", &["pane-stream"]);

        event_hub.push(crate::api::schema::EventEnvelope {
            event: crate::api::schema::EventKind::NotificationPosted,
            data: crate::api::schema::EventData::NotificationPosted {
                kind: crate::api::schema::NotificationEventKind::Sound,
                message: "ding".into(),
                body: None,
            },
        });
        // Give the pump a few ticks; the next frame must still be the pong.
        std::thread::sleep(Duration::from_millis(250));
        send_control(
            &mut client,
            serde_json::json!({"id": "p1", "method": "ping", "params": {}}),
        );
        assert_eq!(read_control(&mut client)["result"]["type"], "pong");

        finish(client, done_rx, thread, path);
    }

    #[test]
    fn framed_stream_closed_event_when_pane_tap_drops() {
        let tap = Arc::new(PaneOutputTap::default());
        let (request_tx, request_rx) = mpsc::channel();
        let (api_tx, responder) = spawn_pane_stream_responder(Arc::downgrade(&tap), request_tx);

        let (mut client, server, path) = local_stream_pair("pane-gone");
        let (done_rx, thread) = spawn_connection_with(server, api_tx.clone(), EventHub::default());

        hello(&mut client, "h11", &["pane-stream"]);
        send_control(
            &mut client,
            serde_json::json!({"id": "s1", "method": "stream.open", "params": {"pane_id": "pane_1"}}),
        );
        let opened = read_control(&mut client);
        let stream_id = opened["result"]["stream"]["stream_id"].as_u64().unwrap();
        assert!(matches!(
            request_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Method::PaneStreamOpen(_)
        ));

        tap.publish_with(b"last words", || ());
        drop(tap);

        // Buffered bytes still arrive, then the stream is closed.
        let frame = read_frame(&mut client).unwrap();
        assert_eq!(frame.frame_type, FrameType::Data);
        assert_eq!(frame.payload, b"last words");
        let closed = read_control(&mut client);
        assert_eq!(closed["event"], "stream.closed");
        assert_eq!(closed["data"]["stream_id"], stream_id);
        assert_eq!(closed["data"]["reason"], "pane_closed");

        // Session stays alive.
        send_control(
            &mut client,
            serde_json::json!({"id": "p1", "method": "ping", "params": {}}),
        );
        assert_eq!(read_control(&mut client)["result"]["type"], "pong");

        finish(client, done_rx, thread, path);
        drop(api_tx);
        responder.join().unwrap();
    }

    #[test]
    fn framed_stream_close_of_unknown_stream_is_an_error() {
        let (mut client, server, path) = local_stream_pair("close-unknown");
        let (done_rx, thread) = spawn_connection(server);

        hello(&mut client, "h12", &["pane-stream"]);
        send_control(
            &mut client,
            serde_json::json!({"id": "c1", "method": "stream.close", "params": {"stream_id": 424242}}),
        );
        let error = read_control(&mut client);
        assert_eq!(error["id"], "c1");
        assert_eq!(error["error"]["code"], "unknown_stream");

        finish(client, done_rx, thread, path);
    }
}

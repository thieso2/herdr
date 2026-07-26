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
    PaneStreamHistory,
};
use crate::pane::write_grant::WriteGrant;
use crate::protocol::framed::{
    decode_frame_header, decode_history_cursor, encode_history_cursor,
    history_page_end_cut_mid_line, history_page_start, negotiate_session_hello,
    parse_stream_opened, write_frame, Frame, FrameType, FramedCodecError, HelloError,
    HistoryCursor, NegotiatedSession, PanePasteImageControlParams, PaneSendBytesControlParams,
    SessionHelloParams, StreamCloseParams, StreamHistoryParams, StreamOpenParams,
    StreamResizeParams, StreamScrollDirection, StreamScrollParams, StreamScrollSource,
    API_REQUEST_METHOD, CAPABILITY_CATALOG, CAPABILITY_NOTIFICATION, CAPABILITY_PANE_STREAM,
    CAPABILITY_PASTE_IMAGE, CAPABILITY_WINDOW_TITLE, CATALOG_EVENT, CATALOG_RESYNC_EVENT,
    CONTROL_STREAM_ID, FRAMED_PROTOCOL_MIN_SUPPORTED, FRAMED_PROTOCOL_VERSION, FRAME_HEADER_BYTES,
    HISTORY_FETCH_MAX_BYTES, HISTORY_PAGE_DEFAULT_BYTES, HISTORY_PAGE_MIN_BYTES,
    HISTORY_PAGE_SERVED_MAX_BYTES, MAX_FRAME_PAYLOAD, NOTIFICATION_POSTED_EVENT,
    PANE_PASTE_IMAGE_METHOD, PANE_SEND_BYTES_METHOD, PING_METHOD, SESSION_HELLO_METHOD,
    SESSION_SNAPSHOT_METHOD, STREAM_CLOSED_EVENT, STREAM_CLOSE_METHOD, STREAM_HISTORY_METHOD,
    STREAM_OPEN_METHOD, STREAM_RESIZE_METHOD, STREAM_REVOKED_EVENT, STREAM_SCROLL_METHOD,
    WINDOW_TITLE_CHANGED_EVENT,
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
///
/// The legacy terminal-attach path draws from this same allocator when it
/// bridges onto the pane write-grant table, so grant holder ids never
/// collide across the two attach paths.
static NEXT_STREAM_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) fn allocate_stream_id() -> Option<u32> {
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

/// One open pane stream on this session.
struct OpenStream {
    /// Live output tail.
    subscription: PaneOutputSubscription,
    /// Public pane id the server resolved the open target to.
    pane_id: String,
    /// Write grant held for as long as a write-mode stream is open. Dropping
    /// it releases the pane for the next writer.
    write_grant: Option<WriteGrant>,
    /// Immutable history capture taken with the stream's snapshot;
    /// `stream.history` pages are byte-contiguous slices of it.
    history: Arc<PaneStreamHistory>,
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

    // Open pane streams, keyed by server-allocated stream id. Dropping an
    // entry detaches it from the pane tap, releases any write grant, and
    // releases its history capture.
    let mut streams: HashMap<u32, OpenStream> = HashMap::new();
    let end = run_negotiated_session(
        &mut stream,
        &mut reader,
        &session,
        &mut streams,
        api_tx,
        event_hub,
        running,
    );

    // Every stream the session still owns goes away with the connection —
    // on clean session ends and on io-error exits alike, so no exit path can
    // leak the pane geometry lock a write grant owned.
    for (stream_id, open) in streams.drain() {
        release_write_stream(api_tx, stream_id, open);
    }

    match end? {
        SessionEnd::PeerClosed => debug!("framed session closed by client"),
        SessionEnd::ServerStopped => debug!("framed session closed on server stop"),
        SessionEnd::ProtocolError => debug!("framed session closed after protocol error"),
        SessionEnd::Overloaded => debug!("framed session closed after output overload"),
    }
    Ok(())
}

/// Negotiated session loop: control-plane requests, output pumping, and
/// event broadcasts until disconnect. Streams opened along the way stay in
/// `streams`, which the caller releases on every exit path, including `?`
/// io-error propagation out of this loop.
fn run_negotiated_session(
    stream: &mut LocalStream,
    reader: &mut FrameReader,
    session: &NegotiatedSession,
    streams: &mut HashMap<u32, OpenStream>,
    api_tx: &ApiRequestSender,
    event_hub: &EventHub,
    running: &Arc<AtomicBool>,
) -> io::Result<SessionEnd> {
    // Events published before the handshake are history, not session traffic.
    let mut event_cursor = event_hub.current_sequence();

    loop {
        match reader.poll_frame(stream)? {
            PollFrame::Closed => return Ok(SessionEnd::PeerClosed),
            PollFrame::Pending => {
                if !running.load(Ordering::Relaxed) {
                    return Ok(SessionEnd::ServerStopped);
                }
                if let Some(end) = pump_session_output_or_end(
                    stream,
                    session,
                    streams,
                    api_tx,
                    event_hub,
                    &mut event_cursor,
                )? {
                    return Ok(end);
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
                        stream,
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
                        return Ok(SessionEnd::PeerClosed);
                    }
                    return Ok(SessionEnd::ProtocolError);
                }
                FrameType::Control => {
                    if !handle_control_request(stream, session, &frame, api_tx, event_hub, streams)?
                    {
                        return Ok(SessionEnd::PeerClosed);
                    }
                    // Drain stream output after every control frame too, so a
                    // burst of control traffic or a slow app dispatch cannot
                    // stall DATA tails long enough to overflow the bounded
                    // subscriber buffers under a fast producer.
                    if let Some(end) = pump_session_output_or_end(
                        stream,
                        session,
                        streams,
                        api_tx,
                        event_hub,
                        &mut event_cursor,
                    )? {
                        return Ok(end);
                    }
                }
            },
        }
    }
}

/// Runs one output pump pass, translating the outcome into the session end
/// it forces, if any. Writes the overload error itself so both call sites
/// (idle poll and post-control-frame drain) behave identically.
fn pump_session_output_or_end(
    stream: &mut LocalStream,
    session: &NegotiatedSession,
    streams: &mut HashMap<u32, OpenStream>,
    api_tx: &ApiRequestSender,
    event_hub: &EventHub,
    event_cursor: &mut u64,
) -> io::Result<Option<SessionEnd>> {
    match pump_session_output(stream, session, streams, api_tx, event_hub, event_cursor)? {
        PumpOutcome::Continue => Ok(None),
        PumpOutcome::PeerClosed => Ok(Some(SessionEnd::PeerClosed)),
        PumpOutcome::Overloaded(stream_id) => {
            let _ = write_control_allow_disconnect(
                stream,
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
            Ok(Some(SessionEnd::Overloaded))
        }
    }
}

/// Drains open stream subscriptions into DATA frames and broadcasts
/// capability-gated events from the hub. Runs on every idle poll tick.
fn pump_session_output(
    stream: &mut LocalStream,
    session: &NegotiatedSession,
    streams: &mut HashMap<u32, OpenStream>,
    api_tx: &ApiRequestSender,
    event_hub: &EventHub,
    event_cursor: &mut u64,
) -> io::Result<PumpOutcome> {
    let mut closed_streams = Vec::new();
    let mut revoked_streams = Vec::new();
    for (&stream_id, open) in streams.iter() {
        let drain = open.subscription.drain();
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
        } else if open
            .write_grant
            .as_ref()
            .is_some_and(WriteGrant::is_revoked)
        {
            revoked_streams.push(stream_id);
        }
    }
    for stream_id in closed_streams {
        if let Some(open) = streams.remove(&stream_id) {
            release_write_stream(api_tx, stream_id, open);
        }
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
    // Another client took the pane's write grant over. Only this stream ends;
    // the connection and its other streams stay up.
    for stream_id in revoked_streams {
        if let Some(open) = streams.remove(&stream_id) {
            release_write_stream(api_tx, stream_id, open);
        }
        let sent = write_control_allow_disconnect(
            stream,
            &serde_json::json!({
                "event": STREAM_REVOKED_EVENT,
                "data": { "stream_id": stream_id, "reason": "taken_over" },
            }),
        )?;
        if !sent {
            return Ok(PumpOutcome::PeerClosed);
        }
    }

    let wants_notifications = session.has_capability(CAPABILITY_NOTIFICATION);
    let wants_window_title = session.has_capability(CAPABILITY_WINDOW_TITLE);
    let wants_catalog = session.has_capability(CAPABILITY_CATALOG);
    if wants_notifications || wants_window_title || wants_catalog {
        let (events, lost) = event_hub.events_after_with_loss(*event_cursor);
        if lost && wants_catalog {
            // The bounded event buffer overflowed past this session's cursor:
            // catalog events are gone for good, so the mirrored catalog can
            // only be repaired by a fresh session.snapshot resync.
            let sent = write_control_allow_disconnect(
                stream,
                &serde_json::json!({
                    "event": CATALOG_RESYNC_EVENT,
                    "data": { "reason": "event_overflow" },
                }),
            )?;
            if !sent {
                return Ok(PumpOutcome::PeerClosed);
            }
        }
        for (sequence, envelope) in events {
            *event_cursor = sequence;
            let payload = match envelope.event {
                crate::api::schema::EventKind::NotificationPosted if wants_notifications => {
                    serde_json::json!({
                        "event": NOTIFICATION_POSTED_EVENT,
                        "seq": sequence,
                        "data": envelope.data,
                    })
                }
                crate::api::schema::EventKind::WindowTitleChanged if wants_window_title => {
                    serde_json::json!({
                        "event": WINDOW_TITLE_CHANGED_EVENT,
                        "seq": sequence,
                        "data": envelope.data,
                    })
                }
                // Notification and window-title facts flow only through their
                // dedicated capability-gated events, never as catalog events.
                crate::api::schema::EventKind::NotificationPosted
                | crate::api::schema::EventKind::WindowTitleChanged => continue,
                _ if wants_catalog => serde_json::json!({
                    "event": CATALOG_EVENT,
                    "seq": sequence,
                    "data": envelope,
                }),
                _ => continue,
            };
            let sent = write_control_allow_disconnect(stream, &payload)?;
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
                "remedy": remedy.as_str(),
                "server_protocol": FRAMED_PROTOCOL_VERSION,
                "server_min_protocol": FRAMED_PROTOCOL_MIN_SUPPORTED,
                "client_protocol": params.protocol,
                "client_min_protocol": params.min_protocol.unwrap_or(params.protocol),
            });
            write_control_allow_disconnect(
                stream,
                &error_response(
                    &request.id,
                    crate::protocol::framed::PROTOCOL_OUT_OF_WINDOW_CODE,
                    &message,
                    Some(data),
                ),
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
    event_hub: &EventHub,
    streams: &mut HashMap<u32, OpenStream>,
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
        STREAM_CLOSE_METHOD => handle_stream_close(stream, session, request, api_tx, streams),
        STREAM_RESIZE_METHOD => handle_stream_resize(stream, session, request, api_tx, streams),
        STREAM_SCROLL_METHOD => handle_stream_scroll(stream, session, request, api_tx, streams),
        STREAM_HISTORY_METHOD => handle_stream_history(stream, session, request, streams),
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
        SESSION_SNAPSHOT_METHOD => {
            handle_session_snapshot(stream, session, request, api_tx, event_hub)
        }
        API_REQUEST_METHOD => handle_api_request_passthrough(stream, session, request, api_tx),
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

/// Answers `session.snapshot`: the full catalog snapshot from the app thread
/// plus the event sequence anchor it is current through.
///
/// The anchor is read *before* the snapshot is built, so an event racing the
/// snapshot is replayed to the client rather than lost; catalog event
/// application is upsert-shaped, so the replay converges.
fn handle_session_snapshot(
    stream: &mut LocalStream,
    session: &NegotiatedSession,
    request: ControlRequest,
    api_tx: &ApiRequestSender,
    event_hub: &EventHub,
) -> io::Result<bool> {
    if let Some(rejection) = capability_rejection(session, CAPABILITY_CATALOG, &request) {
        return write_control_allow_disconnect(stream, &rejection);
    }
    let sequence = event_hub.current_sequence();
    let response = dispatch_to_app_with_timeout(
        crate::api::schema::Request {
            id: request.id.clone(),
            method: crate::api::schema::Method::SessionSnapshot(
                crate::api::schema::EmptyParams::default(),
            ),
        },
        api_tx,
        Some(APP_RESPONSE_TIMEOUT),
    );
    let anchored = match serde_json::from_str::<serde_json::Value>(&response) {
        Ok(mut value) => {
            if let Some(result) = value.get_mut("result").and_then(|r| r.as_object_mut()) {
                result.insert("sequence".to_owned(), serde_json::json!(sequence));
            }
            value.to_string()
        }
        Err(_) => response,
    };
    write_control_raw_allow_disconnect(stream, &anchored)
}

/// Forwards one JSON API request to the app thread verbatim and relays the
/// response, so framed clients reach the whole Method vocabulary without a
/// per-method allowlist here. Long-poll methods are rejected: they would
/// wedge this session thread against the app-response timeout.
fn handle_api_request_passthrough(
    stream: &mut LocalStream,
    session: &NegotiatedSession,
    request: ControlRequest,
    api_tx: &ApiRequestSender,
) -> io::Result<bool> {
    if let Some(rejection) = capability_rejection(session, CAPABILITY_CATALOG, &request) {
        return write_control_allow_disconnect(stream, &rejection);
    }
    let Some(inner) = request.params.get("request").cloned() else {
        return write_control_allow_disconnect(
            stream,
            &error_response(
                &request.id,
                "invalid_params",
                &format!("{API_REQUEST_METHOD} params carry no request"),
                None,
            ),
        );
    };
    let api_request = match serde_json::from_value::<crate::api::schema::Request>(inner) {
        Ok(api_request) => api_request,
        Err(err) => {
            return write_control_allow_disconnect(
                stream,
                &error_response(
                    &request.id,
                    "invalid_params",
                    &format!("invalid api request: {err}"),
                    None,
                ),
            );
        }
    };
    if matches!(
        api_request.method,
        crate::api::schema::Method::EventsSubscribe(_) | crate::api::schema::Method::EventsWait(_)
    ) {
        return write_control_allow_disconnect(
            stream,
            &error_response(
                &request.id,
                "invalid_params",
                "long-poll methods are not available over api.request; negotiate the catalog capability for events",
                None,
            ),
        );
    }
    let response = dispatch_to_app_with_timeout(api_request, api_tx, Some(APP_RESPONSE_TIMEOUT));
    write_control_raw_allow_disconnect(stream, &response)
}

/// Opens a pane output stream: allocates the stream id, registers the pending
/// handoff slot, dispatches to the app thread, and claims the subscription.
fn handle_stream_open(
    stream: &mut LocalStream,
    session: &NegotiatedSession,
    request: ControlRequest,
    api_tx: &ApiRequestSender,
    streams: &mut HashMap<u32, OpenStream>,
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
    let target = params.pane_id.clone();
    let response = dispatch_to_app_with_timeout(
        crate::api::schema::Request {
            id: request.id.clone(),
            method: crate::api::schema::Method::PaneStreamOpen(
                crate::api::schema::PaneStreamOpenParams {
                    pane_id: params.pane_id,
                    stream_id,
                    write: params.mode.is_write(),
                    takeover: params.takeover,
                    cols: params.cols,
                    rows: params.rows,
                },
            ),
        },
        api_tx,
        Some(APP_RESPONSE_TIMEOUT),
    );
    if api_response_outcome(&response) != "ok" {
        // A non-ok outcome can be a dispatch timeout the app handler lost the
        // race against: it may already have attached the stream and inserted
        // the pane geometry lock. Claiming and releasing like a normal close
        // (addressed by the original open target, which the close path also
        // resolves) keeps the lock from leaking; only an unfulfilled slot is
        // plainly cancelled.
        match claim_pending_stream(stream_id) {
            Some(attachment) => release_write_stream(
                api_tx,
                stream_id,
                OpenStream {
                    subscription: attachment.subscription,
                    pane_id: target,
                    write_grant: attachment.write_grant,
                    history: attachment.history,
                },
            ),
            None => cancel_pending_stream(stream_id),
        }
        return write_control_raw_allow_disconnect(stream, &response);
    }

    // The app resolved the open target (pane id, terminal id, or agent name)
    // to a public pane id; later stream methods address the pane by it.
    let pane_id = serde_json::from_str::<serde_json::Value>(&response)
        .ok()
        .and_then(|value| parse_stream_opened(&value).ok())
        .map(|opened| opened.pane_id)
        .unwrap_or_default();

    match claim_pending_stream(stream_id) {
        Some(attachment) => {
            streams.insert(
                stream_id,
                OpenStream {
                    subscription: attachment.subscription,
                    pane_id,
                    write_grant: attachment.write_grant,
                    history: attachment.history,
                },
            );
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
    api_tx: &ApiRequestSender,
    streams: &mut HashMap<u32, OpenStream>,
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

    match streams.remove(&params.stream_id) {
        Some(open) => release_write_stream(api_tx, params.stream_id, open),
        None => {
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

/// Resizes the pane behind a write-mode stream. Read streams do not own pane
/// geometry and are rejected.
fn handle_stream_resize(
    stream: &mut LocalStream,
    session: &NegotiatedSession,
    request: ControlRequest,
    api_tx: &ApiRequestSender,
    streams: &mut HashMap<u32, OpenStream>,
) -> io::Result<bool> {
    if let Some(rejection) = capability_rejection(session, CAPABILITY_PANE_STREAM, &request) {
        return write_control_allow_disconnect(stream, &rejection);
    }
    let params = match serde_json::from_value::<StreamResizeParams>(request.params) {
        Ok(params) => params,
        Err(err) => {
            return write_control_allow_disconnect(
                stream,
                &error_response(
                    &request.id,
                    "invalid_params",
                    &format!("invalid {STREAM_RESIZE_METHOD} params: {err}"),
                    None,
                ),
            );
        }
    };
    let pane_id = match write_stream_pane_id(streams, params.stream_id) {
        Ok(pane_id) => pane_id,
        Err(rejection) => {
            return write_control_allow_disconnect(
                stream,
                &rejection.into_response(&request.id, params.stream_id),
            );
        }
    };

    let response = dispatch_to_app_with_timeout(
        crate::api::schema::Request {
            id: request.id,
            method: crate::api::schema::Method::PaneStreamResize(
                crate::api::schema::PaneStreamResizeParams {
                    pane_id,
                    stream_id: params.stream_id,
                    cols: params.cols,
                    rows: params.rows,
                    cell_width_px: params.cell_width_px,
                    cell_height_px: params.cell_height_px,
                },
            ),
        },
        api_tx,
        Some(APP_RESPONSE_TIMEOUT),
    );
    write_control_raw_allow_disconnect(stream, &response)
}

/// Scrolls the pane behind a write-mode stream using the pane's own wheel and
/// page-key routing.
fn handle_stream_scroll(
    stream: &mut LocalStream,
    session: &NegotiatedSession,
    request: ControlRequest,
    api_tx: &ApiRequestSender,
    streams: &mut HashMap<u32, OpenStream>,
) -> io::Result<bool> {
    if let Some(rejection) = capability_rejection(session, CAPABILITY_PANE_STREAM, &request) {
        return write_control_allow_disconnect(stream, &rejection);
    }
    let params = match serde_json::from_value::<StreamScrollParams>(request.params) {
        Ok(params) => params,
        Err(err) => {
            return write_control_allow_disconnect(
                stream,
                &error_response(
                    &request.id,
                    "invalid_params",
                    &format!("invalid {STREAM_SCROLL_METHOD} params: {err}"),
                    None,
                ),
            );
        }
    };
    let pane_id = match write_stream_pane_id(streams, params.stream_id) {
        Ok(pane_id) => pane_id,
        Err(rejection) => {
            return write_control_allow_disconnect(
                stream,
                &rejection.into_response(&request.id, params.stream_id),
            );
        }
    };

    let response = dispatch_to_app_with_timeout(
        crate::api::schema::Request {
            id: request.id,
            method: crate::api::schema::Method::PaneStreamScroll(
                crate::api::schema::PaneStreamScrollParams {
                    pane_id,
                    stream_id: params.stream_id,
                    direction: match params.direction {
                        StreamScrollDirection::Up => {
                            crate::api::schema::PaneStreamScrollDirection::Up
                        }
                        StreamScrollDirection::Down => {
                            crate::api::schema::PaneStreamScrollDirection::Down
                        }
                    },
                    lines: params.lines,
                    source: match params.source {
                        StreamScrollSource::Wheel => {
                            crate::api::schema::PaneStreamScrollSource::Wheel
                        }
                        StreamScrollSource::PageKey => {
                            crate::api::schema::PaneStreamScrollSource::PageKey
                        }
                    },
                    column: params.column,
                    row: params.row,
                    modifiers: params.modifiers,
                },
            ),
        },
        api_tx,
        Some(APP_RESPONSE_TIMEOUT),
    );
    write_control_raw_allow_disconnect(stream, &response)
}

/// Why a stream method addressed to a stream id cannot proceed.
enum StreamMethodRejection {
    UnknownStream,
    NotWritable,
}

impl StreamMethodRejection {
    fn into_response(self, id: &str, stream_id: u32) -> serde_json::Value {
        match self {
            StreamMethodRejection::UnknownStream => error_response(
                id,
                "unknown_stream",
                &format!("no open stream with id {stream_id}"),
                None,
            ),
            StreamMethodRejection::NotWritable => error_response(
                id,
                "stream_not_write_holder",
                &format!("stream {stream_id} was not opened in write mode"),
                None,
            ),
        }
    }
}

fn write_stream_pane_id(
    streams: &HashMap<u32, OpenStream>,
    stream_id: u32,
) -> Result<String, StreamMethodRejection> {
    let open = streams
        .get(&stream_id)
        .ok_or(StreamMethodRejection::UnknownStream)?;
    if open.write_grant.is_none() {
        return Err(StreamMethodRejection::NotWritable);
    }
    Ok(open.pane_id.clone())
}

/// Drops a stream and, when it held the pane write grant, tells the app to
/// release the pane state that grant owned.
fn release_write_stream(api_tx: &ApiRequestSender, stream_id: u32, open: OpenStream) {
    let held_write_grant = open.write_grant.is_some();
    let pane_id = open.pane_id.clone();
    // Dropping the grant first frees the pane for the next writer.
    drop(open);
    if !held_write_grant || pane_id.is_empty() {
        return;
    }
    let _ = dispatch_to_app_with_timeout(
        crate::api::schema::Request {
            id: format!("framed:stream.close:{stream_id}"),
            method: crate::api::schema::Method::PaneStreamClose(
                crate::api::schema::PaneStreamCloseParams { pane_id, stream_id },
            ),
        },
        api_tx,
        Some(APP_RESPONSE_TIMEOUT),
    );
}

/// Serves one `stream.history` page from the history capture held with the
/// open stream, walking backward from the opaque cursor.
///
/// The capture is immutable and was taken under the pane output tap lock at
/// the `stream.open` sequence, so pages are byte-contiguous slices of one
/// consistent buffer: chaining `next_cursor` yields the exact capture bytes,
/// gap-free and duplicate-free, regardless of concurrent pane output. Pages
/// are unwrapped, content-only ANSI and never re-assert terminal modes.
/// Served session-locally, without dispatching to the app thread. History is
/// served to read and write streams alike: paging back through scrollback
/// never requires the write grant.
fn handle_stream_history(
    stream: &mut LocalStream,
    session: &NegotiatedSession,
    request: ControlRequest,
    streams: &mut HashMap<u32, OpenStream>,
) -> io::Result<bool> {
    if let Some(rejection) = capability_rejection(session, CAPABILITY_PANE_STREAM, &request) {
        return write_control_allow_disconnect(stream, &rejection);
    }
    let params = match serde_json::from_value::<StreamHistoryParams>(request.params) {
        Ok(params) => params,
        Err(err) => {
            return write_control_allow_disconnect(
                stream,
                &error_response(
                    &request.id,
                    "invalid_params",
                    &format!("invalid {STREAM_HISTORY_METHOD} params: {err}"),
                    None,
                ),
            );
        }
    };

    let Some(cursor) = decode_history_cursor(&params.cursor) else {
        return write_control_allow_disconnect(
            stream,
            &error_response(
                &request.id,
                "invalid_cursor",
                "history cursor is not decodable; reopen the stream for a fresh cursor",
                None,
            ),
        );
    };
    let Some(open) = streams.get(&cursor.stream_id) else {
        return write_control_allow_disconnect(
            stream,
            &error_response(
                &request.id,
                "unknown_stream",
                &format!("no open stream with id {}", cursor.stream_id),
                None,
            ),
        );
    };

    let history = &open.history;
    let end = usize::try_from(cursor.offset).unwrap_or(usize::MAX);
    if cursor.pane_id != history.pane_id
        || cursor.sequence != history.sequence
        || end > history.content.len()
        || !history.content.is_char_boundary(end)
    {
        return write_control_allow_disconnect(
            stream,
            &error_response(
                &request.id,
                "invalid_cursor",
                "history cursor does not match the open stream's capture; \
                 reopen the stream for a fresh cursor",
                None,
            ),
        );
    }

    let max_bytes = usize::try_from(
        params
            .max_bytes
            .unwrap_or(HISTORY_PAGE_DEFAULT_BYTES as u64),
    )
    .unwrap_or(HISTORY_FETCH_MAX_BYTES)
    .clamp(HISTORY_PAGE_MIN_BYTES, HISTORY_FETCH_MAX_BYTES)
    // JSON string escaping can expand a content byte up to sixfold, so the
    // sliced page is bounded well below the frame payload limit; larger
    // requests are served across multiple pages via `next_cursor`.
    .min(HISTORY_PAGE_SERVED_MAX_BYTES);
    let start = history_page_start(&history.content, end, max_bytes);
    let next_cursor = (start > 0).then(|| {
        encode_history_cursor(&HistoryCursor {
            pane_id: history.pane_id.clone(),
            sequence: history.sequence,
            stream_id: cursor.stream_id,
            offset: start as u64,
        })
    });

    let response = serde_json::json!({
        "id": request.id,
        "result": {
            "type": "stream_history",
            "stream_id": cursor.stream_id,
            "content": &history.content[start..end],
            "next_cursor": next_cursor,
            "at_top": start == 0,
            // A younger page's hard-capped start can cut a logical line at
            // `end`; the client must then join this page to the content
            // below it without fabricating a line break.
            "end_cut_mid_line": history_page_end_cut_mid_line(&history.content, end),
        },
    });
    let payload = match serde_json::to_vec(&response) {
        Ok(payload) => payload,
        Err(err) => {
            return write_control_allow_disconnect(
                stream,
                &error_response(
                    &request.id,
                    "internal_error",
                    &format!("failed to encode stream_history response: {err}"),
                    None,
                ),
            );
        }
    };
    if payload.len() > MAX_FRAME_PAYLOAD {
        // Unreachable given the escaping headroom in
        // HISTORY_PAGE_SERVED_MAX_BYTES; guard anyway so one oversized page
        // degrades into an error response instead of tearing down the whole
        // session with an oversized frame.
        return write_control_allow_disconnect(
            stream,
            &error_response(
                &request.id,
                "history_page_too_large",
                "history page encodes larger than the frame limit; \
                 retry with a smaller max_bytes",
                None,
            ),
        );
    }
    write_frame_allow_disconnect(stream, FrameType::Control, CONTROL_STREAM_ID, &payload)
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
    use crate::pane::output_tap::{fulfill_pending_stream, PaneOutputTap, PaneStreamHistory};
    use crate::protocol::framed::{read_frame, FRAMED_MAGIC};
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
        spawn_pane_stream_responder_with_history(tap, requests, String::new())
    }

    fn spawn_pane_stream_responder_with_history(
        tap: std::sync::Weak<PaneOutputTap>,
        requests: mpsc::Sender<Method>,
        history: String,
    ) -> (crate::api::ApiRequestSender, std::thread::JoinHandle<()>) {
        let (api_tx, mut api_rx) = tokio::sync::mpsc::unbounded_channel::<ApiRequestMessage>();
        let responder = std::thread::spawn(move || {
            while let Some(msg) = api_rx.blocking_recv() {
                let _ = requests.send(msg.request.method.clone());
                let response = match msg.request.method {
                    Method::PaneStreamOpen(params) => {
                        // Mirrors the real handler: the write grant is taken
                        // before the subscription attaches, and a refused
                        // grant answers with the structured error.
                        let write_grant = if params.write {
                            match crate::pane::write_grant::acquire_write_grant(
                                &params.pane_id,
                                params.stream_id,
                                params.takeover,
                            ) {
                                Ok(grant) => Some(grant),
                                Err(conflict) => {
                                    msg.respond_to
                                        .send(
                                            serde_json::json!({
                                                "id": msg.request.id,
                                                "error": {
                                                    "code": crate::protocol::framed::PANE_WRITE_LOCKED_ERROR,
                                                    "message": format!(
                                                        "pane {} already has a writable stream (stream {})",
                                                        params.pane_id, conflict.holder_stream_id
                                                    ),
                                                },
                                            })
                                            .to_string(),
                                        )
                                        .unwrap();
                                    continue;
                                }
                            }
                        } else {
                            None
                        };
                        let tap = tap.upgrade().expect("tap must be alive during stream.open");
                        let (subscription, snapshot) =
                            tap.subscribe_with_snapshot(|| "SNAPSHOT".to_owned());
                        let sequence = subscription.sequence();
                        let capture = Arc::new(PaneStreamHistory {
                            pane_id: "pane_1".into(),
                            sequence,
                            content: history.clone(),
                        });
                        let history_len = capture.content.len() as u64;
                        assert!(fulfill_pending_stream(
                            params.stream_id,
                            crate::pane::output_tap::PaneStreamAttachment {
                                subscription,
                                write_grant,
                                history: capture,
                            }
                        ));
                        serde_json::to_string(&SuccessResponse {
                            id: msg.request.id,
                            result: ResponseResult::PaneStreamOpened {
                                stream: crate::api::schema::PaneStreamOpenInfo {
                                    pane_id: params.pane_id,
                                    workspace_id: "ws_1".into(),
                                    stream_id: params.stream_id,
                                    sequence,
                                    snapshot,
                                    history_cursor: crate::protocol::framed::encode_history_cursor(
                                        &HistoryCursor {
                                            pane_id: "pane_1".into(),
                                            sequence,
                                            stream_id: params.stream_id,
                                            offset: history_len,
                                        },
                                    ),
                                    cols: 80,
                                    rows: 24,
                                },
                            },
                        })
                        .unwrap()
                    }
                    Method::PaneSendBytes(_)
                    | Method::PanePasteImage(_)
                    | Method::PaneStreamClose(_)
                    | Method::PaneStreamResize(_)
                    | Method::PaneStreamScroll(_) => serde_json::to_string(&SuccessResponse {
                        id: msg.request.id,
                        result: ResponseResult::Ok {},
                    })
                    .unwrap(),
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
        let cursor = crate::protocol::framed::decode_history_cursor(
            opened["result"]["stream"]["history_cursor"]
                .as_str()
                .unwrap(),
        )
        .expect("history cursor decodes");
        assert_eq!(cursor.pane_id, "pane_1");
        assert_eq!(cursor.sequence, 14);
        assert_eq!(cursor.stream_id, stream_id);
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
    fn framed_session_snapshot_requires_the_catalog_capability() {
        let (mut client, server, path) = local_stream_pair("snapshot-gated");
        let (done_rx, thread) = spawn_connection(server);

        hello(&mut client, "h20", &["pane-stream"]);
        send_control(
            &mut client,
            serde_json::json!({"id": "sn1", "method": "session.snapshot", "params": {}}),
        );
        let error = read_control(&mut client);
        assert_eq!(error["error"]["code"], "capability_not_negotiated");
        assert_eq!(error["error"]["data"]["capability"], "catalog");

        finish(client, done_rx, thread, path);
    }

    #[test]
    fn framed_session_snapshot_returns_snapshot_with_sequence_anchor() {
        let event_hub = EventHub::default();
        event_hub.push(crate::api::schema::EventEnvelope {
            event: crate::api::schema::EventKind::PaneClosed,
            data: crate::api::schema::EventData::PaneClosed {
                pane_id: "p_0".into(),
                workspace_id: "ws_0".into(),
            },
        });
        let anchor = event_hub.current_sequence();

        let (api_tx, mut api_rx) = tokio::sync::mpsc::unbounded_channel::<ApiRequestMessage>();
        let responder = std::thread::spawn(move || {
            while let Some(msg) = api_rx.blocking_recv() {
                let Method::SessionSnapshot(_) = msg.request.method else {
                    panic!("unexpected method {:?}", msg.request.method);
                };
                let snapshot = crate::api::schema::SessionSnapshot {
                    version: "test".into(),
                    protocol: 3,
                    focused_workspace_id: Some("ws_1".into()),
                    focused_tab_id: None,
                    focused_pane_id: None,
                    workspaces: Vec::new(),
                    tabs: Vec::new(),
                    panes: Vec::new(),
                    layouts: Vec::new(),
                    agents: Vec::new(),
                };
                let response = serde_json::to_string(&SuccessResponse {
                    id: msg.request.id,
                    result: ResponseResult::SessionSnapshot {
                        snapshot: Box::new(snapshot),
                    },
                })
                .unwrap();
                msg.respond_to.send(response).unwrap();
            }
        });

        let (mut client, server, path) = local_stream_pair("snapshot");
        let (done_rx, thread) = spawn_connection_with(server, api_tx.clone(), event_hub.clone());

        hello(&mut client, "h21", &["catalog"]);
        send_control(
            &mut client,
            serde_json::json!({"id": "sn2", "method": "session.snapshot", "params": {}}),
        );
        let response = read_control(&mut client);
        assert_eq!(response["id"], "sn2");
        assert_eq!(response["result"]["type"], "session_snapshot");
        assert_eq!(response["result"]["sequence"], anchor);
        assert_eq!(response["result"]["snapshot"]["version"], "test");

        // The client-side parser reads the same shape.
        let (snapshot, sequence) =
            crate::protocol::framed::parse_session_snapshot(&response).unwrap();
        assert_eq!(sequence, anchor);
        assert_eq!(snapshot["focused_workspace_id"], "ws_1");

        finish(client, done_rx, thread, path);
        drop(api_tx);
        responder.join().unwrap();
    }

    #[test]
    fn framed_api_request_passthrough_dispatches_and_relays() {
        let (api_tx, mut api_rx) = tokio::sync::mpsc::unbounded_channel::<ApiRequestMessage>();
        let responder = std::thread::spawn(move || {
            while let Some(msg) = api_rx.blocking_recv() {
                let Method::WorkspaceFocus(target) = &msg.request.method else {
                    panic!("unexpected method {:?}", msg.request.method);
                };
                assert_eq!(target.workspace_id, "ws_9");
                msg.respond_to
                    .send(
                        serde_json::json!({
                            "id": msg.request.id,
                            "result": { "type": "ok" },
                        })
                        .to_string(),
                    )
                    .unwrap();
            }
        });

        let (mut client, server, path) = local_stream_pair("api-request");
        let (done_rx, thread) = spawn_connection_with(server, api_tx.clone(), EventHub::default());

        hello(&mut client, "h23", &["catalog"]);
        send_control(
            &mut client,
            serde_json::json!({
                "id": "a1",
                "method": "api.request",
                "params": { "request": {
                    "id": "a1",
                    "method": "workspace.focus",
                    "params": { "workspace_id": "ws_9" },
                }},
            }),
        );
        let response = read_control(&mut client);
        assert_eq!(response["id"], "a1");
        assert_eq!(response["result"]["type"], "ok");

        // Long-poll methods are rejected instead of wedging the session.
        send_control(
            &mut client,
            serde_json::json!({
                "id": "a2",
                "method": "api.request",
                "params": { "request": {
                    "id": "a2",
                    "method": "events.wait",
                    "params": {},
                }},
            }),
        );
        let rejected = read_control(&mut client);
        assert_eq!(rejected["error"]["code"], "invalid_params");

        finish(client, done_rx, thread, path);
        drop(api_tx);
        responder.join().unwrap();
    }

    #[test]
    fn framed_api_request_requires_the_catalog_capability() {
        let (mut client, server, path) = local_stream_pair("api-request-gated");
        let (done_rx, thread) = spawn_connection(server);

        hello(&mut client, "h24", &["pane-stream"]);
        send_control(
            &mut client,
            serde_json::json!({
                "id": "a3",
                "method": "api.request",
                "params": { "request": {
                    "id": "a3",
                    "method": "workspace.focus",
                    "params": { "workspace_id": "ws_1" },
                }},
            }),
        );
        let error = read_control(&mut client);
        assert_eq!(error["error"]["code"], "capability_not_negotiated");

        finish(client, done_rx, thread, path);
    }

    #[test]
    fn framed_catalog_events_broadcast_when_negotiated() {
        let event_hub = EventHub::default();
        let (mut client, server, path) = local_stream_pair("catalog-events");
        let (api_tx, _api_rx) = tokio::sync::mpsc::unbounded_channel();
        let (done_rx, thread) = spawn_connection_with(server, api_tx, event_hub.clone());

        hello(&mut client, "h22", &["catalog"]);
        // A ping round-trip guarantees the session loop is running and has
        // anchored its event cursor, so the pushes below are session traffic.
        send_control(
            &mut client,
            serde_json::json!({"id": "p0", "method": "ping", "params": {}}),
        );
        assert_eq!(read_control(&mut client)["result"]["type"], "pong");

        event_hub.push(crate::api::schema::EventEnvelope {
            event: crate::api::schema::EventKind::PaneClosed,
            data: crate::api::schema::EventData::PaneClosed {
                pane_id: "p_1".into(),
                workspace_id: "ws_1".into(),
            },
        });
        let event = read_control(&mut client);
        assert_eq!(event["event"], "catalog.event");
        assert!(event["seq"].as_u64().unwrap() > 0);
        assert_eq!(event["data"]["event"], "pane_closed");
        assert_eq!(event["data"]["data"]["pane_id"], "p_1");

        // Notification facts never masquerade as catalog events; without the
        // notification capability this event is skipped entirely.
        event_hub.push(crate::api::schema::EventEnvelope {
            event: crate::api::schema::EventKind::NotificationPosted,
            data: crate::api::schema::EventData::NotificationPosted {
                kind: crate::api::schema::NotificationEventKind::Toast,
                message: "not a catalog fact".into(),
                body: None,
            },
        });
        event_hub.push(crate::api::schema::EventEnvelope {
            event: crate::api::schema::EventKind::WorkspaceFocused,
            data: crate::api::schema::EventData::WorkspaceFocused {
                workspace_id: "ws_2".into(),
            },
        });
        let event = read_control(&mut client);
        assert_eq!(event["event"], "catalog.event");
        assert_eq!(event["data"]["event"], "workspace_focused");
        assert_eq!(event["data"]["data"]["workspace_id"], "ws_2");

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
    fn framed_write_stream_grant_is_refused_then_taken_over_across_connections() {
        let tap = Arc::new(PaneOutputTap::default());
        let (request_tx, _request_rx) = mpsc::channel();
        let (api_tx, responder) = spawn_pane_stream_responder(Arc::downgrade(&tap), request_tx);

        let (mut holder, holder_server, holder_path) = local_stream_pair("grant-holder");
        let (holder_done, holder_thread) =
            spawn_connection_with(holder_server, api_tx.clone(), EventHub::default());
        let (mut rival, rival_server, rival_path) = local_stream_pair("grant-rival");
        let (rival_done, rival_thread) =
            spawn_connection_with(rival_server, api_tx.clone(), EventHub::default());

        hello(&mut holder, "h20", &["pane-stream"]);
        hello(&mut rival, "h21", &["pane-stream"]);

        send_control(
            &mut holder,
            serde_json::json!({
                "id": "w1",
                "method": "stream.open",
                "params": {"pane_id": "pane_write", "mode": "write", "cols": 80, "rows": 24},
            }),
        );
        let opened = read_control(&mut holder);
        let stream_id = opened["result"]["stream"]["stream_id"].as_u64().unwrap() as u32;

        // Without takeover the rival is refused and stays connected.
        send_control(
            &mut rival,
            serde_json::json!({
                "id": "w2",
                "method": "stream.open",
                "params": {"pane_id": "pane_write", "mode": "write"},
            }),
        );
        let refused = read_control(&mut rival);
        assert_eq!(
            refused["error"]["code"],
            crate::protocol::framed::PANE_WRITE_LOCKED_ERROR
        );
        send_control(
            &mut rival,
            serde_json::json!({"id": "p1", "method": "ping", "params": {}}),
        );
        assert_eq!(read_control(&mut rival)["result"]["type"], "pong");

        // With takeover the rival wins and the holder is revoked on its own
        // stream, keeping its connection.
        send_control(
            &mut rival,
            serde_json::json!({
                "id": "w3",
                "method": "stream.open",
                "params": {"pane_id": "pane_write", "mode": "write", "takeover": true},
            }),
        );
        let taken = read_control(&mut rival);
        assert_eq!(taken["result"]["type"], "pane_stream_opened");

        let revoked = read_control(&mut holder);
        assert_eq!(revoked["event"], "stream.revoked");
        assert_eq!(revoked["data"]["stream_id"], stream_id);
        assert_eq!(revoked["data"]["reason"], "taken_over");
        send_control(
            &mut holder,
            serde_json::json!({"id": "p2", "method": "ping", "params": {}}),
        );
        assert_eq!(read_control(&mut holder)["result"]["type"], "pong");

        finish(holder, holder_done, holder_thread, holder_path);
        finish(rival, rival_done, rival_thread, rival_path);
        drop(api_tx);
        responder.join().unwrap();
    }

    #[test]
    fn framed_stream_resize_and_scroll_require_a_write_stream() {
        let tap = Arc::new(PaneOutputTap::default());
        let (request_tx, _request_rx) = mpsc::channel();
        let (api_tx, responder) = spawn_pane_stream_responder(Arc::downgrade(&tap), request_tx);

        let (mut client, server, path) = local_stream_pair("stream-methods");
        let (done_rx, thread) = spawn_connection_with(server, api_tx.clone(), EventHub::default());
        hello(&mut client, "h22", &["pane-stream"]);

        send_control(
            &mut client,
            serde_json::json!({
                "id": "r1",
                "method": "stream.open",
                "params": {"pane_id": "pane_read", "mode": "read"},
            }),
        );
        let opened = read_control(&mut client);
        let read_stream_id = opened["result"]["stream"]["stream_id"].as_u64().unwrap();

        send_control(
            &mut client,
            serde_json::json!({
                "id": "rs1",
                "method": "stream.resize",
                "params": {"stream_id": read_stream_id, "cols": 100, "rows": 30},
            }),
        );
        assert_eq!(
            read_control(&mut client)["error"]["code"],
            "stream_not_write_holder"
        );

        send_control(
            &mut client,
            serde_json::json!({
                "id": "sc1",
                "method": "stream.scroll",
                "params": {"stream_id": 987_654, "direction": "up", "lines": 1},
            }),
        );
        assert_eq!(read_control(&mut client)["error"]["code"], "unknown_stream");

        send_control(
            &mut client,
            serde_json::json!({
                "id": "w1",
                "method": "stream.open",
                "params": {"pane_id": "pane_methods", "mode": "write", "cols": 80, "rows": 24},
            }),
        );
        let opened = read_control(&mut client);
        let write_stream_id = opened["result"]["stream"]["stream_id"].as_u64().unwrap();

        send_control(
            &mut client,
            serde_json::json!({
                "id": "rs2",
                "method": "stream.resize",
                "params": {"stream_id": write_stream_id, "cols": 100, "rows": 30},
            }),
        );
        assert_eq!(read_control(&mut client)["result"]["type"], "ok");
        send_control(
            &mut client,
            serde_json::json!({
                "id": "sc2",
                "method": "stream.scroll",
                "params": {"stream_id": write_stream_id, "direction": "down", "lines": 2},
            }),
        );
        assert_eq!(read_control(&mut client)["result"]["type"], "ok");
        finish(client, done_rx, thread, path);
        drop(api_tx);
        responder.join().unwrap();
    }

    #[test]
    fn framed_stream_history_pages_walk_backward_gap_free() {
        let mut history = String::new();
        for line in 0..1500 {
            history.push_str(&format!("history line {line:04}\r\n"));
        }
        history.push_str("newest history line");

        let tap = Arc::new(PaneOutputTap::default());
        let (request_tx, _request_rx) = mpsc::channel();
        let (api_tx, responder) = spawn_pane_stream_responder_with_history(
            Arc::downgrade(&tap),
            request_tx,
            history.clone(),
        );

        let (mut client, server, path) = local_stream_pair("history-pages");
        let (done_rx, thread) = spawn_connection_with(server, api_tx.clone(), EventHub::default());

        hello(&mut client, "h13", &["pane-stream"]);
        send_control(
            &mut client,
            serde_json::json!({"id": "s1", "method": "stream.open", "params": {"pane_id": "pane_1"}}),
        );
        let opened = read_control(&mut client);
        let stream_id = opened["result"]["stream"]["stream_id"].as_u64().unwrap();
        let mut cursor = opened["result"]["stream"]["history_cursor"]
            .as_str()
            .unwrap()
            .to_owned();

        // Page backward with a small budget; the pages must reassemble the
        // capture exactly: gap-free, duplicate-free, newline-aligned.
        let mut pages: Vec<String> = Vec::new();
        let mut cuts: Vec<bool> = Vec::new();
        loop {
            send_control(
                &mut client,
                crate::protocol::framed::stream_history_request("hp", &cursor, 1),
            );
            let page = crate::protocol::framed::parse_stream_history(&read_control(&mut client))
                .expect("history page");
            assert_eq!(u64::from(page.stream_id), stream_id);
            assert!(!page.content.is_empty(), "pages must make progress");
            assert!(
                !page.content.contains("\x1b[?"),
                "history pages must never re-assert modes"
            );
            cuts.push(page.end_cut_mid_line);
            pages.push(page.content);
            match page.next_cursor {
                Some(next) => {
                    assert!(!page.at_top);
                    cursor = next;
                }
                None => {
                    assert!(page.at_top);
                    break;
                }
            }
        }
        assert!(pages.len() > 2, "budget must split the capture");
        let rejoined: String = pages.iter().rev().map(String::as_str).collect();
        assert_eq!(rejoined, history);
        // Newline alignment: every page after the first arrived ends exactly
        // where the previous started, which is right after a newline.
        for page in &pages[1..] {
            assert!(
                page.ends_with('\n'),
                "page boundary must be newline-aligned"
            );
        }
        assert!(
            cuts.iter().all(|cut| !cut),
            "newline-aligned boundaries must never be flagged as mid-line cuts"
        );

        // A jump-to-top sized fetch from the original cursor returns the
        // whole capture in one page.
        let full_cursor = opened["result"]["stream"]["history_cursor"]
            .as_str()
            .unwrap();
        send_control(
            &mut client,
            crate::protocol::framed::stream_history_request(
                "jt",
                full_cursor,
                HISTORY_FETCH_MAX_BYTES,
            ),
        );
        let full = crate::protocol::framed::parse_stream_history(&read_control(&mut client))
            .expect("jump-to-top page");
        assert!(full.at_top);
        assert!(full.next_cursor.is_none());
        assert_eq!(full.content, history);

        finish(client, done_rx, thread, path);
        drop(api_tx);
        responder.join().unwrap();
    }

    #[test]
    fn framed_stream_history_hard_caps_pages_on_newline_free_history() {
        // One giant soft-wrapped logical line: no newline anywhere in the
        // capture. Pages must still honor the byte budget (mid-line cuts on
        // char boundaries) instead of growing without bound.
        let history = "x".repeat(20 * 1024);

        let tap = Arc::new(PaneOutputTap::default());
        let (request_tx, _request_rx) = mpsc::channel();
        let (api_tx, responder) = spawn_pane_stream_responder_with_history(
            Arc::downgrade(&tap),
            request_tx,
            history.clone(),
        );

        let (mut client, server, path) = local_stream_pair("history-newline-free");
        let (done_rx, thread) = spawn_connection_with(server, api_tx.clone(), EventHub::default());

        hello(&mut client, "h16", &["pane-stream"]);
        send_control(
            &mut client,
            serde_json::json!({"id": "s1", "method": "stream.open", "params": {"pane_id": "pane_1"}}),
        );
        let opened = read_control(&mut client);
        let mut cursor = opened["result"]["stream"]["history_cursor"]
            .as_str()
            .unwrap()
            .to_owned();

        let mut pages: Vec<String> = Vec::new();
        let mut cuts: Vec<bool> = Vec::new();
        loop {
            send_control(
                &mut client,
                crate::protocol::framed::stream_history_request("nf", &cursor, 1),
            );
            let page = crate::protocol::framed::parse_stream_history(&read_control(&mut client))
                .expect("history page");
            assert!(!page.content.is_empty(), "pages must make progress");
            assert!(
                page.content.len() <= HISTORY_PAGE_MIN_BYTES + 3,
                "newline-free page must stay hard-capped, got {}",
                page.content.len()
            );
            cuts.push(page.end_cut_mid_line);
            pages.push(page.content);
            match page.next_cursor {
                Some(next) => cursor = next,
                None => {
                    assert!(page.at_top);
                    break;
                }
            }
        }
        assert!(pages.len() > 2, "budget must split the capture");
        let rejoined: String = pages.iter().rev().map(String::as_str).collect();
        assert_eq!(rejoined, history);
        // The first page ends at the capture end (not a cut); every later
        // page ends where a hard-capped start cut the logical line.
        assert!(!cuts[0], "the capture end is never a mid-line cut");
        assert!(
            cuts[1..].iter().all(|cut| *cut),
            "hard-capped newline-free boundaries must be flagged as cuts"
        );

        // The session survives the whole walk.
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
    fn framed_stream_history_rejects_bad_cursors_and_empty_history_is_at_top() {
        let tap = Arc::new(PaneOutputTap::default());
        let (request_tx, _request_rx) = mpsc::channel();
        let (api_tx, responder) = spawn_pane_stream_responder_with_history(
            Arc::downgrade(&tap),
            request_tx,
            String::new(),
        );

        let (mut client, server, path) = local_stream_pair("history-errors");
        let (done_rx, thread) = spawn_connection_with(server, api_tx.clone(), EventHub::default());

        hello(&mut client, "h14", &["pane-stream"]);
        send_control(
            &mut client,
            serde_json::json!({"id": "s1", "method": "stream.open", "params": {"pane_id": "pane_1"}}),
        );
        let opened = read_control(&mut client);
        let stream_id = opened["result"]["stream"]["stream_id"].as_u64().unwrap() as u32;
        let cursor = opened["result"]["stream"]["history_cursor"]
            .as_str()
            .unwrap()
            .to_owned();

        // Empty history: the very first page is already the top.
        send_control(
            &mut client,
            crate::protocol::framed::stream_history_request("e1", &cursor, 4096),
        );
        let page = crate::protocol::framed::parse_stream_history(&read_control(&mut client))
            .expect("empty history page");
        assert!(page.at_top);
        assert!(page.content.is_empty());
        assert!(page.next_cursor.is_none());

        // Undecodable cursor.
        send_control(
            &mut client,
            crate::protocol::framed::stream_history_request("e2", "not-a-cursor", 4096),
        );
        assert_eq!(read_control(&mut client)["error"]["code"], "invalid_cursor");

        // Cursor for a stream this session does not hold.
        let stray = crate::protocol::framed::encode_history_cursor(&HistoryCursor {
            pane_id: "pane_1".into(),
            sequence: 0,
            stream_id: stream_id.wrapping_add(7),
            offset: 0,
        });
        send_control(
            &mut client,
            crate::protocol::framed::stream_history_request("e3", &stray, 4096),
        );
        assert_eq!(read_control(&mut client)["error"]["code"], "unknown_stream");

        // Cursor whose capture identity does not match (stale sequence or
        // out-of-range offset).
        let stale = crate::protocol::framed::encode_history_cursor(&HistoryCursor {
            pane_id: "pane_1".into(),
            sequence: 99,
            stream_id,
            offset: 0,
        });
        send_control(
            &mut client,
            crate::protocol::framed::stream_history_request("e4", &stale, 4096),
        );
        assert_eq!(read_control(&mut client)["error"]["code"], "invalid_cursor");
        let overshoot = crate::protocol::framed::encode_history_cursor(&HistoryCursor {
            pane_id: "pane_1".into(),
            sequence: 0,
            stream_id,
            offset: 10_000,
        });
        send_control(
            &mut client,
            crate::protocol::framed::stream_history_request("e5", &overshoot, 4096),
        );
        assert_eq!(read_control(&mut client)["error"]["code"], "invalid_cursor");

        // The session survives all rejections.
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
    fn framed_stream_history_requires_pane_stream_capability() {
        let (mut client, server, path) = local_stream_pair("history-gated");
        let (done_rx, thread) = spawn_connection(server);

        hello(&mut client, "h15", &[]);
        send_control(
            &mut client,
            crate::protocol::framed::stream_history_request("g1", "whatever", 4096),
        );
        let rejection = read_control(&mut client);
        assert_eq!(rejection["error"]["code"], "capability_not_negotiated");
        assert_eq!(rejection["error"]["data"]["capability"], "pane-stream");

        finish(client, done_rx, thread, path);
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

//! Framed unified-protocol session handler on the API socket.
//!
//! `handle_connection` routes here after consuming the `HRDR` magic. The
//! session must open with a `session.hello` control frame; once negotiated it
//! serves control-plane requests (currently the heartbeat `ping`) until the
//! client disconnects or the server stops.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::debug;

use crate::api::server::CONNECTION_POLL_INTERVAL;
use crate::ipc::{
    is_connection_closed_error, poll_local_stream_read_len, set_local_stream_polling, LocalStream,
    LocalStreamReadLen,
};
use crate::protocol::framed::{
    decode_frame_header, negotiate_session_hello, write_frame, Frame, FrameType, FramedCodecError,
    HelloError, HelloRemedy, NegotiatedSession, SessionHelloParams, CONTROL_STREAM_ID,
    FRAMED_PROTOCOL_MIN_SUPPORTED, FRAMED_PROTOCOL_VERSION, FRAME_HEADER_BYTES, PING_METHOD,
    SESSION_HELLO_METHOD,
};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const READ_CHUNK_BYTES: usize = 64 * 1024;

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
}

/// Serves one framed session. The `HRDR` magic has already been consumed.
pub(super) fn serve(mut stream: LocalStream, running: &Arc<AtomicBool>) -> io::Result<()> {
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

    // Negotiated session loop: control-plane requests until disconnect.
    let end = loop {
        match reader.poll_frame(&mut stream)? {
            PollFrame::Closed => break SessionEnd::PeerClosed,
            PollFrame::Pending => {
                if !running.load(Ordering::Relaxed) {
                    break SessionEnd::ServerStopped;
                }
                std::thread::sleep(CONNECTION_POLL_INTERVAL);
            }
            PollFrame::Frame(frame) => match frame.frame_type {
                FrameType::Data => {
                    // No data streams exist until stream.open lands; data
                    // frames indicate a desynchronized client.
                    let sent = write_control_allow_disconnect(
                        &mut stream,
                        &error_response(
                            "",
                            "unknown_stream",
                            &format!("no open stream with id {}", frame.stream_id),
                            None,
                        ),
                    )?;
                    if !sent {
                        break SessionEnd::PeerClosed;
                    }
                    break SessionEnd::ProtocolError;
                }
                FrameType::Control => {
                    if !handle_control_request(&mut stream, &session, &frame)? {
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
    }
    Ok(())
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

/// Writes a control frame in blocking mode, returning false when the peer has
/// disconnected. The stream is returned to polling mode afterwards so the
/// session read loop keeps working.
fn write_control_allow_disconnect(
    stream: &mut LocalStream,
    value: &serde_json::Value,
) -> io::Result<bool> {
    let payload = serde_json::to_vec(value)
        .map_err(|err| io::Error::other(format!("failed to encode control frame: {err}")))?;

    set_local_stream_polling(stream, false)?;
    let result = write_frame(stream, FrameType::Control, CONTROL_STREAM_ID, &payload);
    set_local_stream_polling(stream, true)?;

    match result {
        Ok(()) => Ok(true),
        Err(FramedCodecError::Io(err)) if is_connection_closed_error(&err) => Ok(false),
        Err(FramedCodecError::Io(err)) => Err(err),
        Err(err) => Err(io::Error::other(err.to_string())),
    }
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
    use crate::api::EventHub;
    use crate::ipc::{bind_local_listener, connect_local_stream};
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
        let (done_tx, done_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let (api_tx, _api_rx) = tokio::sync::mpsc::unbounded_channel();
            let result = super::super::handle_connection(
                server,
                &api_tx,
                &EventHub::default(),
                &Arc::new(AtomicBool::new(true)),
                None,
            );
            done_tx.send(result).unwrap();
        });
        (done_rx, thread)
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

        client.write_all(&FRAMED_MAGIC).unwrap();
        send_control(
            &mut client,
            serde_json::json!({
                "id": "h1",
                "method": "session.hello",
                "params": {
                    "protocol": FRAMED_PROTOCOL_VERSION,
                    "min_protocol": FRAMED_PROTOCOL_MIN_SUPPORTED,
                    "capabilities": ["pane-stream"],
                },
            }),
        );

        let welcome = read_control(&mut client);
        assert_eq!(welcome["id"], "h1");
        assert_eq!(welcome["result"]["type"], "session.welcome");
        assert_eq!(welcome["result"]["protocol"], FRAMED_PROTOCOL_VERSION);
        assert_eq!(
            welcome["result"]["min_protocol"],
            FRAMED_PROTOCOL_MIN_SUPPORTED
        );
        // No capabilities are advertised yet, so the negotiated set is empty.
        assert_eq!(
            welcome["result"]["capabilities"].as_array().unwrap().len(),
            0
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
    fn framed_data_frame_without_open_stream_ends_session() {
        let (mut client, server, path) = local_stream_pair("stray-data");
        let (done_rx, thread) = spawn_connection(server);

        client.write_all(&FRAMED_MAGIC).unwrap();
        send_control(
            &mut client,
            serde_json::json!({
                "id": "h3",
                "method": "session.hello",
                "params": {"protocol": FRAMED_PROTOCOL_VERSION},
            }),
        );
        assert_eq!(
            read_control(&mut client)["result"]["type"],
            "session.welcome"
        );

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

        client.write_all(&FRAMED_MAGIC).unwrap();
        send_control(
            &mut client,
            serde_json::json!({
                "id": "h4",
                "method": "session.hello",
                "params": {"protocol": FRAMED_PROTOCOL_VERSION},
            }),
        );
        assert_eq!(
            read_control(&mut client)["result"]["type"],
            "session.welcome"
        );

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
}

//! Blocking framed-protocol client over any `Read`/`Write` transport pair.
//!
//! Used by the fleet connection manager over SSH bridge child stdio, and by
//! any simple client that already holds both halves of a framed transport.
//! The reader side is generic over `std::io::Read`, so the same code path
//! serves local sockets and child pipes.

use std::io::{self, Read, Write};

use crate::protocol::framed::{
    ping_request, read_frame, session_hello_request, write_frame, FrameType, FramedCodecError,
    SessionWelcome, CONTROL_STREAM_ID, FRAMED_MAGIC,
};

pub(crate) fn codec_error_to_io(err: FramedCodecError) -> io::Error {
    match err {
        FramedCodecError::Io(err) => err,
        FramedCodecError::UnexpectedEof => {
            io::Error::new(io::ErrorKind::UnexpectedEof, "framed transport closed")
        }
        other => io::Error::new(io::ErrorKind::InvalidData, other.to_string()),
    }
}

/// Writes one JSON control request as a control frame.
pub fn send_control<W: Write>(writer: &mut W, value: &serde_json::Value) -> io::Result<()> {
    let payload = serde_json::to_vec(value)
        .map_err(|err| io::Error::other(format!("failed to encode control frame: {err}")))?;
    write_frame(writer, FrameType::Control, CONTROL_STREAM_ID, &payload).map_err(codec_error_to_io)
}

/// Reads frames until the next control frame and decodes its JSON payload.
/// Data frames are skipped; control replies are what a fleet client consumes.
pub fn read_control<R: Read>(reader: &mut R) -> io::Result<serde_json::Value> {
    loop {
        let frame = read_frame(reader).map_err(codec_error_to_io)?;
        if frame.frame_type == FrameType::Control && frame.stream_id == CONTROL_STREAM_ID {
            return serde_json::from_slice(&frame.payload).map_err(|err| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid control frame payload: {err}"),
                )
            });
        }
    }
}

/// Opens a framed session: writes the `HRDR` magic, sends `session.hello`,
/// and parses the resulting `session.welcome`.
// The fleet manager performs this handshake through its channel-driven
// reader instead; this blocking form serves simple framed clients (and the
// unit tests) until the next framed-client consumer lands.
#[allow(dead_code)]
pub fn open_session<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    hello_id: &str,
) -> io::Result<SessionWelcome> {
    writer.write_all(&FRAMED_MAGIC)?;
    writer.flush()?;
    send_control(writer, &session_hello_request(hello_id))?;
    let response = read_control(reader)?;
    crate::protocol::framed::parse_session_welcome(&response).map_err(io::Error::other)
}

/// Sends a heartbeat ping and waits for the matching pong.
// The fleet manager pumps pings through its channel-driven reader instead;
// this blocking form serves simple framed clients (and the unit tests) until
// the next framed-client consumer lands.
#[allow(dead_code)]
pub fn ping<R: Read, W: Write>(reader: &mut R, writer: &mut W, id: &str) -> io::Result<()> {
    send_control(writer, &ping_request(id))?;
    let response = read_control(reader)?;
    crate::protocol::framed::parse_pong(&response).map_err(io::Error::other)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::framed::{FRAMED_PROTOCOL_VERSION, PING_METHOD};
    use std::io::Cursor;

    #[test]
    fn send_control_and_read_control_roundtrip() {
        let mut buf = Vec::new();
        send_control(
            &mut buf,
            &serde_json::json!({"id": "x", "method": PING_METHOD}),
        )
        .unwrap();
        // A data frame in between is skipped by read_control.
        write_frame(&mut buf, FrameType::Data, 7, b"noise").unwrap();
        send_control(&mut buf, &serde_json::json!({"id": "y"})).unwrap();

        let mut cursor = Cursor::new(buf);
        assert_eq!(read_control(&mut cursor).unwrap()["id"], "x");
        assert_eq!(read_control(&mut cursor).unwrap()["id"], "y");
    }

    #[test]
    fn read_control_reports_closed_transport_as_unexpected_eof() {
        let mut cursor = Cursor::new(Vec::new());
        let err = read_control(&mut cursor).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn open_session_writes_magic_then_hello_and_parses_welcome() {
        // Scripted server side: one welcome response ready to be read.
        let mut server_reply = Vec::new();
        send_control(
            &mut server_reply,
            &serde_json::json!({
                "id": "h1",
                "result": {
                    "type": "session.welcome",
                    "protocol": FRAMED_PROTOCOL_VERSION,
                    "min_protocol": FRAMED_PROTOCOL_VERSION,
                    "capabilities": [],
                    "server_version": "test",
                },
            }),
        )
        .unwrap();

        let mut reader = Cursor::new(server_reply);
        let mut written = Vec::new();
        let welcome = open_session(&mut reader, &mut written, "h1").unwrap();
        assert_eq!(welcome.protocol, FRAMED_PROTOCOL_VERSION);
        assert_eq!(welcome.server_version, "test");

        // The client wrote the magic first, then a hello control frame.
        assert_eq!(&written[..4], b"HRDR");
        let mut sent = Cursor::new(&written[4..]);
        let hello = read_control(&mut sent).unwrap();
        assert_eq!(hello["method"], "session.hello");
        assert_eq!(hello["id"], "h1");
    }

    #[test]
    fn open_session_surfaces_hello_rejection() {
        let mut server_reply = Vec::new();
        send_control(
            &mut server_reply,
            &serde_json::json!({
                "id": "h1",
                "error": {"code": "protocol_out_of_window", "message": "upgrade the herdr client"},
            }),
        )
        .unwrap();

        let mut reader = Cursor::new(server_reply);
        let mut written = Vec::new();
        let err = open_session(&mut reader, &mut written, "h1").unwrap_err();
        assert!(
            err.to_string().contains("upgrade the herdr client"),
            "{err}"
        );
    }

    #[test]
    fn ping_roundtrip_accepts_pong() {
        let mut server_reply = Vec::new();
        send_control(
            &mut server_reply,
            &serde_json::json!({
                "id": "p1",
                "result": {"type": "pong", "version": "test", "protocol": 1},
            }),
        )
        .unwrap();

        let mut reader = Cursor::new(server_reply);
        let mut written = Vec::new();
        ping(&mut reader, &mut written, "p1").unwrap();
    }
}

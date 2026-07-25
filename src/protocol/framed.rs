//! Framed unified runtime protocol (v1) spoken on the API socket.
//!
//! A framed client opens its connection with the 4-byte `HRDR` magic; a
//! leading `{` byte on the same socket is a legacy NDJSON API client. After
//! the magic, both directions exchange frames:
//!
//! ```text
//! 10-byte little-endian header:
//!   len       u32   payload length, max 32 MiB
//!   type      u8    CONTROL | DATA
//!   reserved  u8
//!   stream_id u32
//! ```
//!
//! CONTROL frames carry JSON control-plane payloads on stream 0 and use the
//! JSON API request/response vocabulary. DATA frames carry raw stream bytes
//! for server-allocated stream ids. The first control exchange must be
//! `session.hello`, which negotiates the protocol version window and string
//! capability flags.
//!
//! The legacy bincode client protocol in `wire.rs` is frozen; new runtime
//! capabilities land here.

use std::io::{self, Read, Write};

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Protocol constants
// ---------------------------------------------------------------------------

/// Connection-opening magic distinguishing framed sessions from legacy NDJSON
/// clients on the shared API socket.
pub const FRAMED_MAGIC: [u8; 4] = *b"HRDR";

/// Size of the fixed frame header in bytes.
pub const FRAME_HEADER_BYTES: usize = 10;

/// Maximum allowed frame payload size (32 MiB). Frames declaring more are
/// rejected without allocating.
pub const MAX_FRAME_PAYLOAD: usize = 32 * 1024 * 1024;

/// Current framed protocol version.
pub const FRAMED_PROTOCOL_VERSION: u32 = 1;

/// Oldest framed protocol version this build still speaks. Kept within an
/// n/n-1 window of `FRAMED_PROTOCOL_VERSION` so adjacent releases interoperate.
pub const FRAMED_PROTOCOL_MIN_SUPPORTED: u32 = 1;

/// Stream id carrying control-plane frames. Data stream ids are
/// server-allocated and never reuse this value.
pub const CONTROL_STREAM_ID: u32 = 0;

/// Capability flags this server advertises during `session.hello`.
/// Capabilities are additive feature flags; the negotiated set is the
/// intersection with the client's flags. `pane-stream` joins this list once
/// `stream.open` is served.
pub const SERVER_CAPABILITIES: &[&str] = &[];

/// Control-plane method opening a framed session.
pub const SESSION_HELLO_METHOD: &str = "session.hello";

/// Control-plane heartbeat method.
pub const PING_METHOD: &str = "ping";

// ---------------------------------------------------------------------------
// Frame codec
// ---------------------------------------------------------------------------

/// Payload kind carried by a frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameType {
    /// JSON control-plane payload on the control stream.
    Control,
    /// Raw bytes for a server-allocated data stream.
    Data,
}

impl FrameType {
    pub fn to_wire(self) -> u8 {
        match self {
            FrameType::Control => 0,
            FrameType::Data => 1,
        }
    }

    pub fn from_wire(value: u8) -> Option<Self> {
        match value {
            0 => Some(FrameType::Control),
            1 => Some(FrameType::Data),
            _ => None,
        }
    }
}

/// Decoded frame header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub payload_len: u32,
    pub frame_type: FrameType,
    pub stream_id: u32,
}

/// A complete frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub frame_type: FrameType,
    pub stream_id: u32,
    pub payload: Vec<u8>,
}

/// Errors from the framed codec.
#[derive(Debug)]
pub enum FramedCodecError {
    /// The declared payload length exceeds `MAX_FRAME_PAYLOAD`.
    Oversized { claimed: usize, max: usize },
    /// The header carried an unknown frame type byte.
    UnknownFrameType(u8),
    /// An I/O error occurred while reading or writing.
    Io(io::Error),
    /// The connection closed before a complete frame could be read.
    // Only `read_frame` constructs this; see the allow note there.
    #[allow(dead_code)]
    UnexpectedEof,
}

impl std::fmt::Display for FramedCodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FramedCodecError::Oversized { claimed, max } => {
                write!(f, "frame payload {claimed} exceeds maximum {max}")
            }
            FramedCodecError::UnknownFrameType(value) => {
                write!(f, "unknown frame type {value}")
            }
            FramedCodecError::Io(err) => write!(f, "I/O error: {err}"),
            FramedCodecError::UnexpectedEof => write!(f, "unexpected end of stream"),
        }
    }
}

impl std::error::Error for FramedCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FramedCodecError::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for FramedCodecError {
    fn from(err: io::Error) -> Self {
        FramedCodecError::Io(err)
    }
}

/// Encodes a frame header into its 10-byte little-endian wire form.
pub fn encode_frame_header(header: &FrameHeader) -> [u8; FRAME_HEADER_BYTES] {
    let mut bytes = [0u8; FRAME_HEADER_BYTES];
    bytes[0..4].copy_from_slice(&header.payload_len.to_le_bytes());
    bytes[4] = header.frame_type.to_wire();
    // bytes[5] is reserved and always written as zero.
    bytes[6..10].copy_from_slice(&header.stream_id.to_le_bytes());
    bytes
}

/// Decodes a 10-byte header, enforcing the payload bound and known frame
/// types. The reserved byte is ignored for forward compatibility.
pub fn decode_frame_header(
    bytes: &[u8; FRAME_HEADER_BYTES],
) -> Result<FrameHeader, FramedCodecError> {
    let payload_len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    if payload_len as usize > MAX_FRAME_PAYLOAD {
        return Err(FramedCodecError::Oversized {
            claimed: payload_len as usize,
            max: MAX_FRAME_PAYLOAD,
        });
    }
    let frame_type =
        FrameType::from_wire(bytes[4]).ok_or(FramedCodecError::UnknownFrameType(bytes[4]))?;
    let stream_id = u32::from_le_bytes([bytes[6], bytes[7], bytes[8], bytes[9]]);
    Ok(FrameHeader {
        payload_len,
        frame_type,
        stream_id,
    })
}

/// Writes one frame: header followed by payload.
pub fn write_frame<W: Write>(
    writer: &mut W,
    frame_type: FrameType,
    stream_id: u32,
    payload: &[u8],
) -> Result<(), FramedCodecError> {
    if payload.len() > MAX_FRAME_PAYLOAD {
        return Err(FramedCodecError::Oversized {
            claimed: payload.len(),
            max: MAX_FRAME_PAYLOAD,
        });
    }
    let header = FrameHeader {
        payload_len: payload.len() as u32,
        frame_type,
        stream_id,
    };
    writer.write_all(&encode_frame_header(&header))?;
    writer.write_all(payload)?;
    writer.flush()?;
    Ok(())
}

/// Reads one complete frame, reassembling partial reads.
// The blocking-read half of the codec seam. The server uses a non-blocking
// buffered reader instead; this is for framed clients, which arrive with the
// next migration stage. Until then only tests exercise it.
#[allow(dead_code)]
pub fn read_frame<R: Read>(reader: &mut R) -> Result<Frame, FramedCodecError> {
    let mut header_bytes = [0u8; FRAME_HEADER_BYTES];
    read_exact_or_eof(reader, &mut header_bytes)?;
    let header = decode_frame_header(&header_bytes)?;

    let mut payload = vec![0u8; header.payload_len as usize];
    read_exact_or_eof(reader, &mut payload)?;

    Ok(Frame {
        frame_type: header.frame_type,
        stream_id: header.stream_id,
        payload,
    })
}

// Only `read_frame` calls this; see the allow note there.
#[allow(dead_code)]
fn read_exact_or_eof<R: Read>(reader: &mut R, buf: &mut [u8]) -> Result<(), FramedCodecError> {
    reader.read_exact(buf).map_err(|err| {
        if err.kind() == io::ErrorKind::UnexpectedEof {
            FramedCodecError::UnexpectedEof
        } else {
            FramedCodecError::Io(err)
        }
    })
}

// ---------------------------------------------------------------------------
// session.hello negotiation
// ---------------------------------------------------------------------------

/// Parameters of the `session.hello` control request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionHelloParams {
    /// Newest framed protocol version the client speaks.
    pub protocol: u32,
    /// Oldest framed protocol version the client still speaks. Defaults to
    /// `protocol` when absent.
    #[serde(default)]
    pub min_protocol: Option<u32>,
    /// Capability flags the client understands.
    #[serde(default)]
    pub capabilities: Vec<String>,
}

/// Which side of an out-of-window hello must upgrade to restore
/// compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HelloRemedy {
    UpgradeClient,
    UpgradeServer,
}

/// Why a `session.hello` was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HelloError {
    /// The hello itself is malformed (min above max).
    InvalidWindow { message: String },
    /// The client's version window does not overlap the server's. The
    /// message names exactly which side to upgrade.
    OutOfWindow {
        remedy: HelloRemedy,
        message: String,
    },
}

/// Result of a successful `session.hello` negotiation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegotiatedSession {
    /// Protocol version both sides speak for this session.
    pub protocol: u32,
    /// Capability flags active for this session (client ∩ server).
    pub capabilities: Vec<String>,
}

/// Negotiates a `session.hello` against this server's version window and
/// capability flags.
pub fn negotiate_session_hello(
    params: &SessionHelloParams,
) -> Result<NegotiatedSession, HelloError> {
    let protocol = negotiate_version_windows(
        FRAMED_PROTOCOL_MIN_SUPPORTED,
        FRAMED_PROTOCOL_VERSION,
        params.min_protocol.unwrap_or(params.protocol),
        params.protocol,
    )?;
    Ok(NegotiatedSession {
        protocol,
        capabilities: negotiate_capabilities(&params.capabilities, SERVER_CAPABILITIES),
    })
}

/// Pure window negotiation: the session speaks the newest version inside both
/// windows. Exposed separately so tests can exercise windows beyond the
/// current constants.
pub(crate) fn negotiate_version_windows(
    server_min: u32,
    server_max: u32,
    client_min: u32,
    client_max: u32,
) -> Result<u32, HelloError> {
    if client_min > client_max {
        return Err(HelloError::InvalidWindow {
            message: format!(
                "session.hello min_protocol {client_min} exceeds protocol {client_max}"
            ),
        });
    }

    if client_max < server_min {
        return Err(HelloError::OutOfWindow {
            remedy: HelloRemedy::UpgradeClient,
            message: format!(
                "client protocol {client_max} is older than the minimum protocol {server_min} \
                 supported by this server (protocol {server_max}); upgrade the herdr client"
            ),
        });
    }

    if client_min > server_max {
        return Err(HelloError::OutOfWindow {
            remedy: HelloRemedy::UpgradeServer,
            message: format!(
                "client minimum protocol {client_min} is newer than this server's protocol \
                 {server_max}; upgrade this herdr server"
            ),
        });
    }

    Ok(client_max.min(server_max))
}

/// Intersects client capability flags with the server's supported set,
/// preserving client order and dropping duplicates.
pub(crate) fn negotiate_capabilities(client: &[String], server: &[&str]) -> Vec<String> {
    let mut negotiated = Vec::new();
    for capability in client {
        if server.contains(&capability.as_str()) && !negotiated.contains(capability) {
            negotiated.push(capability.clone());
        }
    }
    negotiated
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // ---- Header codec ----

    #[test]
    fn frame_header_roundtrip() {
        let header = FrameHeader {
            payload_len: 0x0102_0304u32.min(MAX_FRAME_PAYLOAD as u32),
            frame_type: FrameType::Data,
            stream_id: 0xAABB_CCDD,
        };
        let encoded = encode_frame_header(&header);
        let decoded = decode_frame_header(&encoded).unwrap();
        assert_eq!(header, decoded);
    }

    #[test]
    fn frame_header_wire_layout_is_locked() {
        let header = FrameHeader {
            payload_len: 7,
            frame_type: FrameType::Control,
            stream_id: 0x0403_0201,
        };
        assert_eq!(
            encode_frame_header(&header),
            [7, 0, 0, 0, 0, 0, 0x01, 0x02, 0x03, 0x04]
        );

        let data = FrameHeader {
            payload_len: 0x0100,
            frame_type: FrameType::Data,
            stream_id: 1,
        };
        assert_eq!(
            encode_frame_header(&data),
            [0x00, 0x01, 0, 0, 1, 0, 1, 0, 0, 0]
        );
    }

    #[test]
    fn frame_header_accepts_payload_at_bound() {
        let mut bytes = [0u8; FRAME_HEADER_BYTES];
        bytes[0..4].copy_from_slice(&(MAX_FRAME_PAYLOAD as u32).to_le_bytes());
        let header = decode_frame_header(&bytes).unwrap();
        assert_eq!(header.payload_len as usize, MAX_FRAME_PAYLOAD);
    }

    #[test]
    fn frame_header_rejects_payload_over_bound() {
        let mut bytes = [0u8; FRAME_HEADER_BYTES];
        bytes[0..4].copy_from_slice(&((MAX_FRAME_PAYLOAD as u32) + 1).to_le_bytes());
        match decode_frame_header(&bytes) {
            Err(FramedCodecError::Oversized { claimed, max }) => {
                assert_eq!(claimed, MAX_FRAME_PAYLOAD + 1);
                assert_eq!(max, MAX_FRAME_PAYLOAD);
            }
            other => panic!("expected oversized error, got {other:?}"),
        }
    }

    #[test]
    fn frame_header_rejects_unknown_frame_type() {
        let mut bytes = [0u8; FRAME_HEADER_BYTES];
        bytes[4] = 0x7F;
        match decode_frame_header(&bytes) {
            Err(FramedCodecError::UnknownFrameType(0x7F)) => {}
            other => panic!("expected unknown frame type error, got {other:?}"),
        }
    }

    #[test]
    fn frame_header_ignores_reserved_byte() {
        let mut bytes = encode_frame_header(&FrameHeader {
            payload_len: 0,
            frame_type: FrameType::Control,
            stream_id: 9,
        });
        bytes[5] = 0xFF;
        let header = decode_frame_header(&bytes).unwrap();
        assert_eq!(header.stream_id, 9);
    }

    // ---- Frame IO ----

    #[test]
    fn frame_write_read_roundtrip() {
        let mut buf = Vec::new();
        write_frame(&mut buf, FrameType::Control, 0, br#"{"id":"1"}"#).unwrap();
        write_frame(&mut buf, FrameType::Data, 3, &[0xde, 0xad, 0xbe, 0xef]).unwrap();

        let mut cursor = Cursor::new(buf);
        let first = read_frame(&mut cursor).unwrap();
        assert_eq!(first.frame_type, FrameType::Control);
        assert_eq!(first.stream_id, CONTROL_STREAM_ID);
        assert_eq!(first.payload, br#"{"id":"1"}"#);

        let second = read_frame(&mut cursor).unwrap();
        assert_eq!(second.frame_type, FrameType::Data);
        assert_eq!(second.stream_id, 3);
        assert_eq!(second.payload, vec![0xde, 0xad, 0xbe, 0xef]);
    }

    #[test]
    fn frame_write_rejects_oversized_payload() {
        struct CountingSink(usize);
        impl Write for CountingSink {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                self.0 += buf.len();
                Ok(buf.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let mut sink = CountingSink(0);
        let payload = vec![0u8; MAX_FRAME_PAYLOAD + 1];
        match write_frame(&mut sink, FrameType::Data, 1, &payload) {
            Err(FramedCodecError::Oversized { .. }) => {}
            other => panic!("expected oversized error, got {other:?}"),
        }
        assert_eq!(sink.0, 0, "oversized frame must not be partially written");
    }

    #[test]
    fn frame_read_empty_payload() {
        let mut buf = Vec::new();
        write_frame(&mut buf, FrameType::Control, 0, &[]).unwrap();
        let frame = read_frame(&mut Cursor::new(buf)).unwrap();
        assert!(frame.payload.is_empty());
    }

    #[test]
    fn frame_read_truncated_header_is_unexpected_eof() {
        let mut cursor = Cursor::new(vec![1, 0, 0]);
        match read_frame(&mut cursor) {
            Err(FramedCodecError::UnexpectedEof) => {}
            other => panic!("expected eof error, got {other:?}"),
        }
    }

    #[test]
    fn frame_read_truncated_payload_is_unexpected_eof() {
        let mut buf = Vec::new();
        write_frame(&mut buf, FrameType::Data, 1, &[1, 2, 3, 4]).unwrap();
        buf.truncate(buf.len() - 2);
        match read_frame(&mut Cursor::new(buf)) {
            Err(FramedCodecError::UnexpectedEof) => {}
            other => panic!("expected eof error, got {other:?}"),
        }
    }

    // ---- session.hello negotiation ----

    fn hello(
        protocol: u32,
        min_protocol: Option<u32>,
        capabilities: &[&str],
    ) -> SessionHelloParams {
        SessionHelloParams {
            protocol,
            min_protocol,
            capabilities: capabilities.iter().map(|s| (*s).to_owned()).collect(),
        }
    }

    #[test]
    fn hello_negotiates_current_protocol() {
        let session = negotiate_session_hello(&hello(FRAMED_PROTOCOL_VERSION, None, &[])).unwrap();
        assert_eq!(session.protocol, FRAMED_PROTOCOL_VERSION);
        assert!(session.capabilities.is_empty());
    }

    #[test]
    fn hello_from_newer_client_with_overlapping_window_speaks_server_protocol() {
        let session = negotiate_session_hello(&hello(
            FRAMED_PROTOCOL_VERSION + 1,
            Some(FRAMED_PROTOCOL_VERSION),
            &[],
        ))
        .unwrap();
        assert_eq!(session.protocol, FRAMED_PROTOCOL_VERSION);
    }

    #[test]
    fn hello_min_protocol_defaults_to_protocol() {
        // A bare newer-version hello with no window must be out of range.
        match negotiate_session_hello(&hello(FRAMED_PROTOCOL_VERSION + 1, None, &[])) {
            Err(HelloError::OutOfWindow {
                remedy: HelloRemedy::UpgradeServer,
                message,
            }) => assert!(message.contains("upgrade this herdr server"), "{message}"),
            other => panic!("expected upgrade-server rejection, got {other:?}"),
        }
    }

    #[test]
    fn hello_with_invalid_window_is_rejected() {
        match negotiate_session_hello(&hello(1, Some(2), &[])) {
            Err(HelloError::InvalidWindow { message }) => {
                assert!(message.contains("min_protocol"), "{message}")
            }
            other => panic!("expected invalid window rejection, got {other:?}"),
        }
    }

    #[test]
    fn version_window_negotiation_covers_n_and_n_minus_one() {
        // Server at n with n-1 support.
        assert_eq!(negotiate_version_windows(1, 2, 2, 2), Ok(2));
        assert_eq!(negotiate_version_windows(1, 2, 1, 1), Ok(1));
        // Newer client spanning back to the server's version.
        assert_eq!(negotiate_version_windows(1, 2, 2, 3), Ok(2));
        // Both windows overlap in the middle.
        assert_eq!(negotiate_version_windows(2, 3, 1, 2), Ok(2));
    }

    #[test]
    fn version_window_negotiation_rejects_out_of_window_with_exact_remedy() {
        match negotiate_version_windows(3, 4, 1, 2) {
            Err(HelloError::OutOfWindow {
                remedy: HelloRemedy::UpgradeClient,
                message,
            }) => {
                assert!(message.contains("client protocol 2"), "{message}");
                assert!(message.contains("minimum protocol 3"), "{message}");
                assert!(message.contains("upgrade the herdr client"), "{message}");
            }
            other => panic!("expected upgrade-client rejection, got {other:?}"),
        }

        match negotiate_version_windows(1, 2, 3, 4) {
            Err(HelloError::OutOfWindow {
                remedy: HelloRemedy::UpgradeServer,
                message,
            }) => {
                assert!(message.contains("client minimum protocol 3"), "{message}");
                assert!(message.contains("upgrade this herdr server"), "{message}");
            }
            other => panic!("expected upgrade-server rejection, got {other:?}"),
        }
    }

    #[test]
    fn capability_negotiation_is_intersection_preserving_client_order() {
        let client = vec![
            "pane-stream".to_owned(),
            "unknown-flag".to_owned(),
            "notification".to_owned(),
            "pane-stream".to_owned(),
        ];
        let negotiated = negotiate_capabilities(&client, &["notification", "pane-stream"]);
        assert_eq!(negotiated, vec!["pane-stream", "notification"]);
    }

    #[test]
    fn hello_params_decode_from_control_json() {
        let params: SessionHelloParams = serde_json::from_value(serde_json::json!({
            "protocol": 1,
            "min_protocol": 1,
            "capabilities": ["pane-stream"],
        }))
        .unwrap();
        assert_eq!(params.protocol, 1);
        assert_eq!(params.min_protocol, Some(1));
        assert_eq!(params.capabilities, vec!["pane-stream"]);

        let bare: SessionHelloParams =
            serde_json::from_value(serde_json::json!({"protocol": 1})).unwrap();
        assert_eq!(bare.min_protocol, None);
        assert!(bare.capabilities.is_empty());
    }
}

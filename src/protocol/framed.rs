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
//!
//! # Version-skew policy (n/n-1)
//!
//! Bump [`FRAMED_PROTOCOL_VERSION`] **only** for breaking frame or control
//! envelope changes. Additive features never bump the integer: they ride
//! string capability flags negotiated in `session.hello` (the session's set
//! is client capabilities ∩ server capabilities), so a peer that does not
//! know a flag simply does not get the feature.
//!
//! When the integer is bumped, [`FRAMED_PROTOCOL_MIN_SUPPORTED`] must move to
//! `FRAMED_PROTOCOL_VERSION - 1` in the same change so any two adjacent
//! releases interoperate; `window_is_n_and_n_minus_one` enforces that
//! invariant. The negotiated session speaks the older side's version. A peer
//! outside the window is rejected with a `protocol_out_of_window` control
//! error whose `data.remedy` names exactly which side must upgrade, so the
//! client can render the exact fix instead of guessing.

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
/// n/n-1 window of `FRAMED_PROTOCOL_VERSION` so adjacent releases
/// interoperate: bumping the version without moving this constant collapses
/// the window and breaks every n-1 peer.
pub const FRAMED_PROTOCOL_MIN_SUPPORTED: u32 = 1;

/// True when a `(version, min_supported)` pair honors the n/n-1 policy: the
/// build speaks its own version and at most one release back.
pub const fn window_honors_policy(version: u32, min_supported: u32) -> bool {
    min_supported <= version && min_supported + 1 >= version
}

// The window is a compile-time invariant, not a convention: a version bump
// that forgets to move the minimum fails the build here.
const _: () = assert!(window_honors_policy(
    FRAMED_PROTOCOL_VERSION,
    FRAMED_PROTOCOL_MIN_SUPPORTED
));

/// Control-error code of an out-of-window `session.hello` rejection.
pub const PROTOCOL_OUT_OF_WINDOW_CODE: &str = "protocol_out_of_window";

/// Stream id carrying control-plane frames. Data stream ids are
/// server-allocated and never reuse this value.
pub const CONTROL_STREAM_ID: u32 = 0;

/// Capability gating `stream.open`, DATA-frame output tails, and the
/// `pane.send_bytes` input method.
pub const CAPABILITY_PANE_STREAM: &str = "pane-stream";

/// Capability gating `notification.posted` event broadcasts.
pub const CAPABILITY_NOTIFICATION: &str = "notification";

/// Capability gating `window_title.changed` event broadcasts.
pub const CAPABILITY_WINDOW_TITLE: &str = "window-title";

/// Capability gating the `pane.paste_image` method.
pub const CAPABILITY_PASTE_IMAGE: &str = "paste-image";

/// Capability gating the `session.snapshot` control method and `catalog.event`
/// broadcasts of the session catalog event stream.
pub const CAPABILITY_CATALOG: &str = "catalog";

/// Capability flags this server advertises during `session.hello`.
/// Capabilities are additive feature flags; the negotiated set is the
/// intersection with the client's flags.
pub const SERVER_CAPABILITIES: &[&str] = &[
    CAPABILITY_PANE_STREAM,
    CAPABILITY_NOTIFICATION,
    CAPABILITY_WINDOW_TITLE,
    CAPABILITY_PASTE_IMAGE,
    CAPABILITY_CATALOG,
];

/// Control-plane method opening a framed session.
pub const SESSION_HELLO_METHOD: &str = "session.hello";

/// Control-plane method returning the full session catalog snapshot plus the
/// event sequence anchor it is current through. Requires the `catalog`
/// capability.
pub const SESSION_SNAPSHOT_METHOD: &str = "session.snapshot";

/// Control-plane method carrying one JSON API request verbatim, so a pure
/// client can drive every session mutation through the existing Method
/// vocabulary over the framed connection. Requires the `catalog` capability.
pub const API_REQUEST_METHOD: &str = "api.request";

/// Control-plane heartbeat method.
pub const PING_METHOD: &str = "ping";

/// Control-plane method opening a pane output stream.
pub const STREAM_OPEN_METHOD: &str = "stream.open";

/// Control-plane method closing an open pane output stream.
pub const STREAM_CLOSE_METHOD: &str = "stream.close";

/// Control-plane method resizing the pane behind a write-mode stream.
pub const STREAM_RESIZE_METHOD: &str = "stream.resize";

/// Control-plane method scrolling the pane behind a write-mode stream with
/// the pane's own wheel and page-key routing rules.
pub const STREAM_SCROLL_METHOD: &str = "stream.scroll";

/// Control-plane method fetching a page of pane scrollback history for an
/// open stream, walking backward from the opaque cursor minted by
/// `stream.open`.
pub const STREAM_HISTORY_METHOD: &str = "stream.history";

/// Default `stream.history` page size in bytes when the request names none.
pub const HISTORY_PAGE_DEFAULT_BYTES: usize = 256 * 1024;

/// Smallest honored `stream.history` page size. Requests below this are
/// clamped up so pathological clients cannot force per-line round trips.
pub const HISTORY_PAGE_MIN_BYTES: usize = 4 * 1024;

/// Largest honored `stream.history` fetch a client may request. Covers a full
/// scrollback budget (jump-to-top); the server additionally caps each served
/// page at `HISTORY_PAGE_SERVED_MAX_BYTES` and pages the remainder through
/// `next_cursor`.
pub const HISTORY_FETCH_MAX_BYTES: usize = 16 * 1024 * 1024;

/// Largest history page content the server serves in one `stream.history`
/// response. JSON string escaping expands control bytes (ESC, CR, ...) into
/// six-byte `\u001b`-style escapes, so a page of `n` content bytes can encode
/// to up to `6 * n` payload bytes. This cap keeps the worst-case encoded
/// response (content plus cursor and envelope headroom) inside
/// `MAX_FRAME_PAYLOAD`, so serving a page can never tear down the session
/// with an oversized frame.
pub const HISTORY_PAGE_SERVED_MAX_BYTES: usize = (MAX_FRAME_PAYLOAD - 64 * 1024) / 6;

/// Control-plane method writing raw bytes to a pane PTY.
pub const PANE_SEND_BYTES_METHOD: &str = "pane.send_bytes";

/// Control-plane method staging an image and pasting its path into a pane.
pub const PANE_PASTE_IMAGE_METHOD: &str = "pane.paste_image";

/// Event name broadcast to framed clients when the server posts a
/// notification.
pub const NOTIFICATION_POSTED_EVENT: &str = "notification.posted";

/// Event name broadcast to framed clients when the requested client window
/// title changes.
pub const WINDOW_TITLE_CHANGED_EVENT: &str = "window_title.changed";

/// Event name sent when the server closes an open stream, for example when
/// the pane behind it goes away.
pub const STREAM_CLOSED_EVENT: &str = "stream.closed";

/// Event name sent on a write-mode stream whose write grant another client
/// took over.
pub const STREAM_REVOKED_EVENT: &str = "stream.revoked";

/// Event frame carrying one session catalog event envelope. Sent only on
/// sessions that negotiated the `catalog` capability; `data` is the JSON API
/// event envelope and `seq` its hub sequence.
pub const CATALOG_EVENT: &str = "catalog.event";

/// Event frame telling a catalog client that the server's bounded event
/// buffer overflowed past the client's cursor: catalog events were lost and
/// the client must request a fresh `session.snapshot` to resync. Sent only
/// on sessions that negotiated the `catalog` capability.
pub const CATALOG_RESYNC_EVENT: &str = "catalog.resync_required";

/// Event frame telling a framed client that its server is going away on
/// purpose, sent before the session ends on server stop.
///
/// Deliberately **not** capability-gated. The other events are data feeds a
/// client opts into; this one is about the session itself, and a client that
/// never asked for the catalog still needs to know its server is going away.
/// Without it a deliberate stop is indistinguishable from a crash: the
/// client sees only EOF and walks its reconnect ladder against a server that
/// is never coming back.
///
/// `data.reason` is [`SERVER_STOPPING_REASON_EMPTY`] or
/// [`SERVER_STOPPING_REASON_REQUESTED`]. Both are terminal for the client;
/// an unrecognised value must be treated as `requested`, so a newer server
/// can add reasons without stranding an older client.
pub const SERVER_STOPPING_EVENT: &str = "server.stopping";

/// `server.stopping` reason: the catalog went empty, so there was nothing
/// left to keep the server alive.
pub const SERVER_STOPPING_REASON_EMPTY: &str = "empty";

/// `server.stopping` reason: someone asked the server to stop.
pub const SERVER_STOPPING_REASON_REQUESTED: &str = "requested";

/// Error code answering a `stream.open` that asked for write mode without
/// takeover while another live stream holds the pane's write grant.
pub const PANE_WRITE_LOCKED_ERROR: &str = "pane_write_locked";

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
///
/// The blocking-read half of the codec seam, generic over any `Read`
/// transport: framed clients use it over local sockets and SSH bridge child
/// stdio alike. The server keeps its own non-blocking buffered reader.
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

impl HelloRemedy {
    /// Wire spelling carried in the rejection's `data.remedy`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UpgradeClient => "upgrade_client",
            Self::UpgradeServer => "upgrade_server",
        }
    }

    /// Parses the wire spelling back into a remedy.
    pub fn from_wire(value: &str) -> Option<Self> {
        match value {
            "upgrade_client" => Some(Self::UpgradeClient),
            "upgrade_server" => Some(Self::UpgradeServer),
            _ => None,
        }
    }
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

impl NegotiatedSession {
    /// True when the capability flag was negotiated for this session.
    pub fn has_capability(&self, capability: &str) -> bool {
        self.capabilities.iter().any(|c| c == capability)
    }
}

// ---------------------------------------------------------------------------
// Pane-stream control vocabulary
// ---------------------------------------------------------------------------

/// Access mode of a pane stream. Read streams are unlimited per pane; a
/// single write stream at a time holds the pane's write grant.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamMode {
    /// Output tail only; no write grant, no pane geometry ownership.
    #[default]
    Read,
    /// Output tail plus the pane write grant: input, resize, and scroll.
    Write,
}

impl StreamMode {
    pub fn is_write(self) -> bool {
        matches!(self, StreamMode::Write)
    }
}

/// Parameters of the `stream.open` control request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamOpenParams {
    /// Pane target the stream should carry: a public pane id, a terminal id,
    /// or an agent name.
    pub pane_id: String,
    /// Requested access mode. Defaults to read.
    #[serde(default)]
    pub mode: StreamMode,
    /// Whether a write-mode open may revoke a live write grant.
    #[serde(default)]
    pub takeover: bool,
    /// Client viewport width applied to the pane while the write grant is
    /// held.
    #[serde(default)]
    pub cols: Option<u16>,
    /// Client viewport height applied to the pane while the write grant is
    /// held.
    #[serde(default)]
    pub rows: Option<u16>,
}

/// Scroll direction of a `stream.scroll` request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamScrollDirection {
    Up,
    Down,
}

/// What produced a `stream.scroll` request. The pane routes wheel and page
/// keys differently depending on the program running in it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamScrollSource {
    #[default]
    Wheel,
    PageKey,
}

/// Parameters of the `stream.resize` control request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamResizeParams {
    /// Server-allocated id of the write-mode stream.
    pub stream_id: u32,
    pub cols: u16,
    pub rows: u16,
    #[serde(default)]
    pub cell_width_px: u32,
    #[serde(default)]
    pub cell_height_px: u32,
}

/// Parameters of the `stream.scroll` control request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamScrollParams {
    /// Server-allocated id of the write-mode stream.
    pub stream_id: u32,
    pub direction: StreamScrollDirection,
    pub lines: u16,
    #[serde(default)]
    pub source: StreamScrollSource,
    #[serde(default)]
    pub column: Option<u16>,
    #[serde(default)]
    pub row: Option<u16>,
    #[serde(default)]
    pub modifiers: u8,
}

/// Parameters of the `stream.close` control request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamCloseParams {
    /// Server-allocated id of the stream to close.
    pub stream_id: u32,
}

/// Parameters of the `stream.history` control request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamHistoryParams {
    /// Opaque history cursor from `stream.open` or a previous
    /// `stream.history` response.
    pub cursor: String,
    /// Requested page size in bytes. Defaults to
    /// `HISTORY_PAGE_DEFAULT_BYTES`; clamped into
    /// `HISTORY_PAGE_MIN_BYTES..=HISTORY_FETCH_MAX_BYTES`.
    #[serde(default)]
    pub max_bytes: Option<u64>,
}

/// Parameters of the `pane.send_bytes` control request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaneSendBytesControlParams {
    /// Public pane id receiving the input.
    pub pane_id: String,
    /// Base64-encoded raw bytes written to the pane PTY.
    pub data_base64: String,
}

/// Parameters of the `pane.paste_image` control request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PanePasteImageControlParams {
    /// Public pane id receiving the paste.
    pub pane_id: String,
    /// Image file extension hint (`png`, `jpg`, ...).
    pub extension: String,
    /// Base64-encoded image bytes.
    pub data_base64: String,
}

/// Version prefix of the opaque history cursor format.
const HISTORY_CURSOR_PREFIX: &str = "hdrc1";

/// Server-side identity of a position in a pane stream's history.
///
/// A cursor names the pane, the pane output byte sequence captured with the
/// `stream.open` snapshot, the open stream serving the history, and the byte
/// offset into the server's immutable history capture up to which content is
/// still unfetched: bytes `[0, offset)` of the capture remain to be paged.
/// Walking `stream.history` therefore yields byte-contiguous pages — no gaps
/// and no duplicates — because every page is a slice of one capture and each
/// response's `next_cursor` carries the exact start offset of the page it
/// returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryCursor {
    /// Public pane id the history belongs to.
    pub pane_id: String,
    /// Pane output byte sequence captured with the `stream.open` snapshot.
    pub sequence: u64,
    /// Server-allocated id of the stream holding the history capture.
    pub stream_id: u32,
    /// Exclusive end offset of the not-yet-fetched history prefix.
    pub offset: u64,
}

/// Encodes an opaque history cursor. Clients must treat the value as opaque;
/// only the server interprets it.
pub fn encode_history_cursor(cursor: &HistoryCursor) -> String {
    use base64::Engine as _;
    let raw = format!(
        "{HISTORY_CURSOR_PREFIX}:{}:{}:{}:{}",
        cursor.sequence, cursor.stream_id, cursor.offset, cursor.pane_id
    );
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw.as_bytes())
}

/// Decodes an opaque history cursor.
pub fn decode_history_cursor(cursor: &str) -> Option<HistoryCursor> {
    use base64::Engine as _;
    let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor.as_bytes())
        .ok()?;
    let raw = String::from_utf8(raw).ok()?;
    let rest = raw.strip_prefix(HISTORY_CURSOR_PREFIX)?.strip_prefix(':')?;
    let (sequence, rest) = rest.split_once(':')?;
    let (stream_id, rest) = rest.split_once(':')?;
    let (offset, pane_id) = rest.split_once(':')?;
    let sequence = sequence.parse().ok()?;
    let stream_id = stream_id.parse().ok()?;
    let offset = offset.parse().ok()?;
    (!pane_id.is_empty()).then(|| HistoryCursor {
        pane_id: pane_id.to_owned(),
        sequence,
        stream_id,
        offset,
    })
}

/// Computes the start of the history page ending at byte `end`, honoring a
/// hard byte budget while preferring newline-aligned page starts.
///
/// The returned start is always strictly below `end` (for non-empty input)
/// and always a UTF-8 char boundary. When a newline falls inside the budget,
/// the page starts just after the first one, so replaying a suffix of pages
/// begins at a line start. When no newline falls inside the budget (one long
/// soft-wrapped logical line), the page is cut at the budget boundary anyway,
/// snapped back onto a char boundary — so a page never exceeds the budget by
/// more than three bytes of one multi-byte character. Mid-line cuts are safe
/// because pages are byte-contiguous slices of one immutable capture: the
/// client reassembles the split line by replaying the next-older page
/// directly in front of this one.
pub fn history_page_start(history: &str, end: usize, max_bytes: usize) -> usize {
    let end = end.min(history.len());
    let max_bytes = max_bytes.max(1);
    if end <= max_bytes {
        return 0;
    }
    let mut candidate = end - max_bytes;
    while candidate > 0 && !history.is_char_boundary(candidate) {
        candidate -= 1;
    }
    if let Some(pos) = history[candidate..end].find('\n') {
        let start = candidate + pos + 1;
        if start < end {
            return start;
        }
    }
    candidate
}

/// True when the history page ending at byte `end` of the capture was cut
/// mid-line: the byte before the boundary is not a newline and younger
/// capture content continues the same logical line at `end`. This happens
/// when the younger page's [`history_page_start`] hit the hard byte cap
/// without a newline inside the budget. Clients must join such a page to the
/// content below it without fabricating a line break. The capture end itself
/// is never mid-line -- the last history row simply has no trailing newline.
pub fn history_page_end_cut_mid_line(history: &str, end: usize) -> bool {
    end > 0 && end < history.len() && history.as_bytes()[end - 1] != b'\n'
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

/// Capability flags a catalog-driving client cannot run without: without
/// them the connection is functionally incompatible even though the version
/// windows overlap. Checked with [`check_required_capabilities`].
// Consumed by the unix-only pure-client run path.
#[cfg_attr(windows, allow(dead_code))]
pub const REQUIRED_CATALOG_CAPABILITIES: &[&str] = &[CAPABILITY_CATALOG];

/// Rejects a negotiated welcome that is missing a capability the client
/// cannot run without. Shaped like the version-window rejection so callers
/// treat "too old to speak my version" and "too old to offer what I need"
/// identically: both are terminal, and both name the side to upgrade.
// Consumed by the unix-only pure-client run path.
#[cfg_attr(windows, allow(dead_code))]
pub fn check_required_capabilities(
    welcome: &SessionWelcome,
    required: &[&str],
) -> Result<(), HelloError> {
    let missing: Vec<&str> = required
        .iter()
        .copied()
        .filter(|capability| !welcome.capabilities.iter().any(|have| have == capability))
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    let server_version = if welcome.server_version.is_empty() {
        "unknown".to_string()
    } else {
        welcome.server_version.clone()
    };
    Err(HelloError::OutOfWindow {
        remedy: HelloRemedy::UpgradeServer,
        message: format!(
            "herdr server {server_version} does not offer the {} capability",
            missing.join(", ")
        ),
    })
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
// Client-side control vocabulary
// ---------------------------------------------------------------------------

/// Capability flags a framed client advertises during `session.hello`.
pub const CLIENT_CAPABILITIES: &[&str] = &[CAPABILITY_PANE_STREAM];

/// Builds the opening `session.hello` control request for this build's
/// version window and client capabilities.
pub fn session_hello_request(id: &str) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "method": SESSION_HELLO_METHOD,
        "params": {
            "protocol": FRAMED_PROTOCOL_VERSION,
            "min_protocol": FRAMED_PROTOCOL_MIN_SUPPORTED,
            "capabilities": CLIENT_CAPABILITIES,
        },
    })
}

/// Builds a `session.hello` control request advertising an explicit
/// capability set, for clients that negotiate more than the default
/// pane-stream vocabulary.
// Consumed by the pure-client run path wired in a later stage of #20.
#[cfg_attr(not(test), allow(dead_code))]
pub fn session_hello_request_with_capabilities(
    id: &str,
    capabilities: &[&str],
) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "method": SESSION_HELLO_METHOD,
        "params": {
            "protocol": FRAMED_PROTOCOL_VERSION,
            "min_protocol": FRAMED_PROTOCOL_MIN_SUPPORTED,
            "capabilities": capabilities,
        },
    })
}

/// Builds a `session.snapshot` control request.
// Consumed by the pure-client run path wired in a later stage of #20.
#[cfg_attr(not(test), allow(dead_code))]
pub fn session_snapshot_request(id: &str) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "method": SESSION_SNAPSHOT_METHOD,
        "params": {},
    })
}

/// Parses a `session.snapshot` control response into the snapshot value and
/// the event sequence anchor the snapshot is current through.
// Consumed by the pure-client run path wired in a later stage of #20.
#[cfg_attr(not(test), allow(dead_code))]
pub fn parse_session_snapshot(
    response: &serde_json::Value,
) -> Result<(serde_json::Value, u64), String> {
    if let Some(error) = control_error(response) {
        return Err(format!("session.snapshot rejected: {}", error.message));
    }
    let result = response
        .get("result")
        .ok_or_else(|| "session.snapshot response carries no result".to_string())?;
    let sequence = result
        .get("sequence")
        .and_then(|value| value.as_u64())
        .ok_or_else(|| "session.snapshot result carries no sequence anchor".to_string())?;
    let snapshot = result
        .get("snapshot")
        .cloned()
        .ok_or_else(|| "session.snapshot result carries no snapshot".to_string())?;
    Ok((snapshot, sequence))
}

/// Builds a heartbeat `ping` control request.
pub fn ping_request(id: &str) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "method": PING_METHOD,
        "params": {},
    })
}

/// Builds a `stream.open` control request.
pub fn stream_open_request(
    id: &str,
    pane_id: &str,
    mode: StreamMode,
    takeover: bool,
    cols: Option<u16>,
    rows: Option<u16>,
) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "method": STREAM_OPEN_METHOD,
        "params": StreamOpenParams {
            pane_id: pane_id.to_owned(),
            mode,
            takeover,
            cols,
            rows,
        },
    })
}

/// Builds a `stream.close` control request.
pub fn stream_close_request(id: &str, stream_id: u32) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "method": STREAM_CLOSE_METHOD,
        "params": StreamCloseParams { stream_id },
    })
}

/// Builds a `stream.resize` control request.
pub fn stream_resize_request(
    id: &str,
    stream_id: u32,
    cols: u16,
    rows: u16,
    cell_width_px: u32,
    cell_height_px: u32,
) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "method": STREAM_RESIZE_METHOD,
        "params": StreamResizeParams {
            stream_id,
            cols,
            rows,
            cell_width_px,
            cell_height_px,
        },
    })
}

/// Builds a `stream.scroll` control request.
pub fn stream_scroll_request(id: &str, params: StreamScrollParams) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "method": STREAM_SCROLL_METHOD,
        "params": params,
    })
}

/// Builds a `stream.history` control request for one page ending at the
/// opaque cursor.
pub fn stream_history_request(id: &str, cursor: &str, max_bytes: usize) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "method": STREAM_HISTORY_METHOD,
        "params": {
            "cursor": cursor,
            "max_bytes": max_bytes as u64,
        },
    })
}

/// Builds a `pane.send_bytes` control request carrying raw pane input.
pub fn pane_send_bytes_request(id: &str, pane_id: &str, data: &[u8]) -> serde_json::Value {
    use base64::Engine as _;
    serde_json::json!({
        "id": id,
        "method": PANE_SEND_BYTES_METHOD,
        "params": PaneSendBytesControlParams {
            pane_id: pane_id.to_owned(),
            data_base64: base64::engine::general_purpose::STANDARD.encode(data),
        },
    })
}

/// Builds a `pane.paste_image` control request.
// Consumed by the unix-only pure-client run path.
#[cfg_attr(windows, allow(dead_code))]
pub fn pane_paste_image_request(
    id: &str,
    pane_id: &str,
    extension: &str,
    data: &[u8],
) -> serde_json::Value {
    use base64::Engine as _;
    serde_json::json!({
        "id": id,
        "method": PANE_PASTE_IMAGE_METHOD,
        "params": PanePasteImageControlParams {
            pane_id: pane_id.to_owned(),
            extension: extension.to_owned(),
            data_base64: base64::engine::general_purpose::STANDARD.encode(data),
        },
    })
}

/// A control-plane error answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlError {
    pub code: String,
    pub message: String,
    pub data: Option<serde_json::Value>,
}

/// The remedy an out-of-window rejection carries in `data.remedy`, when the
/// error is one. The server writes it; this is the client half of that round
/// trip, so an out-of-window *server* is never reported as "upgrade the
/// client".
pub fn parse_hello_remedy(error: &ControlError) -> Option<HelloRemedy> {
    if error.code != PROTOCOL_OUT_OF_WINDOW_CODE {
        return None;
    }
    error
        .data
        .as_ref()
        .and_then(|data| data.get("remedy"))
        .and_then(|value| value.as_str())
        .and_then(HelloRemedy::from_wire)
}

/// Extracts the error body of a control response, if it carries one.
pub fn control_error(response: &serde_json::Value) -> Option<ControlError> {
    let error = response.get("error")?;
    Some(ControlError {
        code: error
            .get("code")
            .and_then(|value| value.as_str())
            .unwrap_or("unknown_error")
            .to_string(),
        message: error
            .get("message")
            .and_then(|value| value.as_str())
            .unwrap_or("unknown error")
            .to_string(),
        data: error.get("data").cloned(),
    })
}

/// The server's answer to a successful `stream.open`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamOpened {
    /// Public pane id the server resolved the target to.
    pub pane_id: String,
    pub stream_id: u32,
    /// Pane output byte sequence the snapshot was captured at.
    pub sequence: u64,
    /// ANSI snapshot of the pane screen at the subscription point.
    pub snapshot: String,
    pub history_cursor: String,
    /// Pane grid size the snapshot was captured at; 0 when the server did
    /// not report it.
    pub cols: u16,
    pub rows: u16,
}

/// Parses a `stream.open` response into the opened stream, or the server's
/// structured rejection.
pub fn parse_stream_opened(
    response: &serde_json::Value,
) -> Result<StreamOpened, Option<ControlError>> {
    if let Some(error) = control_error(response) {
        return Err(Some(error));
    }
    let stream = response
        .get("result")
        .filter(|result| {
            result.get("type").and_then(|value| value.as_str()) == Some("pane_stream_opened")
        })
        .and_then(|result| result.get("stream"))
        .ok_or(None)?;
    Ok(StreamOpened {
        pane_id: stream
            .get("pane_id")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_string(),
        stream_id: stream
            .get("stream_id")
            .and_then(|value| value.as_u64())
            .ok_or(None)? as u32,
        sequence: stream
            .get("sequence")
            .and_then(|value| value.as_u64())
            .unwrap_or_default(),
        snapshot: stream
            .get("snapshot")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_string(),
        history_cursor: stream
            .get("history_cursor")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_string(),
        cols: stream
            .get("cols")
            .and_then(|value| value.as_u64())
            .and_then(|value| u16::try_from(value).ok())
            .unwrap_or_default(),
        rows: stream
            .get("rows")
            .and_then(|value| value.as_u64())
            .and_then(|value| u16::try_from(value).ok())
            .unwrap_or_default(),
    })
}

/// One page of pane scrollback history returned by `stream.history`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamHistoryPage {
    /// Stream the page belongs to.
    pub stream_id: u32,
    /// Unwrapped, content-only ANSI for this page. Never re-asserts terminal
    /// modes; the `stream.open` snapshot alone carries mode/cursor state.
    pub content: String,
    /// Cursor for the next-older page, absent at the top of history.
    pub next_cursor: Option<String>,
    /// True when this page reaches the oldest retained history.
    pub at_top: bool,
    /// True when the page's end is a mid-line cut: the younger content
    /// already at the client continues the same logical line, so the page
    /// must be joined to it without inserting a line break.
    pub end_cut_mid_line: bool,
}

/// Parses a `stream.history` control response.
pub fn parse_stream_history(response: &serde_json::Value) -> Result<StreamHistoryPage, String> {
    if let Some(message) = control_error_message(response) {
        return Err(format!("stream.history rejected: {message}"));
    }
    let result = response
        .get("result")
        .ok_or_else(|| "stream.history response carries no result".to_string())?;
    if result.get("type").and_then(|value| value.as_str()) != Some("stream_history") {
        return Err("stream.history response is not a stream_history".to_string());
    }
    let stream_id = result
        .get("stream_id")
        .and_then(|value| value.as_u64())
        .ok_or_else(|| "stream_history carries no stream_id".to_string())?;
    let content = result
        .get("content")
        .and_then(|value| value.as_str())
        .ok_or_else(|| "stream_history carries no content".to_string())?
        .to_string();
    let next_cursor = result
        .get("next_cursor")
        .and_then(|value| value.as_str())
        .map(str::to_owned);
    let at_top = result
        .get("at_top")
        .and_then(|value| value.as_bool())
        .unwrap_or(next_cursor.is_none());
    let end_cut_mid_line = result
        .get("end_cut_mid_line")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    Ok(StreamHistoryPage {
        stream_id: stream_id as u32,
        content,
        next_cursor,
        at_top,
        end_cut_mid_line,
    })
}

/// The server's answer to a successful `session.hello`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionWelcome {
    pub protocol: u32,
    pub min_protocol: u32,
    pub capabilities: Vec<String>,
    pub server_version: String,
}

fn control_error_message(response: &serde_json::Value) -> Option<String> {
    let error = response.get("error")?;
    Some(
        error
            .get("message")
            .and_then(|message| message.as_str())
            .unwrap_or("unknown error")
            .to_string(),
    )
}

/// Parses a `session.welcome` control response, surfacing server rejections
/// (including out-of-window hellos) as errors.
pub fn parse_session_welcome(response: &serde_json::Value) -> Result<SessionWelcome, String> {
    if let Some(message) = control_error_message(response) {
        return Err(format!("session.hello rejected: {message}"));
    }
    let result = response
        .get("result")
        .ok_or_else(|| "session.hello response carries no result".to_string())?;
    if result.get("type").and_then(|value| value.as_str()) != Some("session.welcome") {
        return Err("session.hello response is not a session.welcome".to_string());
    }
    let protocol = result
        .get("protocol")
        .and_then(|value| value.as_u64())
        .ok_or_else(|| "session.welcome carries no protocol".to_string())?;
    let min_protocol = result
        .get("min_protocol")
        .and_then(|value| value.as_u64())
        .unwrap_or(protocol);
    let capabilities = result
        .get("capabilities")
        .and_then(|value| value.as_array())
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    let server_version = result
        .get("server_version")
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_string();
    Ok(SessionWelcome {
        protocol: protocol as u32,
        min_protocol: min_protocol as u32,
        capabilities,
        server_version,
    })
}

/// Parses a `pong` control response.
pub fn parse_pong(response: &serde_json::Value) -> Result<(), String> {
    if let Some(message) = control_error_message(response) {
        return Err(format!("ping rejected: {message}"));
    }
    if response
        .get("result")
        .and_then(|result| result.get("type"))
        .and_then(|value| value.as_str())
        != Some("pong")
    {
        return Err("ping response is not a pong".to_string());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_hello_with_capabilities_and_snapshot_requests_carry_the_vocabulary() {
        let hello = session_hello_request_with_capabilities(
            "h1",
            &[CAPABILITY_PANE_STREAM, CAPABILITY_CATALOG],
        );
        assert_eq!(hello["method"], SESSION_HELLO_METHOD);
        assert_eq!(hello["params"]["protocol"], FRAMED_PROTOCOL_VERSION);
        assert_eq!(
            hello["params"]["capabilities"],
            serde_json::json!(["pane-stream", "catalog"])
        );

        let snapshot = session_snapshot_request("s1");
        assert_eq!(snapshot["method"], SESSION_SNAPSHOT_METHOD);
        assert_eq!(snapshot["id"], "s1");
    }
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
    fn hello_negotiates_pane_surface_capabilities() {
        let session = negotiate_session_hello(&hello(
            FRAMED_PROTOCOL_VERSION,
            None,
            &[
                "pane-stream",
                "notification",
                "window-title",
                "paste-image",
                "future-unknown",
            ],
        ))
        .unwrap();
        assert_eq!(
            session.capabilities,
            vec!["pane-stream", "notification", "window-title", "paste-image"]
        );
        assert!(session.has_capability(CAPABILITY_PANE_STREAM));
        assert!(session.has_capability(CAPABILITY_NOTIFICATION));
        assert!(session.has_capability(CAPABILITY_WINDOW_TITLE));
        assert!(session.has_capability(CAPABILITY_PASTE_IMAGE));
        assert!(!session.has_capability("future-unknown"));
    }

    #[test]
    fn history_cursor_round_trips_and_is_opaque() {
        let cursor = HistoryCursor {
            pane_id: "p_2_7".to_owned(),
            sequence: 123_456_789,
            stream_id: 42,
            offset: 987_654,
        };
        let encoded = encode_history_cursor(&cursor);
        assert!(!encoded.contains("p_2_7"), "cursor must look opaque");
        assert_eq!(decode_history_cursor(&encoded), Some(cursor));

        assert_eq!(decode_history_cursor(""), None);
        assert_eq!(decode_history_cursor("not-base64!!"), None);
        assert_eq!(decode_history_cursor("aGVsbG8"), None);
        // Truncated field lists never decode.
        use base64::Engine as _;
        let short = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"hdrc1:1:p_1");
        assert_eq!(decode_history_cursor(&short), None);
    }

    #[test]
    fn history_page_start_walks_backward_newline_aligned_without_gaps() {
        let mut history = String::new();
        for line in 0..200 {
            history.push_str(&format!("line {line:03}\r\n"));
        }
        history.push_str("final row without newline");

        let mut end = history.len();
        let mut pages = Vec::new();
        while end > 0 {
            let start = history_page_start(&history, end, 256);
            assert!(start < end, "pages must make progress");
            assert!(
                start == 0 || history.as_bytes()[start - 1] == b'\n',
                "page start must be newline-aligned"
            );
            assert!(history.is_char_boundary(start));
            pages.push(&history[start..end]);
            end = start;
        }
        assert!(pages.len() > 2, "budget must actually split the history");

        // Reassembling the backward walk yields the exact original bytes:
        // no gaps, no duplicates.
        let rejoined: String = pages.iter().rev().copied().collect();
        assert_eq!(rejoined, history);
    }

    #[test]
    fn history_page_start_handles_budget_and_unaligned_edges() {
        // Whole history inside the budget collapses to one page.
        assert_eq!(history_page_start("a\r\nb", 4, 64), 0);
        // A single line larger than the budget is cut at the budget boundary
        // instead of growing without bound to the previous line start.
        let history = format!("first\n{}", "x".repeat(100));
        assert_eq!(
            history_page_start(&history, history.len(), 10),
            history.len() - 10
        );
        // No newline anywhere: still a hard byte cap per page.
        let solid = "y".repeat(100);
        assert_eq!(
            history_page_start(&solid, solid.len(), 10),
            solid.len() - 10
        );
        // Multi-byte characters at the budget edge stay on char boundaries.
        let emoji = format!("top\n{}\nbottom", "👨‍👩‍👧".repeat(20));
        let start = history_page_start(&emoji, emoji.len(), 8);
        assert!(emoji.is_char_boundary(start));
    }

    #[test]
    fn history_page_start_hard_caps_newline_free_pages_without_gaps() {
        // A single soft-wrapped logical line far larger than the budget:
        // pages must stay near the budget and reassemble exactly.
        let history = format!("{}🌍🌍🌍", "z".repeat(40_000));
        let budget = 4 * 1024;
        let mut end = history.len();
        let mut pages = Vec::new();
        while end > 0 {
            let start = history_page_start(&history, end, budget);
            assert!(start < end, "pages must make progress");
            assert!(history.is_char_boundary(start));
            // A char-boundary snap may exceed the budget by at most three
            // bytes of one multi-byte character.
            assert!(
                end - start <= budget + 3,
                "page {} exceeds cap",
                end - start
            );
            pages.push(&history[start..end]);
            end = start;
        }
        assert!(pages.len() > 2, "budget must actually split the history");
        let rejoined: String = pages.iter().rev().copied().collect();
        assert_eq!(rejoined, history);
    }

    #[test]
    fn history_page_end_cut_mid_line_contract() {
        let history = "aaa\nbbb\nccc";
        // The capture end is never a mid-line cut: the last row simply has
        // no trailing newline.
        assert!(!history_page_end_cut_mid_line(history, history.len()));
        // A newline-aligned interior boundary is not a cut.
        assert!(!history_page_end_cut_mid_line(history, 4));
        assert!(!history_page_end_cut_mid_line(history, 8));
        // An interior boundary inside a line is a cut.
        assert!(history_page_end_cut_mid_line(history, 2));
        assert!(history_page_end_cut_mid_line(history, 9));
        // Degenerate boundaries are not cuts.
        assert!(!history_page_end_cut_mid_line(history, 0));
        assert!(!history_page_end_cut_mid_line("", 0));
    }

    #[test]
    fn served_page_cap_bounds_worst_case_encoded_response() {
        // ESC escapes to a six-byte `\u001b` in JSON, the worst-case string
        // expansion. A full served page of ESC bytes plus a maximal cursor
        // must still encode inside the frame payload limit.
        let content = "\u{1b}".repeat(HISTORY_PAGE_SERVED_MAX_BYTES);
        let cursor = encode_history_cursor(&HistoryCursor {
            pane_id: "p_18446744073709551615".to_owned(),
            sequence: u64::MAX,
            stream_id: u32::MAX,
            offset: u64::MAX,
        });
        let response = serde_json::json!({
            "id": "history-worst-case",
            "result": {
                "type": "stream_history",
                "stream_id": u32::MAX,
                "content": content,
                "next_cursor": cursor,
                "at_top": false,
            },
        });
        let encoded = serde_json::to_vec(&response).expect("encodable response");
        assert!(
            encoded.len() <= MAX_FRAME_PAYLOAD,
            "worst-case response {} exceeds frame limit {}",
            encoded.len(),
            MAX_FRAME_PAYLOAD
        );
    }

    #[test]
    fn event_name_constants_match_event_kind_dot_names() {
        assert_eq!(
            NOTIFICATION_POSTED_EVENT,
            crate::api::schema::EventKind::NotificationPosted.dot_name()
        );
        assert_eq!(
            WINDOW_TITLE_CHANGED_EVENT,
            crate::api::schema::EventKind::WindowTitleChanged.dot_name()
        );
    }

    #[test]
    fn stream_open_params_decode_from_control_json() {
        let params: StreamOpenParams =
            serde_json::from_value(serde_json::json!({"pane_id": "p_1"})).unwrap();
        assert_eq!(params.pane_id, "p_1");

        let close: StreamCloseParams =
            serde_json::from_value(serde_json::json!({"stream_id": 7})).unwrap();
        assert_eq!(close.stream_id, 7);

        let history: StreamHistoryParams =
            serde_json::from_value(serde_json::json!({"cursor": "abc"})).unwrap();
        assert_eq!(history.cursor, "abc");
        assert_eq!(history.max_bytes, None);
        let history: StreamHistoryParams =
            serde_json::from_value(serde_json::json!({"cursor": "abc", "max_bytes": 1024}))
                .unwrap();
        assert_eq!(history.max_bytes, Some(1024));

        let send: PaneSendBytesControlParams =
            serde_json::from_value(serde_json::json!({"pane_id": "p_1", "data_base64": "aGk="}))
                .unwrap();
        assert_eq!(send.data_base64, "aGk=");

        let paste: PanePasteImageControlParams = serde_json::from_value(
            serde_json::json!({"pane_id": "p_1", "extension": "png", "data_base64": "aGk="}),
        )
        .unwrap();
        assert_eq!(paste.extension, "png");
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
    fn window_is_n_and_n_minus_one() {
        // The policy invariant: a version bump must move the minimum with
        // it, or every n-1 peer stops interoperating. Kept as a runtime test
        // next to the compile-time assertion so the failure names the rule.
        assert!(
            window_honors_policy(FRAMED_PROTOCOL_VERSION, FRAMED_PROTOCOL_MIN_SUPPORTED),
            "bumping FRAMED_PROTOCOL_VERSION requires FRAMED_PROTOCOL_MIN_SUPPORTED = version - 1 \
             (version {FRAMED_PROTOCOL_VERSION}, min {FRAMED_PROTOCOL_MIN_SUPPORTED})"
        );

        // What the policy accepts and rejects, spelled out.
        assert!(window_honors_policy(1, 1));
        assert!(window_honors_policy(2, 1));
        assert!(window_honors_policy(7, 6));
        // Window collapsed to n-only after a bump: every n-1 peer breaks.
        assert!(!window_honors_policy(3, 1));
        // A minimum above the version is not a window at all.
        assert!(!window_honors_policy(2, 3));
    }

    #[test]
    fn hello_remedy_round_trips_through_the_wire_spelling() {
        for remedy in [HelloRemedy::UpgradeClient, HelloRemedy::UpgradeServer] {
            assert_eq!(HelloRemedy::from_wire(remedy.as_str()), Some(remedy));
        }
        assert_eq!(HelloRemedy::from_wire("upgrade_everything"), None);
    }

    #[test]
    fn parse_hello_remedy_reads_the_servers_remedy_back() {
        let rejection = serde_json::json!({
            "id": "h1",
            "error": {
                "code": PROTOCOL_OUT_OF_WINDOW_CODE,
                "message": "client minimum protocol 4 is newer than this server's protocol 2",
                "data": { "remedy": "upgrade_server", "server_protocol": 2 },
            },
        });
        let error = control_error(&rejection).expect("rejection carries an error");
        assert_eq!(parse_hello_remedy(&error), Some(HelloRemedy::UpgradeServer));

        // Other rejections carry no remedy, and neither does a malformed one.
        let other = ControlError {
            code: "invalid_request".into(),
            message: "nope".into(),
            data: Some(serde_json::json!({ "remedy": "upgrade_server" })),
        };
        assert_eq!(parse_hello_remedy(&other), None);
        let bare = ControlError {
            code: PROTOCOL_OUT_OF_WINDOW_CODE.into(),
            message: "nope".into(),
            data: None,
        };
        assert_eq!(parse_hello_remedy(&bare), None);
    }

    #[test]
    fn required_capabilities_reject_a_server_that_lacks_them() {
        let welcome = SessionWelcome {
            protocol: FRAMED_PROTOCOL_VERSION,
            min_protocol: FRAMED_PROTOCOL_MIN_SUPPORTED,
            capabilities: vec![CAPABILITY_PANE_STREAM.to_string()],
            server_version: "0.9.0".to_string(),
        };
        match check_required_capabilities(&welcome, REQUIRED_CATALOG_CAPABILITIES) {
            Err(HelloError::OutOfWindow { remedy, message }) => {
                assert_eq!(remedy, HelloRemedy::UpgradeServer);
                assert!(message.contains("0.9.0"), "{message}");
                assert!(message.contains(CAPABILITY_CATALOG), "{message}");
            }
            other => panic!("expected an upgrade-server rejection, got {other:?}"),
        }

        let full = SessionWelcome {
            capabilities: vec![
                CAPABILITY_PANE_STREAM.to_string(),
                CAPABILITY_CATALOG.to_string(),
            ],
            ..welcome
        };
        assert!(check_required_capabilities(&full, REQUIRED_CATALOG_CAPABILITIES).is_ok());
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

    // ---- Client-side control vocabulary ----

    #[test]
    fn client_hello_request_carries_this_builds_window() {
        let hello = session_hello_request("h1");
        assert_eq!(hello["id"], "h1");
        assert_eq!(hello["method"], SESSION_HELLO_METHOD);
        assert_eq!(hello["params"]["protocol"], FRAMED_PROTOCOL_VERSION);
        assert_eq!(
            hello["params"]["min_protocol"],
            FRAMED_PROTOCOL_MIN_SUPPORTED
        );
        // The client hello must decode with the server-side params type.
        let params: SessionHelloParams = serde_json::from_value(hello["params"].clone()).unwrap();
        assert!(negotiate_session_hello(&params).is_ok());
    }

    #[test]
    fn parse_session_welcome_roundtrips_the_server_welcome_shape() {
        let welcome = parse_session_welcome(&serde_json::json!({
            "id": "h1",
            "result": {
                "type": "session.welcome",
                "protocol": 1,
                "min_protocol": 1,
                "capabilities": ["pane-stream"],
                "server_version": "0.9.9",
            },
        }))
        .unwrap();
        assert_eq!(
            welcome,
            SessionWelcome {
                protocol: 1,
                min_protocol: 1,
                capabilities: vec!["pane-stream".to_string()],
                server_version: "0.9.9".to_string(),
            }
        );
    }

    #[test]
    fn parse_session_welcome_surfaces_rejections() {
        let err = parse_session_welcome(&serde_json::json!({
            "id": "h1",
            "error": {"code": "protocol_out_of_window", "message": "upgrade this herdr server"},
        }))
        .unwrap_err();
        assert!(err.contains("upgrade this herdr server"), "{err}");

        assert!(parse_session_welcome(&serde_json::json!({
            "id": "h1",
            "result": {"type": "pong"},
        }))
        .is_err());
    }

    #[test]
    fn parse_pong_accepts_pongs_and_rejects_errors() {
        assert!(parse_pong(&serde_json::json!({
            "id": "p1",
            "result": {"type": "pong", "version": "x", "protocol": 1},
        }))
        .is_ok());
        assert!(parse_pong(&serde_json::json!({
            "id": "p1",
            "error": {"code": "unknown_method", "message": "nope"},
        }))
        .is_err());
        assert!(parse_pong(&serde_json::json!({
            "id": "p1",
            "result": {"type": "session.welcome"},
        }))
        .is_err());
    }

    #[test]
    fn stream_open_params_default_to_a_read_stream() {
        let params: StreamOpenParams =
            serde_json::from_value(serde_json::json!({"pane_id": "p_1"})).unwrap();
        assert_eq!(params.mode, StreamMode::Read);
        assert!(!params.mode.is_write());
        assert!(!params.takeover);
        assert_eq!(params.cols, None);
        assert_eq!(params.rows, None);

        let write: StreamOpenParams = serde_json::from_value(serde_json::json!({
            "pane_id": "p_1",
            "mode": "write",
            "takeover": true,
            "cols": 100,
            "rows": 30,
        }))
        .unwrap();
        assert!(write.mode.is_write());
        assert!(write.takeover);
        assert_eq!(write.cols, Some(100));
        assert_eq!(write.rows, Some(30));
    }

    #[test]
    fn stream_requests_decode_with_the_server_side_params_types() {
        let open = stream_open_request("o1", "term_a", StreamMode::Write, true, Some(80), Some(24));
        assert_eq!(open["method"], STREAM_OPEN_METHOD);
        let params: StreamOpenParams = serde_json::from_value(open["params"].clone()).unwrap();
        assert_eq!(params.pane_id, "term_a");
        assert!(params.mode.is_write());
        assert!(params.takeover);

        let close = stream_close_request("c1", 7);
        assert_eq!(close["method"], STREAM_CLOSE_METHOD);
        let params: StreamCloseParams = serde_json::from_value(close["params"].clone()).unwrap();
        assert_eq!(params.stream_id, 7);

        let resize = stream_resize_request("r1", 7, 100, 30, 8, 16);
        assert_eq!(resize["method"], STREAM_RESIZE_METHOD);
        let params: StreamResizeParams = serde_json::from_value(resize["params"].clone()).unwrap();
        assert_eq!((params.cols, params.rows), (100, 30));
        assert_eq!((params.cell_width_px, params.cell_height_px), (8, 16));

        let scroll = stream_scroll_request(
            "s1",
            StreamScrollParams {
                stream_id: 7,
                direction: StreamScrollDirection::Up,
                lines: 3,
                source: StreamScrollSource::PageKey,
                column: Some(4),
                row: Some(5),
                modifiers: 2,
            },
        );
        assert_eq!(scroll["method"], STREAM_SCROLL_METHOD);
        let params: StreamScrollParams = serde_json::from_value(scroll["params"].clone()).unwrap();
        assert_eq!(params.direction, StreamScrollDirection::Up);
        assert_eq!(params.source, StreamScrollSource::PageKey);
        assert_eq!(params.lines, 3);

        let input = pane_send_bytes_request("i1", "p_1", b"hi");
        assert_eq!(input["method"], PANE_SEND_BYTES_METHOD);
        let params: PaneSendBytesControlParams =
            serde_json::from_value(input["params"].clone()).unwrap();
        assert_eq!(params.data_base64, "aGk=");
    }

    #[test]
    fn parse_stream_opened_reads_the_server_result_shape() {
        let opened = parse_stream_opened(&serde_json::json!({
            "id": "o1",
            "result": {
                "type": "pane_stream_opened",
                "stream": {
                    "pane_id": "p_1_1",
                    "workspace_id": "ws_1",
                    "stream_id": 12,
                    "sequence": 40,
                    "snapshot": "screen",
                    "history_cursor": "cursor",
                    "cols": 120,
                    "rows": 40,
                },
            },
        }))
        .unwrap();
        assert_eq!(
            opened,
            StreamOpened {
                pane_id: "p_1_1".to_string(),
                stream_id: 12,
                sequence: 40,
                snapshot: "screen".to_string(),
                history_cursor: "cursor".to_string(),
                cols: 120,
                rows: 40,
            }
        );
    }

    #[test]
    fn parse_stream_opened_surfaces_a_refused_write_grant() {
        let error = parse_stream_opened(&serde_json::json!({
            "id": "o1",
            "error": {
                "code": PANE_WRITE_LOCKED_ERROR,
                "message": "pane p_1_1 already has a writable stream (stream 3); retry with takeover",
            },
        }))
        .unwrap_err()
        .expect("structured error");
        assert_eq!(error.code, PANE_WRITE_LOCKED_ERROR);
        assert!(error.message.contains("retry with takeover"));

        // A non-stream answer is a protocol problem, not a server rejection.
        assert!(parse_stream_opened(&serde_json::json!({
            "id": "o1",
            "result": {"type": "pong"},
        }))
        .unwrap_err()
        .is_none());
    }

    #[test]
    fn client_capabilities_request_pane_streams() {
        let hello = session_hello_request("h1");
        let params: SessionHelloParams = serde_json::from_value(hello["params"].clone()).unwrap();
        assert!(params
            .capabilities
            .iter()
            .any(|capability| capability == CAPABILITY_PANE_STREAM));
        let session = negotiate_session_hello(&params).unwrap();
        assert!(session.has_capability(CAPABILITY_PANE_STREAM));
    }

    #[test]
    fn stream_history_request_and_response_round_trip() {
        let request = stream_history_request("r1", "cursor-x", 1024);
        assert_eq!(request["method"], STREAM_HISTORY_METHOD);
        assert_eq!(request["params"]["cursor"], "cursor-x");
        assert_eq!(request["params"]["max_bytes"], 1024);
        let params: StreamHistoryParams =
            serde_json::from_value(request["params"].clone()).unwrap();
        assert_eq!(params.cursor, "cursor-x");
        assert_eq!(params.max_bytes, Some(1024));

        let page = parse_stream_history(&serde_json::json!({
            "id": "r1",
            "result": {
                "type": "stream_history",
                "stream_id": 9,
                "content": "line\r\n",
                "next_cursor": "older",
                "at_top": false,
            },
        }))
        .unwrap();
        assert_eq!(
            page,
            StreamHistoryPage {
                stream_id: 9,
                content: "line\r\n".to_owned(),
                next_cursor: Some("older".to_owned()),
                at_top: false,
                // Absent on the wire decodes as a newline-aligned boundary.
                end_cut_mid_line: false,
            }
        );

        let cut = parse_stream_history(&serde_json::json!({
            "id": "r1b",
            "result": {
                "type": "stream_history",
                "stream_id": 9,
                "content": "partial line tail",
                "next_cursor": "older",
                "at_top": false,
                "end_cut_mid_line": true,
            },
        }))
        .unwrap();
        assert!(cut.end_cut_mid_line);

        let top = parse_stream_history(&serde_json::json!({
            "id": "r2",
            "result": {
                "type": "stream_history",
                "stream_id": 9,
                "content": "",
                "next_cursor": null,
                "at_top": true,
            },
        }))
        .unwrap();
        assert!(top.at_top);
        assert!(top.next_cursor.is_none());

        assert!(parse_stream_history(&serde_json::json!({
            "id": "r3",
            "error": {"code": "invalid_cursor", "message": "stale"},
        }))
        .is_err());
        assert!(parse_stream_history(&serde_json::json!({
            "id": "r4",
            "result": {"type": "pong"},
        }))
        .is_err());
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

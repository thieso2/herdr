//! Fleet connection manager: one self-healing bridge per enabled remote.
//!
//! Startup is non-blocking: `FleetManager::start` only registers state and
//! spawns one worker thread per enabled remote, so all remotes connect in
//! parallel without blocking the caller. Each worker drives the pure
//! [`ConnectionMachine`] from real transport events: connect, framed
//! `session.hello` handshake, periodic framed-ping heartbeats, and jittered
//! exponential backoff on any failure — indefinitely, until the remote is
//! removed or disabled.
//!
//! The transport is injected via [`FleetTransport`], so the whole manager is
//! unit-testable without SSH.

use std::any::Any;
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

use super::bridge_child::BridgeChild;
use super::client;
use super::config::{diff_remotes, sanitize_entries, RemoteChange, RemoteEntry};
use super::connection::{
    incompatible_status_line, BackoffTuning, ConnectionMachine, ConnectionState,
};
use crate::protocol::framed::{
    parse_pong, parse_session_welcome, ping_request, read_frame, session_hello_request, Frame,
    FrameType, CONTROL_STREAM_ID, FRAMED_MAGIC,
};

/// Fallback wait when a worker has no scheduled deadline.
const IDLE_POLL: Duration = Duration::from_millis(500);

/// Timing knobs, injectable so tests run in milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FleetTuning {
    pub backoff: BackoffTuning,
    pub handshake_timeout: Duration,
    pub ping_interval: Duration,
    pub pong_timeout: Duration,
}

impl Default for FleetTuning {
    fn default() -> Self {
        Self {
            backoff: BackoffTuning::default(),
            handshake_timeout: Duration::from_secs(10),
            ping_interval: Duration::from_secs(5),
            pong_timeout: Duration::from_secs(15),
        }
    }
}

/// A connected transport: framed bytes in/out plus an owner handle whose drop
/// tears the transport down (for SSH, killing the bridge child).
pub struct FleetIo {
    pub reader: Box<dyn Read + Send>,
    pub writer: Box<dyn Write + Send>,
    pub guard: Option<Box<dyn Any + Send>>,
    /// Bounded tail of the transport's out-of-band diagnostic output (for
    /// SSH, the child's stderr), appended to failure reasons so an offline
    /// remote's `last_error` says why (bad key, unknown host, missing herdr).
    pub diagnostics: Option<Arc<Mutex<String>>>,
}

/// Connection factory for one remote. Injected so the connection lifecycle is
/// testable without SSH.
pub trait FleetTransport: Send + Sync {
    fn connect(&self, entry: &RemoteEntry) -> io::Result<FleetIo>;
}

/// The production transport: a persistent SSH stdio bridge child per
/// connection, speaking the framed protocol directly over the child's stdio.
pub struct SshBridgeTransport;

impl FleetTransport for SshBridgeTransport {
    fn connect(&self, entry: &RemoteEntry) -> io::Result<FleetIo> {
        let (child, stdout, stdin) = BridgeChild::spawn(&entry.target, &entry.session)?;
        let diagnostics = Some(child.stderr_tail());
        Ok(FleetIo {
            reader: Box::new(stdout),
            writer: Box::new(stdin),
            guard: Some(Box::new(child)),
            diagnostics,
        })
    }
}

/// Point-in-time connection state of one remote, converted to plain
/// durations for rendering and the API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteStatusKind {
    Disabled,
    Connecting {
        attempt: u32,
    },
    Connected,
    Offline {
        attempt: u32,
        retry_in: Duration,
        last_error: String,
    },
    /// The protocol version windows do not overlap; no automatic retries
    /// until a side is upgraded or a manual reset forces another attempt.
    Incompatible {
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteStatus {
    pub entry: RemoteEntry,
    pub kind: RemoteStatusKind,
}

/// Reseed hook invoked (from a worker thread) with the remote name after each
/// successful handshake. Full VT reseed arrives with the pane stream work;
/// until then callers typically pass a no-op.
pub type ConnectedHook = Arc<dyn Fn(&str) + Send + Sync>;

struct RemoteRuntime {
    entry: RemoteEntry,
    machine: ConnectionMachine,
}

#[derive(Default)]
struct FleetShared {
    order: Vec<String>,
    remotes: HashMap<String, RemoteRuntime>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerCommand {
    Reset,
    Stop,
}

enum WorkerMsg {
    Frame(u64, Frame),
    Closed(u64, String),
    Command(WorkerCommand),
}

struct WorkerHandle {
    tx: Sender<WorkerMsg>,
    thread: Option<JoinHandle<()>>,
}

/// Owns the fleet's connection state and worker threads.
pub struct FleetManager {
    shared: Arc<Mutex<FleetShared>>,
    transport: Arc<dyn FleetTransport>,
    tuning: FleetTuning,
    on_connected: ConnectedHook,
    workers: HashMap<String, WorkerHandle>,
}

impl FleetManager {
    /// Registers `entries` and spawns one worker per enabled remote. Never
    /// blocks on any connection. Entries that fail validation are skipped
    /// with a warning so one bad hand-edited entry cannot take the fleet
    /// down.
    pub fn start(
        entries: Vec<RemoteEntry>,
        transport: Arc<dyn FleetTransport>,
        tuning: FleetTuning,
        on_connected: ConnectedHook,
    ) -> Self {
        let mut manager = Self {
            shared: Arc::new(Mutex::new(FleetShared::default())),
            transport,
            tuning,
            on_connected,
            workers: HashMap::new(),
        };
        for entry in sanitize_entries(entries) {
            manager.add_remote(entry);
        }
        manager
    }

    /// Ordered plain-data snapshot of the fleet's connection state.
    pub fn snapshot(&self) -> Vec<RemoteStatus> {
        let now = Instant::now();
        let Ok(shared) = self.shared.lock() else {
            return Vec::new();
        };
        shared
            .order
            .iter()
            .filter_map(|name| shared.remotes.get(name))
            .map(|runtime| RemoteStatus {
                entry: runtime.entry.clone(),
                kind: status_kind(&runtime.machine, now),
            })
            .collect()
    }

    /// The currently applied config entries, in order.
    pub fn entries(&self) -> Vec<RemoteEntry> {
        let Ok(shared) = self.shared.lock() else {
            return Vec::new();
        };
        shared
            .order
            .iter()
            .filter_map(|name| shared.remotes.get(name))
            .map(|runtime| runtime.entry.clone())
            .collect()
    }

    /// Manual reset: clear the backoff and force an immediate reconnect.
    /// Returns false when the remote is unknown or disabled.
    pub fn reset(&mut self, name: &str) -> bool {
        match self.workers.get(name) {
            Some(handle) => handle
                .tx
                .send(WorkerMsg::Command(WorkerCommand::Reset))
                .is_ok(),
            None => false,
        }
    }

    /// Applies a freshly loaded config: diffs by name against the running
    /// fleet and starts/stops workers accordingly. A changed target or
    /// session tears the old connection down and starts a new one.
    pub fn apply_config(&mut self, new_entries: Vec<RemoteEntry>) -> Vec<RemoteChange> {
        let new_entries = sanitize_entries(new_entries);
        let changes = diff_remotes(&self.entries(), &new_entries);
        for change in &changes {
            match change {
                RemoteChange::Removed(entry) => self.remove_remote(&entry.name),
                RemoteChange::Added(entry) => self.add_remote(entry.clone()),
                RemoteChange::SettingsChanged { new, .. } => {
                    if !new.enabled {
                        self.stop_worker(&new.name);
                    }
                    let now = Instant::now();
                    if let Ok(mut shared) = self.shared.lock() {
                        if let Some(runtime) = shared.remotes.get_mut(&new.name) {
                            runtime.entry = new.clone();
                            runtime.machine.set_enabled(new.enabled, now);
                        }
                    }
                    if new.enabled && !self.workers.contains_key(&new.name) {
                        self.spawn_worker(new.name.clone());
                    }
                }
            }
        }
        // Keep display order aligned with the config file.
        if let Ok(mut shared) = self.shared.lock() {
            shared.order = new_entries
                .iter()
                .map(|entry| entry.name.clone())
                .filter(|name| shared.remotes.contains_key(name))
                .collect();
        }
        changes
    }

    /// Stops all workers and joins their threads.
    pub fn stop(&mut self) {
        let names: Vec<String> = self.workers.keys().cloned().collect();
        for name in names {
            self.stop_worker(&name);
        }
    }

    fn add_remote(&mut self, entry: RemoteEntry) {
        let now = Instant::now();
        let machine = ConnectionMachine::new(entry.enabled, now, self.tuning.backoff);
        let name = entry.name.clone();
        let enabled = entry.enabled;
        if let Ok(mut shared) = self.shared.lock() {
            shared.order.retain(|existing| existing != &name);
            shared.order.push(name.clone());
            shared
                .remotes
                .insert(name.clone(), RemoteRuntime { entry, machine });
        }
        if enabled {
            self.spawn_worker(name);
        }
    }

    fn remove_remote(&mut self, name: &str) {
        self.stop_worker(name);
        if let Ok(mut shared) = self.shared.lock() {
            shared.order.retain(|existing| existing != name);
            shared.remotes.remove(name);
        }
    }

    fn spawn_worker(&mut self, name: String) {
        if self.workers.contains_key(&name) {
            return;
        }
        let (tx, rx) = mpsc::channel();
        let worker = Worker {
            name: name.clone(),
            shared: Arc::clone(&self.shared),
            transport: Arc::clone(&self.transport),
            tuning: self.tuning,
            frame_tx: tx.clone(),
            on_connected: Arc::clone(&self.on_connected),
        };
        let thread = std::thread::Builder::new()
            .name(format!("fleet-{name}"))
            .spawn(move || worker.run(rx));
        match thread {
            Ok(thread) => {
                self.workers.insert(
                    name,
                    WorkerHandle {
                        tx,
                        thread: Some(thread),
                    },
                );
            }
            Err(err) => {
                warn!(remote = %name, err = %err, "failed to spawn fleet worker thread");
            }
        }
    }

    fn stop_worker(&mut self, name: &str) {
        if let Some(mut handle) = self.workers.remove(name) {
            let _ = handle.tx.send(WorkerMsg::Command(WorkerCommand::Stop));
            if let Some(thread) = handle.thread.take() {
                let _ = thread.join();
            }
        }
    }
}

impl Drop for FleetManager {
    fn drop(&mut self) {
        self.stop();
    }
}

fn status_kind(machine: &ConnectionMachine, now: Instant) -> RemoteStatusKind {
    match machine.state() {
        ConnectionState::Disabled => RemoteStatusKind::Disabled,
        ConnectionState::Connecting { attempt } => {
            RemoteStatusKind::Connecting { attempt: *attempt }
        }
        ConnectionState::Connected { .. } => RemoteStatusKind::Connected,
        ConnectionState::Offline {
            attempt,
            retry_at,
            last_error,
        } => RemoteStatusKind::Offline {
            attempt: *attempt,
            retry_in: retry_at.saturating_duration_since(now),
            last_error: last_error.clone(),
        },
        ConnectionState::Incompatible { message } => RemoteStatusKind::Incompatible {
            message: message.clone(),
        },
    }
}

/// A cheap jitter sample in `[0, 1)` without a rand dependency.
fn jitter01() -> f64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.subsec_nanos() as u64)
        .unwrap_or(0);
    let mixed = nanos
        .wrapping_mul(6364136223846793005)
        .wrapping_add(COUNTER.fetch_add(1, Ordering::Relaxed))
        .wrapping_add(u64::from(std::process::id()));
    (mixed % 1_000_000) as f64 / 1_000_000.0
}

struct Worker {
    name: String,
    shared: Arc<Mutex<FleetShared>>,
    transport: Arc<dyn FleetTransport>,
    tuning: FleetTuning,
    frame_tx: Sender<WorkerMsg>,
    on_connected: ConnectedHook,
}

enum SessionEnd {
    Failed(String),
    /// The handshake was rejected because the protocol windows do not
    /// overlap; the machine parks in `Incompatible` with no retries.
    Incompatible(String),
    ManualReset,
    Stop,
}

impl Worker {
    fn run(self, rx: mpsc::Receiver<WorkerMsg>) {
        let mut generation: u64 = 0;
        loop {
            // Wait until the machine schedules a connection attempt.
            match self.wait_until_ready(&rx) {
                Ok(()) => {}
                Err(()) => return,
            }

            let Some(entry) = self.entry() else { return };
            if self
                .with_machine(ConnectionMachine::on_connect_started)
                .is_none()
            {
                return;
            }

            let io = match self.transport.connect(&entry) {
                Ok(io) => io,
                Err(err) => {
                    if !self.fail(format!("connect failed: {err}")) {
                        return;
                    }
                    continue;
                }
            };
            generation += 1;
            let FleetIo {
                reader,
                mut writer,
                guard,
                diagnostics,
            } = io;
            let _transport_guard = guard;
            spawn_reader(&self.name, generation, reader, self.frame_tx.clone());

            match self.run_session(&rx, generation, &mut writer) {
                SessionEnd::Stop => return,
                SessionEnd::Incompatible(message) => {
                    warn!(remote = %self.name, message = %message, "fleet remote protocol incompatible; parking until reset");
                    if self
                        .with_machine(|machine| machine.on_incompatible(message))
                        .is_none()
                    {
                        return;
                    }
                }
                SessionEnd::ManualReset => {
                    let now = Instant::now();
                    if self.with_machine(|machine| machine.on_reset(now)).is_none() {
                        return;
                    }
                }
                SessionEnd::Failed(reason) => {
                    let reason = append_diagnostics(reason, diagnostics.as_ref());
                    debug!(remote = %self.name, reason = %reason, "fleet remote disconnected");
                    if !self.fail(reason) {
                        return;
                    }
                }
            }
            // `_transport_guard` drops here, tearing down the transport (for
            // SSH: killing the bridge child), which also unblocks the reader
            // thread.
        }
    }

    /// Blocks until the machine is ready to connect. Err means stop.
    fn wait_until_ready(&self, rx: &mpsc::Receiver<WorkerMsg>) -> Result<(), ()> {
        loop {
            let now = Instant::now();
            let Some((ready, deadline)) = self
                .with_machine(|machine| (machine.ready_to_connect(now), machine.next_deadline()))
            else {
                return Err(());
            };
            if ready {
                return Ok(());
            }
            let timeout = deadline
                .map(|deadline| deadline.saturating_duration_since(now))
                .unwrap_or(IDLE_POLL)
                .max(Duration::from_millis(1));
            match rx.recv_timeout(timeout) {
                Ok(WorkerMsg::Command(WorkerCommand::Stop)) => return Err(()),
                Ok(WorkerMsg::Command(WorkerCommand::Reset)) => {
                    let now = Instant::now();
                    if self.with_machine(|machine| machine.on_reset(now)).is_none() {
                        return Err(());
                    }
                }
                // Frames from a torn-down connection are stale here.
                Ok(_) => {}
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return Err(()),
            }
        }
    }

    /// Handshakes and then heartbeats one connection until it ends.
    fn run_session<W: Write>(
        &self,
        rx: &mpsc::Receiver<WorkerMsg>,
        generation: u64,
        writer: &mut W,
    ) -> SessionEnd {
        // Handshake: magic + session.hello, then wait for the welcome.
        let hello = (|| -> io::Result<()> {
            writer.write_all(&FRAMED_MAGIC)?;
            writer.flush()?;
            client::send_control(
                writer,
                &session_hello_request(&format!("hello-{generation}")),
            )
        })();
        if let Err(err) = hello {
            return SessionEnd::Failed(format!("handshake write failed: {err}"));
        }

        let handshake_deadline = Instant::now() + self.tuning.handshake_timeout;
        loop {
            let now = Instant::now();
            if now >= handshake_deadline {
                return SessionEnd::Failed("session.hello timed out".to_string());
            }
            match rx.recv_timeout(handshake_deadline - now) {
                Ok(WorkerMsg::Frame(gen, frame)) if gen == generation => {
                    let Some(value) = control_payload(&frame) else {
                        continue;
                    };
                    if let Some(error) = crate::protocol::framed::control_error(&value) {
                        if error.code
                            == crate::protocol::framed::PROTOCOL_OUT_OF_WINDOW_CODE
                        {
                            // The remedy is the remote's own; a rejection
                            // without one is read as "this side is too old",
                            // the only side we can act on locally.
                            let remedy = crate::protocol::framed::parse_hello_remedy(&error)
                                .unwrap_or(
                                    crate::protocol::framed::HelloRemedy::UpgradeClient,
                                );
                            return SessionEnd::Incompatible(incompatible_status_line(
                                &self.name,
                                false,
                                remedy,
                                &error.message,
                            ));
                        }
                    }
                    match parse_session_welcome(&value) {
                        Ok(welcome) => {
                            debug!(
                                remote = %self.name,
                                protocol = welcome.protocol,
                                server_version = %welcome.server_version,
                                "fleet remote negotiated"
                            );
                            break;
                        }
                        Err(err) => return SessionEnd::Failed(err),
                    }
                }
                Ok(WorkerMsg::Closed(gen, err)) if gen == generation => {
                    return SessionEnd::Failed(format!("bridge closed: {err}"));
                }
                Ok(WorkerMsg::Command(WorkerCommand::Stop)) => return SessionEnd::Stop,
                Ok(WorkerMsg::Command(WorkerCommand::Reset)) => return SessionEnd::ManualReset,
                Ok(_) => {}
                Err(RecvTimeoutError::Timeout) => {
                    return SessionEnd::Failed("session.hello timed out".to_string());
                }
                Err(RecvTimeoutError::Disconnected) => return SessionEnd::Stop,
            }
        }

        let now = Instant::now();
        if self
            .with_machine(|machine| machine.on_connected(now))
            .is_none()
        {
            return SessionEnd::Stop;
        }
        info!(remote = %self.name, "fleet remote connected");
        (self.on_connected)(&self.name);

        // Heartbeat loop: periodic framed pings; a missing pong within
        // `pong_timeout` is a failure.
        let mut last_pong = Instant::now();
        let mut next_ping = Instant::now() + self.tuning.ping_interval;
        let mut ping_seq: u64 = 0;
        loop {
            let now = Instant::now();
            if now.duration_since(last_pong) >= self.tuning.pong_timeout {
                return SessionEnd::Failed("heartbeat timed out".to_string());
            }
            if now >= next_ping {
                ping_seq += 1;
                let request = ping_request(&format!("hb-{generation}-{ping_seq}"));
                if let Err(err) = client::send_control(writer, &request) {
                    return SessionEnd::Failed(format!("heartbeat write failed: {err}"));
                }
                next_ping = now + self.tuning.ping_interval;
            }
            let wake_at = next_ping.min(last_pong + self.tuning.pong_timeout);
            let timeout = wake_at
                .saturating_duration_since(now)
                .max(Duration::from_millis(1));
            match rx.recv_timeout(timeout) {
                Ok(WorkerMsg::Frame(gen, frame)) if gen == generation => {
                    if let Some(value) = control_payload(&frame) {
                        if parse_pong(&value).is_ok() {
                            last_pong = Instant::now();
                        }
                    }
                }
                Ok(WorkerMsg::Closed(gen, err)) if gen == generation => {
                    return SessionEnd::Failed(format!("bridge closed: {err}"));
                }
                Ok(WorkerMsg::Command(WorkerCommand::Stop)) => return SessionEnd::Stop,
                Ok(WorkerMsg::Command(WorkerCommand::Reset)) => return SessionEnd::ManualReset,
                Ok(_) => {}
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return SessionEnd::Stop,
            }
        }
    }

    fn entry(&self) -> Option<RemoteEntry> {
        let shared = self.shared.lock().ok()?;
        shared
            .remotes
            .get(&self.name)
            .map(|runtime| runtime.entry.clone())
    }

    fn with_machine<T>(&self, f: impl FnOnce(&mut ConnectionMachine) -> T) -> Option<T> {
        let mut shared = self.shared.lock().ok()?;
        shared
            .remotes
            .get_mut(&self.name)
            .map(|runtime| f(&mut runtime.machine))
    }

    /// Records a failure with jittered backoff. Returns false when the remote
    /// runtime is gone and the worker should exit.
    fn fail(&self, error: String) -> bool {
        let now = Instant::now();
        self.with_machine(|machine| machine.on_disconnected(now, error, jitter01()))
            .is_some()
    }
}

/// Appends the transport's bounded diagnostic tail (ssh stderr) to a failure
/// reason so `remote list` can show why a bridge died, not just that it did.
fn append_diagnostics(reason: String, diagnostics: Option<&Arc<Mutex<String>>>) -> String {
    let Some(tail) = diagnostics else {
        return reason;
    };
    let Ok(tail) = tail.lock() else {
        return reason;
    };
    let tail = tail.trim();
    if tail.is_empty() {
        return reason;
    }
    format!("{reason} (ssh: {})", tail.replace('\n', "; "))
}

fn control_payload(frame: &Frame) -> Option<serde_json::Value> {
    if frame.frame_type != FrameType::Control || frame.stream_id != CONTROL_STREAM_ID {
        return None;
    }
    serde_json::from_slice(&frame.payload).ok()
}

fn spawn_reader(
    name: &str,
    generation: u64,
    mut reader: Box<dyn Read + Send>,
    tx: Sender<WorkerMsg>,
) {
    let thread = std::thread::Builder::new()
        .name(format!("fleet-{name}-read-{generation}"))
        .spawn(move || loop {
            match read_frame(&mut reader) {
                Ok(frame) => {
                    if tx.send(WorkerMsg::Frame(generation, frame)).is_err() {
                        return;
                    }
                }
                Err(err) => {
                    let _ = tx.send(WorkerMsg::Closed(generation, err.to_string()));
                    return;
                }
            }
        });
    if let Err(err) = thread {
        let _ = err;
        warn!(remote = %name, "failed to spawn fleet reader thread");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::SystemTime;

    fn entry(name: &str) -> RemoteEntry {
        RemoteEntry {
            name: name.to_string(),
            target: format!("test@{name}.invalid"),
            session: crate::session::DEFAULT_SESSION_NAME.to_string(),
            enabled: true,
        }
    }

    fn tuning(backoff: Duration) -> FleetTuning {
        FleetTuning {
            backoff: BackoffTuning {
                base: backoff,
                cap: backoff,
                // All in-test session drops count as flaps, deterministically.
                stable_uptime: Duration::from_secs(3600),
            },
            handshake_timeout: Duration::from_millis(500),
            ping_interval: Duration::from_millis(20),
            pong_timeout: Duration::from_millis(120),
        }
    }

    fn noop_hook() -> ConnectedHook {
        Arc::new(|_| {})
    }

    fn wait_for(timeout: Duration, mut condition: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if condition() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        condition()
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum FakeBehavior {
        RefuseConnect,
        ServeWelcomeAndPongs,
        ServeWelcomeNoPongs,
        ServeWelcomeThenClose,
        /// Answers the hello with the server's out-of-window rejection: the
        /// remote is older than this client's window.
        RejectOutOfWindow,
    }

    /// Transport whose connections are scripted in-memory framed servers.
    /// The behavior list is consumed per connection; the last one repeats.
    struct ScriptedTransport {
        behaviors: Mutex<Vec<FakeBehavior>>,
        connects: AtomicUsize,
    }

    impl ScriptedTransport {
        fn new(behaviors: Vec<FakeBehavior>) -> Arc<Self> {
            Arc::new(Self {
                behaviors: Mutex::new(behaviors),
                connects: AtomicUsize::new(0),
            })
        }

        fn connects(&self) -> usize {
            self.connects.load(Ordering::SeqCst)
        }
    }

    impl FleetTransport for ScriptedTransport {
        fn connect(&self, _entry: &RemoteEntry) -> io::Result<FleetIo> {
            let index = self.connects.fetch_add(1, Ordering::SeqCst);
            let behavior = {
                let behaviors = self.behaviors.lock().unwrap();
                *behaviors
                    .get(index)
                    .or_else(|| behaviors.last())
                    .expect("scripted transport needs at least one behavior")
            };
            if behavior == FakeBehavior::RefuseConnect {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    "scripted refusal",
                ));
            }

            let (client_read, server_write) = io::pipe()?;
            let (server_read, client_write) = io::pipe()?;
            std::thread::spawn(move || fake_framed_server(server_read, server_write, behavior));
            Ok(FleetIo {
                reader: Box::new(client_read),
                writer: Box::new(client_write),
                guard: None,
                diagnostics: None,
            })
        }
    }

    fn fake_framed_server(mut reader: impl Read, mut writer: impl Write, behavior: FakeBehavior) {
        let mut magic = [0u8; 4];
        if reader.read_exact(&mut magic).is_err() || magic != FRAMED_MAGIC {
            return;
        }
        loop {
            let Ok(frame) = read_frame(&mut reader) else {
                return;
            };
            let Ok(request) = serde_json::from_slice::<serde_json::Value>(&frame.payload) else {
                return;
            };
            let id = request["id"].clone();
            match request["method"].as_str() {
                Some("session.hello") if behavior == FakeBehavior::RejectOutOfWindow => {
                    let rejection = serde_json::json!({
                        "id": id,
                        "error": {
                            "code": crate::protocol::framed::PROTOCOL_OUT_OF_WINDOW_CODE,
                            "message": "client minimum protocol 2 is newer than this server's protocol 1; upgrade this herdr server",
                            "data": {
                                "remedy": "upgrade_server",
                                "server_protocol": 1,
                                "server_min_protocol": 1,
                            },
                        },
                    });
                    let _ = client::send_control(&mut writer, &rejection);
                    return;
                }
                Some("session.hello") => {
                    let welcome = serde_json::json!({
                        "id": id,
                        "result": {
                            "type": "session.welcome",
                            "protocol": 1,
                            "min_protocol": 1,
                            "capabilities": [],
                            "server_version": "fake",
                        },
                    });
                    if client::send_control(&mut writer, &welcome).is_err() {
                        return;
                    }
                    if behavior == FakeBehavior::ServeWelcomeThenClose {
                        return;
                    }
                }
                Some("ping") => {
                    if behavior == FakeBehavior::ServeWelcomeNoPongs {
                        continue;
                    }
                    let pong = serde_json::json!({
                        "id": id,
                        "result": {"type": "pong", "version": "fake", "protocol": 1},
                    });
                    if client::send_control(&mut writer, &pong).is_err() {
                        return;
                    }
                }
                _ => {}
            }
        }
    }

    fn kind_of(manager: &FleetManager, name: &str) -> Option<RemoteStatusKind> {
        manager
            .snapshot()
            .into_iter()
            .find(|status| status.entry.name == name)
            .map(|status| status.kind)
    }

    #[test]
    fn out_of_window_remote_parks_incompatible_and_stops_the_retry_ladder() {
        let transport = ScriptedTransport::new(vec![FakeBehavior::RejectOutOfWindow]);
        let mut manager = FleetManager::start(
            vec![entry("gpu-1")],
            Arc::clone(&transport) as Arc<dyn FleetTransport>,
            tuning(Duration::from_millis(10)),
            noop_hook(),
        );

        assert!(
            wait_for(Duration::from_secs(5), || matches!(
                kind_of(&manager, "gpu-1"),
                Some(RemoteStatusKind::Incompatible { .. })
            )),
            "an out-of-window remote must park incompatible, not retry as offline: {:?}",
            kind_of(&manager, "gpu-1")
        );
        let Some(RemoteStatusKind::Incompatible { message }) = kind_of(&manager, "gpu-1") else {
            panic!("expected an incompatible status");
        };
        // The greyed-out remote names the machine and the exact fix.
        assert!(message.contains("remote gpu-1"), "{message}");
        assert!(message.contains("herdr remote upgrade gpu-1"), "{message}");

        // Terminal: the backoff ladder stops instead of reconnecting forever.
        let connects = transport.connects();
        std::thread::sleep(Duration::from_millis(150));
        assert_eq!(
            transport.connects(),
            connects,
            "incompatible must not schedule further connects"
        );
        manager.stop();
    }

    #[test]
    fn failing_transport_goes_offline_and_keeps_retrying() {
        let transport = ScriptedTransport::new(vec![FakeBehavior::RefuseConnect]);
        let mut manager = FleetManager::start(
            vec![entry("a")],
            Arc::clone(&transport) as Arc<dyn FleetTransport>,
            tuning(Duration::from_millis(10)),
            noop_hook(),
        );

        assert!(
            wait_for(Duration::from_secs(5), || transport.connects() >= 3),
            "expected indefinite retries, got {} connects",
            transport.connects()
        );
        assert!(wait_for(Duration::from_secs(5), || matches!(
            kind_of(&manager, "a"),
            Some(RemoteStatusKind::Offline { attempt, ref last_error, .. })
                if attempt >= 2 && last_error.contains("connect failed")
        )));
        manager.stop();
    }

    #[test]
    fn connects_heartbeats_and_fires_the_reseed_hook() {
        let transport = ScriptedTransport::new(vec![FakeBehavior::ServeWelcomeAndPongs]);
        let reseeds = Arc::new(AtomicUsize::new(0));
        let hook_reseeds = Arc::clone(&reseeds);
        let mut manager = FleetManager::start(
            vec![entry("a")],
            Arc::clone(&transport) as Arc<dyn FleetTransport>,
            tuning(Duration::from_millis(10)),
            Arc::new(move |_| {
                hook_reseeds.fetch_add(1, Ordering::SeqCst);
            }),
        );

        assert!(wait_for(Duration::from_secs(5), || matches!(
            kind_of(&manager, "a"),
            Some(RemoteStatusKind::Connected)
        )));
        assert_eq!(reseeds.load(Ordering::SeqCst), 1);
        assert_eq!(transport.connects(), 1);

        // The session stays connected across several heartbeat intervals.
        std::thread::sleep(Duration::from_millis(200));
        assert!(matches!(
            kind_of(&manager, "a"),
            Some(RemoteStatusKind::Connected)
        ));
        assert_eq!(transport.connects(), 1);
        manager.stop();
    }

    #[test]
    fn heartbeat_timeout_marks_the_remote_offline() {
        let transport = ScriptedTransport::new(vec![FakeBehavior::ServeWelcomeNoPongs]);
        // A huge backoff keeps the offline state observable.
        let mut manager = FleetManager::start(
            vec![entry("a")],
            Arc::clone(&transport) as Arc<dyn FleetTransport>,
            tuning(Duration::from_secs(60)),
            noop_hook(),
        );

        assert!(wait_for(Duration::from_secs(5), || matches!(
            kind_of(&manager, "a"),
            Some(RemoteStatusKind::Offline { ref last_error, .. })
                if last_error.contains("heartbeat timed out")
        )));
        assert_eq!(transport.connects(), 1);
        manager.stop();
    }

    #[test]
    fn dropped_session_reconnects_and_reseeds_again() {
        let transport = ScriptedTransport::new(vec![
            FakeBehavior::ServeWelcomeThenClose,
            FakeBehavior::ServeWelcomeAndPongs,
        ]);
        let reseeds = Arc::new(AtomicUsize::new(0));
        let hook_reseeds = Arc::clone(&reseeds);
        let mut manager = FleetManager::start(
            vec![entry("a")],
            Arc::clone(&transport) as Arc<dyn FleetTransport>,
            tuning(Duration::from_millis(10)),
            Arc::new(move |_| {
                hook_reseeds.fetch_add(1, Ordering::SeqCst);
            }),
        );

        assert!(wait_for(Duration::from_secs(5), || {
            transport.connects() >= 2
                && matches!(kind_of(&manager, "a"), Some(RemoteStatusKind::Connected))
        }));
        assert_eq!(
            reseeds.load(Ordering::SeqCst),
            2,
            "full reseed per reconnect"
        );
        manager.stop();
    }

    #[test]
    fn manual_reset_forces_immediate_reconnect_through_a_long_backoff() {
        let transport = ScriptedTransport::new(vec![FakeBehavior::RefuseConnect]);
        let mut manager = FleetManager::start(
            vec![entry("a")],
            Arc::clone(&transport) as Arc<dyn FleetTransport>,
            tuning(Duration::from_secs(60)),
            noop_hook(),
        );

        assert!(wait_for(Duration::from_secs(5), || {
            transport.connects() == 1
                && matches!(
                    kind_of(&manager, "a"),
                    Some(RemoteStatusKind::Offline { .. })
                )
        }));

        assert!(manager.reset("a"));
        assert!(
            wait_for(Duration::from_secs(5), || transport.connects() >= 2),
            "reset must bypass the 60s backoff"
        );
        assert!(!manager.reset("missing"));
        manager.stop();
    }

    #[test]
    fn disabled_remotes_stay_visible_but_never_connect() {
        let transport = ScriptedTransport::new(vec![FakeBehavior::ServeWelcomeAndPongs]);
        let mut disabled = entry("a");
        disabled.enabled = false;
        let mut manager = FleetManager::start(
            vec![disabled],
            Arc::clone(&transport) as Arc<dyn FleetTransport>,
            tuning(Duration::from_millis(10)),
            noop_hook(),
        );

        std::thread::sleep(Duration::from_millis(100));
        assert!(matches!(
            kind_of(&manager, "a"),
            Some(RemoteStatusKind::Disabled)
        ));
        assert_eq!(transport.connects(), 0);
        assert!(
            !manager.reset("a"),
            "disabled remotes have no worker to reset"
        );
        manager.stop();
    }

    #[test]
    fn apply_config_diffs_by_name_and_restarts_identity_changes() {
        let transport = ScriptedTransport::new(vec![FakeBehavior::ServeWelcomeAndPongs]);
        let mut manager = FleetManager::start(
            vec![entry("a"), entry("b")],
            Arc::clone(&transport) as Arc<dyn FleetTransport>,
            tuning(Duration::from_millis(10)),
            noop_hook(),
        );
        assert!(wait_for(Duration::from_secs(5), || {
            matches!(kind_of(&manager, "a"), Some(RemoteStatusKind::Connected))
                && matches!(kind_of(&manager, "b"), Some(RemoteStatusKind::Connected))
        }));
        let connects_before = transport.connects();

        // Remove b, change a's target (identity change → remove-plus-add).
        let mut changed = entry("a");
        changed.target = "test@a-two.invalid".to_string();
        let changes = manager.apply_config(vec![changed.clone()]);
        assert!(changes.contains(&RemoteChange::Removed(entry("b"))));
        assert!(changes.contains(&RemoteChange::Removed(entry("a"))));
        assert!(changes.contains(&RemoteChange::Added(changed.clone())));

        let names: Vec<String> = manager
            .snapshot()
            .into_iter()
            .map(|status| status.entry.name)
            .collect();
        assert_eq!(names, vec!["a".to_string()]);
        assert_eq!(manager.entries(), vec![changed.clone()]);
        assert!(
            wait_for(Duration::from_secs(5), || transport.connects()
                > connects_before),
            "identity change must reconnect"
        );

        // Disabling via config stops the connection but keeps the entry.
        let mut disabled = changed;
        disabled.enabled = false;
        let changes = manager.apply_config(vec![disabled.clone()]);
        assert_eq!(
            changes,
            vec![RemoteChange::SettingsChanged {
                old: {
                    let mut old = entry("a");
                    old.target = "test@a-two.invalid".to_string();
                    old
                },
                new: disabled,
            }]
        );
        assert!(matches!(
            kind_of(&manager, "a"),
            Some(RemoteStatusKind::Disabled)
        ));
        manager.stop();
    }

    #[test]
    fn start_skips_invalid_and_duplicate_entries() {
        let transport = ScriptedTransport::new(vec![FakeBehavior::RefuseConnect]);
        let mut bad_target = entry("bad");
        bad_target.target = "-oProxyCommand=evil".to_string();
        let mut reserved = entry("x");
        reserved.name = "local".to_string();
        let mut disabled_duplicate = entry("a");
        disabled_duplicate.enabled = false;

        let manager = FleetManager::start(
            vec![entry("a"), bad_target, reserved, disabled_duplicate],
            Arc::clone(&transport) as Arc<dyn FleetTransport>,
            tuning(Duration::from_secs(60)),
            noop_hook(),
        );
        let names: Vec<String> = manager
            .snapshot()
            .into_iter()
            .map(|status| status.entry.name)
            .collect();
        assert_eq!(names, vec!["a".to_string()]);
        assert!(manager.snapshot()[0].entry.enabled, "first entry wins");
    }

    #[test]
    fn failure_reasons_carry_transport_diagnostics() {
        let reason = "bridge closed: unexpected end of file".to_string();
        assert_eq!(append_diagnostics(reason.clone(), None), reason);

        let tail = Arc::new(Mutex::new(String::new()));
        assert_eq!(append_diagnostics(reason.clone(), Some(&tail)), reason);

        *tail.lock().unwrap() = "can@gpu1.example: Permission denied (publickey).\n".to_string();
        assert_eq!(
            append_diagnostics(reason, Some(&tail)),
            "bridge closed: unexpected end of file \
             (ssh: can@gpu1.example: Permission denied (publickey).)"
        );
    }

    #[test]
    fn jitter_samples_stay_in_unit_range() {
        let _ = SystemTime::now();
        for _ in 0..1000 {
            let sample = jitter01();
            assert!((0.0..1.0).contains(&sample), "jitter {sample} out of range");
        }
    }
}

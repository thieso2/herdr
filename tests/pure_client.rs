//! End-to-end coverage for the pure-client protocol surface (#21).
//!
//! The pure client is a framed session negotiating the `catalog`,
//! `notification`, `window-title`, and `paste-image` capabilities next to
//! `pane-stream`: it resyncs from `session.snapshot`, follows
//! `catalog.event` frames, mutates through `api.request` passthrough, and
//! pastes images through `pane.paste_image`. These tests speak the wire
//! format by hand so they pin the protocol contract the pure client relies
//! on, including coexistence with plain NDJSON API clients on the same
//! socket and the reconnect-plus-resync path across a live handoff.

#![cfg(unix)]

mod support;

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use portable_pty::{native_pty_system, Child, CommandBuilder, MasterPty, PtySize};
use support::{cleanup_test_base, register_runtime_dir, register_spawned_herdr_pid};

const CONTROL_FRAME: u8 = 0;

const PURE_CLIENT_CAPABILITIES: &[&str] = &[
    "pane-stream",
    "catalog",
    "notification",
    "window-title",
    "paste-image",
];

struct SpawnedHerdr {
    _master: Box<dyn MasterPty + Send>,
    child: Box<dyn Child + Send + Sync>,
}

/// The real pure client (`herdr client` with `HERDR_PURE_CLIENT=1`) in a
/// PTY, with its rendered output accumulated on a reader thread.
struct SpawnedPureClient {
    _master: Box<dyn MasterPty + Send>,
    child: Box<dyn Child + Send + Sync>,
    output: std::sync::Arc<Mutex<String>>,
}

impl SpawnedPureClient {
    /// Waits until the accumulated rendered output contains `needle`.
    fn wait_for_output(&self, needle: &str, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self
                .output
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .contains(needle)
            {
                return true;
            }
            thread::sleep(Duration::from_millis(50));
        }
        false
    }

    fn output_snapshot(&self) -> String {
        self.output
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl Drop for SpawnedPureClient {
    fn drop(&mut self) {
        let pid = self.child.process_id();
        let _ = self.child.kill();
        support::unregister_spawned_herdr_pid(pid);
    }
}

fn spawn_pure_client(
    config_home: &Path,
    runtime_dir: &Path,
    api_socket: &Path,
) -> SpawnedPureClient {
    // The fleet is exactly what remotes.toml configures, so the client needs
    // an entry naming the local runtime or it opens an empty fleet and never
    // reaches the server this test spawned.
    support::write_local_remote(config_home);
    let pair = native_pty_system()
        .openpty(PtySize {
            rows: 24,
            cols: 100,
            pixel_width: 0,
            pixel_height: 0,
        })
        .unwrap();
    let mut cmd = CommandBuilder::new(env!("CARGO_BIN_EXE_herdr"));
    cmd.arg("client");
    cmd.env("HERDR_PURE_CLIENT", "1");
    cmd.env("HERDR_DISABLE_SOUND", "1");
    cmd.env("XDG_CONFIG_HOME", config_home);
    cmd.env("XDG_RUNTIME_DIR", runtime_dir);
    cmd.env("HERDR_SOCKET_PATH", api_socket);
    cmd.env_remove("HERDR_CLIENT_SOCKET_PATH");
    cmd.env("SHELL", "/bin/sh");
    cmd.env_remove("HERDR_ENV");
    cmd.env_remove("HERDR_STARTUP_CWD");

    let child = pair.slave.spawn_command(cmd).unwrap();
    support::register_spawned_herdr_pid(child.process_id());
    drop(pair.slave);

    let output = std::sync::Arc::new(Mutex::new(String::new()));
    let sink = std::sync::Arc::clone(&output);
    let mut reader = pair.master.try_clone_reader().unwrap();
    thread::spawn(move || {
        let mut buf = [0u8; 4096];
        while let Ok(n) = reader.read(&mut buf) {
            if n == 0 {
                break;
            }
            let mut out = sink.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            out.push_str(&String::from_utf8_lossy(&buf[..n]));
        }
    });

    SpawnedPureClient {
        _master: pair.master,
        child,
        output,
    }
}

impl Drop for SpawnedHerdr {
    fn drop(&mut self) {
        let pid = self.child.process_id();
        let _ = self.child.kill();
        support::unregister_spawned_herdr_pid(pid);
    }
}

fn test_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn unique_test_dir() -> PathBuf {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    PathBuf::from(format!("/tmp/hpc-{}-{n}", std::process::id()))
}

fn spawn_server(config_home: &Path, runtime_dir: &Path, api_socket: &Path) -> SpawnedHerdr {
    fs::create_dir_all(config_home.join(support::app_dir_name())).unwrap();
    fs::create_dir_all(runtime_dir).unwrap();
    register_runtime_dir(runtime_dir);
    fs::write(
        config_home
            .join(support::app_dir_name())
            .join("config.toml"),
        "onboarding = false\n",
    )
    .unwrap();

    let pair = native_pty_system()
        .openpty(PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .unwrap();
    let mut cmd = CommandBuilder::new(env!("CARGO_BIN_EXE_herdr"));
    cmd.arg("server");
    cmd.env("XDG_CONFIG_HOME", config_home);
    cmd.env("XDG_RUNTIME_DIR", runtime_dir);
    cmd.env("HERDR_SOCKET_PATH", api_socket);
    cmd.env(
        "HERDR_CLIENT_SOCKET_PATH",
        runtime_dir.join("herdr-client.sock"),
    );
    cmd.env("SHELL", "/bin/sh");
    cmd.env_remove("HERDR_ENV");
    cmd.env_remove("HERDR_STARTUP_CWD");

    let child = pair.slave.spawn_command(cmd).unwrap();
    register_spawned_herdr_pid(child.process_id());
    SpawnedHerdr {
        _master: pair.master,
        child,
    }
}

fn try_request(socket_path: &Path, request: serde_json::Value) -> Option<serde_json::Value> {
    let mut stream = UnixStream::connect(socket_path).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .ok()?;
    let line = format!("{request}\n");
    stream.write_all(line.as_bytes()).ok()?;
    let mut reader = BufReader::new(stream);
    let mut response = String::new();
    reader.read_line(&mut response).ok()?;
    serde_json::from_str(&response).ok()
}

fn request(socket_path: &Path, request: serde_json::Value) -> serde_json::Value {
    try_request(socket_path, request).expect("api request failed")
}

fn wait_for_api(socket_path: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if try_request(
            socket_path,
            serde_json::json!({"id":"test:ping","method":"ping"}),
        )
        .is_some()
        {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("api did not become ready at {}", socket_path.display());
}

/// A pure-client-shaped framed session speaking the wire format by hand.
struct FramedClient {
    stream: UnixStream,
}

impl FramedClient {
    fn connect(socket_path: &Path, id: &str) -> Self {
        let mut stream = UnixStream::connect(socket_path).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        stream.write_all(b"HRDR").unwrap();
        let mut client = Self { stream };
        client.send(serde_json::json!({
            "id": id,
            "method": "session.hello",
            "params": {
                "protocol": 1,
                "min_protocol": 1,
                "capabilities": PURE_CLIENT_CAPABILITIES,
            },
        }));
        let welcome = client.read_control();
        assert_eq!(welcome["result"]["type"], "session.welcome");
        let negotiated: Vec<&str> = welcome["result"]["capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|value| value.as_str())
            .collect();
        for capability in PURE_CLIENT_CAPABILITIES {
            assert!(
                negotiated.contains(capability),
                "server must offer {capability}; negotiated {negotiated:?}"
            );
        }
        client
    }

    fn send(&mut self, value: serde_json::Value) {
        let payload = serde_json::to_vec(&value).unwrap();
        let mut header = Vec::with_capacity(10);
        header.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        header.push(CONTROL_FRAME);
        header.push(0);
        header.extend_from_slice(&0u32.to_le_bytes());
        self.stream.write_all(&header).unwrap();
        self.stream.write_all(&payload).unwrap();
    }

    fn read_frame(&mut self) -> (u8, u32, Vec<u8>) {
        let mut header = [0u8; 10];
        self.stream.read_exact(&mut header).unwrap();
        let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let frame_type = header[4];
        let stream_id = u32::from_le_bytes([header[6], header[7], header[8], header[9]]);
        let mut payload = vec![0u8; len];
        self.stream.read_exact(&mut payload).unwrap();
        (frame_type, stream_id, payload)
    }

    fn read_control(&mut self) -> serde_json::Value {
        loop {
            let (frame_type, _, payload) = self.read_frame();
            if frame_type == CONTROL_FRAME {
                return serde_json::from_slice(&payload).unwrap();
            }
        }
    }

    /// Reads control frames until the response with `id` arrives, collecting
    /// event frames seen along the way.
    fn read_response_collecting(
        &mut self,
        id: &str,
        events: &mut Vec<serde_json::Value>,
    ) -> serde_json::Value {
        loop {
            let value = self.read_control();
            if value.get("id").and_then(|value| value.as_str()) == Some(id) {
                return value;
            }
            if value.get("event").is_some() {
                events.push(value);
            }
        }
    }

    /// Waits for an event with the given name, collecting others.
    fn wait_for_event(&mut self, event: &str) -> serde_json::Value {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            assert!(Instant::now() < deadline, "no {event} event arrived");
            let value = self.read_control();
            if value.get("event").and_then(|value| value.as_str()) == Some(event) {
                return value;
            }
        }
    }

    fn snapshot(&mut self, id: &str) -> (serde_json::Value, u64) {
        self.send(serde_json::json!({
            "id": id,
            "method": "session.snapshot",
            "params": {},
        }));
        let mut events = Vec::new();
        let response = self.read_response_collecting(id, &mut events);
        let sequence = response["result"]["sequence"]
            .as_u64()
            .unwrap_or_else(|| panic!("snapshot carries a sequence anchor: {response}"));
        (response["result"]["snapshot"].clone(), sequence)
    }
}

fn create_pane(api_socket: &Path, cwd: &Path) -> (String, String) {
    let created = request(
        api_socket,
        serde_json::json!({
            "id": "test:ws",
            "method": "workspace.create",
            "params": {"cwd": cwd.display().to_string(), "focus": true},
        }),
    );
    assert_eq!(created["result"]["type"], "workspace_created");
    let pane_id = created["result"]["root_pane"]["pane_id"]
        .as_str()
        .unwrap()
        .to_string();
    let info = request(
        api_socket,
        serde_json::json!({
            "id": "test:pane",
            "method": "pane.get",
            "params": {"pane_id": pane_id},
        }),
    );
    let terminal_id = info["result"]["pane"]["terminal_id"]
        .as_str()
        .unwrap()
        .to_string();
    (pane_id, terminal_id)
}

/// The full pure-client control-plane surface on one session: negotiated
/// capabilities, sequence-anchored snapshot resync, api.request passthrough
/// mutations arriving back as catalog events, image paste, and NDJSON API
/// clients coexisting on the same socket throughout.
#[test]
fn pure_client_session_snapshots_mutates_and_pastes_images() {
    let _lock = test_lock();
    let base = unique_test_dir();
    let config_home = base.join("config");
    let runtime_dir = base.join("runtime");
    let api_socket = runtime_dir.join("herdr.sock");

    let server = spawn_server(&config_home, &runtime_dir, &api_socket);
    wait_for_api(&api_socket, Duration::from_secs(10));
    let (pane_id, _terminal_id) = create_pane(&api_socket, &base);

    let mut client = FramedClient::connect(&api_socket, "hello:pure");

    // Sequence-anchored full resync.
    let (snapshot, sequence) = client.snapshot("snap:1");
    let pane_ids: Vec<&str> = snapshot["panes"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|pane| pane["pane_id"].as_str())
        .collect();
    assert!(
        pane_ids.contains(&pane_id.as_str()),
        "snapshot lists the created pane: {pane_ids:?}"
    );

    // Control-plane mutation through api.request passthrough; the catalog
    // event stream reports it past the snapshot anchor.
    client.send(serde_json::json!({
        "id": "api:split",
        "method": "api.request",
        "params": {"request": {
            "id": "api:split",
            "method": "pane.split",
            "params": {"target_pane_id": pane_id, "direction": "right", "focus": true},
        }},
    }));
    let mut events = Vec::new();
    let response = client.read_response_collecting("api:split", &mut events);
    assert!(
        response.get("error").is_none(),
        "pane.split passthrough succeeds: {response}"
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    let created_seq = loop {
        assert!(
            Instant::now() < deadline,
            "no pane_created catalog event arrived; saw {events:?}"
        );
        if let Some(seq) = events
            .iter()
            .filter(|event| event["event"] == "catalog.event")
            .find(|event| event["data"]["data"]["type"] == "pane_created")
            .and_then(|event| event["seq"].as_u64())
        {
            break seq;
        }
        let value = client.read_control();
        if value.get("event").is_some() {
            events.push(value);
        }
    };
    assert!(
        created_seq > sequence,
        "catalog events continue past the snapshot anchor ({created_seq} > {sequence})"
    );

    // Image paste through the negotiated paste-image capability.
    use base64::Engine as _;
    let png = [
        0x89u8, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x00,
    ];
    client.send(serde_json::json!({
        "id": "paste:1",
        "method": "pane.paste_image",
        "params": {
            "pane_id": pane_id,
            "extension": "png",
            "data_base64": base64::engine::general_purpose::STANDARD.encode(png),
        },
    }));
    let response = client.read_response_collecting("paste:1", &mut events);
    assert!(
        response.get("error").is_none(),
        "pane.paste_image succeeds: {response}"
    );

    // NDJSON API clients keep working on the same socket while the framed
    // catalog session is live.
    let listed = request(
        &api_socket,
        serde_json::json!({"id":"test:list","method":"pane.list","params":{}}),
    );
    assert!(
        listed["result"]["panes"].as_array().unwrap().len() >= 2,
        "ndjson client sees the split pane too"
    );

    let pids = support::herdr_server_pids_for_runtime_dir(&runtime_dir).unwrap_or_default();
    drop(client);
    drop(server);
    for pid in pids {
        unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
    }
    cleanup_test_base(&base);
}

/// A live handoff ends the framed catalog session; the pure client's
/// reconnect loop connects to the replacement server and resyncs from a
/// fresh sequence-anchored snapshot that still carries the session's panes.
#[test]
fn pure_client_catalog_session_reconnects_and_resyncs_across_live_handoff() {
    let _lock = test_lock();
    let base = unique_test_dir();
    let config_home = base.join("config");
    let runtime_dir = base.join("runtime");
    let api_socket = runtime_dir.join("herdr.sock");

    let server = spawn_server(&config_home, &runtime_dir, &api_socket);
    wait_for_api(&api_socket, Duration::from_secs(10));
    let (pane_id, _terminal_id) = create_pane(&api_socket, &base);

    let mut client = FramedClient::connect(&api_socket, "hello:pre-handoff");
    let (_snapshot, _sequence) = client.snapshot("snap:pre");

    assert!(
        request(
            &api_socket,
            serde_json::json!({"id":"test:handoff","method":"server.live_handoff","params":{}}),
        )
        .get("result")
        .is_some(),
        "live handoff should be accepted"
    );

    // The framed session ends when the old server exits; the client must
    // observe a clean disconnect rather than hanging.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(
            Instant::now() < deadline,
            "framed catalog session was not disconnected by the handoff"
        );
        let mut header = [0u8; 10];
        match client.stream.read(&mut header) {
            Ok(0) => break,
            Ok(_) => continue,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(err) if err.kind() == std::io::ErrorKind::TimedOut => continue,
            Err(_) => break,
        }
    }
    drop(client);
    drop(server);

    // Reconnect + resync against the replacement server: the handed-off
    // session still carries the pane, so a pure client that reseeds from
    // the fresh snapshot converges on the restored session.
    thread::sleep(Duration::from_millis(300));
    wait_for_api(&api_socket, Duration::from_secs(15));
    let mut reconnected = FramedClient::connect(&api_socket, "hello:post-handoff");
    let (snapshot, _sequence) = reconnected.snapshot("snap:post");
    let pane_ids: Vec<&str> = snapshot["panes"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|pane| pane["pane_id"].as_str())
        .collect();
    assert!(
        pane_ids.contains(&pane_id.as_str()),
        "post-handoff snapshot still lists the pane: {pane_ids:?}"
    );

    // Catalog events flow on the new session too.
    reconnected.send(serde_json::json!({
        "id": "api:ws2",
        "method": "api.request",
        "params": {"request": {
            "id": "api:ws2",
            "method": "workspace.create",
            "params": {"cwd": base.display().to_string(), "focus": true},
        }},
    }));
    let event = reconnected.wait_for_event("catalog.event");
    assert!(
        event["seq"].as_u64().is_some(),
        "catalog events carry sequences after resync: {event}"
    );

    let pids = support::herdr_server_pids_for_runtime_dir(&runtime_dir).unwrap_or_default();
    drop(reconnected);
    for pid in pids {
        unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
    }
    cleanup_test_base(&base);
}

/// The real pure client, flag on, end to end: `herdr client` with
/// `HERDR_PURE_CLIENT=1` renders replica content from the live server, and
/// its own reconnect loop survives a live handoff — after the replacement
/// server takes over, the client resyncs and renders new pane output
/// written on the restored session (restore continuity through the real
/// `run_pure_client` loop, not a hand-rolled wire client).
#[test]
fn real_pure_client_renders_and_resyncs_across_live_handoff() {
    let _lock = test_lock();
    let base = unique_test_dir();
    let config_home = base.join("config");
    let runtime_dir = base.join("runtime");
    let api_socket = runtime_dir.join("herdr.sock");

    let server = spawn_server(&config_home, &runtime_dir, &api_socket);
    wait_for_api(&api_socket, Duration::from_secs(10));
    let (pane_id, _terminal_id) = create_pane(&api_socket, &base);

    // Distinctive pane content the rendered client output must show.
    let sent = request(
        &api_socket,
        serde_json::json!({
            "id": "test:text1",
            "method": "pane.send_text",
            "params": {"pane_id": pane_id, "text": "echo MARK_PURE_ALPHA\n"},
        }),
    );
    assert!(sent.get("error").is_none(), "pane.send_text: {sent}");

    let client = spawn_pure_client(&config_home, &runtime_dir, &api_socket);
    assert!(
        client.wait_for_output("MARK_PURE_ALPHA", Duration::from_secs(15)),
        "pure client must render replica content; output: {:?}",
        client.output_snapshot()
    );

    // Live handoff: the old server exits, a replacement takes over the
    // session, and the pure client's reconnect loop must resync on its own.
    assert!(
        request(
            &api_socket,
            serde_json::json!({"id":"test:handoff","method":"server.live_handoff","params":{}}),
        )
        .get("result")
        .is_some(),
        "live handoff should be accepted"
    );
    thread::sleep(Duration::from_millis(500));
    wait_for_api(&api_socket, Duration::from_secs(15));

    // New output on the handed-off pane only reaches the client if it
    // reconnected, resynced the catalog, and reopened the pane stream.
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut resynced = false;
    while Instant::now() < deadline {
        let sent = try_request(
            &api_socket,
            serde_json::json!({
                "id": "test:text2",
                "method": "pane.send_text",
                "params": {"pane_id": pane_id, "text": "echo MARK_PURE_BRAVO\n"},
            }),
        );
        if sent
            .as_ref()
            .is_some_and(|sent| sent.get("error").is_none())
            && client.wait_for_output("MARK_PURE_BRAVO", Duration::from_secs(2))
        {
            resynced = true;
            break;
        }
    }
    assert!(
        resynced,
        "pure client must resync and render post-handoff pane output; output: {:?}",
        client.output_snapshot()
    );

    let pids = support::herdr_server_pids_for_runtime_dir(&runtime_dir).unwrap_or_default();
    drop(client);
    drop(server);
    for pid in pids {
        unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
    }
    cleanup_test_base(&base);
}

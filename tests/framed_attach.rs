//! End-to-end coverage for framed terminal attach.
//!
//! Attach is a framed pane-stream client on the API socket: `session.hello`
//! with the `pane-stream` capability, `stream.open` in write mode for the
//! snapshot plus the live output tail, control-plane methods for input,
//! resize, and scroll, and a stream-id-keyed write grant instead of the old
//! client-keyed writer lock.

mod support;

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use std::process::{Command, Stdio};

use portable_pty::{native_pty_system, Child, CommandBuilder, MasterPty, PtySize};
use support::{cleanup_test_base, register_runtime_dir, register_spawned_herdr_pid};

const CONTROL_FRAME: u8 = 0;
const DATA_FRAME: u8 = 1;

struct SpawnedHerdr {
    _master: Box<dyn MasterPty + Send>,
    child: Box<dyn Child + Send + Sync>,
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
    PathBuf::from(format!("/tmp/hfa-{}-{n}", std::process::id()))
}

fn spawn_server(config_home: &Path, runtime_dir: &Path, api_socket: &Path) -> SpawnedHerdr {
    fs::create_dir_all(config_home.join("herdr")).unwrap();
    fs::create_dir_all(runtime_dir).unwrap();
    register_runtime_dir(runtime_dir);
    fs::write(
        config_home.join("herdr/config.toml"),
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
    stream.write_all(request.to_string().as_bytes()).ok()?;
    stream.write_all(b"\n").ok()?;
    stream.flush().ok()?;
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line).ok()?;
    serde_json::from_str(&line).ok()
}

fn request(socket_path: &Path, request: serde_json::Value) -> serde_json::Value {
    try_request(socket_path, request.clone())
        .unwrap_or_else(|| panic!("api request failed: {request}"))
}

fn wait_for_api(socket_path: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(response) = try_request(
            socket_path,
            serde_json::json!({"id":"test:ping","method":"ping","params":{}}),
        ) {
            if response.get("result").is_some() {
                return;
            }
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("api did not become ready at {}", socket_path.display());
}

/// A framed pane-stream client speaking the wire format by hand, so the test
/// pins the protocol rather than the client implementation.
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
            "params": {"protocol": 1, "min_protocol": 1, "capabilities": ["pane-stream"]},
        }));
        let welcome = client.read_control();
        assert_eq!(welcome["result"]["type"], "session.welcome");
        assert_eq!(
            welcome["result"]["capabilities"],
            serde_json::json!(["pane-stream"])
        );
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
        self.stream.flush().unwrap();
    }

    fn read_frame(&mut self) -> (u8, u32, Vec<u8>) {
        let mut header = [0u8; 10];
        self.stream.read_exact(&mut header).unwrap();
        let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let mut payload = vec![0u8; len];
        self.stream.read_exact(&mut payload).unwrap();
        (
            header[4],
            u32::from_le_bytes([header[6], header[7], header[8], header[9]]),
            payload,
        )
    }

    /// Reads frames until the next control frame, collecting any data frames
    /// that arrive first.
    fn read_control_collecting(&mut self, tail: &mut Vec<u8>) -> serde_json::Value {
        loop {
            let (frame_type, _, payload) = self.read_frame();
            if frame_type == CONTROL_FRAME {
                return serde_json::from_slice(&payload).unwrap();
            }
            tail.extend_from_slice(&payload);
        }
    }

    fn read_control(&mut self) -> serde_json::Value {
        self.read_control_collecting(&mut Vec::new())
    }

    fn open_stream(&mut self, id: &str, params: serde_json::Value) -> serde_json::Value {
        self.send(serde_json::json!({
            "id": id,
            "method": "stream.open",
            "params": params,
        }));
        self.read_control()
    }

    fn ping(&mut self, id: &str) -> serde_json::Value {
        self.send(serde_json::json!({"id": id, "method": "ping", "params": {}}));
        self.read_control()
    }
}

fn open_write_stream(client: &mut FramedClient, id: &str, target: &str, takeover: bool) -> u32 {
    let opened = client.open_stream(
        id,
        serde_json::json!({
            "pane_id": target,
            "mode": "write",
            "takeover": takeover,
            "cols": 100,
            "rows": 30,
        }),
    );
    assert_eq!(
        opened["result"]["type"], "pane_stream_opened",
        "stream.open failed: {opened}"
    );
    opened["result"]["stream"]["stream_id"].as_u64().unwrap() as u32
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

#[test]
fn framed_attach_streams_snapshot_tail_input_and_enforces_the_write_grant() {
    use base64::Engine as _;

    let _lock = test_lock();
    let base = unique_test_dir();
    let config_home = base.join("config");
    let runtime_dir = base.join("runtime");
    let api_socket = runtime_dir.join("herdr.sock");

    let server = spawn_server(&config_home, &runtime_dir, &api_socket);
    wait_for_api(&api_socket, Duration::from_secs(10));
    let (pane_id, terminal_id) = create_pane(&api_socket, &base);

    // Attach opens a write-mode stream against the pane. A terminal id is a
    // valid attach target too, exactly like `herdr terminal attach`.
    let mut attach = FramedClient::connect(&api_socket, "hello:attach");
    let opened = attach.open_stream(
        "open:attach",
        serde_json::json!({
            "pane_id": terminal_id,
            "mode": "write",
            "takeover": false,
            "cols": 100,
            "rows": 30,
        }),
    );
    assert_eq!(opened["result"]["type"], "pane_stream_opened");
    let stream = &opened["result"]["stream"];
    assert_eq!(
        stream["pane_id"],
        serde_json::json!(pane_id),
        "a terminal-id target resolves to its public pane id"
    );
    assert!(
        stream["snapshot"].is_string(),
        "attach seeds the screen from the stream snapshot"
    );
    let stream_id = stream["stream_id"].as_u64().unwrap() as u32;

    // Input goes out as a control-plane method; the PTY echo comes back as
    // DATA frames on the stream.
    let marker = "herdr_framed_attach_marker";
    attach.send(serde_json::json!({
        "id": "input:1",
        "method": "pane.send_bytes",
        "params": {
            "pane_id": pane_id,
            "data_base64": base64::engine::general_purpose::STANDARD
                .encode(format!("echo {marker}\r")),
        },
    }));
    let mut tail = Vec::new();
    let ack = attach.read_control_collecting(&mut tail);
    assert_eq!(ack["id"], "input:1");
    assert_eq!(ack["result"]["type"], "ok");

    let deadline = Instant::now() + Duration::from_secs(10);
    while !String::from_utf8_lossy(&tail).contains(marker) {
        assert!(
            Instant::now() < deadline,
            "live tail never carried the echo"
        );
        let (frame_type, frame_stream_id, payload) = attach.read_frame();
        if frame_type == DATA_FRAME {
            assert_eq!(frame_stream_id, stream_id);
            tail.extend_from_slice(&payload);
        }
    }

    // Resize and scroll are stream methods gated on the write grant.
    attach.send(serde_json::json!({
        "id": "resize:1",
        "method": "stream.resize",
        "params": {"stream_id": stream_id, "cols": 90, "rows": 25},
    }));
    let resized = attach.read_control_collecting(&mut tail);
    assert_eq!(resized["id"], "resize:1");
    assert_eq!(resized["result"]["type"], "ok", "{resized}");

    attach.send(serde_json::json!({
        "id": "scroll:1",
        "method": "stream.scroll",
        "params": {"stream_id": stream_id, "direction": "up", "lines": 3},
    }));
    let scrolled = attach.read_control_collecting(&mut tail);
    assert_eq!(scrolled["id"], "scroll:1");
    assert_eq!(scrolled["result"]["type"], "ok", "{scrolled}");

    // A read-mode stream needs no grant and coexists with the writer.
    let mut observer = FramedClient::connect(&api_socket, "hello:observe");
    let observed = observer.open_stream(
        "open:observe",
        serde_json::json!({"pane_id": pane_id, "mode": "read"}),
    );
    assert_eq!(observed["result"]["type"], "pane_stream_opened");

    // A second writer without takeover is refused with a structured error,
    // and its connection stays up.
    let mut rival = FramedClient::connect(&api_socket, "hello:rival");
    let refused = rival.open_stream(
        "open:rival",
        serde_json::json!({"pane_id": pane_id, "mode": "write", "takeover": false}),
    );
    assert_eq!(refused["error"]["code"], "pane_write_locked", "{refused}");
    assert!(
        refused["error"]["message"]
            .as_str()
            .unwrap()
            .contains(&stream_id.to_string()),
        "the refusal names the holding stream: {refused}"
    );
    assert_eq!(rival.ping("ping:rival")["result"]["type"], "pong");

    // With takeover the rival wins and the previous holder is revoked on its
    // own stream without losing its connection.
    let rival_stream_id = open_write_stream(&mut rival, "open:rival:takeover", &pane_id, true);
    assert_ne!(rival_stream_id, stream_id);

    let deadline = Instant::now() + Duration::from_secs(10);
    let revoked = loop {
        assert!(Instant::now() < deadline, "revocation event never arrived");
        let (frame_type, _, payload) = attach.read_frame();
        if frame_type != CONTROL_FRAME {
            continue;
        }
        let control: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        if control["event"] == "stream.revoked" {
            break control;
        }
    };
    assert_eq!(revoked["data"]["stream_id"], stream_id);
    assert_eq!(revoked["data"]["reason"], "taken_over");
    assert_eq!(attach.ping("ping:attach")["result"]["type"], "pong");

    // The revoked stream no longer owns the pane.
    attach.send(serde_json::json!({
        "id": "resize:2",
        "method": "stream.resize",
        "params": {"stream_id": stream_id, "cols": 80, "rows": 24},
    }));
    let rejected = attach.read_control();
    assert_eq!(rejected["error"]["code"], "unknown_stream", "{rejected}");

    // Closing the winner's stream releases the grant for the next attach.
    rival.send(serde_json::json!({
        "id": "close:rival",
        "method": "stream.close",
        "params": {"stream_id": rival_stream_id},
    }));
    let closed = rival.read_control();
    assert_eq!(closed["result"]["type"], "stream_closed", "{closed}");

    let mut reattach = FramedClient::connect(&api_socket, "hello:reattach");
    open_write_stream(&mut reattach, "open:reattach", &pane_id, false);

    drop(server);
    cleanup_test_base(&base);
}

#[test]
fn framed_attach_disconnects_cleanly_across_a_live_handoff() {
    let _lock = test_lock();
    let base = unique_test_dir();
    let config_home = base.join("config");
    let runtime_dir = base.join("runtime");
    let api_socket = runtime_dir.join("herdr.sock");

    let server = spawn_server(&config_home, &runtime_dir, &api_socket);
    wait_for_api(&api_socket, Duration::from_secs(10));
    let (pane_id, _terminal_id) = create_pane(&api_socket, &base);

    let mut attach = FramedClient::connect(&api_socket, "hello:handoff");
    let stream_id = open_write_stream(&mut attach, "open:handoff", &pane_id, false);
    assert_ne!(stream_id, 0);

    assert!(
        request(
            &api_socket,
            serde_json::json!({"id":"test:handoff","method":"server.live_handoff","params":{}}),
        )
        .get("result")
        .is_some(),
        "live handoff should be accepted"
    );

    // The attach client sees a clean end of stream instead of hanging. The
    // old server process must produce that disconnect on its own by exiting
    // after the handoff; `server` is dropped (which SIGKILLs) only after the
    // disconnect was observed, so the kill cannot fake this assertion.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(
            Instant::now() < deadline,
            "framed attach client was not disconnected by the handoff"
        );
        let mut header = [0u8; 10];
        match attach.stream.read(&mut header) {
            Ok(0) => break,
            Ok(_) => continue,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(err) if err.kind() == std::io::ErrorKind::TimedOut => continue,
            Err(_) => break,
        }
    }
    drop(attach);
    drop(server);

    // The replacement server serves attach again, and the write grant the old
    // process held is gone with it.
    thread::sleep(Duration::from_millis(300));
    wait_for_api(&api_socket, Duration::from_secs(15));
    let mut reattach = FramedClient::connect(&api_socket, "hello:post-handoff");
    let reattached_stream_id =
        open_write_stream(&mut reattach, "open:post-handoff", &pane_id, false);
    assert_ne!(reattached_stream_id, 0);

    let pids = support::herdr_server_pids_for_runtime_dir(&runtime_dir).unwrap_or_default();
    drop(reattach);
    for pid in pids {
        unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
    }
    cleanup_test_base(&base);
}

#[test]
fn terminal_session_observe_and_control_speak_the_framed_protocol() {
    use base64::Engine as _;

    let _lock = test_lock();
    let base = unique_test_dir();
    let config_home = base.join("config");
    let runtime_dir = base.join("runtime");
    let api_socket = runtime_dir.join("herdr.sock");

    let server = spawn_server(&config_home, &runtime_dir, &api_socket);
    wait_for_api(&api_socket, Duration::from_secs(10));
    let (pane_id, _terminal_id) = create_pane(&api_socket, &base);

    // A read-only observer prints the opening snapshot record and then the
    // pane output tail.
    let mut observe = Command::new(env!("CARGO_BIN_EXE_herdr"))
        .args([
            "terminal", "session", "observe", &pane_id, "--cols", "100", "--rows", "30",
        ])
        .env("XDG_CONFIG_HOME", &config_home)
        .env("XDG_RUNTIME_DIR", &runtime_dir)
        .env("HERDR_SOCKET_PATH", &api_socket)
        .env_remove("HERDR_CLIENT_SOCKET_PATH")
        .env_remove("HERDR_STARTUP_CWD")
        .env_remove("HERDR_ENV")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let observe_stdout = BufReader::new(observe.stdout.take().unwrap());
    let observed: serde_json::Value =
        serde_json::from_str(&observe_stdout.lines().next().unwrap().unwrap()).unwrap();
    assert_eq!(observed["type"], "terminal.frame");
    assert_eq!(observed["full"], true);
    let _ = observe.kill();
    let _ = observe.wait();

    // The real CLI client: a writable session controller reading JSON
    // commands on stdin and printing JSON records on stdout.
    let mut control = Command::new(env!("CARGO_BIN_EXE_herdr"))
        .args([
            "terminal", "session", "control", &pane_id, "--cols", "100", "--rows", "30",
        ])
        .env("XDG_CONFIG_HOME", &config_home)
        .env("XDG_RUNTIME_DIR", &runtime_dir)
        .env("HERDR_SOCKET_PATH", &api_socket)
        .env_remove("HERDR_CLIENT_SOCKET_PATH")
        .env_remove("HERDR_STARTUP_CWD")
        .env_remove("HERDR_ENV")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let mut stdin = control.stdin.take().unwrap();
    let stdout = BufReader::new(control.stdout.take().unwrap());
    let mut lines = stdout.lines();

    // The first record is the opening snapshot of the pane screen.
    let first: serde_json::Value = serde_json::from_str(&lines.next().unwrap().unwrap()).unwrap();
    assert_eq!(first["type"], "terminal.frame");
    assert_eq!(first["full"], true);
    assert_eq!(first["pane_id"], serde_json::json!(pane_id));
    assert!(base64::engine::general_purpose::STANDARD
        .decode(first["bytes"].as_str().unwrap())
        .is_ok());

    let marker = "herdr_session_control_marker";
    stdin
        .write_all(
            format!("{{\"type\":\"terminal.input\",\"text\":\"echo {marker}\\r\"}}\n").as_bytes(),
        )
        .unwrap();
    stdin.flush().unwrap();

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut seen = String::new();
    while !seen.contains(marker) {
        assert!(
            Instant::now() < deadline,
            "session control never streamed the echo; saw {seen:?}"
        );
        let line = lines.next().expect("stdout closed").unwrap();
        let record: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(record["type"], "terminal.frame", "{record}");
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(record["bytes"].as_str().unwrap())
            .unwrap();
        seen.push_str(&String::from_utf8_lossy(&bytes));
    }

    // The controller holds the pane write grant while it runs.
    let mut rival = FramedClient::connect(&api_socket, "hello:rival-cli");
    let refused = rival.open_stream(
        "open:rival-cli",
        serde_json::json!({"pane_id": pane_id, "mode": "write", "takeover": false}),
    );
    assert_eq!(refused["error"]["code"], "pane_write_locked", "{refused}");

    // Releasing gives the grant back.
    stdin
        .write_all(b"{\"type\":\"terminal.release\"}\n")
        .unwrap();
    stdin.flush().unwrap();
    drop(stdin);
    let status_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match control.try_wait().unwrap() {
            Some(_) => break,
            None if Instant::now() >= status_deadline => {
                let _ = control.kill();
                panic!("terminal session control did not exit after release");
            }
            None => thread::sleep(Duration::from_millis(50)),
        }
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let opened = rival.open_stream(
            "open:rival-cli-2",
            serde_json::json!({"pane_id": pane_id, "mode": "write", "takeover": false}),
        );
        if opened["result"]["type"] == "pane_stream_opened" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "write grant was not released when the controller exited: {opened}"
        );
        thread::sleep(Duration::from_millis(50));
    }

    drop(server);
    cleanup_test_base(&base);
}

#[test]
fn terminal_attach_blits_the_live_tail_and_detaches_on_the_escape_key() {
    let _lock = test_lock();
    let base = unique_test_dir();
    let config_home = base.join("config");
    let runtime_dir = base.join("runtime");
    let api_socket = runtime_dir.join("herdr.sock");

    let server = spawn_server(&config_home, &runtime_dir, &api_socket);
    wait_for_api(&api_socket, Duration::from_secs(10));
    let (pane_id, _terminal_id) = create_pane(&api_socket, &base);

    // `herdr terminal attach` takes over a real terminal, so it runs on a pty.
    let pair = native_pty_system()
        .openpty(PtySize {
            rows: 30,
            cols: 100,
            pixel_width: 0,
            pixel_height: 0,
        })
        .unwrap();
    let mut cmd = CommandBuilder::new(env!("CARGO_BIN_EXE_herdr"));
    cmd.args(["terminal", "attach", &pane_id]);
    cmd.env("XDG_CONFIG_HOME", &config_home);
    cmd.env("XDG_RUNTIME_DIR", &runtime_dir);
    cmd.env("HERDR_SOCKET_PATH", &api_socket);
    cmd.env("TERM", "xterm-256color");
    cmd.env_remove("HERDR_CLIENT_SOCKET_PATH");
    cmd.env_remove("HERDR_STARTUP_CWD");
    cmd.env_remove("HERDR_ENV");
    let mut attach = pair.slave.spawn_command(cmd).unwrap();
    register_spawned_herdr_pid(attach.process_id());

    let mut reader = pair.master.try_clone_reader().unwrap();
    let (output_tx, output_rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let mut buf = [0u8; 4096];
        while let Ok(read) = reader.read(&mut buf) {
            if read == 0 || output_tx.send(buf[..read].to_vec()).is_err() {
                return;
            }
        }
    });
    let mut writer = pair.master.take_writer().unwrap();

    // Typing into the attached terminal reaches the pane, and the pane's raw
    // output comes back on the attached screen.
    let marker = "herdr_attach_pty_marker";
    let mut seen = String::new();
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut typed_at = Instant::now();
    writer
        .write_all(format!("echo {marker}\r").as_bytes())
        .unwrap();
    writer.flush().unwrap();
    while !seen.contains(marker) {
        assert!(
            Instant::now() < deadline,
            "attached terminal never showed the echoed marker; saw {seen:?}"
        );
        match output_rx.recv_timeout(Duration::from_millis(250)) {
            Ok(chunk) => seen.push_str(&String::from_utf8_lossy(&chunk)),
            Err(_) => {
                // The pane shell may still have been starting up; retype.
                if typed_at.elapsed() > Duration::from_secs(2) {
                    writer
                        .write_all(format!("echo {marker}\r").as_bytes())
                        .unwrap();
                    writer.flush().unwrap();
                    typed_at = Instant::now();
                }
            }
        }
    }

    // Ctrl+B q detaches and the client exits cleanly.
    writer.write_all(&[0x02, b'q']).unwrap();
    writer.flush().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match attach.try_wait().unwrap() {
            Some(status) => {
                assert!(status.success(), "detach should exit cleanly: {status:?}");
                break;
            }
            None if Instant::now() >= deadline => {
                let _ = attach.kill();
                panic!("attach client did not exit after ctrl+b q");
            }
            None => thread::sleep(Duration::from_millis(50)),
        }
    }
    support::unregister_spawned_herdr_pid(attach.process_id());

    // The write grant went away with the attach client.
    let mut reattach = FramedClient::connect(&api_socket, "hello:after-detach");
    open_write_stream(&mut reattach, "open:after-detach", &pane_id, false);

    drop(server);
    cleanup_test_base(&base);
}

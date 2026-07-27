use super::harness::*;

// Minimal framed-protocol codec for end-to-end bridge tests: 10-byte
// little-endian header (len u32, type u8, reserved u8, stream_id u32)
// followed by the payload. Mirrors src/protocol/framed.rs.

fn write_control(writer: &mut impl Write, value: &serde_json::Value) {
    let payload = serde_json::to_vec(value).unwrap();
    let mut header = [0u8; 10];
    header[0..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    header[4] = 0; // control frame
    header[6..10].copy_from_slice(&0u32.to_le_bytes());
    writer.write_all(&header).unwrap();
    writer.write_all(&payload).unwrap();
    writer.flush().unwrap();
}

fn read_control(reader: &mut impl std::io::Read) -> serde_json::Value {
    let mut header = [0u8; 10];
    reader.read_exact(&mut header).unwrap();
    let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
    assert_eq!(header[4], 0, "expected a control frame");
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).unwrap();
    serde_json::from_slice(&payload).unwrap()
}

#[test]
fn remote_list_reads_config_offline_when_no_server_is_running() {
    let base = unique_test_dir();
    let config_home = base.join("config");
    let runtime_dir = base.join("runtime");
    let app_config = config_home.join(app_dir_name());
    fs::create_dir_all(&app_config).unwrap();
    fs::create_dir_all(&runtime_dir).unwrap();
    register_runtime_dir(&runtime_dir);
    fs::write(
        app_config.join("remotes.toml"),
        concat!(
            "[[remote]]\n",
            "name = \"gpu1\"\n",
            "target = \"can@gpu1.example\"\n",
            "\n",
            "[[remote]]\n",
            "name = \"gpu2\"\n",
            "target = \"can@gpu2.example\"\n",
            "session = \"work\"\n",
            "enabled = false\n",
        ),
    )
    .unwrap();

    let response = run_named_cli_json(&config_home, &runtime_dir, &["remote", "list", "--json"]);
    let remotes = response["result"]["remotes"].as_array().unwrap();
    // The file is the whole fleet: there is no implicit local runtime to
    // prepend. A local runtime is an ordinary target-less entry, and this
    // fixture configures none.
    assert_eq!(remotes.len(), 2);

    assert_eq!(remotes[0]["index"], 0);
    assert_eq!(remotes[0]["name"], "gpu1");
    assert_eq!(remotes[0]["target"], "can@gpu1.example");
    assert_eq!(remotes[0]["session"], "default");
    assert_eq!(remotes[0]["enabled"], true);
    assert_eq!(remotes[0]["state"], "unknown");

    assert_eq!(remotes[1]["name"], "gpu2");
    assert_eq!(remotes[1]["session"], "work");
    assert_eq!(remotes[1]["state"], "disabled");

    // The human-readable table works offline too.
    let output = run_named_cli(&config_home, &runtime_dir, &["remote", "list"]);
    assert!(output.status.success());
    let table = String::from_utf8_lossy(&output.stdout);
    assert!(table.contains("gpu1"), "table missing remote: {table}");

    cleanup_test_base(&base);
}

#[test]
fn remote_list_and_reset_work_against_a_running_server() {
    let base = unique_test_dir();
    let config_home = base.join("config");
    let runtime_dir = base.join("runtime");
    let socket_path = base.join("herdr.sock");
    let spawned = spawn_herdr(&config_home, &runtime_dir, &socket_path);
    wait_for_socket(&socket_path, Duration::from_secs(10));

    // No remotes.toml: the running fleet is empty. There is no implicit
    // local runtime to fall back on - the file is the whole fleet.
    let response = run_cli_json(&socket_path, &["remote", "list", "--json"]);
    let remotes = response["result"]["remotes"].as_array().unwrap();
    assert!(remotes.is_empty(), "unexpected remotes: {remotes:?}");

    // Resetting an unknown remote is a clean API error.
    let output = run_cli(&socket_path, &["remote", "reset", "missing", "--json"]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("remote_not_found"), "stderr: {stderr}");

    // Explicit reload succeeds and returns the fleet, still empty.
    let response = run_cli_json(&socket_path, &["remote", "reload", "--json"]);
    assert!(response["result"]["remotes"]
        .as_array()
        .expect("remotes array")
        .is_empty());

    cleanup_spawned_herdr(spawned, base);
}

#[test]
fn bridge_subcommand_pumps_the_framed_protocol_to_the_api_socket() {
    let base = unique_test_dir();
    let config_home = base.join("config");
    let runtime_dir = base.join("runtime");
    let socket_path = base.join("herdr.sock");
    let spawned = spawn_herdr(&config_home, &runtime_dir, &socket_path);
    // Only the API socket: that is what the bridge pumps against, and it is
    // bound before the client socket. Arriving in that window is exactly the
    // race that used to make the bridge report "no server" and exit.
    wait_for_socket(&socket_path, Duration::from_secs(10));

    let mut bridge = Command::new(env!("CARGO_BIN_EXE_herdr"))
        .arg("bridge")
        .env("XDG_CONFIG_HOME", &config_home)
        .env("XDG_RUNTIME_DIR", &runtime_dir)
        .env("HERDR_SOCKET_PATH", &socket_path)
        .env_remove("HERDR_CLIENT_SOCKET_PATH")
        .env_remove("HERDR_ENV")
        .env_remove("HERDR_SESSION")
        .env_remove("HERDR_STARTUP_CWD")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let mut stdin = bridge.stdin.take().unwrap();
    let mut stdout = bridge.stdout.take().unwrap();

    // Speak the framed protocol end-to-end through the bridge's stdio.
    stdin.write_all(b"HRDR").unwrap();
    write_control(
        &mut stdin,
        &serde_json::json!({
            "id": "h1",
            "method": "session.hello",
            "params": {"protocol": 1, "min_protocol": 1, "capabilities": []},
        }),
    );
    let welcome = read_control(&mut stdout);
    assert_eq!(welcome["id"], "h1");
    assert_eq!(welcome["result"]["type"], "session.welcome");

    write_control(
        &mut stdin,
        &serde_json::json!({"id": "p1", "method": "ping", "params": {}}),
    );
    let pong = read_control(&mut stdout);
    assert_eq!(pong["id"], "p1");
    assert_eq!(pong["result"]["type"], "pong");

    // Closing our side winds the bridge down.
    drop(stdin);
    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = bridge.try_wait().unwrap() {
            break Some(status);
        }
        if Instant::now() >= deadline {
            break None;
        }
        thread::sleep(Duration::from_millis(25));
    };
    if status.is_none() {
        let _ = bridge.kill();
        let _ = bridge.wait();
        panic!("bridge did not exit after stdin closed");
    }

    cleanup_spawned_herdr(spawned, base);
}

#[test]
fn remote_upgrade_requires_explicit_consent_and_never_runs_by_accident() {
    let base = unique_test_dir();
    let config_home = base.join("config");
    let runtime_dir = base.join("runtime");
    let app_config = config_home.join(app_dir_name());
    fs::create_dir_all(&app_config).unwrap();
    fs::create_dir_all(&runtime_dir).unwrap();
    register_runtime_dir(&runtime_dir);
    fs::write(
        app_config.join("remotes.toml"),
        concat!(
            "[[remote]]\n",
            "name = \"gpu1\"\n",
            "target = \"can@gpu1.invalid\"\n",
        ),
    )
    .unwrap();

    // The subcommand is documented in `herdr remote help`.
    let output = run_named_cli(&config_home, &runtime_dir, &["remote", "help"]);
    let help = String::from_utf8_lossy(&output.stderr);
    assert!(help.contains("herdr remote upgrade"), "help: {help}");

    // Missing target: usage, no ssh.
    let output = run_named_cli(&config_home, &runtime_dir, &["remote", "upgrade"]);
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("usage: herdr remote upgrade"), "{stderr}");

    // A name plus --all is contradictory.
    let output = run_named_cli(
        &config_home,
        &runtime_dir,
        &["remote", "upgrade", "--all", "gpu1"],
    );
    assert_eq!(output.status.code(), Some(2));

    // Non-interactive stdin without --yes refuses before touching the host.
    let output = run_named_cli(&config_home, &runtime_dir, &["remote", "upgrade", "gpu1"]);
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("requires --yes"), "{stderr}");

    cleanup_test_base(&base);
}

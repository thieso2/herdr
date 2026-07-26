//! Per-remote connection state machine.
//!
//! Pure data with an injected clock: every transition takes `now` (and, where
//! backoff is scheduled, a jitter sample) so the machine is fully unit-testable
//! without SSH, sockets, or sleeping. The machine is transport-agnostic — the
//! fleet manager drives it from real bridge IO, tests drive it directly.

use std::time::{Duration, Instant};

use crate::protocol::framed::HelloRemedy;

/// Default first-retry delay.
pub const BACKOFF_BASE: Duration = Duration::from_secs(1);
/// Default backoff ceiling (~30s per the fleet reconnect contract).
pub const BACKOFF_CAP: Duration = Duration::from_secs(30);
/// Default minimum connected uptime for a drop to restart the backoff ladder.
pub const BACKOFF_STABLE_UPTIME: Duration = Duration::from_secs(30);

/// Backoff growth parameters, injectable so tests run in milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackoffTuning {
    pub base: Duration,
    pub cap: Duration,
    /// Minimum connected uptime for a drop to count as a fresh outage (fast
    /// first retry). A session that dies sooner is a flap and continues the
    /// previous backoff ladder instead of resetting it, so a remote in a
    /// crash loop cannot pin retries at the base delay.
    pub stable_uptime: Duration,
}

impl Default for BackoffTuning {
    fn default() -> Self {
        Self {
            base: BACKOFF_BASE,
            cap: BACKOFF_CAP,
            stable_uptime: BACKOFF_STABLE_UPTIME,
        }
    }
}

/// Jittered exponential backoff delay for retry `attempt` (1-based).
///
/// The raw delay doubles per attempt from `base`, is scaled by a jitter
/// factor in `[0.5, 1.5]` derived from `jitter` in `[0, 1]`, and never
/// exceeds `cap`.
pub fn backoff_delay(attempt: u32, tuning: BackoffTuning, jitter: f64) -> Duration {
    let exponent = attempt.saturating_sub(1).min(16);
    let raw = tuning
        .base
        .saturating_mul(2u32.saturating_pow(exponent))
        .min(tuning.cap);
    let factor = 0.5 + jitter.clamp(0.0, 1.0);
    raw.mul_f64(factor).min(tuning.cap)
}

/// The exact fix for an out-of-window peer, as a command the user can run.
/// The remedy always comes from the peer's own rejection, never a guess: an
/// older *server* must not be reported as "upgrade the client".
pub fn incompatible_fix_command(name: &str, is_local: bool, remedy: HelloRemedy) -> String {
    match remedy {
        // This side is older than the peer's window: upgrade here.
        HelloRemedy::UpgradeClient => "herdr update".to_string(),
        // The peer is older. The local runtime ships with this install, so it
        // upgrades with `herdr update`; a fleet remote is rolled forward
        // explicitly, by name, and never automatically.
        HelloRemedy::UpgradeServer if is_local => "herdr update".to_string(),
        HelloRemedy::UpgradeServer => format!("herdr remote upgrade {name}"),
    }
}

/// The greyed-out remote's status line: which machine is out of window, why,
/// and the exact command that fixes it. Shared by the fleet manager and the
/// pure client so both name the machine identically.
pub fn incompatible_status_line(
    name: &str,
    is_local: bool,
    remedy: HelloRemedy,
    detail: &str,
) -> String {
    let subject = if is_local {
        "the local herdr server".to_string()
    } else {
        format!("remote {name}")
    };
    let fix = incompatible_fix_command(name, is_local, remedy);
    let detail = detail.trim();
    if detail.is_empty() {
        format!("{subject} is outside the supported protocol window; run `{fix}`")
    } else {
        format!("{subject} is outside the supported protocol window: {detail}; run `{fix}`")
    }
}

/// The dimmed remote's status line: what is wrong and the exact command that
/// fixes it, in the same shape as [`incompatible_status_line`]. Shared by the
/// fleet manager and the pure client so both say the same thing.
pub fn stopped_status_line(name: &str) -> String {
    format!(
        "no {brand} server running on {name}; run `{brand} remote start {name}`",
        brand = crate::identity::BRAND
    )
}

/// Connection lifecycle of one remote. Plain data; renders directly into the
/// `remote list` state column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionState {
    /// A local runtime: reached over this machine's API socket, so this
    /// machine drives no ssh bridge for it and never connects, retries, or
    /// backs off. Terminal and inert, like [`Self::Disabled`].
    Local,
    /// Disabled in config; no connection is attempted.
    Disabled,
    /// A connect-plus-handshake attempt is in flight.
    Connecting { attempt: u32 },
    /// Live negotiated session.
    Connected { since: Instant },
    /// Offline/stale: not connected; the next retry is due at `retry_at`.
    /// Reconnect attempts continue indefinitely until the remote is removed
    /// or disabled.
    Offline {
        attempt: u32,
        retry_at: Instant,
        last_error: String,
    },
    /// Reachable, herdr installed, but no server running there. Terminal
    /// until asked: retrying cannot help, because only an explicit start
    /// writes a new daemon to that host. `remote start` (or the TUI's
    /// confirmation) leaves this state; a manual reset re-probes.
    Stopped { message: String },
    /// The protocol version windows do not overlap. Terminal for this
    /// configuration: no automatic retries until a side is upgraded; a manual
    /// reset or config change forces another attempt.
    Incompatible { message: String },
}

/// State machine driving one remote's connection lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionMachine {
    state: ConnectionState,
    tuning: BackoffTuning,
    /// Attempt number of the connect cycle that produced the current or most
    /// recent `Connected` session. Carried into `Offline` when that session
    /// drops before `stable_uptime`, so flapping remotes keep escalating.
    session_attempt: u32,
    /// Whether this is a local runtime. Kept so an enable flip restores
    /// `Local` instead of scheduling a connect that would never happen.
    local: bool,
}

impl ConnectionMachine {
    pub fn new(enabled: bool, now: Instant, tuning: BackoffTuning) -> Self {
        let state = if enabled {
            ConnectionState::Offline {
                attempt: 0,
                retry_at: now,
                last_error: String::new(),
            }
        } else {
            ConnectionState::Disabled
        };
        Self {
            state,
            tuning,
            session_attempt: 0,
            local: false,
        }
    }

    /// A machine for a local runtime. It never drives a connection: the
    /// client owns the API-socket link, so every transition below is inert
    /// and the state stays `Local` (or `Disabled`) for its whole life.
    pub fn new_local(enabled: bool, tuning: BackoffTuning) -> Self {
        Self {
            state: if enabled {
                ConnectionState::Local
            } else {
                ConnectionState::Disabled
            },
            tuning,
            session_attempt: 0,
            local: true,
        }
    }

    pub fn state(&self) -> &ConnectionState {
        &self.state
    }

    /// Whether the manager should start a connection attempt now.
    pub fn ready_to_connect(&self, now: Instant) -> bool {
        match &self.state {
            ConnectionState::Offline { retry_at, .. } => now >= *retry_at,
            _ => false,
        }
    }

    /// The next instant at which `ready_to_connect` may flip to true.
    pub fn next_deadline(&self) -> Option<Instant> {
        match &self.state {
            ConnectionState::Offline { retry_at, .. } => Some(*retry_at),
            _ => None,
        }
    }

    /// A connection attempt (spawn + handshake) has started.
    pub fn on_connect_started(&mut self) {
        let attempt = match &self.state {
            ConnectionState::Offline { attempt, .. } => attempt.saturating_add(1),
            ConnectionState::Connecting { attempt } => *attempt,
            ConnectionState::Connected { .. }
            | ConnectionState::Local
            | ConnectionState::Disabled
            | ConnectionState::Stopped { .. }
            | ConnectionState::Incompatible { .. } => return,
        };
        self.state = ConnectionState::Connecting { attempt };
    }

    /// Handshake completed; the session is live.
    pub fn on_connected(&mut self, now: Instant) {
        match &self.state {
            ConnectionState::Local | ConnectionState::Disabled => return,
            ConnectionState::Connecting { attempt } => self.session_attempt = *attempt,
            _ => {}
        }
        self.state = ConnectionState::Connected { since: now };
    }

    /// The far side has herdr but no running server. Terminal until an
    /// explicit start: automatic retries cannot bring a daemon up, and
    /// hammering a reachable host to re-learn the same fact is pure noise.
    pub fn on_stopped(&mut self, message: impl Into<String>) {
        if matches!(
            self.state,
            ConnectionState::Local | ConnectionState::Disabled
        ) {
            return;
        }
        self.state = ConnectionState::Stopped {
            message: message.into(),
        };
    }

    /// The handshake failed because the protocol version windows do not
    /// overlap. Terminal until a manual reset or a config change: retrying
    /// cannot succeed while either side stays on its current version.
    pub fn on_incompatible(&mut self, message: impl Into<String>) {
        if matches!(
            self.state,
            ConnectionState::Local | ConnectionState::Disabled
        ) {
            return;
        }
        self.state = ConnectionState::Incompatible {
            message: message.into(),
        };
    }

    /// The attempt failed or a live session dropped (transport error,
    /// handshake rejection, or heartbeat timeout). Schedules the next retry
    /// with jittered exponential backoff; retries never stop on their own.
    pub fn on_disconnected(&mut self, now: Instant, error: String, jitter: f64) {
        let attempt = match &self.state {
            ConnectionState::Connecting { attempt } => *attempt,
            // A drop out of a session that stayed up past `stable_uptime`
            // starts a fresh backoff ladder (fast retry). A shorter-lived
            // session is a flap and continues the ladder that produced it.
            ConnectionState::Connected { since } => {
                if now.saturating_duration_since(*since) >= self.tuning.stable_uptime {
                    1
                } else {
                    self.session_attempt.max(1)
                }
            }
            ConnectionState::Offline { attempt, .. } => attempt.saturating_add(1),
            ConnectionState::Local
            | ConnectionState::Disabled
            | ConnectionState::Stopped { .. }
            | ConnectionState::Incompatible { .. } => return,
        };
        self.state = ConnectionState::Offline {
            attempt,
            retry_at: now + backoff_delay(attempt, self.tuning, jitter),
            last_error: error,
        };
    }

    /// Manual reset: clear the backoff and reconnect immediately. From a live
    /// or in-flight connection this forces a teardown-and-reconnect.
    pub fn on_reset(&mut self, now: Instant) {
        if matches!(
            self.state,
            ConnectionState::Local | ConnectionState::Disabled
        ) {
            return;
        }
        self.session_attempt = 0;
        self.state = ConnectionState::Offline {
            attempt: 0,
            retry_at: now,
            last_error: String::new(),
        };
    }

    /// Applies a config-level enable/disable flip.
    pub fn set_enabled(&mut self, enabled: bool, now: Instant) {
        match (enabled, &self.state) {
            (false, _) => self.state = ConnectionState::Disabled,
            (true, ConnectionState::Disabled) if self.local => {
                self.state = ConnectionState::Local;
            }
            (true, ConnectionState::Disabled) => {
                self.session_attempt = 0;
                self.state = ConnectionState::Offline {
                    attempt: 0,
                    retry_at: now,
                    last_error: String::new(),
                };
            }
            // Incompatible stays terminal until a manual reset; the enable
            // flag alone changes nothing about the version windows.
            (true, _) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: BackoffTuning = BackoffTuning {
        base: Duration::from_secs(1),
        cap: Duration::from_secs(30),
        stable_uptime: Duration::from_secs(30),
    };

    fn machine(now: Instant) -> ConnectionMachine {
        ConnectionMachine::new(true, now, T)
    }

    #[test]
    fn backoff_doubles_and_caps_at_thirty_seconds() {
        // jitter 0.5 → factor 1.0 (deterministic midpoint).
        assert_eq!(backoff_delay(1, T, 0.5), Duration::from_secs(1));
        assert_eq!(backoff_delay(2, T, 0.5), Duration::from_secs(2));
        assert_eq!(backoff_delay(3, T, 0.5), Duration::from_secs(4));
        assert_eq!(backoff_delay(5, T, 0.5), Duration::from_secs(16));
        assert_eq!(backoff_delay(6, T, 0.5), Duration::from_secs(30));
        assert_eq!(backoff_delay(60, T, 0.5), Duration::from_secs(30));
    }

    #[test]
    fn backoff_jitter_scales_within_half_to_full_cap() {
        assert_eq!(backoff_delay(2, T, 0.0), Duration::from_secs(1));
        assert_eq!(backoff_delay(2, T, 1.0), Duration::from_secs(3));
        // Out-of-range jitter samples are clamped.
        assert_eq!(backoff_delay(2, T, 7.5), Duration::from_secs(3));
        // The jittered delay never exceeds the cap.
        assert_eq!(backoff_delay(30, T, 1.0), Duration::from_secs(30));
    }

    #[test]
    fn new_enabled_machine_is_immediately_ready_to_connect() {
        let now = Instant::now();
        let machine = machine(now);
        assert!(machine.ready_to_connect(now));
        assert_eq!(machine.next_deadline(), Some(now));
    }

    #[test]
    fn a_local_machine_never_connects_and_survives_an_enable_flip() {
        let now = Instant::now();
        let mut machine = ConnectionMachine::new_local(true, T);
        assert_eq!(machine.state(), &ConnectionState::Local);

        // Inert in every direction: the client owns the API-socket link, so
        // this machine must never schedule a connect, a retry, or a backoff.
        assert!(!machine.ready_to_connect(now + Duration::from_secs(3600)));
        assert_eq!(machine.next_deadline(), None);
        machine.on_connect_started();
        machine.on_connected(now);
        machine.on_disconnected(now, "noise".into(), 0.5);
        machine.on_incompatible("noise");
        machine.on_reset(now);
        assert_eq!(machine.state(), &ConnectionState::Local);

        // Disabling is a config-level off; re-enabling restores `Local`
        // rather than scheduling a connect that would never happen.
        machine.set_enabled(false, now);
        assert_eq!(machine.state(), &ConnectionState::Disabled);
        machine.set_enabled(true, now);
        assert_eq!(machine.state(), &ConnectionState::Local);
        assert!(!machine.ready_to_connect(now));

        // A local entry that starts disabled starts disabled.
        assert_eq!(
            ConnectionMachine::new_local(false, T).state(),
            &ConnectionState::Disabled
        );
    }

    #[test]
    fn disabled_machine_never_connects() {
        let now = Instant::now();
        let mut machine = ConnectionMachine::new(false, now, T);
        assert_eq!(machine.state(), &ConnectionState::Disabled);
        assert!(!machine.ready_to_connect(now + Duration::from_secs(3600)));
        machine.on_connect_started();
        machine.on_connected(now);
        machine.on_disconnected(now, "x".into(), 0.5);
        machine.on_reset(now);
        assert_eq!(machine.state(), &ConnectionState::Disabled);
    }

    #[test]
    fn failed_attempts_grow_backoff_indefinitely() {
        let now = Instant::now();
        let mut machine = machine(now);

        machine.on_connect_started();
        assert_eq!(machine.state(), &ConnectionState::Connecting { attempt: 1 });
        machine.on_disconnected(now, "connect failed".into(), 0.5);
        assert_eq!(
            machine.state(),
            &ConnectionState::Offline {
                attempt: 1,
                retry_at: now + Duration::from_secs(1),
                last_error: "connect failed".into(),
            }
        );
        assert!(!machine.ready_to_connect(now));
        assert!(machine.ready_to_connect(now + Duration::from_secs(1)));

        machine.on_connect_started();
        assert_eq!(machine.state(), &ConnectionState::Connecting { attempt: 2 });
        machine.on_disconnected(now, "still failing".into(), 0.5);
        assert_eq!(
            machine.next_deadline(),
            Some(now + Duration::from_secs(2)),
            "second failure doubles the delay"
        );
    }

    #[test]
    fn stable_session_drop_restarts_the_backoff_ladder() {
        let now = Instant::now();
        let mut machine = machine(now);
        // Escalate the ladder first so the reset is observable.
        for _ in 0..4 {
            machine.on_connect_started();
            machine.on_disconnected(now, "down".into(), 0.5);
        }
        machine.on_connect_started();
        machine.on_connected(now);
        assert!(matches!(machine.state(), ConnectionState::Connected { .. }));
        assert_eq!(machine.next_deadline(), None);

        // 120s of uptime exceeds `stable_uptime`: the drop retries quickly.
        let later = now + Duration::from_secs(120);
        machine.on_disconnected(later, "heartbeat timed out".into(), 0.5);
        assert_eq!(
            machine.state(),
            &ConnectionState::Offline {
                attempt: 1,
                retry_at: later + Duration::from_secs(1),
                last_error: "heartbeat timed out".into(),
            }
        );
    }

    #[test]
    fn short_lived_sessions_continue_the_backoff_ladder() {
        let now = Instant::now();
        let mut machine = machine(now);
        // Flapping remote: every connect handshakes, then drops before
        // `stable_uptime`. Backoff must keep escalating anyway.
        for cycle in 1u32..=6 {
            machine.on_connect_started();
            assert_eq!(
                machine.state(),
                &ConnectionState::Connecting { attempt: cycle }
            );
            machine.on_connected(now);
            let dropped_at = now + Duration::from_secs(2);
            machine.on_disconnected(dropped_at, "dropped".into(), 0.5);
            assert_eq!(
                machine.next_deadline(),
                Some(dropped_at + backoff_delay(cycle, T, 0.5)),
                "flap cycle {cycle} must escalate backoff"
            );
        }
        // The ladder is capped, not unbounded.
        assert_eq!(backoff_delay(6, T, 0.5), Duration::from_secs(30));
    }

    #[test]
    fn reset_clears_backoff_and_forces_immediate_retry() {
        let now = Instant::now();
        let mut machine = machine(now);
        for _ in 0..6 {
            machine.on_connect_started();
            machine.on_disconnected(now, "down".into(), 0.5);
        }
        assert!(!machine.ready_to_connect(now + Duration::from_secs(5)));

        let reset_at = now + Duration::from_secs(5);
        machine.on_reset(reset_at);
        assert!(machine.ready_to_connect(reset_at));

        // The next failure after a reset starts the backoff ladder over.
        machine.on_connect_started();
        machine.on_disconnected(reset_at, "down".into(), 0.5);
        assert_eq!(
            machine.next_deadline(),
            Some(reset_at + Duration::from_secs(1))
        );
    }

    #[test]
    fn incompatible_status_line_names_the_machine_and_the_exact_fix() {
        // An older *remote* is rolled forward explicitly, by name.
        let line = incompatible_status_line(
            "gpu-1",
            false,
            HelloRemedy::UpgradeServer,
            "client minimum protocol 2 is newer than this server's protocol 1",
        );
        assert!(line.contains("remote gpu-1"), "{line}");
        assert!(line.contains("herdr remote upgrade gpu-1"), "{line}");

        // An older *client* upgrades here, whichever peer reported it.
        let line = incompatible_status_line("gpu-1", false, HelloRemedy::UpgradeClient, "");
        assert!(line.contains("herdr update"), "{line}");
        assert!(!line.contains("remote upgrade"), "{line}");

        // The local runtime is this install: it upgrades with the client.
        let line = incompatible_status_line("local", true, HelloRemedy::UpgradeServer, "detail");
        assert!(line.contains("the local herdr server"), "{line}");
        assert!(line.contains("herdr update"), "{line}");
        assert!(!line.contains("remote upgrade"), "{line}");
    }

    #[test]
    fn stopped_is_terminal_until_started_or_reset() {
        let now = Instant::now();
        let mut machine = machine(now);
        machine.on_connect_started();
        machine.on_stopped("no server running on gpu-1");
        assert_eq!(
            machine.state(),
            &ConnectionState::Stopped {
                message: "no server running on gpu-1".into()
            }
        );

        // Retrying cannot start a daemon, so the ladder stops entirely: no
        // deadline, never ready, and every automatic transition is inert.
        assert!(!machine.ready_to_connect(now + Duration::from_secs(3600)));
        assert_eq!(machine.next_deadline(), None);
        machine.on_connect_started();
        machine.on_disconnected(now, "noise".into(), 0.5);
        assert!(matches!(machine.state(), ConnectionState::Stopped { .. }));

        // A reset re-probes: the user may have started it out of band.
        machine.on_reset(now);
        assert!(machine.ready_to_connect(now));

        // Disabled and local runtimes never become stopped - neither has a
        // bridge that could report it.
        let mut disabled = ConnectionMachine::new(false, now, T);
        disabled.on_stopped("x");
        assert_eq!(disabled.state(), &ConnectionState::Disabled);
        let mut local = ConnectionMachine::new_local(true, T);
        local.on_stopped("x");
        assert_eq!(local.state(), &ConnectionState::Local);
    }

    #[test]
    fn incompatible_is_terminal_until_reset() {
        let now = Instant::now();
        let mut machine = machine(now);
        machine.on_connect_started();
        machine.on_incompatible("protocol windows do not overlap");
        assert_eq!(
            machine.state(),
            &ConnectionState::Incompatible {
                message: "protocol windows do not overlap".into()
            }
        );
        // No automatic retries: never ready, no deadline, transitions inert.
        assert!(!machine.ready_to_connect(now + Duration::from_secs(3600)));
        assert_eq!(machine.next_deadline(), None);
        machine.on_connect_started();
        machine.on_disconnected(now, "noise".into(), 0.5);
        assert!(matches!(
            machine.state(),
            ConnectionState::Incompatible { .. }
        ));

        // A manual reset is the escape hatch.
        machine.on_reset(now);
        assert!(machine.ready_to_connect(now));

        // Disabled machines never become incompatible.
        let mut disabled = ConnectionMachine::new(false, now, T);
        disabled.on_incompatible("x");
        assert_eq!(disabled.state(), &ConnectionState::Disabled);
    }

    #[test]
    fn enable_flip_moves_between_disabled_and_offline() {
        let now = Instant::now();
        let mut machine = machine(now);
        machine.on_connect_started();
        machine.on_connected(now);

        machine.set_enabled(false, now);
        assert_eq!(machine.state(), &ConnectionState::Disabled);

        machine.set_enabled(true, now);
        assert!(machine.ready_to_connect(now));
    }
}

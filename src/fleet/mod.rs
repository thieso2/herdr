//! Fleet of named remote herdr runtimes.
//!
//! A fleet is configured in the app-owned `remotes.toml` (see [`config`]) and
//! held live by the [`manager::FleetManager`]: one persistent SSH stdio bridge
//! child per enabled remote, speaking the framed protocol directly over the
//! child's stdio with no local socket hop. Connections self-heal with jittered
//! exponential backoff and framed-ping heartbeats; offline remotes stay
//! visible until explicitly removed from the config.
//!
//! The far side of each bridge is the `herdr bridge` subcommand (see
//! [`bridge`]), which connects the remote API socket and pumps stdio.

pub mod bridge;
pub mod bridge_child;
pub mod client;
pub mod config;
pub mod connection;
pub mod manager;
pub mod status;

pub use manager::FleetManager;

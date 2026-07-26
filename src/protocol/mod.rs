//! Shared wire protocol and presentation encoding code.

pub(crate) mod framed;
pub(crate) mod render_ansi;
mod wire;

pub use wire::*;

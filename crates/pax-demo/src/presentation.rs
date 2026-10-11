//! Minimal server-rendered HTML with bounded, coalesced JSON updates.
pub mod book;
pub mod latency;
mod publication;
mod server;

pub use server::{serve, serve_listener, serve_with_settings};

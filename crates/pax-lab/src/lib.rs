#![doc = include_str!("../README.md")]

pub mod build_info;
pub mod measurement;
/// Contextual diagnostics at executable and experiment boundaries.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

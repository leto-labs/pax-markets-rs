//! Latency page contract: native SVG charts with separate local clock domains.
//!
//! The assets never infer Pax origination time, sum hop RTTs, or label a frame
//! callback as physical paint. Individual traces and aggregate exclusive stage
//! durations are separate views; browser measurement stays local to that tab.

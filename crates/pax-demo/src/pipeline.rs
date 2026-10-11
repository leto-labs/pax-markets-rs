//! Shared deterministic input path for live observation, recording, and replay.
mod process;
pub mod snapshot;
mod stats;
pub mod view;

use crate::measurement::receive::ReceiveTrace;
use crate::{
    capture::CaptureError,
    measurement::trace::{TraceId, Traces},
};
use pax_book::{
    session::Session,
    state::{Book, Status},
};
use pax_types::{
    market::{ExchangeInfo, MarketUnits},
    protocol::event::Side,
    units::MarketId,
};
use stats::Weighted;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::Instant,
};

struct MarketState {
    units: Arc<MarketUnits>,
    weighted: Weighted,
    weighted_at_ns: u64,
    last_update_ns: Option<u64>,
    last_trace_id: Option<TraceId>,
}
impl MarketState {
    // Charge the previous state for its whole unchanged interval before any
    // mutation. The global input clock guarantees this boundary is monotonic.
    fn close_interval(&mut self, book: &Book, offset_ns: u64) {
        self.weighted.advance(book, offset_ns - self.weighted_at_ns);
        self.weighted_at_ns = offset_ns;
    }
}
/// Single-owner reconstruction state; no networking, file access, or async locks.
pub struct Pipeline {
    markets: BTreeMap<MarketId, MarketState>,
    session: Session,
    // Ordered transport boundaries can lag a rejected input in a queued feed.
    // Session continuity is already invalid while this transport is closing.
    connected: bool,
    offset_ns: u64,
    messages: u64,
    applied: u64,
    rejected: u64,
    last_error: Option<String>,
    last_rejection: Option<view::RejectedInput>,
    traces: Traces,
    probes: crate::measurement::policy::Probes,
    replay_origin: Instant,
    receive_traces: VecDeque<ReceiveTrace>,
    receive_capacity_drops: u64,
    receive_age_expirations: u64,
    last_callback: Option<Instant>,
}
/// Application outcome tells a live supervisor when continuity must be recovered.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProcessResult {
    /// A new connection and snapshot are required.
    pub recover: bool,
    /// A selected book committed a snapshot or a new sequence.
    pub changed: bool,
}
/// Benchmark hook at the existing decode/book completion boundary, before
/// telemetry bookkeeping. The observer supplies its own clock and storage.
#[derive(Clone, Copy, Debug)]
pub struct CommitPoint {
    /// Canonical message identity, independent of optional sampling.
    pub id: TraceId,
    /// Original complete-message availability in the process monotonic domain.
    pub received: Instant,
    /// Committed snapshot/event, buffered, duplicate, control or rejected input.
    pub population: &'static str,
    /// Event/snapshot sequence, absent for transport controls or malformed input.
    pub sequence: Option<pax_types::units::Sequence>,
}
impl Pipeline {
    /// Resolve exact units and create bounded per-market books and telemetry.
    pub fn new(
        metadata: &ExchangeInfo,
        markets: &[MarketId],
        sample_every: u64,
    ) -> Result<Self, CaptureError> {
        let session = Session::new(markets, 1024, 100_000)
            .map_err(|_| CaptureError::Invalid("selected market or book bounds"))?;
        let mut selected = BTreeMap::new();
        for &id in markets {
            let state = MarketState {
                units: Arc::new(metadata.resolve(id)?),
                weighted: Weighted::default(),
                weighted_at_ns: 0,
                last_update_ns: None,
                last_trace_id: None,
            };
            if selected.insert(id, state).is_some() {
                return Err(CaptureError::Invalid("duplicate market"));
            }
        }
        Ok(Self {
            markets: selected,
            session,
            connected: false,
            offset_ns: 0,
            messages: 0,
            applied: 0,
            rejected: 0,
            last_error: None,
            last_rejection: None,
            traces: if cfg!(any(
                feature = "processing-traces",
                feature = "delivery-traces"
            )) {
                Traces::new_prefaulted(2048, 10_000_000_000, sample_every)
            } else {
                Traces::new(2048, 10_000_000_000, 0)
            }
            .map_err(CaptureError::Invalid)?,
            probes: crate::measurement::policy::Probes::new(
                crate::measurement::policy::ProcessingPolicy::every(sample_every),
            )
            .map_err(CaptureError::Invalid)?,
            replay_origin: Instant::now(),
            last_callback: None,
            receive_traces: VecDeque::new(),
            receive_capacity_drops: 0,
            receive_age_expirations: 0,
        })
    }
    /// Atomically change independent processing sample rates between inputs.
    /// Invalid updates preserve every domain's existing policy and sample phase.
    pub fn configure_measurements(
        &mut self,
        policy: crate::measurement::policy::ProcessingPolicy,
    ) -> Result<crate::measurement::policy::ProcessingApplied, CaptureError> {
        let revision = self
            .probes
            .configure(policy)
            .map_err(CaptureError::Invalid)?;
        self.last_callback = None;
        Ok(crate::measurement::policy::ProcessingApplied {
            revision,
            after_message: self.messages,
        })
    }
    /// Current effective processing policy, independent of export cadence.
    pub fn measurement_policy(&self) -> crate::measurement::policy::ProcessingPolicy {
        self.probes.policy
    }
    fn advance(&mut self, offset_ns: u64) -> Result<(), CaptureError> {
        if offset_ns < self.offset_ns {
            return Err(CaptureError::Invalid("backwards input offset"));
        }
        self.offset_ns = offset_ns;
        Ok(())
    }
    /// Close the final interval at a deliberate capture/replay stop boundary.
    pub fn finish(&mut self, offset_ns: u64) -> Result<(), CaptureError> {
        self.advance(offset_ns)?;
        for book in self.session.books() {
            self.markets
                .get_mut(&book.market())
                .unwrap()
                .close_interval(book, offset_ns);
        }
        Ok(())
    }
    /// Digest of exact visible state and accepted/rejected input counts. Local
    /// timing samples and queue delays are deliberately excluded.
    pub fn digest(&self) -> String {
        let mut hash = blake3::Hasher::new();
        for value in [
            self.session.generation(),
            self.offset_ns,
            self.messages,
            self.applied,
            self.rejected,
        ] {
            hash.update(&value.to_le_bytes());
        }
        hash.update(&[u8::from(self.connected)]);
        for book in self.session.books() {
            hash.update(&book.market().0.to_le_bytes());
            hash.update(
                &book
                    .sequence()
                    .map(|s| s.0)
                    .unwrap_or_default()
                    .to_le_bytes(),
            );
            hash.update(&[match book.status() {
                Status::Live => 1,
                Status::AwaitingSnapshot => 0,
                Status::Invalid(_) => 2,
            }]);
            for side in [Side::Ask, Side::Bid] {
                let levels = book.levels(side).unwrap_or_default();
                hash.update(&(levels.len() as u64).to_le_bytes());
                for level in levels {
                    hash.update(&level.ticks.0.to_le_bytes());
                    hash.update(&level.qty.0.to_le_bytes());
                }
            }
        }
        hash.finalize().to_hex().to_string()
    }
}
/// Saturating conversion of a local duration into telemetry nanoseconds.
pub fn ns(duration: std::time::Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

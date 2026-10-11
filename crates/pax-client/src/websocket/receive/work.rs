//! Counts and qualified aggregate work for independently sampled operations.
use super::policy::ReceivePolicy;
use serde::Serialize;

/// A population of operation attempts, independent of message emission sampling.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct ProbeWork {
    /// Eligible operation attempts, including those not selected.
    pub eligible: u64,
    /// Attempts with a captured interval.
    pub samples: u64,
    /// Sum of inclusive wall time for selected attempts only.
    pub sampled_ns: u64,
    /// Exclusive total only when every attempt and required nested span is known.
    pub exclusive_ns: Option<u64>,
}
impl Default for ProbeWork {
    fn default() -> Self {
        Self {
            eligible: 0,
            samples: 0,
            sampled_ns: 0,
            exclusive_ns: Some(0),
        }
    }
}
impl ProbeWork {
    pub(super) fn record(&mut self, inclusive: Option<u64>, exclusive: Option<u64>) {
        self.eligible = self.eligible.saturating_add(1);
        if let Some(value) = inclusive {
            self.samples = self.samples.saturating_add(1);
            self.sampled_ns = self.sampled_ns.saturating_add(value);
            self.exclusive_ns = self
                .exclusive_ns
                .zip(exclusive)
                .map(|(a, b)| a.saturating_add(b));
        } else {
            self.exclusive_ns = None;
        }
    }
    pub(super) fn merge(&mut self, other: Self) {
        self.eligible = self.eligible.saturating_add(other.eligible);
        self.samples = self.samples.saturating_add(other.samples);
        self.sampled_ns = self.sampled_ns.saturating_add(other.sampled_ns);
        self.exclusive_ns = self
            .exclusive_ns
            .zip(other.exclusive_ns)
            .map(|(a, b)| a.saturating_add(b));
    }
    pub(super) fn inclusive_since(self, before: Self) -> Option<u64> {
        (self.eligible.checked_sub(before.eligible)? == self.samples.checked_sub(before.samples)?)
            .then(|| self.sampled_ns.saturating_sub(before.sampled_ns))
    }
}
/// One protocol delivery's operation populations. Shared batches are not charged
/// again to buffered deliveries. Missing/partially observed totals serialize null.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct ReceiveWork {
    /// Complete exclusive socket total, when observed.
    pub socket_ns: Option<u64>,
    /// Complete exclusive TLS total, when nested socket work is observed.
    pub tls_ns: Option<u64>,
    /// Complete exclusive WebSocket total, when nested stream work is observed.
    pub websocket_ns: Option<u64>,
    /// Complete observed wait total, never scheduler CPU attribution.
    pub pending_ns: Option<u64>,
    /// Socket attempt population and sampled inclusive work.
    pub socket: ProbeWork,
    /// Linux ancillary-read internals. Empty when using the portable socket reader.
    pub read_detail: ReadDetail,
    /// TLS attempt population and sampled inclusive work.
    pub tls: ProbeWork,
    /// WebSocket attempt population and sampled inclusive work.
    pub websocket: ProbeWork,
    /// Suspended-poll population and sampled waiting time.
    pub pending: ProbeWork,
    /// WebSocket polls since the preceding protocol delivery.
    pub polls: u64,
    /// Successful nonempty socket reads since that delivery.
    pub reads: u64,
}
impl ReceiveWork {
    pub(super) fn qualify(mut self, policy: ReceivePolicy) -> Self {
        self.socket_ns = policy
            .socket
            .enabled()
            .then_some(self.socket.exclusive_ns)
            .flatten();
        self.tls_ns = policy
            .tls
            .enabled()
            .then_some(self.tls.exclusive_ns)
            .flatten();
        self.websocket_ns = policy
            .websocket
            .enabled()
            .then_some(self.websocket.exclusive_ns)
            .flatten();
        self.pending_ns = policy
            .pending
            .enabled()
            .then_some(self.pending.exclusive_ns)
            .flatten();
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn independently_missing_nested_work_is_not_an_exclusive_duration() {
        let mut nested = ProbeWork::default();
        let before = nested;
        nested.record(Some(7), Some(7));
        nested.record(None, None);
        assert_eq!(nested.inclusive_since(before), None);
        assert_eq!(nested.sampled_ns, 7);
        assert_eq!((nested.eligible, nested.samples), (2, 1));
        let mut full = ReceiveWork::default();
        full.socket.merge(nested);
        assert_eq!(full.qualify(ReceivePolicy::DETAILED).socket_ns, None);
        assert_eq!(
            ReceiveWork::default().qualify(ReceivePolicy::OFF).tls_ns,
            None
        );
    }
}

/// Disjoint internals within ancillary socket polls. All durations are wall time;
/// readiness excludes suspension between polls. Counts cover custom-path attempts.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct ReadDetail {
    /// Readiness check work, including Pending results.
    pub readiness: ProbeWork,
    /// Ensuring syscall destination memory is initialized.
    pub preparation: ProbeWork,
    /// recvmsg execution including errors; excludes readiness and initialization.
    pub syscall: ProbeWork,
    /// Ancillary decoding and clock qualification, excluding syscall and copy.
    pub metadata: ProbeWork,
    /// Copy/advance into the caller's ReadBuf.
    pub delivery: ProbeWork,
    /// Readiness checks returning Pending; never interpreted as scheduler CPU time.
    pub pending: u64,
    /// Actual recvmsg attempts returning WouldBlock.
    pub would_block: u64,
    /// Successful nonempty syscall results.
    pub reads: u64,
    /// Total bytes returned by recvmsg.
    pub bytes: u64,
    /// Bytes newly initialized for syscall destinations, before the syscall.
    pub initialized_bytes: u64,
    /// Bytes copied from an optional initialized scratch buffer.
    pub copied_bytes: u64,
}
impl ReadDetail {
    pub(super) fn merge(&mut self, other: Self) {
        self.readiness.merge(other.readiness);
        self.preparation.merge(other.preparation);
        self.syscall.merge(other.syscall);
        self.metadata.merge(other.metadata);
        self.delivery.merge(other.delivery);
        self.pending = self.pending.saturating_add(other.pending);
        self.would_block = self.would_block.saturating_add(other.would_block);
        self.reads = self.reads.saturating_add(other.reads);
        self.bytes = self.bytes.saturating_add(other.bytes);
        self.initialized_bytes = self
            .initialized_bytes
            .saturating_add(other.initialized_bytes);
        self.copied_bytes = self.copied_bytes.saturating_add(other.copied_bytes);
    }
}

/// Cumulative owner-local selection counts, independent of metadata emission loss.
/// Operation domains reset only when their own policy changes; socket/TLS counters
/// include connection setup. Emission counts cover the whole connection. None means
/// that the adapter or collection group is absent, rather than zero measured work.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct ReceiveSelection {
    /// Nonempty timestamp-read selection.
    pub timestamps: Option<crate::measurement::SelectionCounts>,
    /// Socket poll selection.
    pub socket: Option<crate::measurement::SelectionCounts>,
    /// Ancillary read-poll detail selection.
    pub read_detail: Option<crate::measurement::SelectionCounts>,
    /// TLS/plaintext adapter poll selection.
    pub tls: Option<crate::measurement::SelectionCounts>,
    /// WebSocket poll selection.
    pub websocket: Option<crate::measurement::SelectionCounts>,
    /// Suspended poll selection.
    pub pending: Option<crate::measurement::SelectionCounts>,
    /// Completed application-frame selection, before metadata emission filtering.
    pub association: Option<crate::measurement::SelectionCounts>,
    /// Complete application messages considered and emitted since connection setup.
    pub emission: crate::measurement::SelectionCounts,
}

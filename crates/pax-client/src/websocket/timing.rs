//! Optional transport markers in the complete-message receive clock domain.
use std::time::Instant;

/// Delivery markers for one sampled application message.
#[derive(Clone, Copy, Debug)]
pub struct DeliveryTiming {
    /// Transport control checks have completed; delivery starts here.
    pub prepared: Instant,
    /// The consuming task obtained this input, before caller preparation.
    pub delivered: Option<Instant>,
    /// Inputs still queued immediately after delivery; zero for inline delivery.
    pub pending: Option<usize>,
}

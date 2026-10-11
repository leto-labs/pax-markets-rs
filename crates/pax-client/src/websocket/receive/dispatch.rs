//! Diagnostic routing is chosen once, outside connection polling.
use super::ReceiveTiming;
#[cfg(feature = "diagnostic-handoff")]
use crate::measurement::queue;
use std::time::Instant;

/// Original observation boundaries for deferred joins. Message IDs count delivered
/// application inputs across connections; generation prevents recovery collisions.
#[derive(Clone, Copy, Debug)]
pub struct Observation {
    /// Global feed application-message ordinal, matching the canonical consumer.
    pub message: u64,
    /// Mandatory complete-message receive boundary from the transport owner.
    pub received: Instant,
    /// Qualified original layer observations and policy revision.
    pub timing: ReceiveTiming,
}
/// Single-owner optional sample stream; a stalled consumer cannot delay the feed.
#[cfg(feature = "diagnostic-handoff")]
pub type DiagnosticReceiver = queue::Receiver<Observation>;

pub(crate) enum Dispatch {
    Embedded,
    #[cfg(feature = "diagnostic-handoff")]
    Queued(queue::Sender<Observation>),
}
impl Dispatch {
    pub fn observation(
        &mut self,
        timing: Option<ReceiveTiming>,
        message: u64,
        received: Instant,
    ) -> Option<Box<ReceiveTiming>> {
        match self {
            Self::Embedded => {
                let _ = (message, received);
                timing.map(Box::new)
            }
            #[cfg(feature = "diagnostic-handoff")]
            Self::Queued(sender) => {
                if let Some(timing) = timing {
                    sender.try_push(Observation {
                        message,
                        received,
                        timing,
                    });
                }
                None
            }
        }
    }
    #[cfg(feature = "diagnostic-handoff")]
    pub fn queued(capacity: usize) -> Result<(Self, DiagnosticReceiver), &'static str> {
        use super::{ReceiveWork, kernel::Stamp, policy::ReceivePolicy};
        let seed = Observation {
            message: 0,
            received: Instant::now(),
            timing: ReceiveTiming {
                selection: Default::default(),
                revision: 0,
                complete_policy_window: false,
                policy: ReceivePolicy::OFF,
                latest_read: None,
                first_observed_read: None,
                generation: 0,
                plaintext: None,
                socket: None,
                first_read: None,
                last_read: None,
                shared: false,
                buffered: true,
                coverage: "prefault seed; never published",
                work: ReceiveWork::default(),
                lost: 0,
                sample_every: 1,
                software: Stamp::unavailable("not requested"),
                hardware: Stamp::unavailable("not requested"),
            },
        };
        let (sender, receiver) = queue::channel(capacity, seed)?;
        Ok((Self::Queued(sender), receiver))
    }
}

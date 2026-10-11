//! Optional local model identity. No timers, source requests or hot-path atomics.
#[cfg(feature = "clock-model-details")]
use crate::websocket::receive::kernel::stamp::{StoredQualification, local_qualification};
#[cfg(feature = "clock-model-details")]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "clock-model-details")]
static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);
#[cfg(feature = "clock-model-details")]
fn next_id(counter: &AtomicU64) -> u64 {
    // Exhaustion withholds annotations rather than reusing a clock generation.
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
        .unwrap_or(0)
}
pub(crate) struct Identity {
    #[cfg(feature = "clock-model-details")]
    generation: u64,
    #[cfg(feature = "clock-model-details")]
    epoch: u64,
}
impl Identity {
    pub(crate) fn new() -> Self {
        Self {
            #[cfg(feature = "clock-model-details")]
            generation: next_id(&NEXT_GENERATION),
            #[cfg(feature = "clock-model-details")]
            epoch: 0,
        }
    }
    pub(crate) fn attempted(&mut self) {
        #[cfg(feature = "clock-model-details")]
        {
            self.epoch = self.epoch.saturating_add(1);
        }
    }
    #[cfg(feature = "phc-background")]
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }
    #[cfg(feature = "clock-model-details")]
    pub(super) fn describe(
        &self,
        model: super::Calibration,
        mapped_at: super::Instant,
    ) -> Option<StoredQualification> {
        let (age_ns, bracket_uncertainty_ns) = model.details(mapped_at)?;
        local_qualification(self.generation, self.epoch, age_ns, bracket_uncertainty_ns)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "clock-model-details")]
    #[test]
    fn generations_and_attempt_epochs_never_wrap_or_reuse() {
        let count = AtomicU64::new(u64::MAX - 1);
        assert_eq!(next_id(&count), u64::MAX - 1);
        assert_eq!(next_id(&count), 0);
        assert_eq!(next_id(&count), 0);
        let mut identity = Identity::new();
        let next = Identity::new();
        assert_ne!(identity.generation, next.generation);
        let now = super::super::Instant::now();
        let model = super::super::Calibration::bracket(1000, now, now).unwrap();
        assert!(identity.describe(model, now).is_none());
        identity.attempted();
        let details = identity.describe(model, now).unwrap();
        #[cfg(not(feature = "phc-background"))]
        let details: crate::websocket::receive::kernel::ClockQualification = details.into();
        assert_eq!(details.epoch, 1);
        identity.epoch = u64::MAX;
        identity.attempted();
        assert!(identity.describe(model, now).is_none());
    }
    #[cfg(not(feature = "clock-model-details"))]
    #[test]
    fn compiled_out_identity_has_no_storage() {
        assert_eq!(std::mem::size_of::<Identity>(), 0);
    }
}

//! Immutable timestamp observations and original-boundary clock qualification.
#[cfg(all(feature = "clock-model-details", not(feature = "phc-background")))]
use std::num::NonZeroU64;
use std::time::Instant;

// Read-local models have fixed method/rate flags and need no deferred mapping.
// Retain only their varying fields in the receive record; hydrate the public
// description when requested. The generation niche also removes Option padding.
#[cfg(all(feature = "clock-model-details", not(feature = "phc-background")))]
#[derive(Clone, Copy, Debug)]
pub(crate) struct StoredQualification {
    generation: NonZeroU64,
    epoch: u64,
    age_ns: u64,
    bracket_uncertainty_ns: u64,
}
#[cfg(all(feature = "clock-model-details", feature = "phc-background"))]
pub(crate) type StoredQualification = ClockQualification;

#[cfg(feature = "clock-model-details")]
pub(super) fn local_qualification(
    generation: u64,
    epoch: u64,
    age_ns: u64,
    bracket_uncertainty_ns: u64,
) -> Option<StoredQualification> {
    if generation == 0 || epoch == 0 || epoch == u64::MAX {
        return None;
    }
    Some(StoredQualification {
        #[cfg(not(feature = "phc-background"))]
        generation: NonZeroU64::new(generation)?,
        #[cfg(feature = "phc-background")]
        generation,
        epoch,
        age_ns,
        bracket_uncertainty_ns,
        #[cfg(feature = "phc-background")]
        method: ClockMethod::UserBracket,
        #[cfg(feature = "phc-background")]
        assumed_drift_ppm: 100,
        #[cfg(feature = "phc-background")]
        rate_validated: false,
        #[cfg(feature = "phc-background")]
        mapping_deferred: false,
        #[cfg(feature = "phc-background")]
        model: None,
        #[cfg(feature = "phc-background")]
        read: None,
    })
}
#[cfg(all(feature = "clock-model-details", not(feature = "phc-background")))]
impl From<StoredQualification> for ClockQualification {
    fn from(stored: StoredQualification) -> Self {
        Self {
            generation: stored.generation.get(),
            epoch: stored.epoch,
            age_ns: stored.age_ns,
            bracket_uncertainty_ns: stored.bracket_uncertainty_ns,
            method: ClockMethod::UserBracket,
            assumed_drift_ppm: 100,
            rate_validated: false,
            mapping_deferred: false,
        }
    }
}

/// Method used to relate the source device to the local monotonic clock.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClockMethod {
    /// Kernel driver brackets with an additional Instant/monotonic anchor.
    DriverExtended,
    /// Userspace brackets around a dynamic device clock read.
    UserBracket,
}
/// Immutable connection/model identity and conservative offset qualification.
/// This does not quantify PHC absolute UTC accuracy or TCP packet association.
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub struct ClockQualification {
    /// A newly qualified source-clock descriptor creates a distinct clock generation.
    pub generation: u64,
    /// Calibration attempt identity, including failed or skipped publications.
    pub epoch: u64,
    /// Qualified sampling method.
    pub method: ClockMethod,
    /// Age at mapping, measured within Instant's monotonic domain.
    pub age_ns: u64,
    /// Bracket plus any independent anchor allowance, excluding subsequent drift.
    pub bracket_uncertainty_ns: u64,
    /// Conservative relative rate assumption; not an empirically proven bound.
    pub assumed_drift_ppm: u64,
    /// Whether mapping was deferred until after the original book commit.
    pub mapping_deferred: bool,
    /// True only after held-out rate qualification; the fixed-rate model reports false.
    #[serde(default)]
    pub rate_validated: bool,
    #[cfg(feature = "phc-background")]
    #[serde(skip)]
    pub(crate) model: Option<crate::websocket::receive::clock::Calibration>,
    #[cfg(feature = "phc-background")]
    #[serde(skip)]
    pub(crate) read: Option<Instant>,
}
/// One batch timestamp, explicitly unavailable unless both data and alignment work.
#[derive(Clone, Copy, Debug)]
pub struct Stamp {
    /// Aligned application-monotonic time. Never a remote exchange timestamp.
    pub at: Option<Instant>,
    /// Original source-domain nanoseconds, retained for clock diagnosis.
    pub raw_ns: Option<i128>,
    /// Bracket plus bounded drift uncertainty, when aligned.
    pub uncertainty_ns: Option<u64>,
    /// Optional immutable offset-model details; absent in legacy/detail-free builds.
    #[cfg(feature = "clock-model-details")]
    pub(crate) qualification: Option<StoredQualification>,
    /// `kernel-software/realtime`, `nic-hardware/phc`, or unavailable.
    pub source: &'static str,
    /// Availability or concrete rejection reason. Hardware packet position is
    /// unspecified; TCP ancillary timestamps apply to a read batch, not a message.
    pub status: &'static str,
}
impl Stamp {
    /// Resolve an optional immutable background model using the original read
    /// boundary. Call after book commit or in an exporter; export time is never
    /// substituted for ingress/read/commit time. Existing mapped stamps are unchanged.
    pub fn resolve(self) -> Self {
        #[cfg(feature = "phc-background")]
        {
            self.resolve_background()
        }
        #[cfg(not(feature = "phc-background"))]
        {
            self
        }
    }
    /// Offset-model details, absent when their independent feature is compiled out.
    pub fn qualification(&self) -> Option<ClockQualification> {
        #[cfg(all(feature = "clock-model-details", feature = "phc-background"))]
        {
            self.qualification
        }
        #[cfg(all(feature = "clock-model-details", not(feature = "phc-background")))]
        {
            self.qualification.map(Into::into)
        }
        #[cfg(not(feature = "clock-model-details"))]
        {
            None
        }
    }
    #[cfg(feature = "phc-background")]
    fn resolve_background(mut self) -> Self {
        let Some(mut qualification) = self.qualification else {
            return self;
        };
        let (Some(model), Some(read), Some(raw)) =
            (qualification.model, qualification.read, self.raw_ns)
        else {
            return self;
        };
        let alignment = model.align(
            raw,
            read,
            std::time::Duration::from_secs(2),
            qualification.assumed_drift_ppm,
        );
        qualification.model = None;
        qualification.read = None;
        self.qualification = Some(qualification);
        match alignment {
            Ok((at, uncertainty)) => {
                self.at = Some(at);
                self.uncertainty_ns = Some(uncertainty);
                self.status = match qualification.method {
                    ClockMethod::DriverExtended => {
                        "read-batch timestamp; deferred background driver/monotonic alignment; packet position unspecified"
                    }
                    ClockMethod::UserBracket => {
                        "read-batch timestamp; deferred background user-bracket alignment; packet position unspecified"
                    }
                };
            }
            Err(reason) => {
                self.at = None;
                self.uncertainty_ns = None;
                self.status = match reason {
                    crate::websocket::receive::clock::InvalidClock::Stale => {
                        "clock alignment or observation stale"
                    }
                    crate::websocket::receive::clock::InvalidClock::Step => "clock discontinuity",
                    crate::websocket::receive::clock::InvalidClock::Future => {
                        "timestamp maps to future; interval withheld"
                    }
                    _ => "clock alignment unavailable",
                };
            }
        }
        self
    }
    /// Construct an unavailable observation with no fabricated timestamp.
    pub const fn unavailable(reason: &'static str) -> Self {
        Self {
            at: None,
            raw_ns: None,
            uncertainty_ns: None,
            #[cfg(feature = "clock-model-details")]
            qualification: None,
            source: "unavailable",
            status: reason,
        }
    }
    /// Construct an observer-supplied stamp without a background model. The caller
    /// must qualify its clock domain and uncertainty before supplying `at`.
    pub const fn from_parts(
        at: Option<Instant>,
        raw_ns: Option<i128>,
        uncertainty_ns: Option<u64>,
        source: &'static str,
        status: &'static str,
    ) -> Self {
        Self {
            at,
            raw_ns,
            uncertainty_ns,
            source,
            status,
            #[cfg(feature = "clock-model-details")]
            qualification: None,
        }
    }
}

#[cfg(all(test, feature = "clock-model-details", not(feature = "phc-background")))]
mod local_tests;
#[cfg(all(test, feature = "phc-background"))]
mod tests;

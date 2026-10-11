//! Startup composition of independent collection and output policies.
use super::policy::ProcessingPolicy;
use pax_client::{
    measurement::SampleRate,
    websocket::{
        FeedConfig,
        receive::{ReceiveConfig, policy::ReceivePolicy},
    },
};
use serde::{Deserialize, Serialize};

/// Optional overrides preserve legacy CLI behavior when omitted. Cadences never
/// change ingestion sampling counters. Deserialize with a bounded input reader.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Settings {
    /// Independent callback, JSON, book substages and callback-tail collection.
    pub processing: Option<ProcessingPolicy>,
    /// Overrides receive domains; Some initializes receive adapters even when off.
    pub receive: Option<ReceivePolicy>,
    /// Complete-message receive metadata denominator; zero disables emission.
    pub receive_emit_every: Option<u64>,
    /// Independent delivery marker denominator; zero disables.
    pub delivery_every: Option<u64>,
    /// Numeric publication interval, 10..=60000 milliseconds.
    pub publication_ms: u64,
    /// Diagnostic file selection, independently counted after receive handoff.
    pub sidecar: SampleRate,
    /// Receive-worker idle wake interval, 1..=1000 milliseconds; batches stay bounded.
    pub export_poll_ms: u64,
    /// Browser HTTP polling, 100..=60000 milliseconds.
    pub browser_poll_ms: u64,
    /// Browser timing selections; denominator counts visible, unpaused fetch attempts.
    pub browser: SampleRate,
    /// Route probe interval, 10..=3600 seconds, or zero to suppress probes.
    pub route_seconds: u64,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            processing: None,
            receive: None,
            receive_emit_every: None,
            delivery_every: None,
            publication_ms: 250,
            sidecar: SampleRate::ALL,
            export_poll_ms: 2,
            browser_poll_ms: 250,
            browser: SampleRate::ALL,
            route_seconds: 60,
        }
    }
}
impl Settings {
    /// Disable optional clocks, diagnostic output selection, browser timing and routes.
    /// Canonical input, health, deadlines and display publication remain active.
    pub fn off() -> Self {
        Self {
            processing: Some(ProcessingPolicy::OFF),
            receive: Some(ReceivePolicy::OFF),
            delivery_every: Some(0),
            sidecar: SampleRate::OFF,
            browser: SampleRate::OFF,
            route_seconds: 0,
            ..Self::default()
        }
    }
    /// Read-level receive timestamps only; a source must be requested separately.
    /// No message-specific association or exchange-origin timing is implied.
    pub fn timestamps() -> Self {
        Self {
            receive: Some(ReceivePolicy::TIMESTAMPS),
            ..Self::off()
        }
    }
    /// Full optional collection. Fails validation when requested groups are omitted.
    pub fn detailed() -> Self {
        Self {
            processing: Some(ProcessingPolicy::every(1)),
            receive: Some(ReceivePolicy::DETAILED),
            delivery_every: Some(1),
            ..Self::default()
        }
    }
    /// Validate before starting a connection, allocating workers or opening files.
    pub fn validate(self) -> Result<(), &'static str> {
        if let Some(p) = self.processing {
            p.validate()?;
        }
        if let Some(p) = self.receive {
            p.validate()?;
        }
        if self.receive_emit_every.is_some_and(|n| n > 1_000_000_000) {
            return Err("invalid receive emission rate");
        }
        if self
            .delivery_every
            .is_some_and(|n| n > 1_000_000_000 || (n != 0 && !cfg!(feature = "delivery-traces")))
        {
            return Err("invalid or compiled-out delivery rate");
        }
        self.sidecar.validate()?;
        self.browser.validate()?;
        if !(10..=60000).contains(&self.publication_ms)
            || !(1..=1000).contains(&self.export_poll_ms)
            || !(100..=60000).contains(&self.browser_poll_ms)
            || (self.route_seconds != 0 && !(10..=3600).contains(&self.route_seconds))
        {
            return Err("measurement output cadence outside bounds");
        }
        Ok(())
    }
    /// Apply receive/delivery overrides to the caller's source and socket settings.
    /// Validation completes first; an invalid update leaves the feed unchanged.
    pub fn apply_feed(self, feed: &mut FeedConfig) -> Result<(), &'static str> {
        self.validate()?;
        let mut receive = feed.receive.clone();
        if let Some(policy) = self.receive {
            receive.get_or_insert_with(ReceiveConfig::default).policy = policy;
        }
        if let Some(every) = self.receive_emit_every {
            receive
                .get_or_insert_with(ReceiveConfig::default)
                .sample_every = every;
        }
        if let Some(r) = &receive {
            r.validate()?;
        }
        feed.receive = receive;
        if let Some(every) = self.delivery_every {
            feed.delivery_sample_every = every;
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn zero_receive_emission_is_independent_and_invalid_updates_are_atomic() {
        let mut feed = FeedConfig::public(vec![pax_types::units::MarketId(1)]);
        let settings = Settings {
            receive_emit_every: Some(0),
            ..Settings::off()
        };
        settings.apply_feed(&mut feed).unwrap();
        let receive = feed.receive.as_ref().unwrap();
        assert_eq!(receive.sample_every, 0);
        assert_eq!(receive.policy, ReceivePolicy::OFF);
        receive.validate().unwrap();
        assert!(
            Settings {
                receive_emit_every: Some(1_000_000_001),
                ..settings
            }
            .apply_feed(&mut feed)
            .is_err()
        );
        assert_eq!(feed.receive.as_ref().unwrap().sample_every, 0);
    }

    #[test]
    fn output_cadence_does_not_rewrite_collection_and_bad_input_is_atomic() {
        let mut feed = FeedConfig::public(vec![pax_types::units::MarketId(1)]);
        let settings = Settings {
            publication_ms: 1000,
            browser_poll_ms: 2000,
            sidecar: SampleRate::every(17),
            ..Settings::off()
        };
        settings.apply_feed(&mut feed).unwrap();
        assert_eq!(feed.receive.as_ref().unwrap().policy, ReceivePolicy::OFF);
        let old = feed.delivery_sample_every;
        assert!(
            Settings {
                publication_ms: 0,
                delivery_every: Some(99),
                ..settings
            }
            .apply_feed(&mut feed)
            .is_err()
        );
        assert_eq!(feed.delivery_sample_every, old);
        assert!(serde_json::from_str::<Settings>(r#"{"unknown_domain":1}"#).is_err());
        assert_eq!(
            serde_json::from_str::<Settings>("{}").unwrap(),
            Settings::default()
        );
    }
}

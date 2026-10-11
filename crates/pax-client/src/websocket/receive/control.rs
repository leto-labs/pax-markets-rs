//! Bounded operator control, independent of the market-data and diagnostic queues.
use super::ReceiveConfig;
use serde::Serialize;
use tokio::sync::{mpsc, oneshot, watch};

/// Requested optional measurements; socket source settings can require reconnect.
#[derive(Clone, Debug)]
pub struct MeasurementUpdate {
    /// Receive settings. `None` requests no receive observations.
    pub receive: Option<ReceiveConfig>,
    /// Independent complete-message delivery marker denominator; zero disables.
    pub delivery_sample_every: u64,
}
impl MeasurementUpdate {
    pub(crate) fn validate(&self) -> Result<(), UpdateError> {
        if self.delivery_sample_every != 0 && !cfg!(feature = "delivery-traces") {
            return Err(UpdateError::Invalid("delivery-traces probes compiled out"));
        }
        if let Some(config) = &self.receive {
            config.validate().map_err(UpdateError::Invalid)?;
        }
        Ok(())
    }
}
/// The accepted boundary. Queued older inputs keep their previous revisions.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct Applied {
    /// Monotonic successful policy revision within this feed.
    pub revision: u64,
    /// Connection to which the update applies.
    pub generation: u64,
    /// Complete messages delivered by the supervisor before the update.
    pub after_message: u64,
    /// A partial receive observation was discarded at this boundary.
    pub partial_observation_discarded: bool,
}
/// Updates are rejected atomically; the previous effective settings stay active.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum UpdateError {
    /// A requested setting failed validation.
    #[error("invalid measurement update: {0}")]
    Invalid(&'static str),
    /// An adapter or socket source must be initialized on a new connection.
    #[error("measurement update requires reconnect: {0}")]
    ReconnectRequired(&'static str),
    /// No connection currently owns the measurement adapters.
    #[error("measurement update requires an active connection")]
    Disconnected,
    /// Revision identities must never wrap.
    #[error("measurement policy revision exhausted")]
    RevisionExhausted,
}
/// Failure to submit an operator request; no existing request is overwritten.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SubmitError {
    /// The finite control mailbox is full.
    #[error("measurement control queue full")]
    Full,
    /// The feed has stopped.
    #[error("measurement control queue closed")]
    Closed,
    /// Validation is performed before consuming mailbox capacity.
    #[error(transparent)]
    Invalid(#[from] UpdateError),
}
/// Cloneable operator endpoint. Submission never waits for data or telemetry.
#[derive(Clone, Debug)]
pub struct MeasurementControl {
    sender: mpsc::Sender<Request>,
    effective: watch::Receiver<Effective>,
}
/// Last successfully applied settings, readable even if diagnostics cannot export.
#[derive(Clone, Debug)]
pub struct Effective {
    /// Zero for startup configuration.
    pub revision: u64,
    /// Actual settings retained for subsequent reconnects.
    pub settings: MeasurementUpdate,
}
pub(crate) struct Request {
    pub update: MeasurementUpdate,
    pub reply: oneshot::Sender<Result<Applied, UpdateError>>,
}
impl MeasurementControl {
    pub(crate) fn channel(
        initial: MeasurementUpdate,
    ) -> (Self, mpsc::Receiver<Request>, watch::Sender<Effective>) {
        let (sender, receiver) = mpsc::channel(8);
        let (status, effective) = watch::channel(Effective {
            revision: 0,
            settings: initial,
        });
        (Self { sender, effective }, receiver, status)
    }
    /// Snapshot the last acknowledged policy independently of sample publication.
    pub fn effective(&self) -> Effective {
        self.effective.borrow().clone()
    }
    /// Submit one update, then await the returned acknowledgment separately.
    /// Inline feeds must keep driving `Feed::recv` to service this mailbox.
    /// Connecting feeds respond after connect completes or times out.
    pub fn try_update(
        &self,
        update: MeasurementUpdate,
    ) -> Result<oneshot::Receiver<Result<Applied, UpdateError>>, SubmitError> {
        update.validate()?;
        let (reply, receiver) = oneshot::channel();
        self.sender
            .try_send(Request { update, reply })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => SubmitError::Full,
                mpsc::error::TrySendError::Closed(_) => SubmitError::Closed,
            })?;
        Ok(receiver)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn capacity_closed_and_invalid_requests_do_not_overwrite_pending_updates() {
        let update = MeasurementUpdate {
            receive: None,
            delivery_sample_every: 0,
        };
        let (control, mut receiver, _status) = MeasurementControl::channel(update.clone());
        let mut replies = Vec::new();
        for _ in 0..8 {
            replies.push(control.try_update(update.clone()).unwrap());
        }
        assert!(matches!(
            control.try_update(update.clone()),
            Err(SubmitError::Full)
        ));
        let invalid = MeasurementUpdate {
            receive: Some(ReceiveConfig {
                capacity: 0,
                ..ReceiveConfig::default()
            }),
            ..update.clone()
        };
        assert!(matches!(
            control.try_update(invalid),
            Err(SubmitError::Invalid(_))
        ));
        for reply in &mut replies {
            receiver
                .try_recv()
                .unwrap()
                .reply
                .send(Err(UpdateError::Disconnected))
                .unwrap();
            assert_eq!(reply.try_recv().unwrap(), Err(UpdateError::Disconnected));
        }
        drop(receiver);
        assert!(matches!(
            control.try_update(update),
            Err(SubmitError::Closed)
        ));
    }
}

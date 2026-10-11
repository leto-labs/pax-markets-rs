//! Canonical passive WebSocket frame association, with caller-owned metadata.
//! Tungstenite remains the validator. No frame payload or additional clock is retained.
use crate::measurement::{SampleRate, Sampler};
use std::{collections::VecDeque, io::Cursor};
use tokio_tungstenite::tungstenite::protocol::frame::{
    FrameHeader,
    coding::{Data, OpCode},
};

/// Metadata operations needed by the shared frame observer.
/// Batch IDs must be monotonic within one connection; merging must preserve
/// unavailable contributing origins instead of substituting later metadata.
pub trait Metadata: Copy {
    /// Merge another contributing plaintext span.
    fn extend(&mut self, next: Self);
    /// First plaintext batch contributing to this span.
    fn first_batch(&self) -> u64;
    /// Last plaintext batch contributing to this span.
    fn last_batch(&self) -> u64;
    /// Whether the span shares a plaintext batch with another frame.
    fn shared(&self) -> bool;
    /// Retain shared-batch provenance.
    fn mark_shared(&mut self);
}

/// Bounded association through HTTP upgrade, partial headers and fragments.
pub struct Tracker<A: Metadata> {
    header: [u8; 14],
    used: usize,
    remaining: u64,
    frame: Option<(FrameHeader, A)>,
    fragmented: Option<A>,
    pub(crate) ready: VecDeque<(u64, A)>,
    pub(crate) completed: u64,
    pub(crate) lost: u64,
    capacity: usize,
    sampler: Sampler,
    upgrade_end: bool,
    upgrade_tail: u32,
    last_frame_batch: Option<u64>,
    pub(crate) invalid: bool,
}
impl<A: Metadata> Tracker<A> {
    #[cfg(test)]
    pub(crate) fn new(capacity: usize) -> Self {
        Self::with_rate(capacity, SampleRate::ALL)
    }
    pub(crate) fn with_rate(capacity: usize, rate: SampleRate) -> Self {
        Self::try_with_rate(capacity, rate).expect("validated association policy")
    }
    /// Construct bounded metadata retention with an independent sampling policy.
    ///
    /// # Errors
    /// Rejects capacities outside 1..=65536 and invalid sampling policies.
    pub fn try_with_rate(capacity: usize, rate: SampleRate) -> Result<Self, &'static str> {
        if !(1..=65536).contains(&capacity) {
            return Err("invalid frame association capacity");
        }
        Ok(Self {
            sampler: Sampler::new(rate)?,
            header: [0; 14],
            used: 0,
            remaining: 0,
            frame: None,
            fragmented: None,
            ready: VecDeque::with_capacity(capacity),
            completed: 0,
            lost: 0,
            capacity,
            upgrade_end: false,
            upgrade_tail: 0,
            last_frame_batch: None,
            invalid: false,
        })
    }
    /// Eligible and selected application-message counts.
    pub fn counts(&self) -> crate::measurement::SelectionCounts {
        self.sampler.counts()
    }
    pub(crate) fn configure(&mut self, rate: SampleRate) {
        self.sampler
            .configure(rate)
            .expect("validated association policy");
        self.lost = self.lost.saturating_add(self.ready.len() as u64);
        self.ready.clear();
    }
    /// Observe plaintext bytes; the transport remains the frame validator.
    /// No payload is retained. Input must begin with the HTTP upgrade response.
    pub fn bytes(&mut self, mut bytes: &[u8], mut batch: A) {
        if self.invalid {
            return;
        }
        while !bytes.is_empty() {
            if !self.upgrade_end {
                self.upgrade_tail = (self.upgrade_tail << 8) | u32::from(bytes[0]);
                bytes = &bytes[1..];
                self.upgrade_end = self.upgrade_tail == 0x0d0a0d0a;
                continue;
            }
            if self.frame.is_none() {
                if self.used == 0 {
                    if self.last_frame_batch == Some(batch.first_batch()) {
                        batch.mark_shared();
                    }
                    // Another frame begins in this same batch. Mark prior buffered
                    // completed messages and a fragmented message as sharing work.
                    if batch.shared() {
                        for (_, entry) in self.ready.iter_mut().rev() {
                            if entry.shared() || entry.last_batch() != batch.first_batch() {
                                break;
                            }
                            entry.mark_shared();
                        }
                        if let Some(entry) = &mut self.fragmented {
                            entry.mark_shared();
                        }
                    }
                }
                // Retain the association of split headers too, without payload copies.
                if self.used == 0 {
                    self.frame = Some((FrameHeader::default(), batch));
                }
                // `frame` also holds partial-header association; parsing below distinguishes it.
            }
            if self.remaining == 0 && self.used < 14 {
                self.header[self.used] = bytes[0];
                self.used += 1;
                bytes = &bytes[1..];
                if let Some((_, assoc)) = &mut self.frame {
                    assoc.extend(batch);
                }
                match FrameHeader::parse(&mut Cursor::new(&self.header[..self.used])) {
                    Ok(Some((header, length))) => {
                        self.remaining = length;
                        self.frame.as_mut().expect("header association").0 = header;
                        self.used = 14; // parsed; payload state even for short headers
                    }
                    Ok(None) if self.used < 14 => continue,
                    _ => {
                        self.invalid = true;
                        self.lost += 1;
                        return;
                    }
                }
            }
            let take = self.remaining.min(bytes.len() as u64) as usize;
            if take != 0 {
                self.frame.as_mut().expect("parsed header").1.extend(batch);
                bytes = &bytes[take..];
                self.remaining -= take as u64;
            }
            if self.remaining != 0 {
                return;
            }
            let (header, assoc) = self.frame.take().expect("completed frame");
            self.used = 0;
            self.last_frame_batch = Some(assoc.last_batch());
            match header.opcode {
                OpCode::Data(Data::Text | Data::Binary) if !header.is_final => {
                    self.fragmented = Some(assoc)
                }
                OpCode::Data(Data::Continue) => {
                    let Some(mut message) = self.fragmented.take() else {
                        self.invalid = true;
                        return;
                    };
                    message.extend(assoc);
                    if header.is_final {
                        self.push(message);
                    } else {
                        self.fragmented = Some(message);
                    }
                }
                OpCode::Data(Data::Text | Data::Binary) => self.push(assoc),
                _ => {} // Protocol controls do not consume application-message ordinals.
            }
        }
    }
    /// Completed application messages, including unselected messages.
    #[cfg(feature = "test-support")]
    pub fn completed(&self) -> u64 {
        self.completed
    }
    /// Metadata evicted or refused because of malformed framing.
    #[cfg(feature = "test-support")]
    pub fn lost(&self) -> u64 {
        self.lost
    }
    /// Whether passive parsing failed; the real transport must still validate.
    #[cfg(feature = "test-support")]
    pub fn invalid(&self) -> bool {
        self.invalid
    }
    /// Number of currently retained selected message associations.
    #[cfg(feature = "test-support")]
    pub fn retained(&self) -> usize {
        self.ready.len()
    }
    fn push(&mut self, assoc: A) {
        self.completed = self.completed.saturating_add(1);
        if !self.sampler.observe() {
            return;
        }
        if self.ready.len() == self.capacity {
            self.ready.pop_front();
            self.lost += 1;
        }
        self.ready.push_back((self.completed, assoc));
    }
    /// Remove one selected application-message ordinal without shifting gaps.
    pub fn take(&mut self, ordinal: u64) -> Option<A> {
        while self.ready.front().is_some_and(|(id, _)| *id < ordinal) {
            self.ready.pop_front();
        }
        if self.ready.front().is_some_and(|(id, _)| *id == ordinal) {
            self.ready.pop_front().map(|(_, association)| association)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests;

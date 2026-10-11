//! Preallocated single-owner diagnostic publication. No locks, waits or allocations
//! occur in `try_push`/`try_pop`. Each endpoint is Send but must have one owner.
use rtrb::{Consumer, Producer, RingBuffer};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

#[derive(Default)]
struct Counters {
    accepted: AtomicU64,
    full: AtomicU64,
    closed: AtomicU64,
    consumed: AtomicU64,
    producer: AtomicBool,
    consumer: AtomicBool,
}
/// Independently readable health, even when the diagnostic queue is full or closed.
#[derive(Clone)]
pub struct Status {
    counters: Arc<Counters>,
    capacity: usize,
}
pub use super::status::QueueStatus;
impl Status {
    /// No dependency on successful sample export or on a worker-owned mutex.
    pub fn snapshot(&self) -> QueueStatus {
        let c = &self.counters;
        let accepted = c.accepted.load(Ordering::Relaxed);
        let consumed = c.consumed.load(Ordering::Relaxed);
        QueueStatus {
            capacity: self.capacity,
            accepted,
            full_drops: c.full.load(Ordering::Relaxed),
            closed_drops: c.closed.load(Ordering::Relaxed),
            consumed,
            pending: accepted.saturating_sub(consumed),
            producer_alive: c.producer.load(Ordering::Acquire),
            consumer_alive: c.consumer.load(Ordering::Acquire),
        }
    }
}
/// The unique publication owner. A full queue drops only the supplied optional item.
pub struct Sender<T: Copy> {
    inner: Producer<T>,
    status: Status,
    accepted: u64,
    full: u64,
    closed: u64,
}
/// The unique draining owner. Original timestamps travel in T; polling time is not
/// substituted for an event boundary. Moving the owner to a worker is supported.
pub struct Receiver<T: Copy> {
    inner: Consumer<T>,
    status: Status,
    consumed: u64,
}
/// Construct and touch all slots outside the hot path. `seed` is used only to
/// initialize physical storage and is removed before the endpoints are returned.
///
/// # Errors
/// Rejects capacities outside 1..=65536. Elements must be fixed-size Copy values;
/// any owned payload or formatting belongs outside this queue.
pub fn channel<T: Copy>(
    capacity: usize,
    seed: T,
) -> Result<(Sender<T>, Receiver<T>), &'static str> {
    if !(1..=65536).contains(&capacity) {
        return Err("invalid diagnostic queue capacity");
    }
    let (mut sender, mut receiver) = RingBuffer::new(capacity);
    for _ in 0..capacity {
        assert!(sender.push(seed).is_ok());
    }
    std::hint::black_box(&mut sender);
    for _ in 0..capacity {
        assert!(receiver.pop().is_ok());
    }
    let counters = Arc::new(Counters {
        producer: AtomicBool::new(true),
        consumer: AtomicBool::new(true),
        ..Counters::default()
    });
    let status = Status { counters, capacity };
    Ok((
        Sender {
            inner: sender,
            status: status.clone(),
            accepted: 0,
            full: 0,
            closed: 0,
        },
        Receiver {
            inner: receiver,
            status,
            consumed: 0,
        },
    ))
}
impl<T: Copy> Sender<T> {
    /// Try once. There is no retry, wakeup, clock read, formatting or allocation.
    pub fn try_push(&mut self, value: T) -> bool {
        if self.inner.is_abandoned() {
            self.closed = self.closed.saturating_add(1);
            self.status
                .counters
                .closed
                .store(self.closed, Ordering::Relaxed);
            return false;
        }
        if self.inner.push(value).is_err() {
            self.full = self.full.saturating_add(1);
            self.status
                .counters
                .full
                .store(self.full, Ordering::Relaxed);
            false
        } else {
            self.accepted = self.accepted.saturating_add(1);
            self.status
                .counters
                .accepted
                .store(self.accepted, Ordering::Relaxed);
            true
        }
    }
    /// Clone only the status handle, normally during initialization.
    pub fn status(&self) -> Status {
        self.status.clone()
    }
}
impl<T: Copy> Receiver<T> {
    /// Try once, without waiting. Bound the number of calls in each worker batch.
    pub fn try_pop(&mut self) -> Option<T> {
        let value = self.inner.pop().ok()?;
        self.consumed = self.consumed.saturating_add(1);
        self.status
            .counters
            .consumed
            .store(self.consumed, Ordering::Relaxed);
        Some(value)
    }
    /// Clone only the independent status handle.
    pub fn status(&self) -> Status {
        self.status.clone()
    }
}
impl<T: Copy> Drop for Sender<T> {
    fn drop(&mut self) {
        self.status
            .counters
            .producer
            .store(false, Ordering::Release);
    }
}
impl<T: Copy> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.status
            .counters
            .consumer
            .store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn prefaulted_storage_starts_empty_wraps_and_reports_stalled_or_failed_consumers() {
        let (mut sender, mut receiver) = channel(2, 7u64).unwrap();
        let status = sender.status();
        assert!(receiver.try_pop().is_none());
        for i in 0..10000 {
            assert!(sender.try_push(i));
            assert!(sender.try_push(i + 1));
            assert!(!sender.try_push(0));
            assert_eq!(receiver.try_pop(), Some(i));
            assert_eq!(receiver.try_pop(), Some(i + 1));
        }
        assert!(sender.try_push(123));
        drop(receiver);
        assert!(!sender.try_push(124));
        let s = status.snapshot();
        assert_eq!(s.accepted, 20001);
        assert_eq!(s.consumed, 20000);
        assert_eq!(s.pending, 1);
        assert_eq!(s.full_drops, 10000);
        assert_eq!(s.closed_drops, 1);
        assert!(!s.consumer_alive);
        drop(sender);
        assert!(!status.snapshot().producer_alive);
    }
    #[test]
    fn endpoints_can_move_between_threads_and_preserve_exact_original_values() {
        let (mut sender, mut receiver) = channel(65536, [0u64; 4]).unwrap();
        let status = receiver.status();
        std::thread::spawn(move || {
            for i in 0..65536 {
                assert!(sender.try_push([i, u64::MAX - i, 17, 0]));
            }
        })
        .join()
        .unwrap();
        for i in 0..65536 {
            assert_eq!(receiver.try_pop(), Some([i, u64::MAX - i, 17, 0]));
        }
        assert_eq!(status.snapshot().pending, 0);
        assert!(!status.snapshot().producer_alive);
    }
}

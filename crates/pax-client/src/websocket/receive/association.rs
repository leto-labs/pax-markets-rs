//! Passive frame-boundary observer; Tungstenite remains the protocol validator.
//! No payload is retained. Header parsing reuses Tungstenite's public parser.
use super::{BatchRange, ReadObservation, kernel::Stamp};
use std::time::Instant;

#[derive(Clone, Copy, Debug)]
pub(super) struct Association {
    pub revision: u64,
    pub complete_policy_window: bool,
    pub plain: BatchRange,
    pub socket: Option<BatchRange>,
    pub first: Option<Instant>,
    pub first_observed_read: Option<ReadObservation>,
    pub last: Option<Instant>,
    pub shared: bool,
    pub software: Stamp,
    pub hardware: Stamp,
}
impl crate::frame_tracker::Metadata for Association {
    fn first_batch(&self) -> u64 {
        self.plain.first
    }
    fn last_batch(&self) -> u64 {
        self.plain.last
    }
    fn shared(&self) -> bool {
        self.shared
    }
    fn mark_shared(&mut self) {
        self.shared = true;
    }
    fn extend(&mut self, next: Self) {
        self.complete_policy_window &=
            next.complete_policy_window && self.revision == next.revision;
        self.plain.last = next.plain.last;
        if let (Some(current), Some(next)) = (&mut self.socket, next.socket) {
            current.last = next.last;
        } else {
            self.socket = None;
        }
        self.last = next.last;
        self.shared |= next.shared;
        self.software = next.software;
        self.hardware = next.hardware;
    }
}
pub(super) type Observer = crate::frame_tracker::Tracker<Association>;

#[cfg(test)]
mod tests {
    use super::*;
    pub(super) fn batch(id: u64) -> Association {
        let now = Instant::now();
        Association {
            revision: 0,
            complete_policy_window: true,
            plain: BatchRange {
                first: id,
                last: id,
            },
            socket: Some(BatchRange {
                first: id,
                last: id,
            }),
            first: Some(now),
            first_observed_read: Some(ReadObservation {
                batch: id,
                ended: now,
                software: Stamp::unavailable("test initial read"),
                hardware: Stamp::unavailable("test initial read"),
            }),
            last: Some(now),
            shared: false,
            software: Stamp::unavailable("test"),
            hardware: Stamp::unavailable("test"),
        }
    }
    pub(super) fn observer(capacity: usize) -> Observer {
        let mut observer = Observer::new(capacity);
        for bytes in b"HTTP/1.1 101 Switching Protocols\r\n\r\n".chunks(1) {
            observer.bytes(bytes, batch(1));
        }
        observer
    }
    #[test]
    fn fragmented_header_and_payload_keep_ranges_across_controls() {
        let mut observer = observer(8);
        observer.bytes(&[0x01], batch(2));
        observer.bytes(&[2, b'a'], batch(3));
        observer.bytes(&[b'b', 0x89, 1, b'p', 0x80, 2, b'c'], batch(4));
        observer.bytes(b"d", batch(5));
        let association = observer.take(1).unwrap();
        assert_eq!((association.plain.first, association.plain.last), (2, 5));
        assert_eq!(association.first_observed_read.unwrap().batch, 2);
        assert!(association.shared);
        assert_eq!(observer.completed, 1);
        assert!(!observer.invalid);
    }
    #[test]
    fn shared_buffered_messages_and_capacity_loss_do_not_shift_ordinals() {
        let mut observer = observer(2);
        observer.bytes(&[0x81, 1, b'a', 0x81, 1, b'b', 0x81, 1, b'c'], batch(2));
        assert_eq!(observer.lost, 1);
        assert!(observer.take(1).is_none());
        for ordinal in 2..=3 {
            let a = observer.take(ordinal).unwrap();
            assert!(a.shared);
            assert_eq!(a.plain.first, 2);
            assert_eq!(a.first_observed_read.unwrap().batch, 2);
        }
        observer.bytes(&[0x81, 0], batch(3));
        assert!(!observer.take(4).unwrap().shared);
    }
    #[test]
    fn later_selected_timestamp_cannot_replace_unselected_initial_read() {
        let mut observer = observer(2);
        let first = batch(2);
        let mut later = batch(3);
        later.software = Stamp {
            at: Some(Instant::now()),
            raw_ns: Some(123),
            uncertainty_ns: Some(1),
            #[cfg(feature = "clock-model-details")]
            qualification: None,
            source: "kernel-software/realtime",
            status: "test selected read",
        };
        later.first_observed_read.as_mut().unwrap().software = later.software;
        observer.bytes(&[0x81], first);
        observer.bytes(&[1, b'a'], later);
        let association = observer.take(1).unwrap();
        let initial = association.first_observed_read.unwrap();
        assert_eq!(initial.batch, 2);
        assert!(initial.software.at.is_none());
        assert_eq!(initial.software.status, "test initial read");
        assert_eq!(association.software.raw_ns, Some(123));
    }

    #[test]
    fn fragmented_message_retains_initial_source_through_control_and_completion() {
        let mut observer = observer(4);
        let stamp = |id: u64| Stamp {
            at: Some(Instant::now()),
            raw_ns: Some(i128::from(id)),
            uncertainty_ns: Some(id),
            #[cfg(feature = "clock-model-details")]
            qualification: None,
            source: "nic-hardware/phc",
            status: "test qualified read",
        };
        let mut first = batch(2);
        first.hardware = stamp(2);
        first.first_observed_read.as_mut().unwrap().hardware = first.hardware;
        let mut last = batch(4);
        last.hardware = stamp(4);
        last.first_observed_read.as_mut().unwrap().hardware = last.hardware;
        observer.bytes(&[0x01, 1, b'a'], first);
        observer.bytes(&[0x89, 1, b'p'], batch(3));
        observer.bytes(&[0x80, 1, b'b'], last);
        let association = observer.take(1).unwrap();
        assert_eq!(
            association.first_observed_read.unwrap().hardware.raw_ns,
            Some(2)
        );
        assert_eq!(association.hardware.raw_ns, Some(4));
        assert_eq!(observer.completed, 1);
    }
    #[test]
    fn unsupported_frame_header_does_not_allocate_or_scan_payload() {
        let mut observer = observer(1);
        observer.bytes(&[0xff, 0xff, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0], batch(2));
        assert!(observer.invalid);
        assert!(observer.ready.is_empty());
    }
}

#[cfg(test)]
mod density_tests {
    use super::tests::{batch, observer};
    #[test]
    fn dense_empty_frames_remain_bounded_and_all_shared() {
        let mut o = observer(65536);
        let bytes = [0x81, 0].repeat(65536);
        o.bytes(&bytes, batch(2));
        assert_eq!(o.ready.len(), 65536);
        assert_eq!(o.completed, 65536);
        assert_eq!(o.lost, 0);
        assert!(o.ready.iter().all(|(_, entry)| entry.shared));
        // Every entry transitions to shared at most once; the marking loop stops
        // on an already shared entry rather than revisiting the entire batch.
        o.bytes(&bytes, batch(3));
        assert_eq!(o.ready.len(), 65536);
        assert_eq!(o.lost, 65536);
    }
}

//! Independent byte-position oracle and an explicit observer-only cost screen.
use super::*;
use std::{hint::black_box, ops::Range, time::Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Span {
    first: u64,
    last: u64,
    available: bool,
    shared: bool,
}
impl Metadata for Span {
    fn first_batch(&self) -> u64 {
        self.first
    }
    fn last_batch(&self) -> u64 {
        self.last
    }
    fn shared(&self) -> bool {
        self.shared
    }
    fn mark_shared(&mut self) {
        self.shared = true;
    }
    fn extend(&mut self, next: Self) {
        self.last = next.last;
        self.available &= next.available;
        self.shared |= next.shared;
    }
}
fn batch(index: usize) -> Span {
    Span {
        first: index as u64 + 1,
        last: index as u64 + 1,
        available: index % 7 != 3,
        shared: false,
    }
}
struct Input {
    bytes: Vec<u8>,
    frames: Vec<(Range<usize>, Option<usize>)>,
    messages: usize,
}
impl Input {
    fn new() -> Self {
        Self {
            bytes: Vec::new(),
            frames: Vec::new(),
            messages: 0,
        }
    }
    // Independent wire encoder: no production parser or formatter supplies boundaries.
    fn frame(&mut self, first: u8, length: usize, masked: bool, message: Option<usize>) {
        let start = self.bytes.len();
        self.bytes.push(first);
        let mask = if masked { 128 } else { 0 };
        match length {
            0..=125 => self.bytes.push(mask | length as u8),
            126..=65535 => {
                self.bytes.push(mask | 126);
                self.bytes.extend_from_slice(&(length as u16).to_be_bytes());
            }
            _ => {
                self.bytes.push(mask | 127);
                self.bytes.extend_from_slice(&(length as u64).to_be_bytes());
            }
        }
        if masked {
            self.bytes.extend_from_slice(&[1, 2, 3, 4]);
        }
        self.bytes.resize(self.bytes.len() + length, b'x');
        self.frames.push((start..self.bytes.len(), message));
    }
    fn message(&mut self, length: usize, masked: bool, fragmented: bool) {
        let id = self.messages;
        self.messages += 1;
        if fragmented {
            self.frame(0x01, length / 2, masked, Some(id));
            self.frame(0x89, 1, masked, None);
            self.frame(0x80, length - length / 2, masked, Some(id));
        } else {
            self.frame(0x81, length, masked, Some(id));
        }
    }
    fn expected(&self, chunks: &[Range<usize>]) -> Vec<Span> {
        let mut result = vec![None; self.messages];
        for (index, chunk) in chunks.iter().enumerate() {
            let contributing: Vec<_> = self
                .frames
                .iter()
                .filter(|(frame, _)| frame.start < chunk.end && chunk.start < frame.end)
                .collect();
            let mut span = batch(index);
            span.shared = contributing.len() > 1;
            for (_, message) in contributing {
                if let Some(message) = message {
                    let entry: &mut Option<Span> = &mut result[*message];
                    if let Some(current) = entry {
                        current.last = span.last;
                        current.available &= span.available;
                        current.shared |= span.shared;
                    } else {
                        *entry = Some(span);
                    }
                }
            }
        }
        result.into_iter().map(Option::unwrap).collect()
    }
}
fn chunks(length: usize, size: usize) -> Vec<Range<usize>> {
    (0..length)
        .step_by(size)
        .map(|start| start..(start + size).min(length))
        .collect()
}
fn tracker(capacity: usize) -> Tracker<Span> {
    let mut tracker = Tracker::new(capacity);
    tracker.bytes(b"HTTP/1.1 101 Switching Protocols\r\n\r\n", batch(0));
    tracker
}
fn verify(tracker: &mut Tracker<Span>, expected: &[Span], capacity: usize) {
    assert!(!tracker.invalid);
    assert_eq!(tracker.completed, expected.len() as u64);
    let lost = expected.len().saturating_sub(capacity);
    assert_eq!(tracker.lost, lost as u64);
    for (index, span) in expected.iter().enumerate() {
        assert_eq!(
            tracker.take(index as u64 + 1),
            (index >= lost).then_some(*span)
        );
    }
    assert!(tracker.ready.is_empty());
}

#[test]
fn wire_positions_preserve_every_header_width_fragment_control_and_missing_batch() {
    for masked in [false, true] {
        for fragmented in [false, true] {
            let mut input = Input::new();
            for length in [0, 1, 125, 126, 65535, 65536] {
                input.message(length, masked, fragmented);
            }
            for size in [1, 2, 3, 7, 17, 65536, input.bytes.len()] {
                let chunks = chunks(input.bytes.len(), size);
                let expected = input.expected(&chunks);
                for capacity in [2, input.messages] {
                    let mut tracker = tracker(capacity);
                    for (index, chunk) in chunks.iter().enumerate() {
                        tracker.bytes(&input.bytes[chunk.clone()], batch(index));
                    }
                    verify(&mut tracker, &expected, capacity);
                }
            }
        }
    }
}

#[test]
fn reserved_opcodes_fail_after_every_complete_header_width() {
    for length in [0, 126, 65536] {
        for masked in [false, true] {
            let mut input = Input::new();
            input.frame(0x83, length, masked, Some(0));
            let header =
                2 + if length == 126 {
                    2
                } else if length == 65536 {
                    8
                } else {
                    0
                } + if masked { 4 } else { 0 };
            for split in 0..=header {
                let mut tracker = tracker(1);
                tracker.bytes(&input.bytes[..split], batch(0));
                assert_eq!(tracker.invalid, split == header);
                tracker.bytes(&input.bytes[split..], batch(1));
                assert!(tracker.invalid);
                assert_eq!(tracker.lost, 1);
                assert_eq!(tracker.completed, 0);
            }
        }
    }
}

#[test]
#[ignore = "explicit release-perf observer component; batch totals are not event tails"]
fn passive_frame_association_screen() {
    let cohort = std::env::var("PAX_FRAME_SCREEN_CASE").expect("select a fixed screen cohort");
    let (length, fragmented, split) = match cohort.as_str() {
        "short" => (32, false, false),
        "extended" => (256, false, false),
        "fragmented" => (256, true, false),
        "split" => (256, false, true),
        _ => panic!("unknown frame screen cohort"),
    };
    let mut input = Input::new();
    for _ in 0..256 {
        input.message(length, false, fragmented);
    }
    let chunks: Vec<_> = if split {
        input
            .frames
            .iter()
            .flat_map(|(frame, _)| {
                [
                    frame.start..frame.start + 1,
                    frame.start + 1..frame.start + 2,
                    frame.start + 2..frame.start + 3,
                    frame.start + 3..frame.end,
                ]
            })
            .collect()
    } else {
        std::iter::once(0..input.bytes.len()).collect()
    };
    let expected = input.expected(&chunks);
    let mut times = Vec::with_capacity(4096);
    for iteration in 0..4608 {
        let mut tracker = tracker(input.messages);
        let start = Instant::now();
        for (index, chunk) in chunks.iter().enumerate() {
            black_box(&mut tracker).bytes(black_box(&input.bytes[chunk.clone()]), batch(index));
        }
        let ns = start.elapsed().as_nanos() as u64;
        if iteration >= 512 {
            times.push(ns);
        }
        verify(&mut tracker, &expected, input.messages);
    }
    times.sort_unstable();
    println!(
        "{}",
        serde_json::json!({
            "kind":"passive_frame_association_component", "cohort":cohort,
            "input_digest":blake3::hash(&input.bytes).to_hex().to_string(),
            "messages_per_batch":input.messages,"chunks_per_batch":chunks.len(),
            "metadata_bytes":std::mem::size_of::<Span>(),"warmup_batches":512,"samples":times.len(),
            "batch_p50_ns":times[2047],"batch_p99_ns":times[4055],
            "scope":"passive header/metadata observer; synthetic metadata, no TLS or network; allocation and exact byte-position oracle outside timing; raw batch percentiles include two clock reads and are not individual event tails"
        })
    );
}

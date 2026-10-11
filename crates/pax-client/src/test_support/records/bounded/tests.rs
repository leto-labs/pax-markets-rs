//! Independent generated-record partitions, authenticated bytes and refusal paths.
use super::*;

fn ranges(bytes: &[u8]) -> Vec<(u64, u64)> {
    let mut at = 0usize;
    let mut ranges = Vec::new();
    while at < bytes.len() {
        let size = usize::from(u16::from_be_bytes([bytes[at + 3], bytes[at + 4]]));
        let end = at + size + 5;
        assert!(end <= bytes.len());
        ranges.push((at as u64, end as u64));
        at = end;
    }
    ranges
}

#[test]
fn authenticated_plaintext_and_intervals_ignore_input_chunk_boundaries() {
    let first = b"first record";
    let long = vec![b'z'; 2049];
    let last = b"last record";
    let input: &[&[u8]] = &[first, &long, last];
    for chunk in [1, 3, 5, 17, 1472, 65536] {
        let mut prepared = prepare(input, 256, 256).unwrap();
        let mut cipher_at = 0;
        let mut plain_at = 0;
        let mut expected_ranges = Vec::new();
        for (batch, expected) in prepared.batches.iter().zip(input) {
            for (start, end) in ranges(&batch.ciphertext) {
                // This verified TLS 1.3 fixture emits no padding: encrypted app
                // bytes include one inner type and a 16-byte authentication tag.
                let plain_end = plain_at + end - start - 5 - 17;
                expected_ranges.push((cipher_at + start, cipher_at + end, plain_at, plain_end));
                plain_at = plain_end;
            }
            let mut output = vec![0; expected.len()];
            let written = prepared
                .receiver
                .receive(&batch.ciphertext, &mut output, chunk)
                .unwrap();
            assert_eq!(written, expected.len());
            assert_eq!(&output, expected);
            assert!(!prepared.receiver.has_partial_record());
            cipher_at += batch.ciphertext.len() as u64;
        }
        let actual: Vec<_> = prepared
            .receiver
            .records()
            .enumerate()
            .map(|(index, record)| {
                assert_eq!(record.ordinal, index as u64 + 1);
                (
                    record.cipher_start,
                    record.cipher_end,
                    record.plain_start,
                    record.plain_end,
                )
            })
            .collect();
        assert_eq!(actual, expected_ranges);
        let counts = prepared.receiver.counts();
        assert_eq!(counts.records as usize, expected_ranges.len());
        assert_eq!(counts.cipher_bytes, cipher_at);
        assert_eq!(
            counts.plain_bytes,
            input.iter().map(|bytes| bytes.len() as u64).sum::<u64>()
        );
        assert_eq!(counts.lost, 0);
        assert!(counts.reads >= counts.records * 2);
    }
}

#[test]
fn partial_headers_and_bodies_keep_state_across_separate_input_calls() {
    let plain = vec![b'x'; 1000];
    for cut in [1, 4, 5, 6, 255, 500, 999] {
        let mut prepared = observe(super::super::prepare(&[&plain], 256).unwrap(), 64).unwrap();
        let encrypted = &prepared.batches[0].ciphertext;
        let mut output = vec![0; plain.len()];
        let first = prepared
            .receiver
            .receive(&encrypted[..cut], &mut output, 17)
            .unwrap();
        assert!(prepared.receiver.has_partial_record());
        let rest = prepared
            .receiver
            .receive(&encrypted[cut..], &mut output[first..], 17)
            .unwrap();
        assert_eq!(first + rest, plain.len());
        assert_eq!(output, plain);
        assert!(!prepared.receiver.has_partial_record());
        assert_eq!(
            prepared.receiver.counts().cipher_bytes as usize,
            encrypted.len()
        );
    }
}

#[test]
fn valid_encrypted_key_updates_have_empty_plain_intervals() {
    let input: &[&[u8]] = &[b"first", b"second", b"third"];
    let mut prepared = prepare_with_key_updates(input, 256, 64).unwrap();
    for (batch, expected) in prepared.batches.iter().zip(input) {
        let mut output = vec![0; expected.len()];
        assert_eq!(
            prepared
                .receiver
                .receive(&batch.ciphertext, &mut output, 17)
                .unwrap(),
            expected.len()
        );
        assert_eq!(&output, expected);
        assert!(batch.records >= 2);
    }
    let records: Vec<_> = prepared.receiver.records().collect();
    assert_eq!(
        records
            .iter()
            .filter(|r| r.plain_start == r.plain_end)
            .count(),
        3
    );
    assert_eq!(
        records
            .iter()
            .filter(|r| r.plain_start != r.plain_end)
            .count(),
        3
    );
    assert_eq!(prepared.receiver.counts().plain_bytes, 16);
}

#[test]
fn bounded_loss_and_zero_retention_do_not_change_authenticated_bytes() {
    let plain = vec![b'p'; 4096];
    for capacity in [0, 1, 3] {
        let mut prepared =
            observe(super::super::prepare(&[&plain], 64).unwrap(), capacity).unwrap();
        let mut output = vec![0; plain.len()];
        prepared
            .receiver
            .receive(&prepared.batches[0].ciphertext, &mut output, 1472)
            .unwrap();
        assert_eq!(output, plain);
        let counts = prepared.receiver.counts();
        assert_eq!(prepared.receiver.records().len(), capacity);
        assert_eq!(counts.lost, counts.records - capacity as u64);
        if let Some(record) = prepared.receiver.records().next() {
            assert_eq!(record.ordinal, counts.lost + 1);
        }
    }
}

#[test]
fn authentication_output_and_counter_failures_poison_without_false_record() {
    let plain = b"authenticated";
    for failure in ["authentication", "output", "offset", "read_count"] {
        let mut prepared = observe(super::super::prepare(&[plain], 256).unwrap(), 64).unwrap();
        if failure == "authentication" {
            *prepared.batches[0].ciphertext.last_mut().unwrap() ^= 1;
        }
        if failure == "offset" {
            prepared.receiver.state.offset = u64::MAX;
        }
        if failure == "read_count" {
            prepared.receiver.counts.reads = u64::MAX;
        }
        let mut output = vec![0; if failure == "output" { 0 } else { plain.len() }];
        assert!(
            prepared
                .receiver
                .receive(&prepared.batches[0].ciphertext, &mut output, 65536)
                .is_err()
        );
        assert_eq!(prepared.receiver.records().len(), 0);
        assert_eq!(prepared.receiver.counts().records, 0);
        assert!(
            prepared
                .receiver
                .receive(&[], &mut [], 1)
                .unwrap_err()
                .to_string()
                .contains("failed previously")
        );
    }
}

#[test]
fn empty_reads_and_zero_chunk_refusal_do_not_consume_input() {
    let mut state = State::default();
    let mut input = Limited {
        input: &b"header"[..],
        state: &mut state,
    };
    assert_eq!(input.read(&mut []).unwrap(), 0);
    assert_eq!(input.state.offset, 0);
    let mut prepared = observe(super::super::prepare(&[b"ok"], 256).unwrap(), 4).unwrap();
    assert!(
        prepared
            .receiver
            .receive(&prepared.batches[0].ciphertext, &mut [0; 2], 0)
            .is_err()
    );
    let mut output = [0; 2];
    assert_eq!(
        prepared
            .receiver
            .receive(&prepared.batches[0].ciphertext, &mut output, 17)
            .unwrap(),
        2
    );
    assert_eq!(&output, b"ok");
    assert!(observe(super::super::prepare(&[b"ok"], 256).unwrap(), 4097).is_err());
}

#[test]
fn later_authentication_failure_preserves_only_prior_verified_intervals() {
    let mut prepared = observe(
        super::super::prepare(&[b"first", b"second"], 256).unwrap(),
        8,
    )
    .unwrap();
    let first_bytes = prepared.batches[0].ciphertext.len();
    let mut encrypted = prepared.batches[0].ciphertext.clone();
    encrypted.extend_from_slice(&prepared.batches[1].ciphertext);
    *encrypted.last_mut().unwrap() ^= 1;
    let mut output = [0; 11];
    assert!(
        prepared
            .receiver
            .receive(&encrypted, &mut output, 65536)
            .is_err()
    );
    let records: Vec<_> = prepared.receiver.records().collect();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].cipher_end, first_bytes as u64);
    assert_eq!(records[0].plain_end, 5);
    assert_eq!(&output[..5], b"first");
    assert!(prepared.receiver.finish().is_err());
}

#[test]
fn truncated_final_input_is_explicit_and_a_completed_reader_never_crosses_records() {
    let mut prepared = observe(super::super::prepare(&[b"plain"], 256).unwrap(), 8).unwrap();
    let encrypted = &prepared.batches[0].ciphertext;
    let mut output = [0; 5];
    prepared
        .receiver
        .receive(&encrypted[..encrypted.len() - 1], &mut output, 17)
        .unwrap();
    assert!(prepared.receiver.finish().is_err());
    assert_eq!(prepared.receiver.records().len(), 0);
    assert_eq!(
        prepared
            .receiver
            .receive(&encrypted[encrypted.len() - 1..], &mut output, 17)
            .unwrap(),
        5
    );
    prepared.receiver.finish().unwrap();
    assert_eq!(&output, b"plain");

    let mut state = State {
        complete: Some((1, 0, 5)),
        ..State::default()
    };
    let mut input = Limited {
        input: &b"next"[..],
        state: &mut state,
    };
    assert_eq!(input.read(&mut [0; 8]).unwrap(), 0);
    assert_eq!(input.input, b"next");
    assert_eq!(state.offset, 0);
}

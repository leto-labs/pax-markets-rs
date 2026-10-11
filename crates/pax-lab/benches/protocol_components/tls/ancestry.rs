//! Post-timing independent record partitions and an untimed authenticated control.
use pax_client::test_support::records::{self, bounded::Record};
use pax_lab::Error;
use serde_json::{Value, json};

pub fn report(
    prepared: &records::bounded::Prepared,
    capacity: usize,
    mut ordinary: records::Prepared,
    plaintext: &[&[u8]],
    chunk: usize,
) -> Result<Value, Error> {
    prepared.receiver.finish()?;
    if prepared.cipher != ordinary.cipher || prepared.version != ordinary.version {
        return Err("ancestry counterpart negotiated a different TLS suite".into());
    }
    let expected = partitions(&prepared.batches)?;
    if expected != partitions(&ordinary.batches)? || plaintext.len() != ordinary.batches.len() {
        return Err("ancestry counterpart has different record or plaintext boundaries".into());
    }
    let mut ordinary_reads = 0u64;
    for (batch, plain) in ordinary.batches.iter().zip(plaintext) {
        let mut output = vec![0; plain.len()];
        let (written, reads) =
            ordinary
                .receiver
                .receive_counted(&batch.ciphertext, &mut output, chunk)?;
        if written != plain.len() || output != *plain {
            return Err("ancestry counted counterpart changed authenticated plaintext".into());
        }
        ordinary_reads = ordinary_reads
            .checked_add(reads)
            .ok_or("ordinary read count overflow")?;
    }
    let counts = prepared.receiver.counts();
    let retained = expected.len().min(capacity);
    let suffix = &expected[expected.len() - retained..];
    if counts.records as usize != expected.len()
        || counts.lost as usize != expected.len() - retained
        || !prepared
            .receiver
            .records()
            .copied()
            .eq(suffix.iter().copied())
        || counts.cipher_bytes != expected.last().map_or(0, |r| r.cipher_end)
        || counts.plain_bytes != expected.last().map_or(0, |r| r.plain_end)
    {
        return Err(
            "ancestry work or retained intervals disagree with independent partitions".into(),
        );
    }
    Ok(json!({
        "diagnostic":true,"ordinary_performance_eligible":false,
        "scope":"intrusive offline record boundaries; post-handshake fixture offsets, no packet clock or socket syscall claim",
        "retention_capacity":capacity,"retained":retained,"lost":counts.lost,
        "records":counts.records,"ciphertext_bytes":counts.cipher_bytes,"plaintext_bytes":counts.plain_bytes,
        "read_tls_calls":counts.reads,"ordinary_read_tls_calls":ordinary_reads,
        "extra_read_tls_calls":i128::from(counts.reads)-i128::from(ordinary_reads),
        "ordinary_count_scope":"untimed fresh authenticated counterpart; same suite, plaintext and record partitions, not identical ciphertext bytes",
        "intervals_verified":true,"authenticated_counterpart_verified":true,
        "first_retained":suffix.first().map(interval),"last_retained":suffix.last().map(interval),
        "included_cost":"record-limited reads, process_new_packets, plaintext copying, checked counters and bounded metadata retention",
        "excluded_cost":"handshake, encryption, output allocation, independent partitions and counted counterpart validation"
    }))
}
fn interval(record: &Record) -> Value {
    json!({"ordinal":record.ordinal,"cipher_start":record.cipher_start,"cipher_end":record.cipher_end,
        "plain_start":record.plain_start,"plain_end":record.plain_end})
}
fn partitions(batches: &[records::Batch]) -> Result<Vec<Record>, Error> {
    let mut expected = Vec::new();
    let mut cipher_base = 0u64;
    let mut plain_base = 0u64;
    for batch in batches {
        let mut at = 0usize;
        let mut batch_plain = 0usize;
        let mut batch_records = 0usize;
        while at < batch.ciphertext.len() {
            let header = batch
                .ciphertext
                .get(at..at + 5)
                .ok_or("truncated fixture TLS header")?;
            let bytes = usize::from(u16::from_be_bytes([header[3], header[4]]));
            let end = at
                .checked_add(5 + bytes)
                .ok_or("fixture TLS offset overflow")?;
            if end > batch.ciphertext.len() || header[0] != 23 {
                return Err("invalid fixture application record partition".into());
            }
            // These generated TLS 1.3 application-only suites have no padding:
            // one inner content type and a 16-byte tag. Not a general TLS model.
            let plain = bytes
                .checked_sub(17)
                .ok_or("fixture record lacks TLS 1.3 overhead")?;
            expected.push(Record {
                ordinal: expected.len() as u64 + 1,
                cipher_start: cipher_base + at as u64,
                cipher_end: cipher_base + end as u64,
                plain_start: plain_base + batch_plain as u64,
                plain_end: plain_base + (batch_plain + plain) as u64,
            });
            batch_plain += plain;
            batch_records += 1;
            at = end;
        }
        if batch_plain != batch.plaintext_bytes || batch_records != batch.records {
            return Err("fixture byte/record partition changed".into());
        }
        cipher_base = cipher_base
            .checked_add(batch.ciphertext.len().try_into()?)
            .ok_or("cipher total overflow")?;
        plain_base = plain_base
            .checked_add(batch_plain.try_into()?)
            .ok_or("plain total overflow")?;
    }
    Ok(expected)
}

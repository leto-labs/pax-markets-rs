//! Optional cold guest intervals. Older or ambiguous output stays unavailable.
use serde_json::{Value, json};

const MAX_NS: u64 = 3_600_000_000_000;

pub(super) fn read(receipt: &Value) -> Option<Value> {
    let output = receipt["StandardOutputContent"].as_str()?;
    if output.len() > 65536 {
        return None;
    }
    let rows: Vec<Value> = output
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|row| {
            row["kind"] == "pax_release_guest_timing" || row["kind"] == "pax_release_preflight"
        })
        .collect();
    let phases: Vec<_> = rows
        .iter()
        .filter(|row| row["kind"] == "pax_release_guest_timing")
        .collect();
    if !phases.is_empty() {
        if phases.len() != 1 {
            return None;
        }
        let row = phases[0];
        if row["schema"] != 1 || row["clock"] != "guest_monotonic" {
            return None;
        }
        let stage = row["stage_ns"].as_u64()?;
        let startup = row["startup_ns"].as_u64()?;
        let health = row["health_ns"].as_u64()?;
        let total = row["total_ns"].as_u64()?;
        if total > MAX_NS || stage.checked_add(startup)?.checked_add(health)? != total {
            return None;
        }
        return Some(
            json!({"phase":"activation","clock":"guest_monotonic", "stage_ns":stage,
            "startup_ns":startup,"health_ns":health,"total_ns":total}),
        );
    }
    if rows.len() != 1 {
        return None;
    }
    let eligible = rows[0]["eligible"].as_bool()?;
    let total = rows[0]["validation_ns"].as_u64()?;
    let health = rows[0]["health_ns"].as_u64()?;
    (health <= total && total <= MAX_NS).then(|| {
        json!({"phase":"preflight","clock":"guest_monotonic", "healthy_match":eligible,"health_ns":health,"total_ns":total})
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn receipt(rows: &[Value]) -> Value {
        json!({"StandardOutputContent":rows.iter().map(Value::to_string).collect::<Vec<_>>().join("\n")})
    }

    #[test]
    fn nested_partitions_and_noop_health_keep_their_own_clock() {
        let preflight = json!({"kind":"pax_release_preflight","eligible":true,"health_ns":10,"validation_ns":30});
        assert_eq!(
            read(&receipt(std::slice::from_ref(&preflight))).unwrap()["phase"],
            "preflight"
        );
        let phases = json!({"kind":"pax_release_guest_timing","schema":1,"clock":"guest_monotonic","stage_ns":40,"startup_ns":20,"health_ns":10,"total_ns":70});
        let parsed = read(&receipt(&[preflight.clone(), phases.clone()])).unwrap();
        assert_eq!(parsed["total_ns"], 70);
        assert_eq!(parsed["phase"], "activation");
        assert!(read(&receipt(&[phases.clone(), phases.clone()])).is_none());
        for (key, value) in [
            ("total_ns", json!(71)),
            ("schema", json!(2)),
            ("clock", json!("operator_monotonic")),
            ("health_ns", json!(-1)),
            ("stage_ns", json!(u64::MAX)),
        ] {
            let mut changed = phases.clone();
            changed[key] = value;
            assert!(read(&receipt(&[preflight.clone(), changed])).is_none());
        }
        assert!(read(&json!({"StandardOutputContent":"pax_release_noop=1\n"})).is_none());
        let mut invalid = preflight;
        invalid["health_ns"] = json!(31);
        assert!(read(&receipt(&[invalid])).is_none());
    }
}

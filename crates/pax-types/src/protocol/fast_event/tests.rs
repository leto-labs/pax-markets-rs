use super::*;
use crate::protocol::{Message, decode, decode_general, event::EventType};

fn parse_text(text: &str) -> Option<Event> {
    super::parse(text.as_bytes())
}

fn encoded(event: &Event) -> String {
    format!(
        r#"{{"type":"md_event_detail","data":{}}}"#,
        serde_json::to_string(event).unwrap()
    )
}
fn example() -> Event {
    let Message::Event(event) =
        decode_general(include_str!("../../../tests/fixtures/xa.json")).unwrap()
    else {
        panic!("event fixture");
    };
    event
}

// Sort borrowed raw values rather than an intermediate numeric Value tree:
// extreme i128 tokens must remain exact in this independent general-decoder gate.
fn sorted(event: &Event) -> String {
    use serde_json::value::RawValue;
    use std::collections::BTreeMap;
    let text = serde_json::to_string(event).unwrap();
    let groups: BTreeMap<&str, &RawValue> = serde_json::from_str(&text).unwrap();
    let groups: BTreeMap<_, _> = groups
        .into_iter()
        .map(|(key, raw)| {
            let fields: BTreeMap<&str, &RawValue> = serde_json::from_str(raw.get()).unwrap();
            (
                key,
                RawValue::from_string(serde_json::to_string(&fields).unwrap()).unwrap(),
            )
        })
        .collect();
    format!(
        r#"{{"data":{},"type":"md_event_detail"}}"#,
        serde_json::to_string(&groups).unwrap()
    )
}

#[test]
fn sorted_compact_events_match_independent_general_decoding_and_integer_limits() {
    for line in include_str!("../../../../pax-book/tests/fixtures/public-active.jsonl").lines() {
        if let Message::Event(event) = decode_general(line).unwrap() {
            let text = sorted(&event);
            assert_eq!(super::parse_sorted(text.as_bytes()), Some(event));
            assert_eq!(decode_general(&text).unwrap(), Message::Event(event));
            assert_eq!(decode(text.as_bytes()).unwrap(), Message::Event(event));
        }
    }
    for signed in [i128::MIN, -1, 0, i128::MAX] {
        let mut event = example();
        event.data.event_delta_qty = Signed(signed);
        event.data.event_total_px = Signed(signed);
        event.data.seq_num = Sequence(u64::MAX);
        event.order.market_id = MarketId(u64::MAX);
        event.tob.ask_qty = Quantity(u64::MAX);
        event.tob.ask_ticks = PriceTicks(u32::MAX);
        let text = sorted(&event);
        for text in [text.clone(), format!("{text} \r\n\t")] {
            assert_eq!(super::parse_sorted(text.as_bytes()), Some(event));
            assert_eq!(decode_general(&text).unwrap(), Message::Event(event));
        }
    }
}

#[test]
fn sorted_recognition_never_skips_extensions_escapes_or_invalid_json() {
    let event = example();
    let text = sorted(&event);
    for changed in [
        text.replace("\"seq_num\":3", "\"seq_num\":03"),
        text.replace("\"seq_num\":3", "\"seq_num\":18446744073709551616"),
        text.replace("\"seq_num\":3", "\"seq_num\":3.0"),
        text.replace("\"event_qual\":4", "\"event_qual\":9"),
        format!("{text}{{}}"),
        format!("{text}\u{000b}"),
        text.replace(
            "\"type\":\"md_event_detail\"",
            "\"type\":\"md_event_detail\",\"type\":\"pong\"",
        ),
    ] {
        assert_ne!(changed, text);
        assert!(super::parse_sorted(changed.as_bytes()).is_none());
        assert!(decode(changed.as_bytes()).is_err());
        assert!(decode_general(&changed).is_err());
    }
    for changed in [
        text.replace("\"seq_num\":3", "\"seq_num\":\"\\u0033\""),
        text.replace(
            "\"event_abs_ticks\":",
            "\"extra\":true,\"event_abs_ticks\":",
        ),
        text.replace("\"type\":", "\"t\\u0079pe\":"),
    ] {
        assert_ne!(changed, text);
        assert!(super::parse_sorted(changed.as_bytes()).is_none());
        assert_eq!(decode(changed.as_bytes()).unwrap(), Message::Event(event));
        assert_eq!(decode_general(&changed).unwrap(), Message::Event(event));
    }
}

#[test]
fn every_retained_public_event_matches_the_general_decoder() {
    let mut count = 0;
    for stream in [
        include_str!("../../../../pax-book/tests/fixtures/public-sample.jsonl"),
        include_str!("../../../../pax-book/tests/fixtures/public-active.jsonl"),
    ] {
        for line in stream.lines() {
            let expected = decode_general(line).unwrap();
            if let Message::Event(event) = expected {
                assert_eq!(parse_text(line), Some(event));
                count += 1;
            } else {
                assert!(parse_text(line).is_none());
            }
        }
    }
    assert_eq!(count, 284);
}

#[test]
fn numeric_limits_and_both_integer_encodings_are_exact() {
    let mut event = example();
    event.tob.ask_qty = Quantity(u64::MAX);
    event.tob.ask_ticks = PriceTicks(u32::MAX);
    event.data.event_delta_qty = Signed(i128::MIN);
    event.data.event_total_px = Signed(i128::MAX);
    event.data.seq_num = Sequence(u64::MAX);
    event.order.market_id = MarketId(u64::MAX);
    let numeric = encoded(&event);
    let quoted = numeric
        .replace(&i128::MIN.to_string(), &format!("\"{}\"", i128::MIN))
        .replace(&i128::MAX.to_string(), &format!("\"{}\"", i128::MAX))
        .replace(&u64::MAX.to_string(), &format!("\"{}\"", u64::MAX))
        .replace(&u32::MAX.to_string(), &format!("\"{}\"", u32::MAX));
    for text in [numeric, quoted] {
        assert_eq!(parse_text(&text), Some(event));
        assert_eq!(decode_general(&text).unwrap(), Message::Event(event));
    }
    for value in ["0", "-0", "\"000\"", "\"-00\""] {
        let text = encoded(&example()).replace(
            "\"event_tick_depth\":2",
            &format!("\"event_tick_depth\":{value}"),
        );
        // Check the fixture has the replaced field, rather than a vacuous test.
        assert!(text.contains(&format!("\"event_tick_depth\":{value}")));
        assert!(parse_text(&text).is_some());
        assert_eq!(
            decode(text.as_bytes()).unwrap(),
            decode_general(&text).unwrap()
        );
    }
}

#[test]
fn unrecognized_layouts_use_the_full_decoder_without_losing_contracts() {
    let compact = encoded(&example());
    let value: serde_json::Value = serde_json::from_str(&compact).unwrap();
    for text in [
        serde_json::to_string_pretty(&value).unwrap(),
        format!(r#"{{"data":{},"type":"md_event_detail"}}"#, value["data"]),
        compact.replacen("\"tob\":{", "\"extra\":true,\"tob\":{", 1),
        compact.replacen("\"type\"", "\"t\\u0079pe\"", 1),
        compact.replacen("\"seq_num\":3", "\"seq_num\":\"\\u0033\"", 1),
        format!("{},\"extra\":true}}", compact.strip_suffix('}').unwrap()),
    ] {
        assert!(parse_text(&text).is_none(), "{text}");
        assert_eq!(decode(text.as_bytes()).unwrap(), Message::Event(example()));
    }
    for text in [
        compact.replace("\"seq_num\":3", "\"seq_num\":03"),
        compact.replace("\"event_tick_depth\":2", "\"event_tick_depth\":-00"),
        compact.replace("\"seq_num\":3", "\"seq_num\":3.0"),
        compact.replace("\"seq_num\":3", "\"seq_num\":3e0"),
        compact.replace("\"seq_num\":3", "\"seq_num\":\"+3\""),
        compact.replace("\"origin\":1", "\"origin\":9"),
        compact.replace("\"event_qual\":4", "\"event_qual\":9"),
        compact.replace("\"seq_num\":3", "\"seq_num\":18446744073709551616"),
        format!("{},\"type\":\"pong\"}}", compact.strip_suffix('}').unwrap()),
        format!("{compact}\u{000b}"),
        format!("{compact}\u{000c}"),
        format!("{compact}\u{00a0}"),
        format!("{compact}\u{2028}"),
        format!("{compact}\u{feff}"),
        format!("{compact}{{}}"),
    ] {
        assert!(parse_text(&text).is_none(), "{text}");
        assert!(decode(text.as_bytes()).is_err(), "{text}");
    }
    assert_eq!(parse_text(&format!("{compact} \r\n\t")), Some(example()));
    for (key, value) in [
        ("event_type", 2),
        ("event_qual", 4),
        ("event_side", 1),
        ("side", 2),
        ("tif", 2),
        ("origin", 1),
    ] {
        let text = compact.replace(
            &format!("\"{key}\":{value}"),
            &format!("\"{key}\":\"{value}\""),
        );
        assert_ne!(text, compact);
        assert!(parse_text(&text).is_none(), "{text}");
        assert!(decode(text.as_bytes()).is_err(), "{text}");
    }
}

#[test]
fn quoted_units_preserve_the_general_integer_string_grammar() {
    let event = example();
    let compact = encoded(&event);
    for (field, original) in [
        ("ask_ticks", event.tob.ask_ticks.0.to_string()),
        ("seq_num", event.data.seq_num.0.to_string()),
        ("event_delta_qty", event.data.event_delta_qty.0.to_string()),
    ] {
        for token in [
            "", "0", "00", "-0", "-00", "+0", "+1", "--1", " 1", "1 ", "1_0", "1.0", "1e0", "é",
            "0é", "١", "\\u0031", "\"",
        ] {
            let replacement = format!("\"{field}\":{}", serde_json::to_string(token).unwrap());
            let text = compact.replace(&format!("\"{field}\":{original}"), &replacement);
            assert_ne!(text, compact, "fixture field must be replaced");
            let expected = decode_general(&text).ok();
            if let Some(event) = parse_text(&text) {
                assert_eq!(Some(Message::Event(event)), expected, "{text}");
            }
            assert_eq!(decode(text.as_bytes()).ok(), expected, "{text}");
        }
    }
}

#[test]
fn padded_integer_boundaries_remain_exact() {
    let event = example();
    let compact = encoded(&event);
    for (field, original, limit) in [
        (
            "seq_num",
            event.data.seq_num.0.to_string(),
            u64::MAX.to_string(),
        ),
        (
            "ask_ticks",
            event.tob.ask_ticks.0.to_string(),
            u32::MAX.to_string(),
        ),
        (
            "event_delta_qty",
            event.data.event_delta_qty.0.to_string(),
            i128::MIN.to_string(),
        ),
        (
            "event_total_px",
            event.data.event_total_px.0.to_string(),
            i128::MAX.to_string(),
        ),
    ] {
        for zeroes in [0, 1, 7, 8, 15, 16, 17, 31, 32, 33, 64, 257, 1024] {
            let digits = limit.strip_prefix('-').unwrap_or(&limit);
            let sign = if limit.starts_with('-') { "-" } else { "" };
            let padded = format!("{sign}{}{digits}", "0".repeat(zeroes));
            let text = compact.replace(
                &format!("\"{field}\":{original}"),
                &format!("\"{field}\":\"{padded}\""),
            );
            let Message::Event(expected) = decode_general(&text).unwrap() else {
                panic!("event")
            };
            assert_eq!(
                parse_text(&text),
                Some(expected),
                "{field}, padding {zeroes}"
            );
            let overflow = text.replace(&format!("\"{padded}\""), &format!("\"{padded}0\""));
            assert!(decode_general(&overflow).is_err());
            assert!(parse_text(&overflow).is_none(), "{field}, padding {zeroes}");
        }
    }
}

#[test]
fn all_numeric_code_values_preserve_general_decoder_results() {
    // An add's qualifier is inapplicable; even multi-digit values remain
    // compatible via general decoding. Removal qualifiers are constrained.
    let mut add = example();
    add.data.event_type = EventType::Add;
    for event in [example(), add] {
        let compact = encoded(&event);
        for (field, original) in [
            ("event_type", u8::from(event.data.event_type)),
            ("event_qual", event.data.event_qual),
            ("event_side", u8::from(event.data.event_side)),
            ("side", u8::from(event.order.side)),
            ("tif", event.order.tif),
            ("origin", event.order.origin),
        ] {
            for code in 0..=u8::MAX {
                let text = compact.replace(
                    &format!("\"{field}\":{original}"),
                    &format!("\"{field}\":{code}"),
                );
                let expected = decode_general(&text).ok();
                if let Some(event) = parse_text(&text) {
                    assert_eq!(Some(Message::Event(event)), expected, "{text}");
                }
                assert_eq!(decode(text.as_bytes()).ok(), expected, "{text}");
            }
        }
    }
}

#[test]
fn byte_mutations_cannot_bypass_general_json_or_protocol_validation() {
    let originals = [
        encoded(&example()),
        include_str!("../../../../pax-book/tests/fixtures/public-active.jsonl")
            .lines()
            .find(|line| line.starts_with(r#"{"type":"md_event_detail""#))
            .unwrap()
            .to_owned(),
    ];
    let mut recognized = 0;
    let mut check = |bytes: &[u8]| {
        if let Some(event) = super::parse(bytes) {
            assert!(
                bytes.is_ascii(),
                "compact recognition must account for every byte"
            );
            let text = std::str::from_utf8(bytes).unwrap();
            assert_eq!(
                decode_general(text).unwrap(),
                Message::Event(event),
                "{text}"
            );
            recognized += 1;
        }
        if std::str::from_utf8(bytes).is_err() {
            assert!(matches!(
                decode(bytes),
                Err(crate::protocol::DecodeError::Encoding(_))
            ));
        }
    };
    for text in originals {
        let bytes = text.as_bytes();
        for position in 0..bytes.len() {
            check(&bytes[..position]);
            let mut removed = bytes.to_vec();
            removed.remove(position);
            check(&removed);
            for replacement in u8::MIN..=u8::MAX {
                let mut modified = bytes.to_vec();
                modified[position] = replacement;
                check(&modified);
            }
        }
    }
    assert!(
        recognized > 100,
        "exercise successful altered numeric values too"
    );
}

#[test]
fn seeded_full_width_integers_match_the_general_decoder() {
    let mut seed = 0xe9b2_7531_8674_c0adu64;
    let mut next = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for index in 0..4096 {
        let mut event = example();
        event.tob.ask_ticks = PriceTicks((next() >> (index % 32)) as u32);
        event.tob.ask_qty = Quantity(next() >> (index % 64));
        event.data.seq_num = Sequence(next());
        let wide = ((u128::from(next()) << 64) | u128::from(next())) >> (index % 128);
        event.data.event_delta_qty = Signed(wide as i128);
        event.data.event_total_px = Signed((wide.rotate_left(47)) as i128);
        let numeric = encoded(&event);
        let mut quoted = numeric.clone();
        for (field, value) in [
            ("ask_ticks", event.tob.ask_ticks.0.to_string()),
            ("ask_qty", event.tob.ask_qty.0.to_string()),
            ("seq_num", event.data.seq_num.0.to_string()),
            ("event_delta_qty", event.data.event_delta_qty.0.to_string()),
            ("event_total_px", event.data.event_total_px.0.to_string()),
        ] {
            let (sign, digits) = value
                .strip_prefix('-')
                .map_or(("", value.as_str()), |v| ("-", v));
            let padded = format!("{sign}{}{digits}", "0".repeat(index % 65));
            quoted = quoted.replace(
                &format!("\"{field}\":{value}"),
                &format!("\"{field}\":\"{padded}\""),
            );
        }
        for text in [numeric, quoted] {
            assert_eq!(decode_general(&text).unwrap(), Message::Event(event));
            assert_eq!(parse_text(&text), Some(event), "case {index}: {text}");
        }
    }
}

//! Public protocol and metadata behavior, independent of the live API.
use pax_types::{
    market::{ExchangeInfo, MetadataError, Ratio},
    protocol::{DecodeError, Message, decode},
    units::{MarketId, PriceTicks, Quantity},
};

const METADATA: &[u8] = include_bytes!("fixtures/metadata.json");
const SNAPSHOT: &[u8] = include_bytes!("fixtures/snapshot.json");
const XA: &[u8] = include_bytes!("fixtures/xa.json");

#[test]
fn utf8_validation_covers_ignored_fields_without_rejecting_valid_unicode() {
    for invalid in [
        &b"{\"type\":\"po\xffng\"}"[..],
        &b"{\"type\":\"pong\",\"extra\":\"\xff\"}"[..],
        &b"{\"type\":\"error\",\"message\":\"\xc0\xaf\"}"[..],
        &b"{\"type\":\"pong\",\"extra\":\"\xed\xa0\x80\"}"[..],
    ] {
        assert!(matches!(decode(invalid), Err(DecodeError::Encoding(_))));
    }
    assert_eq!(
        decode("{\"type\":\"error\",\"message\":\"é 🚀\\u0031\"}".as_bytes()).unwrap(),
        Message::Error("é 🚀1".into())
    );
    assert_eq!(
        decode("{\"type\":\"pong\",\"extra\":\"é 🚀\"}".as_bytes()).unwrap(),
        Message::Pong
    );
    // UTF-8 validity does not replace JSON escape validation.
    assert!(decode(br#"{"type":"error","message":"\ud800"}"#).is_err());
}

#[test]
fn malformed_quantities_identify_paths_in_both_envelope_orders() {
    let snapshot = String::from_utf8(SNAPSHOT.to_vec()).unwrap();
    let invalid = snapshot.replace("\"2000\"", "\"2.5\"");
    let value: serde_json::Value = serde_json::from_str(&invalid).unwrap();
    for input in [
        invalid,
        format!("{{\"data\":{},\"type\":\"snapshot\"}}", value["data"]),
    ] {
        let error = decode(input.as_bytes()).unwrap_err();
        assert!(
            matches!(&error, DecodeError::Field { path, .. } if path == "data.ask_agg_data[0].qty"),
            "{error}"
        );
    }
    assert!(matches!(
        decode(br#"{"type":"pong"} garbage"#),
        Err(DecodeError::Invalid(_))
    ));
}

#[test]
fn documented_snapshot_and_xa_preserve_signed_fields() {
    let Message::Snapshot(book) = decode(SNAPSHOT).unwrap() else {
        panic!()
    };
    assert_eq!(book.ask_agg_data.len(), 3);
    let Message::Event(event) = decode(XA).unwrap() else {
        panic!()
    };
    assert_eq!(event.data.event_level_delta_qty.0, -4000);
    assert_eq!(event.data.event_total_px.0, -400044000);
}

#[test]
fn control_messages_and_explicit_unsupported_messages() {
    assert_eq!(
        decode(br#"{"type":"pong","extra":42}"#).unwrap(),
        Message::Pong
    );
    assert_eq!(
        decode(br#"{"type":"subscribed","market_ids":[1,"2"]}"#).unwrap(),
        Message::Subscribed(vec![MarketId(1), MarketId(2)])
    );
    assert!(matches!(
        decode(br#"{"type":"other"}"#),
        Err(DecodeError::Unsupported(_))
    ));
    assert!(decode(br#"{"type":"snapshot","data":{}}"#).is_err());
    let invalid = String::from_utf8(XA.to_vec())
        .unwrap()
        .replace("\"event_qual\":4", "\"event_qual\":9");
    assert!(matches!(
        decode(invalid.as_bytes()),
        Err(DecodeError::Unsupported(_))
    ));
}

#[test]
fn metadata_converts_non_default_ticks_exactly() {
    let mut info: ExchangeInfo = serde_json::from_slice(METADATA).unwrap();
    info.markets[0].tick_size = Quantity(125);
    let units = info.resolve(MarketId(1)).unwrap();
    assert_eq!(
        units.price(PriceTicks(123)).unwrap().decimal(8).unwrap(),
        "0.015375"
    );
    assert_eq!(
        units
            .quantity(Quantity(123456789))
            .unwrap()
            .decimal(8)
            .unwrap(),
        "1.23456789"
    );
    assert_eq!(
        units.notional(800000).unwrap(),
        Ratio::new(100000000, 100000000).unwrap()
    );
    assert_eq!(units.notional(u128::MAX), Err(MetadataError::Overflow));
}

#[test]
fn broken_metadata_and_zero_units_are_rejected() {
    let mut info: ExchangeInfo = serde_json::from_slice(METADATA).unwrap();
    info.assets.pop();
    assert!(info.validate().is_err());
    let mut info: ExchangeInfo = serde_json::from_slice(METADATA).unwrap();
    info.markets[0].qty_step_size = Quantity(0);
    assert!(info.validate().is_err());
}

#[test]
fn decimal_rounding_is_explicit_and_bounded() {
    assert_eq!(Ratio::new(2, 3).unwrap().decimal(4).unwrap(), "0.6666");
    assert_eq!(Ratio::new(1, 10000).unwrap().decimal(2).unwrap(), "0");
    assert!(Ratio::new(1, 0).is_err());
    assert!(Ratio::new(1, 2).unwrap().decimal(19).is_err());
}

#[test]
fn envelope_member_order_escaping_and_duplicate_rules_are_preserved() {
    for original in [
        include_str!("fixtures/snapshot.json"),
        include_str!("fixtures/xa.json"),
    ] {
        let expected = pax_types::protocol::decode(original.as_bytes()).unwrap();
        let mut value: serde_json::Value = serde_json::from_str(original).unwrap();
        let data = value["data"].take();
        let kind = value["type"].as_str().unwrap();
        // The exact-value boundaries have separate i128 tests; these fixture
        // values fit this test-only DOM used to permute member order.
        for text in [
            format!("{{\"data\":{data},\"type\":\"{kind}\"}}"),
            format!("{{\"type\":\"{kind}\",\"extra\":[{{\"ignored\":true}}],\"data\":{data}}}"),
            format!("{{\"t\\u0079pe\":\"{kind}\",\"data\":{data}}}"),
        ] {
            assert_eq!(
                pax_types::protocol::decode(text.as_bytes()).unwrap(),
                expected
            );
        }
    }
    for invalid in [
        r#"{"type":"pong","type":"snapshot"}"#,
        r#"{"type":"snapshot","data":null,"data":{}}"#,
        r#"{"type":"pong","market_ids":null,"market_ids":[]}"#,
    ] {
        assert!(pax_types::protocol::decode(invalid.as_bytes()).is_err());
    }
}

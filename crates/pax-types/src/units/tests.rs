use super::*;

#[test]
fn number_and_string_preserve_all_unsigned_bits() {
    for text in ["0", "9007199254740993", "18446744073709551615"] {
        let number: Quantity = serde_json::from_str(text).unwrap();
        let string: Quantity = serde_json::from_str(&format!("\"{text}\"")).unwrap();
        assert_eq!(number, string);
        assert_eq!(number.0, text.parse::<u64>().unwrap());
    }
}

#[test]
fn signed_numbers_and_strings_preserve_i128_boundaries() {
    for value in [i128::MIN, -6000, 0, i128::MAX] {
        let text = value.to_string();
        assert_eq!(
            serde_json::from_str::<Signed>(&text).unwrap(),
            Signed(value)
        );
        assert_eq!(
            serde_json::from_str::<Signed>(&format!("\"{text}\"")).unwrap(),
            Signed(value)
        );
    }
}

#[test]
fn invalid_encodings_and_overflow_are_rejected() {
    for text in [
        "-1",
        "1.0",
        "1e3",
        "true",
        "null",
        "[]",
        "{}",
        "18446744073709551616",
        "\"+1\"",
        "\" 1\"",
        "\"1.0\"",
        "\"\"",
    ] {
        assert!(serde_json::from_str::<Quantity>(text).is_err(), "{text}");
    }
    assert!(serde_json::from_str::<PriceTicks>("4294967296").is_err());
    assert!(serde_json::from_str::<Signed>("170141183460469231731687303715884105728").is_err());
}

#[test]
fn escaped_digits_remain_exact() {
    assert_eq!(
        serde_json::from_str::<Quantity>(r#""\u0031\u0032""#).unwrap(),
        Quantity(12)
    );
    assert_eq!(
        serde_json::from_str::<Signed>(r#""\u002d\u0031\u0032""#).unwrap(),
        Signed(-12)
    );
    assert!(serde_json::from_str::<Signed>(r#""\u002b1""#).is_err());
    assert!(serde_json::from_str::<Quantity>(r#""\u002b1""#).is_err());
}

#[test]
fn wide_integer_string_mutations_match_standard_conversions() {
    for source in [
        "0",
        "12345678",
        "9876543210123456",
        "4294967295",
        "18446744073709551615",
        "170141183460469231731687303715884105727",
        "-170141183460469231731687303715884105728",
        "000000000000000000000000000000000000000000000",
    ] {
        for offset in [0, source.len() / 2, source.len()] {
            for byte in u8::MIN..=u8::MAX {
                let mut text = source.to_owned();
                text.insert(offset, char::from(byte));
                let json = serde_json::to_string(&text).unwrap();
                let allowed = !text.starts_with('+');
                assert_eq!(
                    serde_json::from_str::<Quantity>(&json).ok().map(|v| v.0),
                    text.parse::<u64>().ok().filter(|_| allowed),
                    "quantity {text:?}"
                );
                assert_eq!(
                    serde_json::from_str::<PriceTicks>(&json).ok().map(|v| v.0),
                    text.parse::<u32>().ok().filter(|_| allowed),
                    "ticks {text:?}"
                );
                assert_eq!(
                    serde_json::from_str::<Signed>(&json).ok().map(|v| v.0),
                    text.parse::<i128>().ok().filter(|_| allowed),
                    "signed {text:?}"
                );
            }
        }
    }
}

#[test]
fn short_integer_strings_match_the_wire_grammar() {
    // Independent small-value oracle: a minus is allowed only in the first
    // position of a signed value, and at least one ASCII digit must follow.
    // Enumerate invalid digits/signs in every position, including JSON escapes.
    let alphabet = [
        "", "0", "1", "9", "+", "-", ".", "e", "_", " ", "\n", "\0", "é",
    ];
    for a in alphabet {
        for b in alphabet {
            for c in alphabet {
                let text = format!("{a}{b}{c}");
                let (negative, digits) = text
                    .strip_prefix('-')
                    .map_or((false, text.as_str()), |s| (true, s));
                let expected = (!digits.is_empty()
                    && digits.bytes().all(|byte| byte.is_ascii_digit()))
                .then(|| {
                    let magnitude = digits
                        .bytes()
                        .fold(0i128, |sum, byte| sum * 10 + i128::from(byte - b'0'));
                    if negative { -magnitude } else { magnitude }
                });
                let json = serde_json::to_string(&text).unwrap();
                assert_eq!(
                    serde_json::from_str::<Signed>(&json).ok().map(|v| v.0),
                    expected,
                    "signed {text:?}"
                );
                assert_eq!(
                    serde_json::from_str::<Quantity>(&json)
                        .ok()
                        .map(|v| i128::from(v.0)),
                    expected.filter(|_| !negative),
                    "unsigned {text:?}"
                );
            }
        }
    }
    for invalid in [
        "170141183460469231731687303715884105728",
        "-170141183460469231731687303715884105729",
    ] {
        assert!(serde_json::from_str::<Signed>(invalid).is_err());
        assert!(serde_json::from_str::<Signed>(&serde_json::to_string(invalid).unwrap()).is_err());
    }
}

//! Error-path tracking only after failure; valid messages use the normal parser.
use super::DecodeError;
use serde::Deserialize;

#[cold]
#[inline(never)]
pub(super) fn locate<'de, T: Deserialize<'de>>(
    bytes: &'de [u8],
    prefix: &str,
    original: serde_json::Error,
) -> DecodeError {
    // The caller enforces its input byte bound. This second pass is diagnostic
    // only and can never turn a rejected message into a successful decode.
    let mut parser = serde_json::Deserializer::from_slice(bytes);
    match serde_path_to_error::deserialize::<_, T>(&mut parser) {
        Err(error) => {
            let path = error.path().to_string();
            let path = match (prefix.is_empty(), path == ".") {
                (true, _) => path,
                (false, true) => prefix.to_owned(),
                (false, false) => format!("{prefix}.{path}"),
            };
            DecodeError::Field {
                path,
                source: original,
            }
        }
        // A streaming deserializer can succeed before trailing garbage, which
        // from_slice correctly rejects. Preserve that original failure.
        Ok(_) => DecodeError::Invalid(original),
    }
}

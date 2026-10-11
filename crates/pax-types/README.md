# pax-types

Exact JSON integer units, validated public market metadata, and typed Pax public
messages. Numeric tokens and integer strings preserve their full integer range;
unknown events and missing reconstruction fields fail explicitly.

`protocol::decode` recognizes the complete compact event layout observed in the
public captures without allocating. Other layouts use the general typed decoder
with borrowed raw JSON fields. Both preserve the same protocol validation;
reordered/additional fields and escapes remain supported without an object tree.
Compact numeric fields use checked prefix conversion, including full signed
128-bit values, with separate JSON grammar and cursor-boundary checks. Portable
CPU targeting remains the default; the dependency's native SIMD path is optional
through explicit compiler flags.
Complete compact recognition accounts for every byte as ASCII; all other
layouts receive full-message UTF-8 validation before general decoding.
Numeric deserializers support JSON slice/string inputs; deserialize recorded
bytes directly rather than converting through `serde_json::Value`.

Modules: `units`, `market`, `protocol/{event,snapshot}`. This crate has no network,
async runtime, storage, or UI dependencies.

[Architecture](../../docs/architecture.md)

Run `scripts/cargo.sh test -p pax-types --locked` from the repository root.

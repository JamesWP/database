# Phase BJ — Compact ScalarValue Encoding: Native CBOR Primitives

Replace the serde-derive CBOR encoding for `ScalarValue` with a hand-written
`Serialize`/`Deserialize` that maps each variant to the corresponding native CBOR primitive,
eliminating the per-value string-key overhead entirely.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 1 | 3 | Encoding size exploration: `cbor_sizes` example binary | — |
| 2 | 3 | Custom `Serialize` + `Deserialize` for `ScalarValue` using native CBOR types | 1 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

`ScalarValue` carries `#[derive(Serialize, Deserialize)]`. Serde's enum serialization
strategy for CBOR maps every variant to a CBOR map with a string key:

- **Unit variants** (Null) → CBOR text string `"Null"` (5 bytes)
- **Tuple variants** (Integer, Floating, …) → CBOR map `{"VariantName": payload}`

The map entry alone costs 9–10 bytes before the payload. For typical integer columns
(rowids, small counts) the overhead dominates the payload.

`ScalarValue` happens to have a clean 1:1 mapping to CBOR's native type system:

| Rust variant | Current bytes (example) | New encoding | New bytes |
|---|---|---|---|
| `Null` | `64 "Null"` — 5 | CBOR null `f6` | 1 |
| `Integer(1)` | `a1 67 "Integer" 01` — 10 | CBOR uint `01` | 1 |
| `Integer(256)` | `a1 67 "Integer" 19 01 00` — 12 | CBOR uint `19 01 00` | 3 |
| `Integer(-1)` | `a1 67 "Integer" 20` — 10 | CBOR nint `20` | 1 |
| `Floating(3.14)` | `a1 68 "Floating" fb …` — 19 | CBOR double `fb …` | 9 |
| `Boolean(true)` | `a1 67 "Boolean" f5` — 10 | CBOR true `f5` | 1 |
| `String("hello")` | `a1 66 "String" 65 …` — 14 | CBOR text `65 …` | 6 |
| `Blob([1,2,3])` | `a1 64 "Blob" 43 …` — 10 | CBOR bytes `43 …` | 4 |

Row-level impact (measured in `examples/cbor_sizes.rs`):

| Row | Current | New |
|-----|---------|-----|
| `[1, "alice", 30]` | 36 bytes | 10 bytes |
| `[NULL, "bob", NULL]` | 23 bytes | 7 bytes |
| `[1, 2, 3, 4, 5]` | 51 bytes | 6 bytes |
| `[42, 3.14, true, "x"]` | 51 bytes | 15 bytes |

This is a **breaking on-disk format change**. All databases written before this phase are
unreadable after it. Because all tests create fresh temp databases, no migration is
required for the test suite. Users with existing `.db` files must recreate them.

---

## Stubs

None.

---

## 1. Encoding size exploration: `cbor_sizes` example (Track 3)

### What Changes

A new example binary at `examples/cbor_sizes.rs` that encodes representative
`ScalarValue` instances and rows using three strategies — current (serde derive),
integer-tagged arrays, and native CBOR primitives — and prints a side-by-side byte-count
table. This serves as the specification and sanity-check for item 2.

The example is already present in the working directory as of this phase's branch.

### Key Files

- `examples/cbor_sizes.rs` — example binary (already written)

### Tests

None — this is a standalone diagnostic binary, not part of `cargo test`.

Run manually: `cargo run -p database --example cbor_sizes`

### Implementation Steps (1 commit)

#### Step 1.1 — Commit the sizing example

Stage and commit `examples/cbor_sizes.rs` as a standalone infrastructure commit.

**Commit:** `examples: add cbor_sizes to measure ScalarValue CBOR encoding overhead`

---

## 2. Custom `Serialize` + `Deserialize` for `ScalarValue` (Track 3)

### What Changes

`ScalarValue` loses its `#[derive(Serialize, Deserialize)]` and gains hand-written impls
that use serde's primitive serializer methods and a `deserialize_any` visitor.

#### Serialize

```rust
impl serde::Serialize for ScalarValue {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            ScalarValue::Null        => s.serialize_unit(),   // f6
            ScalarValue::Integer(n)  => s.serialize_i64(*n),  // uint or nint
            ScalarValue::Floating(f) => s.serialize_f64(*f),  // fb …
            ScalarValue::Boolean(b)  => s.serialize_bool(*b), // f4 / f5
            ScalarValue::String(s)   => s.serialize_str(s),   // text
            ScalarValue::Blob(b)     => s.serialize_bytes(b), // bytes
        }
    }
}
```

`serialize_unit()` emits CBOR `f6` (null) via ciborium. `serialize_bytes` emits a CBOR
byte string directly — no `serde_bytes` wrapper needed on the variant.

#### Deserialize

```rust
impl<'de> serde::Deserialize<'de> for ScalarValue {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(ScalarValueVisitor)
    }
}

struct ScalarValueVisitor;

impl<'de> serde::de::Visitor<'de> for ScalarValueVisitor {
    type Value = ScalarValue;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("a CBOR integer, float, bool, null, text, or bytes")
    }

    // CBOR uint / nint → Integer
    fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<ScalarValue, E> {
        Ok(ScalarValue::Integer(v))
    }
    fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<ScalarValue, E> {
        i64::try_from(v)
            .map(ScalarValue::Integer)
            .map_err(|_| E::custom(format!("u64 {v} overflows i64")))
    }
    fn visit_i128<E: serde::de::Error>(self, v: i128) -> Result<ScalarValue, E> {
        i64::try_from(v)
            .map(ScalarValue::Integer)
            .map_err(|_| E::custom(format!("i128 {v} overflows i64")))
    }
    fn visit_u128<E: serde::de::Error>(self, v: u128) -> Result<ScalarValue, E> {
        i64::try_from(v)
            .map(ScalarValue::Integer)
            .map_err(|_| E::custom(format!("u128 {v} overflows i64")))
    }

    // CBOR double → Floating
    fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<ScalarValue, E> {
        Ok(ScalarValue::Floating(v))
    }
    fn visit_f32<E: serde::de::Error>(self, v: f32) -> Result<ScalarValue, E> {
        Ok(ScalarValue::Floating(v as f64))
    }

    // CBOR true/false → Boolean
    fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<ScalarValue, E> {
        Ok(ScalarValue::Boolean(v))
    }

    // CBOR text → String
    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<ScalarValue, E> {
        Ok(ScalarValue::String(v.to_owned()))
    }
    fn visit_string<E: serde::de::Error>(self, v: String) -> Result<ScalarValue, E> {
        Ok(ScalarValue::String(v))
    }

    // CBOR bytes → Blob
    fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<ScalarValue, E> {
        Ok(ScalarValue::Blob(v.to_owned()))
    }
    fn visit_byte_buf<E: serde::de::Error>(self, v: Vec<u8>) -> Result<ScalarValue, E> {
        Ok(ScalarValue::Blob(v))
    }

    // CBOR null / undefined → Null
    fn visit_unit<E: serde::de::Error>(self) -> Result<ScalarValue, E> {
        Ok(ScalarValue::Null)
    }
    fn visit_none<E: serde::de::Error>(self) -> Result<ScalarValue, E> {
        Ok(ScalarValue::Null)
    }
}
```

`deserialize_any` tells ciborium to sniff the next CBOR token and call the matching
visitor method. No ambiguity exists: CBOR has distinct major types for uint/nint, floats,
booleans, null, text strings, and byte strings — each maps to exactly one `ScalarValue`
variant.

#### Changes to `ScalarValue` declaration

Before:
```rust
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum ScalarValue {
    // ...
    Blob(#[serde(with = "serde_bytes")] Vec<u8>),
    // ...
}
```

After:
```rust
#[derive(Clone, Debug)]
pub enum ScalarValue {
    // ...
    Blob(Vec<u8>),
    // ...
}
```

`serde_bytes` is no longer needed on the `Blob` variant; `serialize_bytes` / `visit_bytes`
in the manual impls handle it directly. The `serde_bytes` crate remains in `Cargo.toml`
because `Cell` still uses `serde_bytes::Bytes` and `serde_bytes::ByteBuf` for key
serialization.

### Background

`deserialize_any` is the serde idiom for self-describing formats: the deserializer drives
the visitor by calling whichever `visit_*` matches the token it reads. This is how
`serde_json::Value` and `ciborium::value::Value` are deserialized.

The only edge case is large unsigned integers. CBOR allows unsigned values up to 2^64-1,
but `ScalarValue::Integer` is `i64`. Any CBOR uint that does not fit in `i64` returns a
deserialization error. In practice the database only writes values that were originally
`i64`, so this path is never hit during normal operation.

`visit_f32` is included because ciborium may emit CBOR half-precision (`f9`) or
single-precision (`fa`) for floats that round-trip exactly at lower precision. In practice
`serialize_f64` always emits double-precision `fb`, but the visitor handles downcasts to
be safe.

### Key Files

- `src/engine/scalarvalue.rs` — remove `#[derive(Serialize, Deserialize)]`, add
  `Serialize` and `Deserialize` impls manually

### Tests

#### Byte-exact encoding tests (step 2.1, written first)

These tests verify the actual bytes produced, making the expected encoding explicit. They
are written as a TDD step before the manual impls: they **fail** against the current serde
derive output, which is the correct signal.

```rust
fn encode(v: &ScalarValue) -> Vec<u8> {
    let mut buf = Vec::new();
    ciborium::ser::into_writer(v, &mut buf).unwrap();
    buf
}
```

| Test | Expected bytes |
|------|----------------|
| `encode_null` | `[0xf6]` |
| `encode_integer_zero` | `[0x00]` |
| `encode_integer_one` | `[0x01]` |
| `encode_integer_23` | `[0x17]` |
| `encode_integer_24` | `[0x18, 0x18]` |
| `encode_integer_255` | `[0x18, 0xff]` |
| `encode_integer_negative_one` | `[0x20]` |
| `encode_floating_3_14` | `[0xfb, 0x40, 0x09, 0x1e, 0xb8, 0x51, 0xeb, 0x85, 0x1f]` |
| `encode_boolean_true` | `[0xf5]` |
| `encode_boolean_false` | `[0xf4]` |
| `encode_string_empty` | `[0x60]` |
| `encode_string_hello` | `[0x65, 0x68, 0x65, 0x6c, 0x6c, 0x6f]` |
| `encode_blob_empty` | `[0x40]` |
| `encode_blob_three_bytes` | `[0x43, 0x01, 0x02, 0x03]` |

#### Round-trip tests (step 2.2, verifying Deserialize)

Each variant round-trips through `into_writer` → `from_reader` and produces an equal
value. One test per variant plus a multi-value `Vec<ScalarValue>` row test.

- `roundtrip_null`
- `roundtrip_integer` — several values including negative and large
- `roundtrip_floating` — including `f64::NAN`, `f64::INFINITY`, `-0.0`
- `roundtrip_boolean`
- `roundtrip_string` — empty and non-empty
- `roundtrip_blob` — empty and non-empty
- `roundtrip_row` — `[Integer(1), String("alice"), Integer(30), Null]` as `Vec<ScalarValue>`

### Implementation Steps (2 commits)

#### Step 2.1 — Byte-exact unit tests (TDD: these fail against current derive)

1. Add a `#[cfg(test)]` helper `fn cbor(v: &ScalarValue) -> Vec<u8>` in `scalarvalue.rs`.
2. Add all byte-exact test functions listed above.
3. Run `cargo test`; confirm the new tests fail with mismatched bytes.

**Commit:** `scalarvalue: add byte-exact CBOR encoding tests (spec for compact format)`

#### Step 2.2 — Custom Serialize + Deserialize; remove derive

1. Remove `serde::Serialize, serde::Deserialize` from `#[derive(...)]` on `ScalarValue`.
2. Remove `#[serde(with = "serde_bytes")]` from the `Blob` variant.
3. Implement `Serialize` for `ScalarValue` as shown above.
4. Implement `Deserialize` for `ScalarValue` with `ScalarValueVisitor` as shown above.
5. `cargo fmt && cargo build --workspace && cargo test --workspace` — all tests pass
   including the new byte-exact tests from step 2.1.

**Commit:** `scalarvalue: replace serde derive with compact native-CBOR Serialize/Deserialize`

---

## Verification

- [ ] `cargo test --workspace` — all tests pass after each commit independently
- [ ] `cargo fmt && cargo build --workspace 2>&1 | grep -i warning` — zero warnings
- [ ] `cargo run -p database --example cbor_sizes` — native column shows expected byte counts
- [ ] `ScalarValue` has no `#[derive(Serialize, Deserialize)]`
- [ ] `Blob` variant has no `#[serde(with = "serde_bytes")]`
- [ ] Byte-exact tests confirm: `Null`=1 byte, `Integer(1)`=1 byte, `Boolean`=1 byte
- [ ] Row `[1, "alice", 30]` encodes to 10 bytes (down from 36)

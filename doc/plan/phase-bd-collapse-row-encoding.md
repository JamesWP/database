# Phase BD — Collapse Row Encoding: Eliminate Double-CBOR

Remove the intermediate `Vec<u8>` serialisation step between `Vec<ScalarValue>` and the
on-disk page format, eliminating the double-CBOR encoding that currently costs one heap
allocation and two codec passes per row on every read and write.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 135 | 3 | Change `Cell.value: Vec<u8>` → `Cell.values: Vec<ScalarValue>`; update Serde; bump format version to 3; update all call sites and tests in one commit | — |
| 136 | 3 | Overflow path with lazy serialisation; update `CHUNK_THRESHOLD` framing constants for the new cell format | 135 |
| 137 | 3 | Simplify `CellReader` (inline variant needs no `io::Read`); add framing-overhead measurement test; add `cbor_cell_inline` / `cbor_cell_overflow` probes | 136 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

Every row that flows through the database currently passes through two independent CBOR
encoding rounds before reaching disk, and two decoding rounds on the way back:

**Write path today:**
```
engine.rs WriteCursor:
  Vec<ScalarValue>
      ↓  ciborium::ser::into_writer   ← allocation + CBOR encode #1
  Vec<u8> bytes
      ↓  Cursor::insert(key, bytes)
  Cell { key, value: bytes, cont }    ← bytes stored as opaque blob
      ↓  NodePageStore::write → encode_page
  [key_bytes, byte_string(bytes), cont?]   ← CBOR encode #2: wraps blob as byte-string
```

**Read path today:**
```
NodePageStore::read → decode_page:
  [key_bytes, byte_string(bytes), cont?]   ← CBOR decode #1
      ↓
  Cell { value: bytes }
      ↓  CellReader::decode_as_array
  ciborium::de::from_reader(&bytes)        ← CBOR decode #2: unpacks blob
      ↓
  Vec<ScalarValue>
```

The `value` field in `Cell` is already CBOR, yet the outer page encoding wraps it again as
a CBOR byte-string (major type 2). This double-encoding costs:
- One `Vec<u8>` heap allocation per INSERT (the inner encoded blob).
- One extra `ciborium::ser` call per INSERT.
- One extra `ciborium::de` call per row read.
- 1–3 bytes of CBOR byte-string framing around the value on every cell.

The `cbor_row_encode` and `cbor_row_decode` probes (added in earlier phases) confirm these
two passes are hot in the Sakila INSERT benchmark.

**After this phase:**

```
Write:
  Vec<ScalarValue>
      ↓  Cursor::insert(key, values)   ← no intermediate allocation
  Cell { key, values: Vec<ScalarValue>, cont }
      ↓  encode_page (single CBOR pass)
  [key_bytes, [scalar, scalar, ...], cont?]   ← values encoded inline as CBOR array

Read:
  [key_bytes, [scalar, scalar, ...], cont?]   ← decode_page (single CBOR pass)
      ↓
  Cell { values: Vec<ScalarValue> }
      ↓  CellReader::decode_as_array
  values.clone()   ← no decode, just return
```

For large rows that still need overflow pages the write path serialises `Vec<ScalarValue>`
to bytes exactly once, splits at `CHUNK_THRESHOLD`, and stores raw bytes in the overflow
chain. The Cell for an overflow row stores an empty `values` vec and a continuation
pointer. The read path for overflow cells remains unchanged.

---

## Stubs

None.

---

## 135. Change `Cell` to `Vec<ScalarValue>`; update all call sites (Track 3)

### What Changes

#### `src/storage/cell.rs`

**Before:**
```rust
pub type Value = Vec<u8>;

pub struct Cell {
    key: Key,
    value: Value,
    continuation: Option<u32>,
}
impl Cell {
    pub fn new(key: Key, value: Value, continuation: Option<u32>) -> Cell { ... }
    pub fn value(&self) -> ValueRef<'_> { &self.value }
    pub fn continuation(&self) -> Option<u32> { self.continuation }
}
```

The custom `Serialize` impl encodes the value as a CBOR byte-string (`serde_bytes::Bytes`).

**After:**
```rust
// `Value`, `ValueRef` type aliases removed — no longer needed

pub struct Cell {
    key: Key,
    values: Vec<ScalarValue>,      // renamed; typed
    continuation: Option<u32>,
}
impl Cell {
    pub fn new(key: Key, values: Vec<ScalarValue>, continuation: Option<u32>) -> Cell { ... }
    pub fn values(&self) -> &[ScalarValue] { &self.values }
    pub fn continuation(&self) -> Option<u32> { self.continuation }
}
```

The custom `Serialize` impl changes one line — the value field is no longer wrapped in
`serde_bytes::Bytes`; it is serialised as a normal `Vec<ScalarValue>` (a CBOR array):

```rust
// Before:
tup.serialize_element(serde_bytes::Bytes::new(&self.value))?;
// After:
tup.serialize_element(&self.values)?;
```

The `Deserialize` impl changes the element type from `serde_bytes::ByteBuf` to `Vec<ScalarValue>`:

```rust
// Before:
let value: serde_bytes::ByteBuf = seq.next_element()?.unwrap();
// After:
let values: Vec<ScalarValue> = seq.next_element()?.unwrap_or_default();
```

**On-disk format change:**

Old Cell CBOR:
```
Array(2|3) [ Bytes([key...]), Bytes([cbor-blob...]), u32? ]
```

New Cell CBOR:
```
Array(2|3) [ Bytes([key...]), Array([scalar1, scalar2, ...]), u32? ]
```

For overflow rows the values array is empty: `Array(3) [ Bytes([key...]), Array([]), u32 ]`.

This is a breaking on-disk format change — format version bumps from 2 → 3.

#### `src/storage/pager.rs`

Bump default format version:
```rust
// Before:
format_version: 2,
// After:
format_version: 3,
```

#### `src/storage/node_page_store.rs`

Accept version 3 as current; mark 2 as legacy:
```rust
Some(2) => Err(Error::FormatError(
    "Database format version 2 is no longer supported. \
     Please recreate your database."
        .into(),
)),
Some(3) => Ok(()),
```

#### `src/storage/btree.rs` — `Cursor::insert`

Signature change:
```rust
// Before:
pub fn insert(&mut self, key: &[u8], value: Value) { ... }
pub fn insert_u64(&mut self, key: u64, value: Value) { ... }
// After:
pub fn insert(&mut self, key: &[u8], values: Vec<ScalarValue>) { ... }
pub fn insert_u64(&mut self, key: u64, values: Vec<ScalarValue>) { ... }
```

The overflow check changes from a byte-length test to a serialised-size estimate (see Item 136
for the final implementation; in this commit use a `todo!()` placeholder or a simple
`ciborium::ser::into_writer` size check as a stand-in):

```rust
// Transitional overflow check in item 135 — refined in item 136:
let mut probe_buf = Vec::new();
ciborium::ser::into_writer(&values, &mut probe_buf).unwrap();
let (first_chunk, continuation) = if probe_buf.len() > CHUNK_THRESHOLD { ... overflow ... }
else { (probe_buf, None) };
// Store the serialised bytes as the first_chunk; Cell.values decoded from them:
let cell_values: Vec<ScalarValue> = ciborium::de::from_reader(&first_chunk[..]).unwrap_or_default();
let cell = Cell::new(key.to_vec(), cell_values, continuation);
```

> **Note:** This "encode then immediately decode" round-trip in item 135 is intentional — it
> preserves the existing overflow chunking logic without changing the overflow path in this
> commit. Item 136 eliminates this round-trip by storing `values` directly and using
> `cbor_size_estimate` for the inline/overflow decision.

#### `src/engine.rs` — `WriteCursor`

Remove the inner CBOR encode and pass `scalar_values` directly:

```rust
// Before:
let mut bytes = Vec::new();
probe!(database, cbor_row_encode);
ciborium::ser::into_writer(&scalar_values, &mut bytes).unwrap();
c.insert_u64(key, bytes);

// After:
probe!(database, cbor_row_encode);   // keep probe for now; removed in item 137
c.insert_u64(key, scalar_values);
```

#### `src/storage/btree.rs` — `insert_row_values` (catalog)

```rust
// Before:
let mut row = Vec::new();
ciborium::ser::into_writer(&row_values, &mut row).unwrap();
cursor.open_cursor().insert_u64(key, row);

// After:
cursor.open_cursor().insert_u64(key, row_values);
```

#### `src/storage/cell_reader.rs` — `CellReader`

Add an `Inline` variant alongside the existing byte-buffer path:

```rust
pub enum CellReader {
    Inline {
        key: Vec<u8>,
        values: Vec<ScalarValue>,
    },
    Overflow {
        key: Vec<u8>,
        buf: Vec<u8>,
        buf_pos: usize,
    },
}
```

Construction in `CellReader::new`:
```rust
let (key, values, continuation) = {
    let node = store.read(PageId(leaf_page_idx)).ok()?;
    let leaf = node.leaf()?;
    let cell = leaf.get_item_at_index(cell_idx)?;
    (cell.key().to_vec(), cell.values().to_vec(), cell.continuation())
};

if continuation.is_none() {
    return Some(CellReader::Inline { key, values });
}
// ... overflow path: follow chain, assemble bytes as before ...
```

`decode_as_array`:
```rust
pub fn decode_as_array(&mut self) -> Vec<ScalarValue> {
    match self {
        CellReader::Inline { values, .. } => values.clone(),
        CellReader::Overflow { .. } => {
            probe!(database, cbor_row_decode);
            ciborium::de::from_reader(self).unwrap()
        }
    }
}
```

`key()` — delegate to each variant.

`std::io::Read` — implement only for `CellReader::Overflow` (used by the overflow decode
path). The inline variant never needs it.

#### `src/repl/modes/btree.rs`

The two `insert_u64` call sites in the REPL change to pass a `Vec<ScalarValue>`:

```rust
// "insert key value" command — wrap user string:
cursor.handle.open_cursor().insert_u64(key, vec![ScalarValue::String(value)]);

// "random insert" command — wrap random bytes as a hex string:
let hex = bytes.iter().map(|b| format!("{:02x}", b)).collect::<String>();
cursor.handle.open_cursor().insert_u64(key, vec![ScalarValue::String(hex)]);
```

#### `src/storage/btree.rs` tests

All `insert_u64(k, raw_bytes)` calls change to `insert_u64(k, vec![ScalarValue::Integer(k as i64)])` or an equivalent typed value. Tests that care only about key ordering or B-tree structure can use any consistent `ScalarValue`:

```rust
// Before:
cursor.insert_u64(i, i.to_be_bytes().to_vec());
// After:
cursor.insert_u64(i, vec![ScalarValue::Integer(i as i64)]);

// Before:
cursor.insert_u64(1, b"one".to_vec());
// After:
cursor.insert_u64(1, vec![ScalarValue::String("one".to_string())]);
```

#### `src/storage/cell_reader.rs` tests

Tests that previously CBOR-encoded before inserting now pass values directly:

```rust
// Before:
let mut value_bytes = Vec::new();
ciborium::ser::into_writer(&values, &mut value_bytes).unwrap();
cursor.insert_u64(key, value_bytes.clone());

// After:
cursor.insert_u64(key, values.clone());
```

Tests that called `reader.read_to_end(&mut buf)` to get raw bytes and then decoded those
bytes should instead call `reader.decode_as_array()` directly.

### Background

The `Value = Vec<u8>` type alias was introduced when `Cell` was a general-purpose key-value
store. In practice every caller provides a CBOR-encoded `Vec<ScalarValue>` — the BTree is
not used to store arbitrary bytes anywhere in production code. This type-erasing abstraction
is the root cause of the double-encoding: callers pre-encode because the API demands bytes,
so the values end up encoded twice.

Making `Cell.values: Vec<ScalarValue>` is the structurally correct representation. The
`storage` layer already imports `ScalarValue` (see `btree.rs:11`, `cell_reader.rs:1`), so
this is not a new cross-layer dependency.

### Key Files

- `src/storage/cell.rs` — struct; Serde impl; remove `Value`/`ValueRef` type aliases
- `src/storage/pager.rs` — format version default `2` → `3`
- `src/storage/node_page_store.rs` — validate_format_version: version 2 → legacy, version 3 → Ok
- `src/storage/btree.rs` — Cursor::insert / insert_u64 signatures; catalog insert; all tests
- `src/engine.rs` — WriteCursor: remove ciborium::ser
- `src/storage/cell_reader.rs` — Inline/Overflow enum; decode_as_array; tests
- `src/repl/modes/btree.rs` — insert and random insert commands

### Tests

- All existing SQL integration tests (`cargo test test_sql_`) must pass unchanged.
- All existing BTree unit tests must pass with updated value types.
- `test_cell_cbor_roundtrip` and `test_cell_cbor_roundtrip_with_continuation` in `cell.rs`
  must be updated to use `Vec<ScalarValue>` values.

### Implementation Steps (1 commit)

Because `Cell::new` and `Cursor::insert` signatures change simultaneously with their call
sites, this item lands as a single commit that leaves the tree in a compilable, all-tests-
pass state.

Work order to minimise compile-error churn:
1. Update `cell.rs`: struct, Serde, accessor names; update tests in `cell.rs`.
2. Update `node.rs` tests: any `Cell::new(…, raw_bytes, …)` → typed values.
3. Update `pager.rs` format version; update `node_page_store.rs` validation.
4. Update `btree.rs` `Cursor::insert` / `insert_u64`; update catalog insert; update all tests.
5. Update `cell_reader.rs`: new enum + `decode_as_array`; update tests.
6. Update `engine.rs` `WriteCursor`.
7. Update `repl/modes/btree.rs` insert commands.
8. `cargo fmt && cargo build && cargo test`.

**Commit:** `storage: collapse row encoding — Cell.values: Vec<ScalarValue>, format version 3`

---

## 136. Overflow path and framing constants for new format (Track 3)

### What Changes

Item 135 introduced a temporary "encode then decode" round-trip inside `Cursor::insert` to
preserve the existing overflow chunking logic. This item removes that round-trip and
implements overflow detection without a full pre-serialisation for the common (inline) case.

#### `cbor_size_estimate` helper

Add a private helper that computes a **conservative upper bound** of the CBOR-encoded size
of a `Vec<ScalarValue>` in O(n) time with no allocation:

```rust
fn cbor_size_estimate(values: &[ScalarValue]) -> usize {
    let mut n: usize = 5; // conservative CBOR array header (up to 5 bytes)
    for v in values {
        n += match v {
            ScalarValue::Null      => 1,
            ScalarValue::Boolean(_) => 1,
            ScalarValue::Integer(i) => {
                let abs = (*i as i64).unsigned_abs();
                if abs <= 23 { 1 } else if abs <= 0xFF { 2 }
                else if abs <= 0xFFFF { 3 } else if abs <= 0xFFFF_FFFF { 5 } else { 9 }
            }
            ScalarValue::Floating(_) => 9,
            ScalarValue::String(s)  => {
                let len = s.len();
                let header = if len <= 23 { 1 } else if len <= 0xFF { 2 }
                             else if len <= 0xFFFF { 3 } else { 5 };
                header + len
            }
        };
    }
    n
}
```

The function over-estimates (array header is at most 5 bytes; string headers are worst-case).
Over-estimation is safe: a row estimated above the threshold serialises to bytes for exact
check and possible overflow; it never silently produces a page that's too large.

#### `Cursor::insert` — remove the round-trip

```rust
pub fn insert(&mut self, key: &[u8], values: Vec<ScalarValue>) {
    // ...
    let (cell_values, continuation) = if cbor_size_estimate(&values) > CHUNK_THRESHOLD {
        // Only serialise when we know (conservatively) it may overflow.
        let mut buf = Vec::new();
        ciborium::ser::into_writer(&values, &mut buf).unwrap();
        if buf.len() > CHUNK_THRESHOLD {
            let (first, rest) = buf.split_at(CHUNK_THRESHOLD);
            let cont_page = split_and_store(&mut *self.store, rest);
            // Overflow cell: reconstruct the inline portion as ScalarValues
            let inline: Vec<ScalarValue> =
                ciborium::de::from_reader(&first[..]).unwrap_or_default();
            (inline, Some(cont_page))
        } else {
            // Estimate was pessimistic; row fits inline after all.
            (values, None)
        }
    } else {
        (values, None)
    };

    let cell = Cell::new(key.to_vec(), cell_values, continuation);
    // ... tree insertion as before ...
}
```

> The decode from the first overflow chunk is required because CBOR arrays can't be
> cleanly truncated mid-stream — the on-disk overflow cell stores whatever ScalarValues
> fit completely in CHUNK_THRESHOLD bytes of CBOR. For the vast majority of rows (no
> overflow) this branch is never entered, so the allocation cost is zero.

#### Update `CELL_FRAMING_BYTES`

The Cell's value field is now a CBOR array (major type 4) instead of a byte-string (major
type 2). The framing overhead per cell in the worst case is:

```
1  outer CBOR array header (tuple of 2 or 3)
1  key byte-string header  (8-byte u64 rowid → 1-byte CBOR length prefix)
8  key data bytes
```

The value array header is **not** counted in the framing constant; it is part of the value
CBOR that `cbor_size_estimate` already accounts for.

```rust
// Before: CELL_FRAMING_BYTES = 13  (included 3-byte value byte-string header)
// After:  CELL_FRAMING_BYTES = 10  (no value byte-string header)
const CELL_FRAMING_BYTES: usize = 10;
```

Recalculate `CHUNK_THRESHOLD`:
```
CHUNK_THRESHOLD = (4096 - 15) / 4 - 10 = 4081 / 4 - 10 = 1020 - 10 = 1010 bytes
```

(Previously 1007 bytes — slightly more room for inline values.)

Update the `measure_cbor_framing_overhead` test in `node.rs` to reflect the new Cell
format. The assertion on `CELL_FRAMING_BYTES` changes from 13 to 10.

### Key Files

- `src/storage/btree.rs` — `cbor_size_estimate`; `Cursor::insert` overflow path; `CELL_FRAMING_BYTES`; `CHUNK_THRESHOLD` (derived)
- `src/storage/node.rs` — `measure_cbor_framing_overhead` test assertion

### Tests

- All existing tests pass.
- Add `test_insert_large_row_overflow` in `btree.rs`: insert a row with a string value
  longer than `CHUNK_THRESHOLD` bytes; read it back and verify all values round-trip.
- Add `test_cbor_size_estimate_is_upper_bound` in `btree.rs`: for a variety of
  `Vec<ScalarValue>` inputs, assert that `cbor_size_estimate(&v) >= actual_cbor_size(&v)`.

### Implementation Steps (1 commit)

1. Add `cbor_size_estimate` to `btree.rs`.
2. Rewrite the overflow branch in `Cursor::insert` to use it.
3. Update `CELL_FRAMING_BYTES` and let `CHUNK_THRESHOLD` recompute.
4. Update `measure_cbor_framing_overhead` assertion in `node.rs`.
5. Add the two new tests.
6. `cargo fmt && cargo build && cargo test`.

**Commit:** `storage: overflow path with cbor_size_estimate; update CELL_FRAMING_BYTES`

---

## 137. CellReader cleanup; framing measurement; probes (Track 3)

### What Changes

#### Remove `std::io::Read` from the `Inline` variant

`std::io::Read` on `CellReader` was used only by the ciborium decoder in
`decode_as_array`. After item 135 the inline variant never reads from a byte stream, so
the `io::Read` impl applies only to the `Overflow` variant. Implement it on `Overflow`
only via a helper method, or keep the impl on the enum with `Inline` returning `Ok(0)`.
The simplest clean-up: move the `buf_pos` cursor and `Read` impl to be internal to the
Overflow arm:

```rust
impl std::io::Read for CellReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            CellReader::Inline { .. } => Ok(0),
            CellReader::Overflow { buf: data, buf_pos, .. } => {
                let available = data.len() - *buf_pos;
                if available == 0 { return Ok(0); }
                let n = available.min(buf.len());
                buf[..n].copy_from_slice(&data[*buf_pos..*buf_pos + n]);
                *buf_pos += n;
                Ok(n)
            }
        }
    }
}
```

(The `Inline` branch returns `Ok(0)` so any accidental callers get EOF rather than a panic.
No production code path reaches that branch; it's a safety backstop.)

#### Remove `cbor_row_encode` and `cbor_row_decode` probes for inline path

The `cbor_row_encode` probe in `engine.rs` is no longer meaningful for the inline path
(no encode occurs). Remove it from the WriteCursor handler.

The `cbor_row_decode` probe in `CellReader::decode_as_array` fires only for the overflow
path now. Rename to `cbor_overflow_row_decode` for clarity, and add a new
`cell_read_inline` probe for the inline path:

```rust
pub fn decode_as_array(&mut self) -> Vec<ScalarValue> {
    match self {
        CellReader::Inline { values, .. } => {
            probe!(database, cell_read_inline);
            values.clone()
        }
        CellReader::Overflow { .. } => {
            probe!(database, cbor_overflow_row_decode);
            ciborium::de::from_reader(self).unwrap()
        }
    }
}
```

Similarly add `cell_write_inline` and `cell_write_overflow` probes in `Cursor::insert`
(alongside the existing `overflow_write` probe).

#### Framing overhead measurement test

Update `measure_cbor_framing_overhead` in `node.rs` to also measure the new Cell format.
Add assertions for `CELL_FRAMING_BYTES = 10` (verified by encoding a `Cell` with a
`Vec<ScalarValue>` value and measuring overhead = `cbor_size - value_cbor_size`).

### Key Files

- `src/storage/cell_reader.rs` — `std::io::Read` impl; probe names
- `src/engine.rs` — remove `cbor_row_encode` probe; add `cell_write_inline`/`cell_write_overflow`
- `src/storage/btree.rs` — add `cell_write_inline`/`cell_write_overflow` probe sites
- `src/storage/node.rs` — `measure_cbor_framing_overhead` updated assertions

### Tests

- All existing tests pass.
- The framing measurement test asserts `CELL_FRAMING_BYTES <= 10`.

### Implementation Steps (1 commit)

1. Tidy `std::io::Read` on `CellReader` as described.
2. Update probe names; add new probes at write sites.
3. Remove `cbor_row_encode` probe from `engine.rs`.
4. Update `measure_cbor_framing_overhead` assertions in `node.rs`.
5. `cargo fmt && cargo build && cargo test`.

**Commit:** `storage: CellReader cleanup; update probes; framing measurement for format v3`

---

## Verification

- [ ] `cargo test` — all tests pass after each commit independently
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` — zero warnings
- [ ] `cargo test test_sql_` — all SQL integration tests pass
- [ ] Format version is `3` in `pager.rs`; `validate_format_version` accepts only `Some(3)`
- [ ] No `ciborium::ser::into_writer` call remains in `engine.rs` for the normal write path
- [ ] No `ciborium::de::from_reader` call remains in `CellReader::decode_as_array` for the inline path
- [ ] `CELL_FRAMING_BYTES = 10` (was 13); `CHUNK_THRESHOLD` recomputes to 1010 (was 1007)
- [ ] Perf: `cbor_row_encode` / `cbor_row_decode` probe counts drop to zero for non-overflow rows in `perf script` / `bpftrace` against the Sakila INSERT benchmark

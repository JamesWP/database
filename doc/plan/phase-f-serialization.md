# Phase F — Serialization Overhaul

Phase F replaces JSON-based serialization with compact binary formats, reducing storage overhead and improving performance.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 28 | 3.2 | Binary cell format | — |
| 29 | 3.4 | Record format | 28 |
| 30 | 3.3 | Binary page format | 28 |
| 31 | 6.4 | ZeroPage free list | — |

---

## 28. Binary Cell Format (Track 3.2)

### What Changes

Replace JSON cell serialization `[key, value, continuation]` with binary layout.

### Key Files

- `src/storage/cell.rs` — cell write path
- `src/storage/cell_reader.rs` — cell read path

### Implementation Approach

Binary layout:
```
| key: u64 (8 bytes BE) | value_len: u32 (4 bytes BE) | value: [u8; value_len] | continuation: Option<u32> (4 bytes if present) |
```
Use high bit of value_len to indicate continuation. Replace serde_json with direct byte encoding using `to_be_bytes()` / `from_be_bytes()`.

### Tests

- Cell roundtrip / with continuation / empty value / large value / key ordering via byte comparison

---

## 29. Record Format (Track 3.4)

### What Changes

Replace JSON row arrays (`[1, "alice", 30]`) with typed binary record format.

### Key Files

- New: `src/storage/record.rs` (or in `cell.rs`)
- `src/engine/scalarvalue.rs` — ScalarValue to/from binary

### Implementation Approach

Type tags: `0x00=NULL, 0x01=i64(8B), 0x02=f64(8B), 0x03=text(len+utf8), 0x04=blob(len+bytes), 0x05=bool(1B)`. Row = `[column_count: u16] [type_tag, value_bytes]...`.

Create `Record::encode(values: &[ScalarValue]) -> Vec<u8>` and `Record::decode(bytes: &[u8]) -> Vec<ScalarValue>`. Replace all `serde_json::to_vec` / `decode_as_json_array()` calls.

### Tests

- Roundtrip: integers / strings / mixed types / with NULL / bool / empty row / size vs JSON

---

## 30. Binary Page Format (Track 3.3)

### What Changes

Replace JSON node serialization with slotted page layout (inspired by SQLite).

### Key Files

- `src/storage/node.rs` — node serialization
- `src/storage/pager.rs` — page read/write

### Implementation Approach

Page layout (4096 bytes):
```
Offset  Size  Field
0       1     Node type (0x0D=leaf, 0x05=interior)
1       2     Cell count (u16 BE)
3       2     Cell content area start (u16 BE)
5       4     Rightmost child (u32, interior only)
9+      ...   Cell pointer array (u16 offsets)
...           Free space
...           Cell content area (grows from end)
```

Cell pointers are u16 offsets into the page. Cells stored from end of page backward.

### Tests

- Leaf/interior roundtrip / cell ordering / page full detection / exactly 4096 bytes / integration with INSERT+SELECT

---

## 31. ZeroPage Free List (Track 6.4)

### What Changes

Replace `free_page_list: Vec<u32>` (can overflow page size) with linked list of free pages.

### Key Files

- `src/storage/pager.rs`

### Implementation Approach

ZeroPage stores only `free_list_head: u32` and `free_page_count: u32`. Each free list page: `[next: u32, count: u32, page_ids: [u32; ...]]` — holds up to 1022 page IDs per page.

### Tests

- Alloc+free roundtrip / many pages / overflow to multiple list pages / persistence / empty free list

---

## Migration Strategy

Items 28-30 change the on-disk format. Bump a format version in ZeroPage. On open, check version and refuse old formats with clear error. Pre-1.0 databases are disposable. Implement in order: cells (28) → records (29) → pages (30).

## Verification

For each item:
- [ ] Tests written first (TDD)
- [ ] All tests pass: `cargo test --bin database`
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning`

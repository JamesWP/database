# Phase AD — Page-Geometry-Aware Overflow Thresholds

Replace the two hardcoded overflow constants (`CHUNK_THRESHOLD = 55`, `OVERFLOW_LIMIT = 100`) with values derived from `PAGE_SIZE` and a minimum-cells-per-page requirement, so overflow only occurs when a value genuinely cannot fit inline on a node page.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 112 | 3 | Measure CBOR framing overhead for `LeafNodePage` and `OverflowPage` | — |
| 113 | 3 | Derive `OVERFLOW_LIMIT` from `PAGE_SIZE` minus `OverflowPage` framing overhead | 112 |
| 114 | 3 | Derive `CHUNK_THRESHOLD` from page capacity and `MIN_CELLS_PER_PAGE` | 113 |
| 115 | 7 | Tests: verify typical SQL rows don't overflow; verify overflow chain correctness at new sizes | 114 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

The current storage layer overflows a cell value into separate overflow pages whenever the value exceeds **55 bytes** (`CHUNK_THRESHOLD`). Each overflow page holds at most **100 bytes** (`OVERFLOW_LIMIT`). Both constants are arbitrary — neither is derived from the 4 096-byte `PAGE_SIZE`.

The consequences are severe in practice:

- A typical SQL row `(1, "Alice Smith", "alice@example.com", 42)` CBOR-encodes to ≈ 60 bytes and triggers overflow.
- A 300-byte value requires **3 overflow page fetches** (100 bytes each) — one extra I/O per page — where a single 4 KB page could hold the entire value with room to spare.
- Overflow pages are wasted: a 100-byte chunk inside a 4 096-byte page leaves 97.5 % of that page unused.

The fix is to derive both constants from first principles:

1. **`OVERFLOW_LIMIT`** — an overflow page should fill nearly a full page. Set it to `PAGE_SIZE - OVERFLOW_PAGE_FRAMING_BYTES`, where the framing overhead is the CBOR encoding cost for `NodePage::OverflowPage { content: Vec<u8>, continuation: Option<u32> }`.

2. **`CHUNK_THRESHOLD`** — an inline value must be small enough that `MIN_CELLS_PER_PAGE` cells can fit on a single leaf page. Derive the maximum inline size from `(PAGE_SIZE - LEAF_PAGE_FRAMING_BYTES) / MIN_CELLS_PER_PAGE - CELL_FRAMING_BYTES`.

These changes are **backward-compatible at the read path**: `CellReader` already follows the continuation chain regardless of chunk size. Existing databases written with the old constants continue to work — the new constants only affect newly written cells.

---

## 112. Measure CBOR framing overhead (Track 3)

### What Changes

Add a Rust test (or `const`-annotated comment with measured values) that serializes:
- An empty `OverflowPage { content: vec![], continuation: None }` — the base CBOR framing cost.
- An empty `OverflowPage { content: vec![], continuation: Some(0) }` — with the optional page pointer present.
- A minimal `LeafNodePage` with zero cells — the base leaf framing cost.
- A single minimal `Cell { key: [0;8], value: vec![], continuation: None }` — per-cell framing cost.

Record the actual byte counts as named constants near `PAGE_SIZE` in `pager.rs` or `btree.rs`.

### Background

CBOR encoding for a `NodePage::OverflowPage` wraps the enum variant (1-byte major type + tag), then the struct (2-field map or array). Empirically:

| Structure | Estimated CBOR overhead |
|-----------|------------------------|
| `OverflowPage` (no continuation) | ≈ 8 bytes |
| `OverflowPage` (with continuation) | ≈ 13 bytes |
| `LeafNodePage` base (0 cells) | ≈ 15 bytes |
| Per-`Cell` framing (key + overhead) | ≈ 20 bytes |

These estimates must be validated by the measurement test.

### Key Files

- `src/storage/pager.rs` — `PAGE_SIZE` constant lives here
- `src/storage/btree.rs` — `CHUNK_THRESHOLD` and `split_and_store` live here
- `src/storage/node.rs` — `OverflowPage`, `LeafNodePage`, `Cell` structs

### Tests

```rust
#[test]
fn measure_cbor_framing_overhead() {
    use crate::storage::node::{Cell, LeafNodePage, NodePage, OverflowPage};
    let overflow_no_cont = NodePage::OverflowPage(OverflowPage::new(vec![], None));
    let overflow_with_cont = NodePage::OverflowPage(OverflowPage::new(vec![], Some(0)));
    let leaf_empty = NodePage::Leaf(LeafNodePage::default());
    let cell_empty = Cell::new(vec![0u8; 8], vec![], None);

    let enc = |v: &dyn erased_serde::Serialize| -> usize {
        let mut buf = vec![];
        ciborium::ser::into_writer(v, &mut buf).unwrap();
        buf.len()
    };
    // Print or assert these values; use them to calibrate the constants below.
    println!("overflow_no_cont: {}", enc(&overflow_no_cont));
    println!("overflow_with_cont: {}", enc(&overflow_with_cont));
    println!("leaf_empty: {}", enc(&leaf_empty));
    // cell needs to be encoded as part of a leaf to measure correctly
}
```

### Implementation Steps (1 commit)

#### Step 112.1 — Add framing-overhead measurement test and record constants

**Commit:** `Measure CBOR framing overhead for OverflowPage and LeafNodePage`

---

## 113. Derive `OVERFLOW_LIMIT` from `PAGE_SIZE` (Track 3)

### What Changes

Replace the hardcoded `const OVERFLOW_LIMIT: usize = 100` inside `split_and_store` with a derived constant in module scope:

```rust
// Framing overhead for NodePage::OverflowPage { content, continuation: Some(u32) }
// Measured empirically (see test_cbor_framing_overhead).
const OVERFLOW_PAGE_FRAMING_BYTES: usize = 15; // conservative upper bound

/// Maximum bytes stored in a single overflow page.
/// Sized to fill a page while leaving room for CBOR framing.
const OVERFLOW_LIMIT: usize = PAGE_SIZE as usize - OVERFLOW_PAGE_FRAMING_BYTES;
```

With `PAGE_SIZE = 4096`, this gives `OVERFLOW_LIMIT ≈ 4081`. A 300-byte value then fits in a **single** overflow page instead of three.

### Background

The current 100-byte limit stores one row's worth of data per overflow page, wasting most of the 4 KB allocation. SQLite uses a similar derivation: the usable bytes on an overflow page is `PAGE_SIZE - 4` (for a 4-byte next-page pointer). Our CBOR framing is a few bytes larger, but the principle is identical.

### Key Files

- `src/storage/btree.rs:657` — `const OVERFLOW_LIMIT: usize = 100;` (move to module scope and derive)

### Tests

- Existing `test_cell_overflow_value` and `test_cell_multi_page_overflow` in `cell_reader.rs` should pass unchanged (they test the chain-following logic, not the chunk size).
- Add a new test: a value of size `OVERFLOW_LIMIT + 1` should create exactly **2** overflow pages (chain of length 2).
- Add a test: a value of size `PAGE_SIZE - OVERFLOW_PAGE_FRAMING_BYTES - 1` (i.e., `OVERFLOW_LIMIT - 1`) should create exactly **1** overflow page.

### Implementation Steps (1 commit)

#### Step 113.1 — Move `OVERFLOW_LIMIT` to module scope and derive from `PAGE_SIZE`

**Commit:** `Derive OVERFLOW_LIMIT from PAGE_SIZE minus framing overhead`

---

## 114. Derive `CHUNK_THRESHOLD` from page capacity (Track 3)

### What Changes

Replace the hardcoded `const CHUNK_THRESHOLD: usize = 55` with a derived constant:

```rust
/// Minimum number of cells that must fit on a leaf page to maintain
/// adequate B-tree fill factor after a split.
const MIN_CELLS_PER_PAGE: usize = 4;

/// Conservative CBOR framing overhead for a LeafNodePage with N cells.
/// Measured empirically (see test_cbor_framing_overhead).
const LEAF_PAGE_BASE_FRAMING_BYTES: usize = 20;

/// Conservative CBOR framing overhead per Cell (key bytes + struct wrapper).
/// Key is always 8 bytes (u64 rowid as big-endian). The inline value bytes
/// are added on top of this.
const CELL_FRAMING_BYTES: usize = 25;

/// Maximum inline value bytes stored directly in a Cell on a leaf page.
/// Derived so that MIN_CELLS_PER_PAGE cells can always fit on one page.
const CHUNK_THRESHOLD: usize =
    (PAGE_SIZE as usize - LEAF_PAGE_BASE_FRAMING_BYTES) / MIN_CELLS_PER_PAGE
    - CELL_FRAMING_BYTES;
```

With `PAGE_SIZE = 4096`, `LEAF_PAGE_BASE_FRAMING_BYTES = 20`, `CELL_FRAMING_BYTES = 25`:

```
CHUNK_THRESHOLD = (4096 - 20) / 4 - 25 = 1019 - 25 = 994
```

So a typical SQL row (< 994 bytes) fits **inline** with no overflow at all.

### Background

The comment at `btree.rs:121-122` explains the invariant:

> *values must be small enough so that a few can fit on each page*
> *this is to ensure when splitting nodes we always end up with at least 50% free space*

The current threshold of 55 bytes satisfies this invariant for a 4 KB page — but far too conservatively. With `MIN_CELLS_PER_PAGE = 4`, after a split each half-page contains ≥ 2 cells, which is the minimum for a valid B-tree node. The formula preserves this invariant while allowing much larger inline values.

**Choosing `MIN_CELLS_PER_PAGE = 4`:** A leaf page holding ≥ 4 cells will split into two pages of ≥ 2 cells each. Two cells is the minimum for a non-root leaf (in a B-tree of order 2). Using 4 instead of 2 gives a 50 % fill safety margin.

### Key Files

- `src/storage/btree.rs:98` — `const CHUNK_THRESHOLD: usize = 55;`

### Tests

- Add a SQL integration test in `tests/sql/` that inserts a row with a TEXT column of 200 characters and SELECT verifies the round-trip. This row should no longer overflow.
- Add a unit test in `btree.rs` or `cell_reader.rs` asserting that a value of `CHUNK_THRESHOLD` bytes stores **inline** (no overflow page), and `CHUNK_THRESHOLD + 1` bytes spills to one overflow page.

### Implementation Steps (1 commit)

#### Step 114.1 — Derive CHUNK_THRESHOLD from PAGE_SIZE and MIN_CELLS_PER_PAGE

**Commit:** `Derive CHUNK_THRESHOLD from page geometry and minimum-cells-per-page constant`

---

## 115. Tests: verify typical rows fit inline and chain correctness at new sizes (Track 7)

### What Changes

Add tests that specifically validate the new overflow behaviour:

1. **SQL integration test** — `tests/sql/overflow_inline.sql`: insert a row with a TEXT column of 500 characters; SELECT should return it correctly.
2. **Unit test** — a value of exactly `CHUNK_THRESHOLD` bytes stores with no continuation pointer.
3. **Unit test** — a value of `CHUNK_THRESHOLD + 1` bytes stores with exactly one overflow page, which contains `CHUNK_THRESHOLD + 1 - CHUNK_THRESHOLD = 1` byte on its first overflow page.
4. **Unit test** — a value of `OVERFLOW_LIMIT * 2 + 1` bytes creates exactly three overflow pages.
5. **Property test** (optional) — for values of arbitrary length in `[0, OVERFLOW_LIMIT * 3]`, a round-trip `insert` → `read` returns the original bytes.

### Key Files

- `tests/sql/overflow_inline.sql` — new integration test
- `src/storage/cell_reader.rs` — existing overflow unit tests to extend

### Implementation Steps (1 commit)

#### Step 115.1 — Add overflow inline and chain unit + integration tests

**Commit:** `Add tests for page-geometry-derived overflow thresholds`

---

## Verification

- [ ] `cargo test` — all tests pass
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` — zero warnings
- [ ] Each commit is independently testable
- [ ] `CHUNK_THRESHOLD` and `OVERFLOW_LIMIT` are now derived expressions, not magic numbers
- [ ] A typical SQL row (< 200 bytes) inserts without creating any overflow pages
- [ ] Existing databases with old-format overflow chains are still read correctly

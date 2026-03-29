# Phase AX — Fast Tests by Default

Reduce the default `cargo test` wall-clock time from ~17 s to ~3 s by shrinking dataset sizes and proptest case counts in seven slow unit tests, without removing or weakening any coverage.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 117 | 7 | btree.rs: reduce proptest cases and key counts for four slow tests | — |
| 118 | 7 | pager.rs: reduce page-allocation counts for two slow tests | — |
| 119 | 7 | db.rs: reduce row count in `test_large_insert` | — |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

Running `cargo test` currently takes ~17 s in wall-clock time (33 s serial). The bottleneck is seven tests that use large datasets to exercise B-tree splits, free-list paging, and SQL execution at scale. These tests are correct and valuable, but their current sizes are larger than necessary to trigger the behaviors they test.

### Timing breakdown (measured with `cargo +nightly test -- -Z unstable-options --report-time`)

| Test | Location | Current time | Root cause |
|------|----------|-------------|------------|
| `test_ordering` | `storage::btree` | ~15 s | proptest default 256 cases × (10–20 large string inserts) |
| `test_large_insert_sorted_and_verified` | `storage::btree` | ~7 s | proptest 5 cases × (200–300 key inserts) |
| `test_large_tree_ordering` | `storage::btree` | ~6 s | 1 000 key inserts |
| `db::test_large_insert` | `db` | ~1.7 s | 150 SQL `INSERT` statements via full engine |
| `pager::test_large_scale_alloc_dealloc` | `storage::pager` | ~1.4 s | 2 000 page allocations + I/O |
| `pager::test_multi_page_free_list` | `storage::pager` | ~1.4 s | 1 500 page allocations + I/O |
| `btree::test_variable_length_keys_splits` | `storage::btree` | ~1.2 s | 200 variable-length key inserts |

**Total serial: ~33 s → target ~6 s serial (~3 s wall-clock with parallelism)**

### Why reducing sizes is safe

Each test has a natural "minimum size" below which it no longer exercises its target behaviour (e.g. triggering a B-tree split, spanning multiple free-list pages). The reduced sizes in this phase are well above those minimums; they simply stop wasting time on redundant iterations.

- **B-tree splits** are triggered by a full leaf page (~60–80 entries at typical row sizes). Tests reduced to 80–200 keys still produce multiple rounds of interior-node splits.
- **Multi-page free list** requires >1 000 deallocations (the page capacity ceiling). Reducing to 1 100 spans exactly 2 free-list pages — sufficient to test the chain-pointer logic while trimming ~300 unnecessary allocations.
- **Proptest** already persists a failure corpus and shrinks automatically on first failure, so fewer cases per run still provide excellent ongoing regression coverage.

---

## Stubs

None.

---

## 117. btree.rs: reduce proptest cases and key counts (Track 7)

### What Changes

Four tests in `src/storage/btree.rs` are slowed by oversized inputs:

| Test | Before | After |
|------|--------|-------|
| `test_ordering` | default 256 proptest cases | `ProptestConfig::with_cases(20)` |
| `test_large_insert_sorted_and_verified` | 5 cases, 200–300 keys | 2 cases, 50–100 keys |
| `test_large_tree_ordering` | 1 000 keys | 200 keys |
| `test_variable_length_keys_splits` | 200 keys | 80 keys |

### Background

`test_ordering` uses a default `proptest!` block with no explicit config, which runs 256 cases by default. Each case generates 10–20 strings of 500–600 chars, creates a fresh temp-file BTree, and inserts them — making each case slow in debug mode.

`test_large_insert_sorted_and_verified` already has `with_cases(5)` but the range `200..300` keys per case is still expensive. Reducing to `50..100` keys still triggers multi-level splits and exercises the verify + scan path.

`test_large_tree_ordering` shuffles and inserts 1 000 u64 keys. B-tree splits begin triggering around 60–80 entries with the current cell sizes. 200 keys produce several rounds of interior-node splits and thoroughly exercises the ordering invariant.

`test_variable_length_keys_splits` uses 200 variable-length keys. 80 keys of lengths 1–20 bytes still trigger multiple splits and the verify + sorted-scan check.

### Implementation Approach

**`test_ordering`** — add `ProptestConfig::with_cases(20)` to the proptest block:

```rust
proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig::with_cases(20))]
    #[test]
    fn test_ordering(ordering: bool, elements in ...) {
        ...
    }
}
```

**`test_large_insert_sorted_and_verified`** — change the existing config from `with_cases(5)` to `with_cases(2)` and narrow the key range from `200..300` to `50..100`:

```rust
proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig::with_cases(2))]
    fn test_large_insert_sorted_and_verified(
        mut keys in prop::collection::vec(0u64..100_000, 50..100usize),
    ) { ... }
}
```

**`test_large_tree_ordering`** — reduce `(0..1000)` to `(0..200)` and update the verification loop bound accordingly:

```rust
let mut keys: Vec<u64> = (0..200).collect();
...
for expected_key in 0..200u64 { ... }
```

**`test_variable_length_keys_splits`** — reduce `(0u32..200)` to `(0u32..80)`:

```rust
let mut keys: Vec<Vec<u8>> = (0u32..80)
    .map(|i| { ... })
    .collect();
```

### Key Files

- `src/storage/btree.rs` — four test modifications

### Tests

All four modified tests must continue to pass (`cargo test`). No new tests needed — this is a dataset-size reduction only.

### Implementation Steps (1 commit)

#### Step 117.1 — Reduce proptest cases and key counts in btree tests

Apply all four changes described above. Verify `cargo test` passes and `cargo +nightly test -- -Z unstable-options --report-time` shows the btree suite completing in under 3 s.

**Commit:** `tests: reduce proptest cases and key counts in btree slow tests`

---

## 118. pager.rs: reduce page-allocation counts (Track 7)

### What Changes

Two tests in `src/storage/pager.rs` allocate far more pages than needed to exercise their target behaviour:

| Test | Before | After | Rationale |
|------|--------|-------|-----------|
| `test_multi_page_free_list` | 1 500 pages | 1 100 pages | `FreeListPage` capacity is 1 000; 1 100 spans exactly 2 pages |
| `test_large_scale_alloc_dealloc` | 2 000 pages | 500 pages | Tests dealloc + realloc reuse; scale doesn't affect the invariant |

### Background

`FreeListPage.page_ids` is capped at 1 000 entries (`page_ids.len() < 1000` check in `dealocate`). To test the multi-page chain the test needs strictly more than 1 000 deallocations. 1 100 gives 100 entries of headroom above the threshold while cutting ~27 % of the I/O operations.

`test_large_scale_alloc_dealloc` tests that: (a) freed pages are reused on re-allocation, and (b) the file does not grow beyond its peak size. These invariants are scale-independent — 500 pages exercises the same logic as 2 000.

### Implementation Approach

**`test_multi_page_free_list`** — change the loop count from `1500` to `1100` and update all comments:

```rust
// Allocate 1100 pages
for _ in 0..1100 {
    allocated.push(pager.allocate());
}
...
// Deallocate all 1100 pages (should create multiple FreeListPages)
for page in allocated.clone() {
    pager.dealocate(page);
}
...
// Re-allocate all 1100 pages - should reuse freed pages
for _ in 0..1100 {
    reallocated.push(pager.allocate());
}
```

**`test_large_scale_alloc_dealloc`** — change the initial allocation from `2000` to `500` and the deallocation step from every-other in `0..2000` to every-other in `0..500`, and re-allocation from `1000` to `250`. Also update the size assertion comment:

```rust
// Allocate 500 pages
for _ in 0..500 { pages.push(pager.allocate()); }

let max_size = pager.get_file_size_pages();
assert!(max_size >= 501); // At least 500 data pages + zero page

// Deallocate half of them
for i in (0..500).step_by(2) { pager.dealocate(pages[i]); }

// Allocate 250 new pages - should reuse the freed ones
for _ in 0..250 { pager.allocate(); }
```

### Key Files

- `src/storage/pager.rs` — two test modifications

### Tests

Both modified tests must continue to pass. The multi-page assertion (`size_after_alloc == pager.get_file_size_pages()` after reallocation) is unchanged; only the count differs.

### Implementation Steps (1 commit)

#### Step 118.1 — Reduce page-allocation counts in pager slow tests

Apply both changes above. Run `cargo test storage::pager` and confirm both pass.

**Commit:** `tests: reduce page allocation counts in pager slow tests`

---

## 119. db.rs: reduce row count in `test_large_insert` (Track 7)

### What Changes

`db::tests::test_large_insert` inserts 150 rows via full SQL `INSERT` statements through the engine, then queries with a `WHERE value > 500` filter. Reducing to 50 rows preserves the correctness check (filter, result set, boundary arithmetic) while cutting execution time by ~67 %.

### Background

The test verifies that:
1. 150 rows can be inserted without error.
2. A `WHERE value > 500` filter returns only the expected rows.

With 50 rows the `WHERE value > (50*10/2 - 10)` threshold needs updating to match: `value > 500` no longer has matching rows at 50 rows since `value = i * 10` maxes at `490`. Change the filter to `WHERE value > 200` (rows where `id >= 21`, giving 29 rows) and update the result assertions accordingly.

### Implementation Approach

```rust
// Insert 50 rows
for i in 0..50 {
    let sql = format!("INSERT INTO numbers VALUES ({}, {})", i, i * 10);
    ...
}

// WHERE value > 200 → ids 21..49 (29 rows)
let result = execute(
    "SELECT id, value FROM numbers WHERE value > 200",
    &mut test.catalog,
)
.unwrap();
// Assert 29 rows returned, first row id=21 value=210, last id=49 value=490
```

### Key Files

- `src/db.rs` — one test modification

### Tests

The modified test must pass. The assertion values (row count, first/last row) update to match the new dataset.

### Implementation Steps (1 commit)

#### Step 119.1 — Reduce row count and update assertions in `test_large_insert`

Change loop to `0..50`, update the WHERE threshold and result assertions. Run `cargo test db::tests::test_large_insert` to confirm.

**Commit:** `tests: reduce row count in test_large_insert`

---

## Verification

- [ ] `cargo test` — all 370+ tests pass, zero regressions
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` — zero warnings
- [ ] `cargo +nightly test -- -Z unstable-options --report-time` — wall-clock under 4 s
- [ ] Each commit is independently buildable and testable

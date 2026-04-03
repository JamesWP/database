# Phase AW — Decoded Page Cache

Add a decoded-`NodePage` cache to the `Pager` so that CBOR deserialization only happens once per page per write, eliminating the dominant CPU hotspot identified by `perf` profiling of the sakila bulk-insert workload.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 115 | 3 | Pager: add `decoded` cache and `get_node_page` / `set_node_page` accessors | — |
| 116 | 3 | BTree: route all `NodePage` reads and writes through the decoded cache | 115 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

`perf` profiling of a sakila bulk INSERT (`~46 k` rows) shows **~84 % of CPU time** spent inside the `ciborium` CBOR decoder (`ciborium_ll::dec::Decoder::pull` 49 %, `Header::try_from` 18 %, `Deserializer::integer` 11 %). The root cause is that `Pager::get_and_decode` re-runs the full CBOR deserializer on every call, even though the underlying raw page bytes are already held in an in-memory byte cache (`Pager::cache`). The bytes never hit disk twice — but the CPU parses them over and over.

A decoded-page cache stores the fully-constructed `NodePage` struct after the first deserialization. Subsequent reads for the same page number return the cached struct directly, skipping the CBOR decode entirely. Writes go to the cache and to disk simultaneously (matching the existing write-through policy). This is a pure performance change: the on-disk format and all existing behaviour are unchanged.

### Why only `NodePage`?

`get_and_decode` is generic and also used for `ZeroPage` (once on open) and `FreeListPage` (on allocate/deallocate). Those paths are cold. Adding a typed cache for `NodePage` — the only type on the hot path — keeps the change focused and avoids a complex type-erased cache.

### Expected impact

Every B-tree traversal for INSERT currently decodes ~3–15 pages (catalog lookup + data tree + index trees). With the cache, each page is decoded at most once per write. Root pages and high-level interior nodes — accessed on every INSERT — become O(1) cache lookups.

---

## Stubs

None.

---

## 115. Pager: decoded cache and NodePage accessors (Track 3)

### What Changes

`Pager` gains a second cache field holding decoded `NodePage` structs. Two new methods — `get_node_page` and `set_node_page` — replace direct calls to the generic `get_and_decode` / `encode_and_set` for the `NodePage` type. The existing generic methods remain for `ZeroPage` and `FreeListPage`.

### Implementation Approach

```rust
pub struct Pager {
    path: String,
    file: RefCell<File>,
    /// Raw-bytes page cache (avoids disk I/O).
    cache: RefCell<HashMap<u32, (Page, bool)>>,
    /// Decoded NodePage cache (avoids CBOR deserialization).
    decoded: RefCell<HashMap<u32, NodePage>>,
}
```

`get_node_page`:

```rust
pub fn get_node_page(&self, idx: u32) -> NodePage {
    if let Some(page) = self.decoded.borrow().get(&idx) {
        return page.clone();
    }
    let page: NodePage = self.get_and_decode(idx);
    self.decoded.borrow_mut().insert(idx, page.clone());
    page
}
```

`set_node_page`:

```rust
pub fn set_node_page(&mut self, idx: u32, page: NodePage) -> Result<(), EncodingError> {
    self.decoded.borrow_mut().insert(idx, page.clone());
    self.encode_and_set(idx, &page)
}
```

`deallocate` must evict the freed page from the decoded cache to prevent stale data:

```rust
self.decoded.borrow_mut().remove(&idx);
```

### Key Files

- `src/storage/pager.rs` — add `decoded` field, `get_node_page`, `set_node_page`; evict in `deallocate`

### Tests

- Existing pager tests must continue to pass.
- Add a test that calls `get_node_page` twice on the same page number and verifies no extra CBOR decode occurs (can be checked via a call-count wrapper or simply by asserting the decoded cache length).

### Implementation Steps (1 commit)

#### Step 115.1 — Add decoded cache and NodePage accessors to Pager

Add `decoded: RefCell<HashMap<u32, NodePage>>` to the struct and initialise it as `HashMap::new()` in `Pager::new`. Implement `get_node_page` and `set_node_page` as shown above. Add the `decoded.borrow_mut().remove(&idx)` call in `deallocate`. No callers are changed yet.

**Commit:** `Pager: add decoded NodePage cache and get_node_page/set_node_page accessors`

---

## 116. BTree: route NodePage reads/writes through decoded cache (Track 3)

### What Changes

All 24 call sites in `src/storage/btree.rs` that currently call `pager.get_and_decode(idx)` or `pager.encode_and_set(idx, &node_page)` with a `NodePage` are updated to call `pager.get_node_page(idx)` and `pager.set_node_page(idx, node_page)` respectively.

### Background

The call sites fall into two categories:

**Reads** (`get_and_decode` → `get_node_page`):
- Tree traversal: finding the insertion point, navigating interior nodes, reading leaf cells
- Split logic: loading the overfull page and its parent
- Cursor operations: `first`, `next`, `prev`, `find`
- Debug: `verify`, `print`

**Writes** (`encode_and_set` → `set_node_page`):
- After inserting a cell into a leaf
- After splitting: writing both halves and the updated parent
- When creating an empty root node (`create_tree`)

The overflow page path uses `NodePage::OverflowPage` and also goes through `encode_and_set` / `get_and_decode` — these should be routed through the new accessors too, since overflow pages benefit from caching during large-value reads.

### Implementation Approach

Mechanical substitution: replace each `let page: NodePage = self.pager.get_and_decode(idx)` with `let page = self.pager.get_node_page(idx)` and each `self.pager.encode_and_set(idx, &page)` (where `page: NodePage`) with `self.pager.set_node_page(idx, page.clone())` (or take ownership where possible). The return type of `set_node_page` matches `encode_and_set` so error propagation is unchanged.

Note: a handful of `encode_and_set` calls pass a reference (`&page`) while `set_node_page` takes ownership for cache insertion. Adjust each call site accordingly — either pass `page.clone()` or restructure to move.

### Key Files

- `src/storage/btree.rs` — 24 call sites updated

### Tests

- `cargo test` must pass with zero regressions.
- Run the sakila benchmark (`make test-sakila`) before and after and record the time improvement.

### Implementation Steps (1 commit)

#### Step 116.1 — Route all BTree NodePage accesses through decoded cache

Update all 24 `get_and_decode` / `encode_and_set` call sites in `btree.rs` that deal with `NodePage`. Verify `cargo test` passes. Run `make test-sakila` and record timing.

**Commit:** `BTree: use get_node_page/set_node_page for all NodePage accesses`

---

## Verification

- [ ] `cargo test` — all tests pass, zero regressions
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` — zero warnings
- [ ] `make test-sakila` — sakila schema + data load succeeds; record time vs pre-patch baseline (~4m42s)
- [ ] Each commit is independently buildable and testable

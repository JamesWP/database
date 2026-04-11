# Phase BB — Storage Layer Redesign: NodePageStore

Replace the current monolithic `Pager` with a clean three-layer storage stack and consolidate `Catalog` inside `BTree`, making `BTree` the sole public database entry point.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 128 | 6 | Introduce `PageId` newtype and `Error` enum | — |
| 129 | 6 | Introduce `NodePageStore`: wraps `Pager`, owns NodePage cache, exposes `read`/`take`/`write`/`allocate`/`free`/`flush` | 128 |
| 130 | 6 | Switch `BTree`, `CursorHandle`, and `Cursor` to `NodePageStore`; replace all `pager.*` call sites; collapse `open_readonly`/`open_readwrite` split | 129 |
| 131 | 6 | Strip `Pager` to raw I/O only: remove all decode/encode/cache methods, make unexported; remove `EncodingError`; delete raw page cache | 130 |
| 132 | 6 | Move `Catalog` inside `BTree`; expose catalog methods on `BTree`; update all 15 external `Catalog` callers to go through `BTree`; update `PageId` types throughout | 130 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

The current `Pager` conflates three concerns: raw file I/O, CBOR encode/decode, and a decoded `NodePage` cache. This leads to the problems documented in `pager-design/DESIGN.md`:

- `get_and_decode_node`, `write_node_page`, and `encode_and_set` make `Pager` aware of `NodePage` and CBOR — a B-tree concern in the wrong layer.
- Two caches (`cache: RefCell<HashMap<u32, (Page, bool)>>` for raw bytes, `decoded: RefCell<HashMap<u32, NodePage>>` for decoded nodes) can diverge when updated by different code paths.
- Raw `u32` page IDs have no type safety.
- `get_zero_page` and `set_file_size_pages` expose pager internals to callers.
- `Pager` panics on recoverable errors instead of returning `Result`.

The target architecture (specified in `pager-design/DESIGN.md`) is:

```
BTree / Cursor   — tree structure, splits, rowid cache
                   └── Catalog (internal) — CatalogSnapshot, invalidated at DDL sites
      ↕
NodePageStore    — CBOR encode/decode, single NodePage cache (all pages)
      ↕
Pager            — file I/O, free list, ZeroPage, format version (private)
      ↕
disk
```

**Relationship to Phase BA.** Phase BA (`phase-ba-arc-page-cache.md`) planned an incremental improvement using `Rc<NodePage>` and a `mutate_node` closure API within the existing architecture. Phase BB implements the target architecture from `pager-design/DESIGN.md`, which uses a different ownership model (`take`/`write` with owned `NodePage`) that makes `Rc` and `mutate_node` unnecessary. Phase BA is **superseded by Phase BB**.

The design document that produced this plan is `pager-design/DESIGN.md`. The prototype in `pager-design/src/main.rs` verifies that the ownership and borrow structure compiles.

---

## Stubs

None.

---

## 128. `PageId` newtype and `Error` enum (Track 6)

### What Changes

**New additions:**

- `pub struct PageId(u32)` with `#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]` and an `as_u32(self) -> u32` method.
- `pub enum Error` with variants:
  - `Io(std::io::Error)` — wraps file I/O errors
  - `PageFull(NodePage)` — encoding exceeded page size; returns ownership so caller can split without re-fetch or clone
  - `Decode(String)` — CBOR decode failure
  - `FormatError(String)` — unknown format version or bad magic

**No cleanups in this item** — all existing code is unchanged. The existing `EncodingError` in `pager.rs` is removed in item 131 once its call sites are gone.

### Background

Raw `u32` page IDs are currently passed everywhere — `pager.get(idx)`, `pager.write_node_page(idx, &page)`, `BTree::open(root_page: u32)`. Nothing prevents passing an arbitrary integer as a page ID. `PageId` is a newtype that makes signatures self-documenting and prevents accidental arithmetic on page numbers.

`Error::PageFull(NodePage)` is the key that makes the write path clone-free. When `write` fails because the encoded node exceeds `PAGE_SIZE`, it returns the node back to the caller inside the error. The caller can split the node and write the halves without a re-fetch or clone.

### Implementation Approach

```rust
// src/storage/page_id.rs  (or inline alongside NodePageStore)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId(pub(crate) u32);

impl PageId {
    pub fn as_u32(self) -> u32 { self.0 }
}

pub enum Error {
    Io(std::io::Error),
    PageFull(NodePage),
    Decode(String),
    FormatError(String),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self { Error::Io(e) }
}
```

### Key Files

- `src/storage/` — new file or additions to `mod.rs`; export `PageId` and `Error`

### Tests

No behaviour change; verify `cargo build` compiles cleanly.

### Implementation Steps (1 commit)

#### Step 128.1 — Add `PageId` and `Error`

1. Add `PageId` newtype with derives and `as_u32()`.
2. Add `Error` enum with all four variants; implement `Debug`, `Display`, `std::error::Error`, `From<io::Error>`.
3. Export from `src/storage/mod.rs`.
4. `cargo fmt && cargo build && cargo test`.

**Commit:** `storage: add PageId newtype and Error enum`

---

## 129. `NodePageStore` (Track 6)

### What Changes

**New additions:**

A new `pub struct NodePageStore` wraps the existing `Pager` and owns a `HashMap<PageId, NodePage>` decoded cache. It exposes the full public API for the storage layer. Private `encode_node` / `decode_node` helpers live alongside it.

```rust
pub struct NodePageStore {
    pager: Pager,                        // private
    cache: HashMap<PageId, NodePage>,    // private
}

impl NodePageStore {
    pub fn open(path: &Path) -> Result<Self, Error>;
    pub fn page_count(&self) -> u32;
    pub fn validate_format_version(&self) -> Result<(), Error>;
    pub fn allocate(&mut self) -> Result<PageId, Error>;
    pub fn free(&mut self, id: PageId) -> Result<(), Error>;
    pub fn read(&mut self, id: PageId) -> Result<&NodePage, Error>;
    pub fn take(&mut self, id: PageId) -> Result<NodePage, Error>;
    pub fn write(&mut self, id: PageId, node: NodePage) -> Result<(), Error>;
    pub fn flush(&mut self) -> Result<(), Error>;
}
```

**No cleanups in this item** — `NodePageStore` is added alongside the existing `Pager`. Existing `BTree` code is untouched until item 130.

### Background

#### `read` — borrow into cache

```rust
pub fn read(&mut self, id: PageId) -> Result<&NodePage, Error> {
    self.ensure_cached(id)?;          // &mut borrow ends here
    Ok(self.cache.get(&id).unwrap())  // new shared borrow
}
```

Two separate calls so the borrow checker sees the `&mut` borrow end before the return reference begins. Use for read-only traversal: borrow scoped to extract a `Copy` result (e.g. `SearchResult` containing `PageId`), then dropped before any mutation.

Takes `&mut self` so cache population on a miss does not require `RefCell` on the cache.

#### `take` — owned access for mutation

Removes the cache entry and returns the node owned. On a cache miss, loads from disk without inserting (the caller re-inserts via `write`). Because no borrow is held afterward, `allocate` and `write` can be called freely between `take` and `write`.

```rust
pub fn take(&mut self, id: PageId) -> Result<NodePage, Error> {
    if let Some(node) = self.cache.remove(&id) {
        return Ok(node);
    }
    let raw = self.pager.read_raw(id)?;
    decode_node(&raw)
}
```

#### `write` — consumes, no clone

```rust
pub fn write(&mut self, id: PageId, node: NodePage) -> Result<(), Error> {
    match encode_node(&node) {
        None => Err(Error::PageFull(node)),   // return ownership — no re-fetch, no clone
        Some(bytes) => {
            self.pager.write_raw(id, &bytes)?;
            self.cache.insert(id, node);       // move into cache — no clone
            Ok(())
        }
    }
}
```

#### `free` — evicts cache before returning to pager

```rust
pub fn free(&mut self, id: PageId) -> Result<(), Error> {
    self.cache.remove(&id);
    self.pager.free(id)
}
```

Ensures that if the page is reallocated and written with different content, the next `read` is a clean miss.

#### Allocate before borrowing

`allocate` takes `&mut self`. Calling it while holding a `&NodePage` from `read` would conflict. The discipline: **all `allocate` calls for an operation must happen before any `read` or `take` calls for that operation.** The borrow checker enforces this — code that violates it will not compile.

### Implementation Approach

At this stage `NodePageStore` may call into the existing `Pager` using its current `get`/`set`/`encode_and_set` API internally — the Pager strip happens in item 131. The important thing is the public interface and cache ownership model are correct.

`encode_node` / `decode_node` can be thin wrappers around `ciborium`, moving the logic that currently lives in `Pager::encode_and_set` and `Pager::get_and_decode`.

### Key Files

- `src/storage/node_page_store.rs` — new file
- `src/storage/mod.rs` — export `NodePageStore`

### Tests

Add unit tests for `NodePageStore` directly:
- `read` populates cache on miss; cache hit avoids re-decode
- `take` removes from cache; subsequent `read` re-fetches from disk
- `write` after `take` re-populates cache
- `write` returning `PageFull` gives back the node without a clone
- `free` evicts cache entry

### Implementation Steps (1 commit)

#### Step 129.1 — `NodePageStore` implementation

1. Create `src/storage/node_page_store.rs`.
2. Add private `encode_node` / `decode_node` helpers.
3. Implement `open`, `page_count`, `validate_format_version`, `allocate`, `free`, `flush` delegating to `Pager`.
4. Implement `ensure_cached`, `read`, `take`, `write`.
5. Export from `src/storage/mod.rs`.
6. Add unit tests.
7. `cargo fmt && cargo build && cargo test`.

**Commit:** `storage: add NodePageStore with read/take/write cache layer`

---

## 130. Switch `BTree`, `CursorHandle`, and `Cursor` to `NodePageStore` (Track 6)

### What Changes

**Structural changes:**

```rust
// Before:
pub struct BTree {
    pub(super) pager: Arc<RefCell<pager::Pager>>,
    rowid_cache: Arc<RefCell<HashMap<u32, u64>>>,
}

pub struct CursorHandle {
    pager: Arc<RefCell<Pager>>,
    state: CursorState,
}

pub struct Cursor<'a, PagerRef> {   // generic over Ref<Pager> or RefMut<Pager>
    pager: PagerRef,
    cursor_state: &'a mut CursorState,
}

// After:
pub struct BTree {
    store: Arc<RefCell<NodePageStore>>,
    rowid_cache: Arc<RefCell<HashMap<u32, u64>>>,  // key type updated to PageId in item 132
}

pub struct CursorHandle {
    store: Arc<RefCell<NodePageStore>>,
    state: CursorState,
}

pub struct Cursor<'a> {             // generic eliminated
    store: RefMut<'a, NodePageStore>,
    cursor_state: &'a mut CursorState,
}
```

**Cleanups:**

- Remove `use super::pager::{self, Pager}` import from `btree.rs`.
- Remove `pub(super) pager` field from `BTree`.
- Remove `CursorHandle::open_readonly` and `CursorHandle::open_readwrite` — collapse to a single `CursorHandle::open_cursor() -> Cursor<'_>` that takes `RefMut<NodePageStore>`. Because `NodePageStore::read` takes `&mut self` (no `RefCell` on the cache), all cursors need mutable access regardless of intent. The readonly/readwrite distinction at the type level is gone.
- Remove `Cursor<'a, PagerRef>` generic; `Cursor<'a>` is no longer parameterised.
- Remove `update_page` helper in `btree.rs` — the `PageFull` error is handled inline at each `write` call site.
- Remove all `pager.get_and_decode_node(idx)` call patterns.
- Remove all `pager.write_node_page(idx, &page)` and `pager.mutate_node(...)` call patterns.
- Remove all `pager.allocate()` / `pager.dealocate(idx)` call patterns.
- Remove `pub(super)` visibility on `pager` field — the new `store` field is private.

### Background

#### Read-only traversal: `read`

Interior nodes visited during tree search are read-only. Each borrow is scoped to extract a `SearchResult` (which contains only `Copy` values like `PageId`), then dropped before the next call:

```rust
let result = {
    let node = self.store.read(page_id)?;
    node.search(key)   // SearchResult contains only Copy values
};                     // borrow dropped here
match result {
    SearchResult::GoDown(_, child_id) => page_id = child_id,
    ...
}
```

#### Mutation: `take` + mutate + `write`

Replaces the old `pager.get_and_decode_node(idx)` + `pager.write_node_page(idx, &page)` pattern and Phase BA's `mutate_node` closure:

```rust
// leaf insert
let mut node = self.store.take(leaf_id)?;
node.leaf_mut()?.insert_at(idx, key.to_vec(), value);
match self.store.write(leaf_id, node) {
    Ok(()) => Ok(()),
    Err(Error::PageFull(node)) => self.split_page(node, stack),
    Err(e) => Err(e),
}

// parent interior update after split
let mut parent = self.store.take(parent_id)?;
parent.interior_mut()?.insert_child_page(separator_key, right_id);
self.store.write(parent_id, parent)?;
```

#### Allocate before read/take

All `allocate` calls for an operation happen before any `read` or `take` calls. The borrow checker enforces this.

#### Root split reordering

The existing code writes `left_half` to the root page then immediately re-reads it to copy to `left_id`. The new design allocates both fresh pages first, then writes each half directly to its final destination, then overwrites root with the interior node. One fewer write, one fewer cache round-trip, no clone:

```
Before:  write(root, left_half) → read(root) → write(left_id, left_half)
After:   write(left_id, left_half) → write(right_id, right_half) → write(root, interior)
```

#### `cell_reader.rs`

`CellReader` currently holds a reference to `Arc<RefCell<Pager>>` to follow overflow chains. Update to hold `Arc<RefCell<NodePageStore>>` and use `store.read(continuation_id)?` with a short borrow to extract continuation pointer and content.

#### `btree_verify.rs` and `btree_graph.rs`

Both traverse the tree read-only. Update to use `store.read(page_id)?` with short-lived borrows.

### Key Files

- `src/storage/btree.rs` — primary migration site (~600 lines, many call sites)
- `src/storage/cell_reader.rs` — overflow chain reads
- `src/storage/btree_verify.rs` — verification traversal
- `src/storage/btree_graph.rs` — graphviz dump traversal

### Tests

All existing `cargo test` tests must pass. Verify Sakila data load produces correct results after migration.

### Implementation Steps (2 commits)

#### Step 130.1 — Migrate `BTree`, `CursorHandle`, `Cursor` struct definitions

1. Change `BTree.pager` → `store: Arc<RefCell<NodePageStore>>`.
2. Update `BTree::new`, `BTree::open`, `BTree::flush`, `BTree::create_tree`, `BTree::file_size_pages`.
3. Update `CursorHandle` — replace `pager` field with `store`.
4. Remove `open_readonly` and `open_readwrite`; add `open_cursor() -> Cursor<'_>` returning `RefMut<NodePageStore>`.
5. Remove `Cursor<'a, PagerRef>` generic; update to `Cursor<'a>` with `store: RefMut<'a, NodePageStore>`.
6. `cargo fmt && cargo build` (will fail on method call sites — expected at this step).

**Commit:** `btree: switch BTree/CursorHandle/Cursor structs to NodePageStore`

#### Step 130.2 — Migrate all method call sites; clean up removed helpers

1. Replace all `pager.get_and_decode_node(idx)` with `store.read(PageId(idx))?` (short borrow) or `store.take(PageId(idx))?` (for mutation).
2. Replace all `pager.write_node_page(idx, &page)` / `pager.mutate_node(...)` with `store.write(PageId(idx), node)?` (consuming).
3. Replace all `pager.allocate()` → `store.allocate()?` returning `PageId`.
4. Replace all `pager.dealocate(idx)` → `store.free(PageId(idx))?`.
5. Remove `update_page` helper; handle `Error::PageFull(node)` inline at write sites.
6. Reorder root split to the allocate-first pattern.
7. Update `cell_reader.rs`, `btree_verify.rs`, `btree_graph.rs`.
8. `cargo fmt && cargo build && cargo test`.

**Commit:** `btree: migrate all call sites to NodePageStore read/take/write; remove update_page`

---

## 131. Strip `Pager` to raw I/O, make unexported (Track 6)

### What Changes

After item 130, `Pager`'s decode/encode/cache methods are dead code. This item removes everything that is not raw file I/O or freelist/ZeroPage management.

**Removed from `Pager`:**

| Field / method | Reason |
|---|---|
| `decoded: RefCell<HashMap<u32, NodePage>>` | Replaced by `NodePageStore.cache` |
| `cache: RefCell<HashMap<u32, (Page, bool)>>` | Raw page cache — NodePageStore is the sole cache; Pager becomes stateless I/O |
| `get_and_decode_node`, `get_and_decode` | NodePage decoding moved to NodePageStore |
| `write_node_page`, `encode_and_set` | NodePage encoding moved to NodePageStore |
| `mutate_node`, `take_decoded_node` | Phase BA additions — superseded |
| `get(idx) -> Page` | Replaced by `read_raw(id: PageId) -> Result<[u8; PAGE_SIZE], Error>` |
| `set(idx, page)` | Replaced by `write_raw(id: PageId, bytes: &[u8; PAGE_SIZE]) -> Result<(), Error>` |
| `get_zero_page() -> Option<ZeroPage>` | Internal; callers must not know about ZeroPage |
| `set_file_size_pages` | Internal; callers must not resize the file directly |
| `EncodingError` | Replaced by `Error` from item 128 |
| `Page` struct | No longer part of the public API; used only inside `Pager` if needed |
| Unused generics (`PageNo: Borrow<u32>`, `P: Borrow<Page>`) | Noise with no benefit |

**Renamed / updated:**

- `allocate() -> u32` → `allocate() -> Result<PageId, Error>`
- `dealocate(idx: u32)` → `free(id: PageId) -> Result<(), Error>` (fix spelling)
- `validate_format_version()` (panics) → returns `Result<(), Error>` with `Error::FormatError`

**Visibility:**

- `Pager` becomes `pub(super)` — accessible within `src/storage/` but not re-exported from `mod.rs`.
- `ZeroPage` and `FreeListPage` remain private implementation details inside `pager.rs`.

**Updated in `NodePageStore`:**

`encode_node` / `decode_node` absorb the CBOR logic previously in `Pager::encode_and_set` and `Pager::get_and_decode`. `NodePageStore` calls `pager.read_raw` / `pager.write_raw` directly.

### Background

With all call sites migrated in item 130, the compiler's dead-code warnings will identify exactly what to remove. The raw page cache (`cache: RefCell<HashMap<u32, (Page, bool)>>`) is removed because `NodePageStore` is the sole cache — Pager becomes a pure I/O adapter over the file, with ZeroPage/freelist as internal bookkeeping. This matches the design principle: `Pager` knows nothing about `NodePage` or CBOR.

### Key Files

- `src/storage/pager.rs` — strip and simplify; ~250 lines should become ~150
- `src/storage/node_page_store.rs` — absorb encode/decode logic from `Pager`
- `src/storage/mod.rs` — remove `Pager`, `EncodingError`, `Page` from public exports

### Tests

All existing `cargo test` tests must pass. `cargo build 2>&1 | grep warning` must be clean — zero warnings.

### Implementation Steps (1 commit)

#### Step 131.1 — Strip `Pager`

1. Remove `decoded` field and all decode/encode methods.
2. Remove raw page `cache` field — Pager reads directly from file on every `read_raw`.
3. Rename `get` → `read_raw(id: PageId) -> Result<[u8; PAGE_SIZE], Error>`.
4. Rename `set` → `write_raw(id: PageId, bytes: &[u8; PAGE_SIZE]) -> Result<(), Error>`.
5. Fix `allocate` / `dealocate` to use `PageId` and return `Result`.
6. Change `validate_format_version` from panic to `Result<(), Error>`.
7. Move CBOR logic into `node_page_store.rs` helpers (or confirm they're already there from item 129).
8. Delete `EncodingError` and `Page` from public API surface.
9. Set `Pager` visibility to `pub(super)`; remove from `mod.rs` exports.
10. `cargo fmt && cargo build && cargo test`.

**Commit:** `storage: strip Pager to raw I/O only; remove caches and decode logic; make unexported`

---

## 132. Move `Catalog` inside `BTree`; expose catalog API on `BTree` (Track 6)

### Current state

`Catalog` currently wraps `BTree` as a public entry point:

```rust
pub struct Catalog {
    btree: BTree,
    cache: RefCell<Option<CatalogSnapshot>>,
}
```

15 files across the codebase (`db.rs`, `planner/`, `repl/`, `test.rs`, etc.) hold a `Catalog` directly and use it as their primary database handle. `BTree` is accessed via `catalog.btree()` / `catalog.btree_mut()` or via `From<Catalog> for BTree` conversions.

`Catalog::build_snapshot()` scans the catalog B-tree by calling `self.btree.open(CATALOG_ROOT)` to get a cursor. This works because `Catalog` owns the `BTree`.

### What Changes

**Structural inversion:** `Catalog` moves from wrapping `BTree` to being owned by `BTree`. `BTree` becomes the sole public entry point. `Catalog` is no longer publicly exported.

```rust
// After:
pub struct BTree {
    store: Arc<RefCell<NodePageStore>>,
    rowid_cache: Arc<RefCell<HashMap<PageId, u64>>>,
    catalog: Catalog,                              // moved inside BTree
}

// Catalog becomes internal:
struct Catalog {
    cache: RefCell<Option<CatalogSnapshot>>,       // btree field removed
}
```

**`BTree` gains catalog methods** — forwarding to the internal `Catalog`:

```rust
impl BTree {
    pub fn lookup_table(&self, name: &str) -> Option<(PageId, String)>;
    pub fn lookup_table_by_rootpage(&self, id: PageId) -> Option<String>;
    pub fn lookup_indexes_for_table(&self, name: &str) -> Vec<IndexInfo>;
    pub fn insert_catalog_entry(&mut self, obj_type: &str, name: &str,
                                tbl_name: &str, rootpage: PageId, sql: &str);
    pub fn delete_catalog_entries_for_table(&mut self, name: &str) -> bool;
    pub fn scan_catalog_entries(&self) -> Vec<(String, String, String, PageId, String)>;
}
```

**`build_snapshot` interface change.** Currently `build_snapshot` creates a cursor via `self.btree.open(CATALOG_ROOT)`. Once `Catalog` lives inside `BTree`, it cannot call back into its owner. The signature changes to take a cursor or a store reference passed in from `BTree`:

```rust
// Before (Catalog owns BTree):
fn build_snapshot(&self) -> CatalogSnapshot {
    let mut cursor = self.btree.open(CATALOG_ROOT);
    ...
}

// After (BTree passes cursor down):
fn build_snapshot(cursor: &mut CursorHandle) -> CatalogSnapshot {
    let mut c = cursor.open_cursor();
    ...
}
```

`BTree` calls `Catalog::ensure_cache(&mut self.catalog, &mut self.open(CATALOG_ROOT))` — or an equivalent that passes a cursor — before any lookup.

**Cleanups:**

- Delete `pub struct Catalog` from `src/catalog.rs` public API; make it `pub(crate)` or private.
- Remove `Catalog::btree()`, `Catalog::btree_mut()` — callers use `BTree` directly.
- Remove `impl From<BTree> for Catalog` and `impl From<Catalog> for BTree` conversion impls.
- Remove `pub catalog: Catalog` / `catalog.btree_mut()` patterns from `btree.rs` tests.
- Update `TempCatalog` test helper in `src/test.rs` to wrap `BTree` instead of `Catalog`.
- Update all 15 external call sites to use `BTree` methods: `db.rs`, `planner/schema.rs`, `planner/dml.rs`, `planner/mod.rs`, `planner/select.rs`, `planner/optimizer.rs`, `repl/shared.rs`, `repl/modes/planner.rs`, `repl/tui_debugger.rs`, `storage/btree_graph.rs`, `testing/sql_runner.rs`, `main.rs`.
- Update `rowid_cache` key type from `u32` to `PageId`.
- Update all `CatalogSnapshot` rootpage fields and `IndexInfo.rootpage` from `u32` to `PageId`.
- Update `insert_catalog_entry` signature from `rootpage: u32` to `rootpage: PageId` at all call sites.

### Key Files

- `src/catalog.rs` — remove `btree` field; change `build_snapshot` to accept a cursor parameter; make `Catalog` non-public
- `src/storage/btree.rs` — add `catalog: Catalog` field; add catalog forwarding methods; update `rowid_cache` key type
- `src/db.rs` — primary external caller; replace `Catalog` with `BTree` throughout
- `src/planner/schema.rs`, `src/planner/dml.rs`, `src/planner/mod.rs`, `src/planner/select.rs`, `src/planner/optimizer.rs` — update catalog access
- `src/repl/shared.rs`, `src/repl/modes/planner.rs`, `src/repl/tui_debugger.rs` — update catalog access
- `src/storage/btree_graph.rs` — currently takes `&Catalog`; update to `&BTree`
- `src/test.rs`, `src/testing/sql_runner.rs` — update `TempCatalog` helper
- `src/main.rs` — update entry point

### Tests

- All existing catalog unit tests in `src/catalog.rs` must pass (may need to become `BTree` tests).
- All SQL integration tests must pass.
- `cargo build 2>&1 | grep warning` — zero warnings; no remaining raw `u32` page IDs in the storage/catalog layers.

### Implementation Steps (2 commits)

#### Step 132.1 — Move `Catalog` inside `BTree`; update `build_snapshot` interface

1. Remove `btree: BTree` field from `Catalog`; add `catalog: Catalog` field to `BTree`.
2. Change `Catalog::build_snapshot` to accept a `&mut CursorHandle` parameter; update `ensure_cache` caller in `BTree` to pass `&mut self.open(CATALOG_ROOT)`.
3. Add catalog forwarding methods to `BTree` (`lookup_table`, `insert_catalog_entry`, etc.).
4. Remove `Catalog::btree()`, `Catalog::btree_mut()`, `From<BTree> for Catalog`, `From<Catalog> for BTree`.
5. Make `Catalog` non-public (`pub(crate)` in `catalog.rs`).
6. `cargo fmt && cargo build` (call sites still broken — expected at this step).

**Commit:** `catalog: move Catalog inside BTree; expose catalog API on BTree`

#### Step 132.2 — Update all 15 external callers; PageId cleanup

1. Update all external callers in `db.rs`, `planner/`, `repl/`, `main.rs`, `test.rs`, `testing/` to use `BTree` methods instead of `Catalog`.
2. Update `TempCatalog` test helper to wrap `BTree`.
3. Update `btree_graph.rs` to accept `&BTree` instead of `&Catalog`.
4. Update `rowid_cache` key type from `u32` to `PageId`.
5. Update `CatalogSnapshot` rootpage fields and `IndexInfo.rootpage` to `PageId`.
6. Audit for any remaining raw `u32` page numbers in catalog/storage layers.
7. `cargo fmt && cargo build && cargo test`.

**Commit:** `catalog: update all callers to use BTree; switch rootpage fields to PageId`

---

## Verification

- [ ] `cargo test` — all tests pass after each item independently
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` — zero warnings after each item
- [ ] Phase BA (`phase-ba-arc-page-cache.md`) marked **Superseded by Phase BB** in README
- [ ] `pager-design/` prototype remains as reference; no changes needed to it
- [ ] Sakila data load produces correct results after item 130
- [ ] No raw `u32` page IDs remain in the public surface of the storage layer after item 132
- [ ] Each commit is independently buildable and testable

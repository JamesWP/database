# Phase BA — Eliminate Page Clones

Remove unnecessary `NodePage` and raw `Page` copies from the hot paths in the pager and B-tree.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 125 | 3 | `Rc<NodePage>` in decoded cache; `get_and_decode_node` returns `Rc<NodePage>` (O(1) clone); `write_node_page` takes `Rc<NodePage>`; stale-read enforcement via `strong_count` debug assertions | — |
| 126 | 3 | `Pager::set` takes `Page` by value — eliminate 4 KB raw-page clone on every write | — |
| 127 | 3 | Zero-copy mutation: `Pager::mutate_node` closure API; add `interior_mut`/`leaf_mut` to `NodePage`; refactor btree mutation paths | 125 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

Six `.clone()` calls in `pager.rs` copy page data unnecessarily. Annotated by frequency:

| Line | Site | Cost | Frequency |
|------|------|------|-----------|
| 191 | `get_and_decode_node` cache hit: `page.clone()` | full `NodePage` | **hot — every cursor read** |
| 256 | `write_node_page`: `page.clone()` into decoded cache | full `NodePage` | **every node write** |
| 211 | `set()`: `page.clone()` into raw cache | 4 KB `Page` | **every node write** |
| 199 | `get_and_decode_node` cache miss: `node.clone()` into decoded cache | full `NodePage` | cold — first decode |
| 168 | `get()` cache miss: `p.clone()` into raw cache | 4 KB `Page` | cold — disk read |
| 155 | `get()` cache hit: `page.clone()` | 4 KB `Page` | cold after item 125 |

**Item 125** eliminates lines 191, 199, and 256.
**Item 126** eliminates line 211 (and incidentally 168 along the way).
**Item 127** eliminates the remaining `NodePage` clone in the mutation path introduced by item 125.

Lines 155/168 become cold paths for NodePages after item 125 (decoded cache handles all subsequent
reads); they are not worth a dedicated item.

---

## Caching Invariants & Enforcement

Three correctness assumptions underpin the Rc approach. Each has an enforcement option.

### Invariant 1 — No stale Rc references across a write

With `Rc<NodePage>`, a caller can hold an Rc to page X while another operation replaces page X's
entry in the decoded cache (e.g. via `write_node_page`). The caller now has stale data. In the
current sequential single-threaded design this doesn't happen because Rc clones are short-lived
(read, use, drop within one operation; never stored in cursor state). But it is a silent assumption
with no enforcement.

**Debug-time enforcement — `Rc::strong_count` assertion at write and take sites:**

When `write_node_page` is about to replace a cache entry, if any caller still holds a clone of the
existing `Rc`, `Rc::strong_count` will be > 1. Assert this in debug builds:

```rust
pub fn write_node_page(&mut self, idx: u32, page: Rc<NodePage>) -> Result<(), EncodingError> {
    #[cfg(debug_assertions)]
    if let Some(existing) = self.decoded.borrow().get(&idx) {
        debug_assert!(
            Rc::strong_count(existing) == 1,
            "writing page {} while {} readers still hold a reference",
            idx,
            Rc::strong_count(existing) - 1
        );
    }
    let result = self.encode_and_set(idx, &*page);
    if result.is_ok() {
        self.decoded.borrow_mut().insert(idx, page);
    }
    result
}
```

Similarly in `take_decoded_node` (used by `mutate_node`):

```rust
fn take_decoded_node(&self, page_no: u32) -> Option<Rc<NodePage>> {
    let rc = self.decoded.borrow_mut().remove(&page_no)?;
    #[cfg(debug_assertions)]
    debug_assert!(
        Rc::strong_count(&rc) == 1,
        "taking page {} for mutation while {} readers hold references",
        page_no,
        Rc::strong_count(&rc) - 1
    );
    Some(rc)
}
```

This catches exactly the stale-read scenario at test time with zero release overhead. Unlike
`Ref<'_, NodePage>`, callers receive an owned `Rc` — no borrow lifetime to manage, no risk of a
runtime panic from holding a guard too long, and no dual read-path API.

**Recommendation:** use `Rc<NodePage>` throughout (not `Arc`). `Rc` is appropriate for
single-threaded code; `Arc` would add unnecessary atomic reference-count overhead. Add the
`strong_count` assertions at `write_node_page` and `take_decoded_node`.

### Invariant 2 — `take_decoded_node` must always be paired with `write_node_page`

Between `take_decoded_node(idx)` and the subsequent `write_node_page(idx, ...)`, the decoded cache
has no entry for that page. A `get_and_decode_node(idx)` call in that window would fall back to the
raw/disk level and return the pre-mutation on-disk version — silently inconsistent with the
in-memory state the caller is about to write.

This is safe in single-threaded sequential code (no interleaving), but the pairing is invisible.

**Compile-time enforcement — closure API:**

```rust
/// Fetch page `page_no`, pass a mutable reference to the closure, then write the
/// result back. Returns the closure's return value and the write result.
/// On `Err(NotEnoughSpaceInPage)`, the mutated (overfull) page is returned in the
/// error so the caller can split it — the decoded cache is not populated in that case.
pub fn mutate_node<R>(
    &mut self,
    page_no: u32,
    f: impl FnOnce(&mut NodePage) -> R,
) -> (R, Result<(), (EncodingError, NodePage)>) {
    let rc = self.take_decoded_node(page_no)
        .unwrap_or_else(|| self.get_decoded_rc(page_no));
    let mut page = Rc::try_unwrap(rc).unwrap_or_else(|r| (*r).clone());
    let r = f(&mut page);
    let result = self.write_node_page(page_no, Rc::new(page));
    // write_node_page consumes the Rc; on error the page is gone. We need to return
    // the page on failure so the caller can split it. Restructure so the page is kept:
    (r, result.map_err(|e| (e, /* see implementation note below */)))
}
```

**Implementation note:** because `write_node_page` consumes the `Rc<NodePage>` and only stores
it in the decoded cache on success, the error path would lose the mutated page. Instead,
`mutate_node` should hold the page as an owned `NodePage` and pass `Rc::new` only on success:

```rust
pub fn mutate_node<R>(
    &mut self,
    page_no: u32,
    f: impl FnOnce(&mut NodePage) -> R,
) -> (R, Result<(), (EncodingError, NodePage)>) {
    let rc = self.take_decoded_node(page_no)
        .unwrap_or_else(|| self.get_decoded_rc(page_no));
    let mut page = Rc::try_unwrap(rc).unwrap_or_else(|r| (*r).clone());
    let r = f(&mut page);
    // Try encoding without consuming `page`:
    let result = self.encode_and_set(page_no, &page);
    match result {
        Ok(()) => {
            self.decoded.borrow_mut().insert(page_no, Rc::new(page));
            (r, Ok(()))
        }
        Err(e) => {
            // Page is NOT put back in the decoded cache — caller must handle or split.
            (r, Err((e, page)))
        }
    }
}
```

The `NotEnoughSpaceInPage` error (overfull page → split) is returned together with the owned
`NodePage` so `split_page` can consume it directly — no extra clone. The caller (`update_page`)
pattern-matches the error and passes the returned page to `split_page`.

**Recommendation:** introduce `Pager::mutate_node` for item 127 instead of exposing
`take_decoded_node` directly.

### Invariant 3 — Page repurposing must invalidate the decoded cache

If a NodePage's page number is ever freed and reallocated for a different use (another NodePage,
or a FreeListPage), the decoded cache would serve the old `Rc<NodePage>` for that page number.
Today this doesn't arise because v1 leaks freed pages (DELETE doesn't reclaim space, DROP TABLE is
incomplete). But any future page-reclamation path must call:

```rust
pub fn invalidate_decoded(&self, page_no: u32) {
    self.decoded.borrow_mut().remove(&page_no);
}
```

for every page returned to the freelist. Add a `// SAFETY: must call invalidate_decoded` comment
in the allocator's freelist return path as a reminder.

---

## Stubs

None.

---

## 125. `Rc<NodePage>` in decoded cache; updated `write_node_page` (Track 3)

### What Changes

- `Pager::decoded` changes from `RefCell<HashMap<u32, NodePage>>` to
  `RefCell<HashMap<u32, Rc<NodePage>>>`.
- `get_and_decode_node` returns `Rc<NodePage>` via `Rc::clone` of the cache entry.
  - Cache hit: returns `Rc::clone` — O(1), no allocation. **Was: full NodePage clone.**
  - Cache miss: decodes, wraps in `Rc::new`, inserts into cache (no clone of NodePage), then
    returns `Rc::clone` of the new cache entry. **Was: `node.clone()` into cache (one clone).**
  - Stale-read enforcement via `Rc::strong_count` assertions in `write_node_page` and
    `take_decoded_node` (debug builds only — see Invariant 1).
- `write_node_page` signature changes from `&NodePage` to `Rc<NodePage>`. The `Rc` is stored
  directly in the decoded cache. **Was: `page.clone()` into cache (one full NodePage clone per
  write).** The encoding path (`encode_and_set`) receives `&*page` (`&NodePage`) — no change to
  the serialisation logic.
- All callers of `get_and_decode_node` updated (see Implementation Approach below).
- All callers of `write_node_page` updated: pass `Rc::new(owned_page)` instead of `&page`.

### Background

`Rc<T>: Deref<Target=T>`, so method calls like `page.search(key)` and `page.leaf()` work on
`Rc<NodePage>` via auto-deref without any change. The only patterns that need updating are:

1. **Match-destructure** — `match page { NodePage::Leaf(l) => ... }` cannot move out of `Rc`.
   Change to `match page.as_ref() { ... }`. `l` and `i` become `&LeafNodePage` /
   `&InteriorNodePage`; all methods called on them take `&self`, so no further changes are needed.

2. **Consuming methods** — `interior(self)`, `split(self)` take ownership. Callers use
   `Rc::unwrap_or_clone(page)` to get an owned `NodePage` first. Because the decoded cache also
   holds an `Rc` at these sites, the strong count is 2 and `unwrap_or_clone` always clones. Item
   127 eliminates that clone by removing the cache entry first via `mutate_node`.

`Rc::unwrap_or_clone` is stable since Rust 1.76.

### Implementation Approach

**`src/storage/pager.rs` — decoded cache and `get_and_decode_node`:**

```rust
// Field:
decoded: RefCell<HashMap<u32, Rc<NodePage>>>,

// get_and_decode_node — cache hit (was page.clone()):
if let Some(page) = self.decoded.borrow().get(&page_no) {
    return Rc::clone(page);  // O(1) — single integer increment
}

// get_and_decode_node — cache miss (was node.clone() into cache):
let node = Rc::new(self.get_and_decode::<NodePage, _>(page_no));
// fire typed probe on &node ...
self.decoded.borrow_mut().insert(page_no, Rc::clone(&node));
node
```

**`src/storage/pager.rs` — `write_node_page` (was `&NodePage`, cloning into cache):**

```rust
pub fn write_node_page(&mut self, idx: u32, page: Rc<NodePage>) -> Result<(), EncodingError> {
    #[cfg(debug_assertions)]
    if let Some(existing) = self.decoded.borrow().get(&idx) {
        debug_assert!(
            Rc::strong_count(existing) == 1,
            "writing page {} while {} readers still hold a reference",
            idx,
            Rc::strong_count(existing) - 1
        );
    }
    let result = self.encode_and_set(idx, &*page);  // serialize from &NodePage
    if result.is_ok() {
        self.decoded.borrow_mut().insert(idx, page);  // store Rc directly — no clone
    }
    result
}
```

Note: `encode_and_set` takes `v: P` where `P: Serialize`. Passing `&*page` gives `&NodePage`
which implements `Serialize`.

**`src/storage/btree.rs` — callers of `write_node_page`:**

Most call sites pass a locally-owned `NodePage` that isn't needed after the write. Change to
`Rc::new(owned_page)` (moves the local into the Rc — no clone):

```rust
// Before:
self.pager.write_node_page(overfull_idx, &left_half)?;

// After:
self.pager.write_node_page(overfull_idx, Rc::new(left_half))?;
```

**`update_page` — split fallback:**

`update_page` passes `modified_page` by value. With `Rc<NodePage>`, `write_node_page` consumes the
`Rc`. On `NotEnoughSpaceInPage` the page is gone. The fix: hold the page as `NodePage` (owned, not
`Rc`), try encoding, then move into `Rc` only on success:

```rust
fn update_page(&mut self, modified_page: NodePage, stack: Vec<u32>) {
    let modified_page_idx = *stack.last().unwrap();
    let result = self.pager.encode_and_set(modified_page_idx, &modified_page);
    match result {
        Ok(()) => {
            self.pager.decoded.borrow_mut()
                .insert(modified_page_idx, Rc::new(modified_page));
            /* fire probe */
        }
        Err(EncodingError::NotEnoughSpaceInPage) => {
            // modified_page still owned here — pass directly to split, no clone.
            self.split_page(modified_page, stack);
        }
        Err(EncodingError::SerializationError(e)) => panic!("{}", e),
    }
}
```

Note: this is item 125's `update_page`. Item 127 replaces it with `mutate_node` which handles
the same logic internally.

**`src/storage/btree.rs` — read-only match callers** (`select_leftmost_of_idx`,
`select_rightmost_of_idx`, etc.):

```rust
// Before:
let page = self.pager.get_and_decode_node(page_idx);
match page {
    NodePage::Leaf(l)     => { ... l.num_items() ... }
    NodePage::Interior(i) => { ... i.num_edges() ... }
    NodePage::OverflowPage(_) => panic!(),
}

// After:
let page = self.pager.get_and_decode_node(page_idx);
match page.as_ref() {
    NodePage::Leaf(l)     => { ... l.num_items() ... }
    NodePage::Interior(i) => { ... i.num_edges() ... }
    NodePage::OverflowPage(_) => panic!(),
}
```

**`src/storage/btree.rs` — `find()` and cursor methods that call `page.search(key)`:**

No change needed. `Rc<NodePage>` auto-derefs to `NodePage`; `search()` takes `&self`.

**`src/storage/btree.rs` — insert loop (`btree.rs:191`):**

Interior pages visited during GoDown are read-only; the `Rc` is dropped at the end of each
iteration — no clone paid. Only the leaf page is mutated:

```rust
let top_page = self.pager.get_and_decode_node(top_page_idx);
match top_page.search(key) {  // via Deref — no clone
    Found(i) => {
        let mut owned = Rc::unwrap_or_clone(top_page);  // clones — item 127 eliminates this
        owned.set_item_at_index(i, cell);
        self.update_page(owned, stack);
    }
    NotPresent(i) => {
        let mut owned = Rc::unwrap_or_clone(top_page);  // ditto
        owned.insert_item_at_index(i, cell);
        self.update_page(owned, stack);
    }
    GoDown(_, child) => { stack.push(child); }  // top_page dropped — free
}
```

**`src/storage/btree.rs` — split path** (`btree.rs:314`):

```rust
// Before:
let parent_page = self.pager.get_and_decode_node(parent_idx);
let mut parent_interior = parent_page.interior().unwrap();

// After:
let parent_page = self.pager.get_and_decode_node(parent_idx);
let mut parent_interior = Rc::unwrap_or_clone(parent_page).interior().unwrap();
```

**`src/storage/cell_reader.rs`** — drop the `Box<NodePage>` wrapper; use `Rc<NodePage>` directly:

```rust
// Before:
let node: Box<NodePage> = Box::new(self.pager.get_and_decode_node(continuation));

// After:
let node: Rc<NodePage> = self.pager.get_and_decode_node(continuation);
```

**`src/storage/btree_verify.rs`** and **`src/storage/btree_graph.rs`** — change `match page { ... }` to `match page.as_ref() { ... }` at all 7 call sites. No consuming calls expected.

### Key Files

- `src/storage/pager.rs` — `decoded` field type, `get_and_decode_node`, `write_node_page`
- `src/storage/btree.rs` — ~12 `get_and_decode_node` sites + ~6 `write_node_page` sites
- `src/storage/cell_reader.rs` — 2 sites
- `src/storage/btree_verify.rs` — 3 sites
- `src/storage/btree_graph.rs` — 4 sites

### Tests

All existing `cargo test` tests must pass — this is a refactor with no behaviour change.

### Implementation Steps (1 commit)

The return type of `get_and_decode_node` and signature of `write_node_page` both change; all
callers must be updated in one go for the build to succeed.

#### Step 125.1 — Rc decoded cache, updated API, all call sites

1. Change `Pager::decoded` field type to `RefCell<HashMap<u32, Rc<NodePage>>>`.
2. Update `get_and_decode_node` to return `Rc<NodePage>`.
3. Update `write_node_page` to take `Rc<NodePage>`, store Rc directly, add `strong_count` assertion.
4. Update `update_page` in `btree.rs` to hold `NodePage` by value and call `encode_and_set` directly
   so the owned page is available for `split_page` on `NotEnoughSpaceInPage`.
5. Update all call sites across `btree.rs`, `cell_reader.rs`, `btree_verify.rs`, `btree_graph.rs`.
6. `cargo fmt && cargo build && cargo test`.

**Commit:** `storage: cache Rc<NodePage> in decoded cache; write_node_page takes Rc`

---

## 126. `Pager::set` takes `Page` by value (Track 3)

### What Changes

`set()` currently takes `P: Borrow<Page>` and does `page.clone()` to insert the 4 KB raw `Page`
into the raw cache before writing to disk (line 211). Since the only caller is `encode_and_set`,
which builds a fresh `Page` locally, it can simply move ownership:

```rust
// Before:
pub fn set<P: Borrow<Page>, PageNo: Borrow<u32>>(&mut self, idx: PageNo, page: P) {
    let page = page.borrow();
    self.cache.borrow_mut().insert(page_no, (page.clone(), false));  // 4 KB clone
    file.write_all(&page.content).unwrap();
}

// After:
pub fn set(&mut self, idx: u32, page: Page) {
    probe!(database, page_write, idx);
    let mut file = self.file.borrow_mut();
    let offset = PAGE_SIZE * (idx as u64);
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&page.content).unwrap();
    self.cache.borrow_mut().insert(idx, (page, false));  // moved — no clone
}
```

`encode_and_set` builds `page` locally and passes it by value — no change to its logic, just
removes the borrow indirection. The same goes for `get()` cache miss (line 168): change
`(p.clone(), false)` → `(p, false)` and return a copy from the raw cache only if explicitly
needed, or read the returned value directly from the disk read.

### Background

Every `write_node_page` call (one per leaf or interior page update) flows through `encode_and_set`
→ `set()`. Each call currently clones a 4 KB `[u8; 4096]` array onto the heap (the `Page` is
stack-allocated, but `HashMap::insert` copies it). Passing by value lets the HashMap take
ownership without an extra copy.

The `get()` cache miss path (line 168) can be fixed simultaneously: read the page into `p`, write
it into the cache by value, then return a clone from the cache entry (or restructure so `p` is
kept and only the cache entry is the copy). Since this path is cold after item 125, the primary
win is in `set()`.

### Key Files

- `src/storage/pager.rs` — simplify `set()`, update `encode_and_set` call site, fix `get()` clone

### Tests

All existing `cargo test` tests must pass.

### Implementation Steps (1 commit)

#### Step 126.1 — `set()` by value, fix `get()` cache-miss clone

1. Rewrite `set()` to take `(idx: u32, page: Page)` by value; write to disk first, then move into
   cache.
2. Update `encode_and_set` to pass the locally-built `Page` by value (remove `&`).
3. In `get()` cache miss path: insert `p` by value into cache, return a clone from the cache entry
   (keeps the cache as the owner).
4. `cargo fmt && cargo build && cargo test`.

**Commit:** `storage: Pager::set takes Page by value, eliminating 4 KB raw-cache clone`

---

## 127. Zero-copy mutation: `Pager::mutate_node` closure API (Track 3)

### What Changes

After item 125, mutation paths in `btree.rs` call `Rc::unwrap_or_clone()` before modifying a
page. Because the decoded cache still holds a reference, the strong count is always 2 and
`unwrap_or_clone` always clones. This item eliminates that clone and, as described in **Invariant
2**, enforces the take→mutate→write pairing at the API level via a closure:

```rust
pub fn mutate_node<R>(
    &mut self,
    page_no: u32,
    f: impl FnOnce(&mut NodePage) -> R,
) -> (R, Result<(), (EncodingError, NodePage)>)
```

The closure receives `&mut NodePage`. After the closure returns, `mutate_node` tries to encode the
page; on success it wraps the page in `Rc::new` and stores it in the decoded cache (no clone — moves
ownership). On `NotEnoughSpaceInPage`, the owned `NodePage` is returned in the `Err` variant so the
caller can pass it directly to `split_page` without cloning. The write-back or error-return is
mandatory and compile-time impossible to omit.

A private `take_decoded_node` is added internally to `Pager` for use by `mutate_node`; it is not
exposed publicly.

**Note on consuming methods in closures:** `interior(self)` and `split(self)` on `NodePage` take
ownership. Since the closure receives `&mut NodePage`, these cannot be called directly inside a
`mutate_node` closure. Split and parent-update paths that need to consume the page call
`mutate_node` with a closure that only performs `insert_child_page` (on `&mut InteriorNodePage` via
a new `interior_mut(&mut self)` method) or `remove_cell` (on `&mut LeafNodePage` via `leaf_mut`).
The `split` call itself happens outside `mutate_node`, using the owned `NodePage` returned from the
error variant. This requires adding `interior_mut` and `leaf_mut` methods to `NodePage`.

### Background

The complete zero-copy mutation flow after items 125+127:

```
mutate_node(idx, |p| { ... })
  ├─ take_decoded_node(idx)    → Rc (refcount = 1, removed from cache)
  ├─ Rc::try_unwrap(rc)        → NodePage (zero copy — sole owner)
  ├─ f(&mut page)              → closure mutates page
  ├─ encode_and_set(idx, &page) → serialise to disk
  ├─ Ok  → decoded cache insert(Rc::new(page))   — no clone
  └─ Err → return (r, Err((error, page)))        — page returned to caller
```

vs. post-125, pre-127:

```
get_and_decode_node(idx)    → Rc (refcount = 2)
Rc::unwrap_or_clone(rc)     → NodePage (always clones — refcount was 2)
mutate page inline
update_page(page, stack)    → encode_and_set + cache insert
```

### Implementation Approach

**`src/storage/node.rs` — add mutable accessor methods:**

```rust
impl NodePage {
    pub fn interior_mut(&mut self) -> Option<&mut InteriorNodePage> {
        match self {
            NodePage::Interior(i) => Some(i),
            _ => None,
        }
    }

    pub fn leaf_mut(&mut self) -> Option<&mut LeafNodePage> {
        match self {
            NodePage::Leaf(l) => Some(l),
            _ => None,
        }
    }
}
```

**`src/storage/pager.rs`:**

```rust
/// Internal: remove decoded cache entry so the caller gets sole Rc ownership.
fn take_decoded_node(&self, page_no: u32) -> Option<Rc<NodePage>> {
    let rc = self.decoded.borrow_mut().remove(&page_no)?;
    #[cfg(debug_assertions)]
    debug_assert!(
        Rc::strong_count(&rc) == 1,
        "taking page {} for mutation while {} readers hold references",
        page_no,
        Rc::strong_count(&rc) - 1
    );
    Some(rc)
}

/// Internal: get Rc from decoded cache, populating from disk if absent.
fn get_decoded_rc(&self, page_no: u32) -> Rc<NodePage> {
    self.get_and_decode_node(page_no)
}

/// Fetch page `page_no`, apply `f` to a mutable reference, then write the result back.
/// Returns `(closure_result, Ok(()))` on success.
/// Returns `(closure_result, Err((error, page)))` on encoding failure — the owned page
/// is returned so the caller can split it without cloning.
pub fn mutate_node<R>(
    &mut self,
    page_no: u32,
    f: impl FnOnce(&mut NodePage) -> R,
) -> (R, Result<(), (EncodingError, NodePage)>) {
    let rc = self.take_decoded_node(page_no)
        .unwrap_or_else(|| self.get_decoded_rc(page_no));
    // sole owner after take — try_unwrap always succeeds
    let mut page = Rc::try_unwrap(rc).unwrap_or_else(|r| (*r).clone());
    let r = f(&mut page);
    let result = self.encode_and_set(page_no, &page);
    match result {
        Ok(()) => {
            self.decoded.borrow_mut().insert(page_no, Rc::new(page));
            (r, Ok(()))
        }
        Err(e) => (r, Err((e, page))),
    }
}
```

**`src/storage/btree.rs` — insert loop:**

```rust
let search_result = self.pager.get_and_decode_node(top_page_idx).search(key);
// Rc dropped here after search returns owned SearchResult
match search_result {
    Found(i) => {
        let (_, result) = self.pager.mutate_node(top_page_idx, |p| {
            p.set_item_at_index(i, cell);
        });
        match result {
            Ok(()) => { /* fire probe */ }
            Err((EncodingError::NotEnoughSpaceInPage, page)) => {
                self.split_page(page, stack);
            }
            Err((EncodingError::SerializationError(e), _)) => panic!("{}", e),
        }
    }
    NotPresent(i) => {
        let (_, result) = self.pager.mutate_node(top_page_idx, |p| {
            p.insert_item_at_index(i, cell);
        });
        // same error handling
    }
    GoDown(_, child) => { stack.push(child); }
}
```

**Split path** — parent update uses `interior_mut` inside the closure:

```rust
let (_, result) = self.pager.mutate_node(parent_idx, |p| {
    p.interior_mut().unwrap().insert_child_page(right_first_key, right_idx);
});
// handle result (may recurse into split_page if parent is also overfull)
```

**Delete path** — uses `leaf_mut` inside the closure:

```rust
let (_, result) = self.pager.mutate_node(leaf_page_idx, |p| {
    p.leaf_mut().unwrap().remove_cell(cell_index);
});
```

### Key Files

- `src/storage/node.rs` — add `interior_mut` and `leaf_mut` methods to `NodePage`
- `src/storage/pager.rs` — add private `take_decoded_node`, public `mutate_node`
- `src/storage/btree.rs` — replace `Rc::unwrap_or_clone` + inline mutation with `mutate_node` calls at insert, split, and delete sites

### Tests

All existing `cargo test` tests must pass.

### Implementation Steps (1 commit)

#### Step 127.1 — `mutate_node` closure API + zero-copy mutation sites

1. Add `NodePage::interior_mut` and `NodePage::leaf_mut` to `node.rs`.
2. Add private `Pager::take_decoded_node()` (with `strong_count` assertion) and public
   `Pager::mutate_node()` returning `(R, Result<(), (EncodingError, NodePage)>)`.
3. Replace inline `Rc::unwrap_or_clone` + mutation in the insert loop with `mutate_node`.
4. Replace split-path parent-update with `mutate_node` using `interior_mut`.
5. Replace delete-path leaf mutation with `mutate_node` using `leaf_mut`.
6. `cargo fmt && cargo build && cargo test`.

**Commit:** `storage: zero-copy page mutation via Pager::mutate_node closure API`

---

## Verification

- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` — zero warnings
- [ ] `cargo test` — all tests pass after each item independently
- [ ] `perf stat -e instructions cargo test test_sql_` — instruction count lower after item 125
  vs baseline (scan-heavy tests benefit most from eliminated NodePage clones)
- [ ] Sakila data load instruction count lower after item 127 vs after item 125 (INSERT path)
- [ ] Each commit is independently buildable and testable

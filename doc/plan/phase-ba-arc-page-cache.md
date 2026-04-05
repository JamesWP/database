# Phase BA — Eliminate Page Clones

Remove unnecessary `NodePage` and raw `Page` copies from the hot paths in the pager and B-tree.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 125 | 3 | `Arc<NodePage>` in decoded cache; `get_and_decode_node` returns `Ref<'_, NodePage>` (zero-copy, RefCell-enforced); `write_node_page` takes `Arc<NodePage>` | — |
| 126 | 3 | `Pager::set` takes `Page` by value — eliminate 4 KB raw-page clone on every write | — |
| 127 | 3 | Zero-copy mutation: `Pager::take_decoded_node`; refactor btree mutation paths | 125 |

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

Three correctness assumptions underpin the Arc approach. Each has an enforcement option.

### Invariant 1 — No stale Arc references across a write

With `Arc<NodePage>`, a caller can hold an Arc to page X while another operation replaces page X's
entry in the decoded cache (e.g. via `write_node_page`). The caller now has stale data. In the
current sequential single-threaded design this doesn't happen because Arcs are short-lived (read,
use, drop within one operation; never stored in cursor state). But it is a silent assumption with no
enforcement.

**Runtime enforcement — return `Ref<'_, NodePage>` instead of `Arc<NodePage>`:**

`get_and_decode_node` can return `Ref<'_, NodePage>` by mapping into the `RefCell`-guarded
HashMap:

```rust
pub fn get_decoded_ref<PageNo: Borrow<u32>>(&self, idx: PageNo) -> Ref<'_, NodePage> {
    let page_no = *idx.borrow();
    // Populate decoded cache if absent (via borrow_mut, then release before borrow)
    if !self.decoded.borrow().contains_key(&page_no) {
        let node = Arc::new(self.get_and_decode::<NodePage, _>(page_no));
        self.decoded.borrow_mut().insert(page_no, node);
    }
    Ref::map(self.decoded.borrow(), |m| m.get(&page_no).unwrap().as_ref())
}
```

`Ref<'_, NodePage>` is zero-copy (a reference into the HashMap entry) and its lifetime is tied to
the `RefCell` borrow. Because `take_decoded_node` calls `borrow_mut()`, attempting to call it
while any `Ref<'_, NodePage>` is live **panics** — the RefCell borrow checker enforces "no write
while holding a read reference" automatically at runtime with no extra code.

This is strictly better than returning `Arc<NodePage>` for reads: zero-copy, zero heap allocation,
and self-enforcing. The cache can keep `Arc<NodePage>` internally so `take_decoded_node` can still
extract sole ownership without cloning. Callers never see the `Arc`.

**Recommendation:** change `get_and_decode_node` to return `Ref<'_, NodePage>` rather than
`Arc<NodePage>`. The `Arc` becomes a private implementation detail of the cache.

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
pub fn mutate_node<R>(
    &mut self,
    page_no: u32,
    f: impl FnOnce(&mut NodePage) -> R,
) -> (R, Result<(), EncodingError>) {
    let arc = self.take_decoded_node(page_no)
        .unwrap_or_else(|| self.get_and_decode_node_arc(page_no));
    let mut page = Arc::try_unwrap(arc).unwrap_or_else(|a| (*a).clone());
    let r = f(&mut page);
    let result = self.write_node_page(page_no, Arc::new(page));
    (r, result)
}
```

The write-back is impossible to forget: it always happens at the end of `mutate_node`. The closure
forces the take→mutate→write cycle to be atomic from the caller's perspective.

The `NotEnoughSpaceInPage` error (overfull page → split) is returned as the second tuple element;
the caller (`update_page`) handles it by calling `split_page`. The split path itself would call
`mutate_node` for the parent page.

**Recommendation:** introduce `Pager::mutate_node` for item 127 instead of exposing
`take_decoded_node` directly.

### Invariant 3 — Page repurposing must invalidate the decoded cache

If a NodePage's page number is ever freed and reallocated for a different use (another NodePage,
or a FreeListPage), the decoded cache would serve the old `Arc<NodePage>` for that page number.
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

## 125. `Arc<NodePage>` in decoded cache; updated `write_node_page` (Track 3)

### What Changes

- `Pager::decoded` changes from `RefCell<HashMap<u32, NodePage>>` to
  `RefCell<HashMap<u32, Arc<NodePage>>>`.
- `get_and_decode_node` (renamed `get_decoded_ref`) returns `Ref<'_, NodePage>` via
  `Ref::map` into the decoded cache HashMap.
  - Cache hit: returns a `Ref` — zero-copy, no allocation. **Was: full NodePage clone.**
  - Cache miss: decodes, wraps in `Arc::new`, inserts into cache (no clone of NodePage), then
    returns a `Ref` into the new cache entry. **Was: `node.clone()` into cache (one clone).**
  - While any `Ref` is live, `borrow_mut()` on the decoded cache panics — which means
    `write_node_page` (and `take_decoded_node` used by `mutate_node`) cannot be called,
    enforcing "no write while holding a read reference" at runtime automatically.
- `write_node_page` signature changes from `&NodePage` to `Arc<NodePage>`. The `Arc` is stored
  directly in the decoded cache. **Was: `page.clone()` into cache (one full NodePage clone per
  write).** The encoding path (`encode_and_set`) receives `&*page` (`&NodePage`) — no change to
  the serialisation logic.
- All callers of `get_and_decode_node` updated (see Implementation Approach below).
- All callers of `write_node_page` updated: pass `Arc::new(owned_page)` instead of `&page`.

### Background

`Arc<T>: Deref<Target=T>`, so method calls like `page.search(key)` and `page.leaf()` work on
`Arc<NodePage>` via auto-deref without any change. The only patterns that need updating are:

1. **Match-destructure** — `match page { NodePage::Leaf(l) => ... }` cannot move out of `Arc`.
   Change to `match page.as_ref() { ... }`. `l` and `i` become `&LeafNodePage` /
   `&InteriorNodePage`; all methods called on them take `&self`, so no further changes are needed.

2. **Consuming methods** — `interior(self)`, `split(self)` take ownership. Callers use
   `Arc::unwrap_or_clone(page)` to get an owned `NodePage` first. Because the decoded cache also
   holds an `Arc` at these sites, the strong count is 2 and `unwrap_or_clone` always clones. Item
   127 eliminates that clone by removing the cache entry first.

`Arc::unwrap_or_clone` is stable since Rust 1.76.

### Implementation Approach

**`src/storage/pager.rs` — decoded cache and `get_and_decode_node`:**

```rust
// Field:
decoded: RefCell<HashMap<u32, Arc<NodePage>>>,

// get_and_decode_node — cache hit (was page.clone()):
if let Some(page) = self.decoded.borrow().get(&page_no) {
    return page.clone();  // Arc::clone() — O(1)
}

// get_and_decode_node — cache miss (was node.clone() into cache):
let node = Arc::new(self.get_and_decode::<NodePage, _>(page_no));
// fire typed probe on &node ...
self.decoded.borrow_mut().insert(page_no, node.clone());
node
```

**`src/storage/pager.rs` — `write_node_page` (was `&NodePage`, cloning into cache):**

```rust
pub fn write_node_page(&mut self, idx: u32, page: Arc<NodePage>) -> Result<(), EncodingError> {
    let result = self.encode_and_set(idx, &*page);  // serialize from &NodePage
    if result.is_ok() {
        self.decoded.borrow_mut().insert(idx, page);  // store Arc directly — no clone
    }
    result
}
```

Note: `encode_and_set` takes `v: P` where `P: Serialize`. Passing `&*page` gives `&NodePage`
which implements `Serialize`.

**`src/storage/btree.rs` — callers of `write_node_page`:**

Most call sites pass a locally-owned `NodePage` that isn't needed after the write. Change to
`Arc::new(owned_page)` (moves the local into the Arc — no clone):

```rust
// Before:
self.pager.write_node_page(overfull_idx, &left_half)?;

// After:
self.pager.write_node_page(overfull_idx, Arc::new(left_half))?;
```

**`update_page` — split fallback requires the Arc to survive the write attempt:**

`update_page` passes `modified_page` by value to `write_node_page` and, on `NotEnoughSpaceInPage`,
to `split_page`. With `Arc<NodePage>`, the write attempt must not consume the sole Arc — otherwise
there's nothing to pass to `split_page`. The fix is to clone the Arc for the write attempt and keep
the original for the split fallback:

```rust
fn update_page(&mut self, modified_page: Arc<NodePage>, stack: Vec<u32>) {
    let modified_page_idx = *stack.last().unwrap();
    // Clone the Arc for the write; keep `modified_page` for the split fallback.
    let result = self.pager.write_node_page(modified_page_idx, modified_page.clone());
    match result {
        Ok(_) => { /* fire probe */ }
        Err(EncodingError::NotEnoughSpaceInPage) => {
            // Arc::try_unwrap succeeds here: write_node_page stored the clone in the
            // decoded cache, making the refcount 2; but we just replaced the cache entry
            // with the clone — wait, the clone is now in cache and we hold the original.
            // unwrap_or_clone to get an owned NodePage for split_page.
            self.split_page(Arc::unwrap_or_clone(modified_page), stack);
        }
        Err(EncodingError::SerializationError(e)) => panic!("{}", e),
    }
}
```

Note: `write_node_page` stores its argument (the cloned Arc) in the decoded cache. The `modified_page`
Arc held by `update_page` is a separate reference (refcount ≥ 2). `Arc::unwrap_or_clone` on it will
clone. Item 127's `mutate_node` closure API avoids this by never putting the overfull page into the
decoded cache before splitting — the write failure returns the page to the caller immediately.

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

No change needed. `Arc<NodePage>` auto-derefs to `NodePage`; `search()` takes `&self`.

**`src/storage/btree.rs` — insert loop (`btree.rs:191`):**

Interior pages visited during GoDown are read-only; the `Arc` is dropped at the end of each
iteration — no clone paid. Only the leaf page is mutated:

```rust
let top_page = self.pager.get_and_decode_node(top_page_idx);
match top_page.search(key) {  // via Deref — no clone
    Found(i) => {
        let mut owned = Arc::unwrap_or_clone(top_page);  // clones — item 127 eliminates this
        owned.set_item_at_index(i, cell);
        self.update_page(owned, stack);
    }
    NotPresent(i) => {
        let mut owned = Arc::unwrap_or_clone(top_page);  // ditto
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
let mut parent_interior = Arc::unwrap_or_clone(parent_page).interior().unwrap();
```

**`src/storage/cell_reader.rs`** — drop the `Box<NodePage>` wrapper; use `Arc<NodePage>` directly:

```rust
// Before:
let node: Box<NodePage> = Box::new(self.pager.get_and_decode_node(continuation));

// After:
let node: Arc<NodePage> = self.pager.get_and_decode_node(continuation);
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

#### Step 125.1 — Arc decoded cache, updated API, all call sites

1. Change `Pager::decoded` field type.
2. Update `get_and_decode_node` to return `Arc<NodePage>`.
3. Update `write_node_page` to take `Arc<NodePage>`, store Arc directly.
4. Update all call sites across `btree.rs`, `cell_reader.rs`, `btree_verify.rs`, `btree_graph.rs`.
5. `cargo fmt && cargo build && cargo test`.

**Commit:** `storage: cache Arc<NodePage> in decoded cache; write_node_page takes Arc`

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

After item 125, mutation paths in `btree.rs` call `Arc::unwrap_or_clone()` before modifying a
page. Because the decoded cache still holds a reference, the strong count is always 2 and
`unwrap_or_clone` always clones. This item eliminates that clone and, as described in **Invariant
2**, enforces the take→mutate→write pairing at the API level via a closure:

```rust
pub fn mutate_node<R>(
    &mut self,
    page_no: u32,
    f: impl FnOnce(&mut NodePage) -> R,
) -> (R, Result<(), EncodingError>)
```

The closure receives `&mut NodePage`. After the closure returns, `mutate_node` wraps the page in
`Arc::new` (no clone — moves ownership) and writes it back via `write_node_page`. The write-back
is mandatory and compile-time impossible to omit.

A private `take_decoded_node` is still added internally to `Pager` for use by `mutate_node`; it is
not exposed publicly.

### Background

The complete zero-copy mutation flow after items 125+127:

```
mutate_node(idx, |p| { ... })
  ├─ take_decoded_node(idx)    → Arc (refcount = 1, removed from cache)
  ├─ Arc::try_unwrap(arc)      → NodePage (zero copy — sole owner)
  ├─ f(&mut page)              → closure mutates page
  └─ write_node_page(Arc::new(page))  → moves page into Arc (no clone)
                                       → serialise + store Arc in cache
```

vs. post-125, pre-127:

```
get_and_decode_node(idx)    → Arc (refcount = 2)
Arc::unwrap_or_clone(arc)   → NodePage (always clones — refcount was 2)
mutate page inline
write_node_page(Arc::new(page))
```

### Implementation Approach

**`src/storage/pager.rs`:**

```rust
/// Internal: remove decoded cache entry so the caller gets sole Arc ownership.
fn take_decoded_node(&self, page_no: u32) -> Option<Arc<NodePage>> {
    self.decoded.borrow_mut().remove(&page_no)
}

/// Fetch page `page_no`, apply `f` to a mutable reference, then write the result back.
/// Returns `(closure_result, write_result)`.
/// `write_result` is `Err(NotEnoughSpaceInPage)` when the page is overfull after mutation;
/// the caller is responsible for splitting in that case.
pub fn mutate_node<R>(
    &mut self,
    page_no: u32,
    f: impl FnOnce(&mut NodePage) -> R,
) -> (R, Result<(), EncodingError>) {
    let arc = self.take_decoded_node(page_no)
        .unwrap_or_else(|| self.get_decoded_arc(page_no));  // private: returns Arc directly
    // sole owner after take — try_unwrap always succeeds
    let mut page = Arc::try_unwrap(arc).unwrap_or_else(|a| (*a).clone());
    let r = f(&mut page);
    let result = self.write_node_page(page_no, Arc::new(page));
    (r, result)
}
```

**`src/storage/btree.rs` — insert loop:**

```rust
// Read via Ref (zero-copy, enforced by RefCell):
let search_result = self.pager.get_decoded_ref(top_page_idx).search(key);
// Ref dropped here — borrow released
match search_result {
    Found(i) => {
        let (_, result) = self.pager.mutate_node(top_page_idx, |p| {
            p.set_item_at_index(i, cell);
        });
        self.handle_write_result(result, top_page_idx, stack);
    }
    NotPresent(i) => {
        let (_, result) = self.pager.mutate_node(top_page_idx, |p| {
            p.insert_item_at_index(i, cell);
        });
        self.handle_write_result(result, top_page_idx, stack);
    }
    GoDown(_, child) => { stack.push(child); }
}
```

Where `handle_write_result` reads the (now-written) page back from the decoded cache to pass to
`split_page` on overflow — or no-ops on success.

**Split path and delete path** use `mutate_node` in the same pattern: pass a closure that calls
`interior().unwrap()` + `insert_child_page`, or `remove_cell`, etc.

### Key Files

- `src/storage/pager.rs` — add private `take_decoded_node`, public `mutate_node`
- `src/storage/btree.rs` — replace `Arc::unwrap_or_clone` + inline mutation with `mutate_node` calls at insert, split, and delete sites

### Tests

All existing `cargo test` tests must pass.

### Implementation Steps (1 commit)

#### Step 127.1 — `mutate_node` closure API + zero-copy mutation sites

1. Add private `Pager::take_decoded_node()` and public `Pager::mutate_node()`.
2. Replace inline `Arc::unwrap_or_clone` + mutation in the insert loop with `mutate_node`.
3. Replace split-path and delete-path mutation sites with `mutate_node`.
4. `cargo fmt && cargo build && cargo test`.

**Commit:** `storage: zero-copy page mutation via Pager::mutate_node closure API`

---

## Verification

- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` — zero warnings
- [ ] `cargo test` — all tests pass after each item independently
- [ ] `perf stat -e instructions cargo test test_sql_` — instruction count lower after item 125
  vs baseline (scan-heavy tests benefit most from eliminated NodePage clones)
- [ ] Sakila data load instruction count lower after item 127 vs after item 125 (INSERT path)
- [ ] Each commit is independently buildable and testable

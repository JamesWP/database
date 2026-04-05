# Phase BA — Eliminate Page Clones

Remove unnecessary `NodePage` and raw `Page` copies from the hot paths in the pager and B-tree.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 125 | 3 | `Arc<NodePage>` in decoded cache; `get_and_decode_node` returns `Arc<NodePage>`; `write_node_page` takes `Arc<NodePage>` | — |
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

## Stubs

None.

---

## 125. `Arc<NodePage>` in decoded cache; updated `write_node_page` (Track 3)

### What Changes

- `Pager::decoded` changes from `RefCell<HashMap<u32, NodePage>>` to
  `RefCell<HashMap<u32, Arc<NodePage>>>`.
- `get_and_decode_node` returns `Arc<NodePage>`.
  - Cache hit: `Arc::clone()` — refcount bump, no data copied. **Was: full NodePage clone.**
  - Cache miss: `node` moves into `Arc::new(node)` — no copy. `Arc::clone()` stored in cache;
    `Arc` returned. **Was: `node.clone()` into cache, then `node` returned (one clone).**
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

Every call site currently passes `&owned_page`. Change to `Arc::new(owned_page)` (moves the local
into the Arc — no clone):

```rust
// Before:
self.pager.write_node_page(overfull_idx, &left_half)?;

// After:
self.pager.write_node_page(overfull_idx, Arc::new(left_half))?;
```

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

## 127. Zero-copy mutation: `Pager::take_decoded_node` (Track 3)

### What Changes

After item 125, mutation paths in `btree.rs` call `Arc::unwrap_or_clone()` before modifying a
page. Because the decoded cache still holds a reference at that point, the strong count is always 2
and `unwrap_or_clone` always clones. This item eliminates that clone by introducing
`Pager::take_decoded_node(page_no)`, which removes the cache entry and hands back the sole
`Arc<NodePage>` — the caller's subsequent `Arc::try_unwrap()` succeeds without copying.

After mutation the caller passes `Arc::new(owned_page)` to `write_node_page` (which re-inserts
into the decoded cache without a clone, per item 125).

### Background

The flow after items 125+127 at a mutation site:

```
take_decoded_node(idx)      → Arc (refcount = 1, removed from cache)
Arc::try_unwrap(arc)        → NodePage (zero copy — sole owner)
mutate page
Arc::new(page)              → Arc (moves owned page into Arc — zero copy)
write_node_page(idx, arc)   → serialise + cache Arc (no NodePage clone)
```

vs. current (post-125, pre-127):

```
get_and_decode_node(idx)    → Arc (refcount = 2)
Arc::unwrap_or_clone(arc)   → NodePage (clone — refcount was 2)
mutate page
Arc::new(page)              → Arc
write_node_page(idx, arc)   → serialise + cache Arc
```

### Implementation Approach

**`src/storage/pager.rs` — add `take_decoded_node`:**

```rust
/// Remove the decoded cache entry for `page_no` and return the Arc if present.
/// Use before mutating a page so `Arc::try_unwrap()` succeeds without cloning.
/// The caller must re-insert via `write_node_page` after mutation.
pub fn take_decoded_node(&self, page_no: u32) -> Option<Arc<NodePage>> {
    self.decoded.borrow_mut().remove(&page_no)
}
```

**`src/storage/btree.rs` — factor into a helper on `Cursor`:**

```rust
fn take_page_for_mutation(&mut self, page_idx: u32) -> NodePage {
    let arc = self.pager.take_decoded_node(page_idx)
        .unwrap_or_else(|| self.pager.get_and_decode_node(page_idx));
    Arc::try_unwrap(arc).unwrap_or_else(|a| (*a).clone())
}
```

Apply at:
- **Insert loop** (leaf mutation — `Found` and `NotPresent` branches)
- **Split path** — parent page (`parent_page.interior().unwrap()`)
- **Split root** — left-half page before `write_node_page`
- **Delete path** — leaf page before `remove_cell()`

The insert loop also needs `take_decoded_node` for the leaf search before mutation so the
`search()` result is obtained without keeping an extra Arc reference:

```rust
// Read search result with a short-lived Arc (dropped immediately after search):
let search_result = self.pager.get_and_decode_node(top_page_idx).search(key);
match search_result {
    Found(i) => {
        let mut owned = self.take_page_for_mutation(top_page_idx);
        owned.set_item_at_index(i, cell);
        self.update_page(Arc::new(owned), stack);
    }
    NotPresent(i) => {
        let mut owned = self.take_page_for_mutation(top_page_idx);
        owned.insert_item_at_index(i, cell);
        self.update_page(Arc::new(owned), stack);
    }
    GoDown(_, child) => { stack.push(child); }
}
```

Note: `update_page` signature will need to change from `(NodePage, Vec<u32>)` to
`(Arc<NodePage>, Vec<u32>)` to match `write_node_page`'s new signature, or it wraps internally.
Check existing signature and update accordingly.

### Key Files

- `src/storage/pager.rs` — add `take_decoded_node()`
- `src/storage/btree.rs` — add `take_page_for_mutation()` helper; refactor insert, split, delete sites

### Tests

All existing `cargo test` tests must pass.

### Implementation Steps (1 commit)

#### Step 127.1 — `take_decoded_node` + zero-copy mutation sites

1. Add `Pager::take_decoded_node()`.
2. Add `Cursor::take_page_for_mutation()` helper.
3. Update the insert loop, split path, and delete path to use the helper.
4. Update `update_page` if its signature needs adjusting.
5. `cargo fmt && cargo build && cargo test`.

**Commit:** `storage: zero-copy page mutation via take_decoded_node`

---

## Verification

- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` — zero warnings
- [ ] `cargo test` — all tests pass after each item independently
- [ ] `perf stat -e instructions cargo test test_sql_` — instruction count lower after item 125
  vs baseline (scan-heavy tests benefit most from eliminated NodePage clones)
- [ ] Sakila data load instruction count lower after item 127 vs after item 125 (INSERT path)
- [ ] Each commit is independently buildable and testable

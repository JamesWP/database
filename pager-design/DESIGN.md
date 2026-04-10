# Storage Layer Redesign — Requirements and Principles

This document captures the requirements, design decisions, and principles developed
during the brainstorm that produced the prototype in `src/main.rs`. It is intended
as a reference for the implementation phase.

---

## Problems with the current interface

The existing `Pager` has several issues that motivated this redesign:

- **Wrong abstraction level.** `get_and_decode_node`, `write_node_page`, and
  `encode_and_set` make the pager aware of `NodePage` and CBOR. That is a
  B-tree concern, not a page-I/O concern.
- **Raw `u32` page IDs.** No type safety. Nothing stops passing an arbitrary integer.
- **`set_file_size_pages` is public.** Internal detail; callers should not resize
  the file directly.
- **`get_zero_page` returns an internal struct.** `ZeroPage` is pager metadata;
  leaking it forces callers to understand free-list internals.
- **Inconsistent error handling.** Some methods `unwrap()`, some return `Result`.
  The public interface should never panic on recoverable errors.
- **Unnecessary generics.** `PageNo: Borrow<u32>` on `get`/`set` adds noise with
  no practical benefit.
- **Two caches that can diverge.** The raw page cache and the decoded `NodePage`
  cache are updated by different code paths, making it possible to have stale
  decoded entries.
- **Forced clones.** `get_and_decode_node` returns an owned `NodePage` on every
  call, whether or not mutation follows. The write path clones again when inserting
  into the decoded cache.

---

## Layer responsibilities

```
BTree  ←→  NodePageStore  ←→  Pager  ←→  disk
```

| Layer            | Owns                                         | Does not know about         |
|------------------|----------------------------------------------|-----------------------------|
| `Pager`          | File I/O, free list, format version, ZeroPage | `NodePage`, CBOR             |
| `NodePageStore`  | CBOR encode/decode, decoded node cache        | B-tree invariants, cursors   |
| `BTree` / cursor | Tree structure, splits, rowid cache           | Free list, raw page bytes    |

**`Pager` is unexported.** All access goes through `NodePageStore`. BTree never
holds a reference to the pager.

---

## Requirements

### Pager (raw I/O layer)

**Must have:**
1. Allocate a fresh page — return a typed ID
2. Free a page by ID
3. Read a page's raw bytes by ID
4. Write a page's raw bytes by ID
5. Flush to disk (explicit durability point)
6. Format version validation on open
7. `page_count()` for sizing and bootstrap detection

**Nice to have:**
- Future: WAL / transactional write support

### NodePageStore (decode cache layer)

**Must have:**
1. Read a node — cache hit avoids CBOR decode
2. Write a node — updates cache and writes to disk atomically; **no clone at the call site**
3. In-place mutation path — obtain an owned node, mutate it, write it back without any `NodePage` clone
4. Allocate a fresh page (delegated to pager)
5. Free a page — evicts from cache before returning to pager
6. Flush
7. Format version validation (delegated to pager)
8. Cache coherence — a page written through any path must be visible on the next read

**Nice to have:**
- Bounded cache with LRU eviction
- Hit/miss counters
- Explicit `clear_cache()` for release after large batch operations

### Clone elimination (primary constraint)

The write path must not clone `NodePage`. Specifically:

- Callers who already own a `NodePage` (e.g. freshly split halves) must be able to
  pass it to `write` by move, with no internal clone when inserting into the cache.
- Callers who need to read-then-mutate a node must be able to do so without a clone.
- On `PageFull`, ownership of the node must be returned to the caller so it can
  split without a re-fetch.

---

## API design

### `PageId`

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId(u32);
```

A newtype over `u32`. Prevents accidental integer arithmetic on page numbers and
makes signatures self-documenting.

---

### `Error`

```rust
pub enum Error {
    Io(std::io::Error),
    PageFull(NodePage),   // returns ownership on overflow
    Decode(String),
    FormatError(String),
}
```

`PageFull` carries the node back to the caller. This is what makes the write path
clone-free: the caller destructures the node out of the error and splits it without
any re-fetch.

---

### `NodePageStore`

```rust
pub struct NodePageStore { /* pager + cache, both private */ }

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

#### `read` — borrow into cache

Takes `&mut self` so cache population on a miss does not require `RefCell`. Returns
`&NodePage` tied to the store's lifetime. Implemented as two separate calls so the
borrow checker can see the `&mut` borrow ends before the return reference begins:

```rust
pub fn read(&mut self, id: PageId) -> Result<&NodePage, Error> {
    self.ensure_cached(id)?;          // &mut borrow ends here
    Ok(self.cache.get(&id).unwrap())  // new shared borrow
}
```

Use `read` for traversal (interior nodes while searching). The borrow is scoped to
extract a `SearchResult` (which contains only `Copy` values like `PageId`), then
dropped before any mutation.

#### `take` — owned access for mutation

Removes the cache entry and returns the node owned. No borrow is held afterward,
so `allocate` and `write` can be called freely. The caller mutates the node and
passes it back to `write`.

```rust
let mut node = store.take(id)?;
node.interior_mut()?.insert_child_page(key, right_id);
store.write(id, node)?;
```

On a cache miss, `take` loads from disk without inserting into the cache. This is
safe because the caller will re-insert via `write`.

#### `write` — consumes, no clone

Takes ownership. On success, moves the node into the cache and writes bytes to the
pager — one path, no internal clone.

On `PageFull`, returns `Err(Error::PageFull(node))`. The caller pattern:

```rust
match store.write(id, node) {
    Ok(()) => Ok(()),
    Err(Error::PageFull(node)) => split_and_retry(store, node, stack),
    Err(e) => Err(e),
}
```

---

## Principles

### Allocate before borrowing

`allocate` takes `&mut self`. Calling it while holding a `&NodePage` from `read`
would conflict. The discipline is: **all `allocate` calls for an operation must
happen before any `read` or `take` calls for that operation.**

This is enforced by the borrow checker — the code simply will not compile otherwise.

### The root split reordering

The current implementation writes `left_half` to the root page, then immediately
re-reads the root page to copy it to a new `left_id`. This is a write followed by
an immediate re-read of the same data — the node is already in hand.

The fix is to reorder: allocate `left_id` and `right_id` before any writes, then
write each half directly to its final destination, then overwrite the root with the
new interior node.

```
Before:  write(root, left_half)  →  read(root)  →  write(left_id, left_half)
After:   write(left_id, left_half)  →  write(right_id, right_half)  →  write(root, interior)
```

One fewer write, one fewer cache round-trip, no clone.

### Single write path

There is no raw-byte escape hatch for node pages in the public API. `write` is the
only path from `NodePage` to disk. This is what makes cache coherence trivially
provable: anything in the cache was placed there by `write`, and `write` always
writes to disk atomically with the cache update.

### Cache invalidation on free

`free` removes the cache entry before returning the page to the pager's free list.
This ensures that if the page is reallocated and written with a different type of
content, the next `read` is a clean miss and decodes from the new bytes.

### `read` vs `take` discipline

| Situation | Use |
|---|---|
| Read-only traversal (following child pointers) | `read` — borrow, no clone |
| Read then mutate the same page | `take` — owned, mutate, `write` back |
| Write a freshly constructed page | `write` directly — no read needed |
| Need to hold node reference across `allocate` | Not possible with `read`; use `take` |

### Schema cache ownership

`BTree` owns a decoded schema cache — a `HashMap<String, TableSchema>` (or
similar) that maps table and index names to their parsed definitions. This
avoids re-parsing catalog cell bytes on every query plan.

`NodePageStore` provides coherent page-level caching for all pages, including
catalog pages. The catalog is a B-tree rooted at a well-known page and its
`NodePage`s are cached identically to user data pages — there is no separate
raw-byte cache for catalog pages.

The schema cache sits one level above `NodePageStore`, owned by `BTree`. It is
invalidated **explicitly at DDL call sites** (CREATE TABLE, DROP TABLE, CREATE
INDEX, DROP INDEX). These are the only points at which catalog pages are
written, and `BTree` controls all of them, so the invalidation point is always
reachable and always known.

This is a convention enforced by code structure, not the type system. The
current implementation diverges from this — the existing pager has a separate
decoded `NodePage` cache inside `Pager` alongside the raw page cache, and the
schema cache is scattered. The new design collapses this to:

```
BTree         — owns schema cache (TableSchema, IndexSchema, …)
NodePageStore — owns the single NodePage cache (all pages, including catalog)
Pager         — raw I/O only, no caching
```

A write to any catalog page goes through `NodePageStore::write`, which keeps
the `NodePage` cache coherent. The `BTree` DDL path then explicitly invalidates
the schema cache. No other code path writes catalog pages.

### Where clones remain unavoidable

One case: saving a cursor key before an insert for `RequiresSeek` repositioning.
This is a `Vec<u8>` clone of the key bytes, not a `NodePage` clone, and it must
outlive the page borrow. This is acceptable.

Interior node traversal during search uses `read` and never clones. The full insert
path through a tree of depth N clones exactly one `Vec<u8>` regardless of depth.

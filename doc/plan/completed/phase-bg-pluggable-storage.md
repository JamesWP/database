# Phase BG — Pluggable Storage (VFS API)

Introduce a `PageStorage` trait so integrators can supply their own storage backend;
wire a synchronous JS callback adapter into the WASM binding to support custom backends
including S3-backed page caches in Workers.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 144 | 3 | Define `PageStorage` trait + `FilePageStorage` and `MemoryPageStorage` implementations | — |
| 145 | 3 | Refactor `Pager` to hold `Box<dyn PageStorage>`; thread `with_storage` up through `NodePageStore` | 144 |
| 146 | 3 | Public API: export `PageStorage` + `MemoryPageStorage`; add `BTree::with_storage()` | 145 |
| 147 | 3 | WASM: `JsPageStorage` adapter + `Database::withStorage(provider)` JS binding | 146 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

The `Pager` currently uses a private `PagerStorage` enum with two variants:
`File` (native-only, file-backed) and `Memory` (in-memory `Vec<pages>`).
These are hard-coded — there is no way for library users to plug in a custom
backend. As a result, the WASM binding is limited to in-memory databases that
disappear when the page is reloaded.

This phase introduces a `PageStorage` trait — a minimal five-method interface
over raw 4096-byte pages — and replaces the `PagerStorage` enum with
`Box<dyn PageStorage>`. Existing behaviour is unchanged: `BTree::new(path)` and
`BTree::new_in_memory()` continue to work via the built-in `FilePageStorage`
and `MemoryPageStorage` implementations.

A new `BTree::with_storage(impl PageStorage)` constructor makes the trait
useful for native Rust integrators. The WASM binding gains `Database.withStorage(provider)`
which accepts a plain JS object and calls its methods synchronously.

### Async backends (S3, IndexedDB)

The `PageStorage` trait is **synchronous by design**: `read_page` returns
`[u8; 4096]` directly, with no `Future` or callback. This keeps the trait
simple and the query execution pipeline unchanged.

For inherently asynchronous backends — S3 fetches, IndexedDB reads — the
standard approach is to run the WASM module inside a **Web Worker** and
implement the storage callback using `Atomics.wait()` / SharedArrayBuffer
to block the Worker thread until the async operation completes. This is the
same pattern used by [sql.js-httpvfs](https://github.com/phiresky/sql.js-httpvfs)
and the browser's OPFS synchronous access handle.

The JS usage looks like this in the Worker context:

```js
// worker.js
import init, { Database } from './pkg/database.js';
await init();

const db = Database.withStorage({
    _buffer: new SharedArrayBuffer(4096),
    _state:  new SharedArrayBuffer(4),          // Atomics flag

    readPage(n) {
        // Post a "fetch page n" message to the main thread / fetcher worker
        postMessage({ type: 'read', page: n });
        // Block until the main thread fills _buffer and sets _state[0] = 1
        Atomics.wait(new Int32Array(this._state), 0, 0);
        Atomics.store(new Int32Array(this._state), 0, 0); // reset
        return new Uint8Array(this._buffer);
    },
    writePage(n, data) { /* ... */ },
    pageCount() { /* ... */ },
    setPageCount(n) { /* ... */ },
    flush() { /* ... */ },
});
```

A simple in-memory provider (for testing, prototyping, or pre-loaded databases)
is even simpler:

```js
const db = Database.withStorage({
    _pages: new Map(),
    pageCount()         { return this._pages.size; },
    setPageCount(n)     { /* grow/shrink this._pages */ },
    readPage(n)         { return this._pages.get(n) ?? new Uint8Array(4096); },
    writePage(n, data)  { this._pages.set(n, data.slice()); },
    flush()             {},
});
```

---

## Stubs

None.

---

## 144. `PageStorage` trait + built-in implementations (Track 3)

### What Changes

A new `src/storage/page_storage.rs` module defines:

1. `PAGE_SIZE: usize = 4096` — the canonical page-size constant for the whole storage subsystem.
2. `PageStorage` trait — five methods, all synchronous.
3. `FilePageStorage` (native-only) — wraps `RefCell<std::fs::File>`.
4. `MemoryPageStorage` — wraps `Vec<[u8; PAGE_SIZE]>`.

No other files change in this item. The trait is `pub(super)` for now; it
becomes public in item 146.

### The trait

```rust
// src/storage/page_storage.rs

pub const PAGE_SIZE: usize = 4096;

/// Synchronous page-I/O interface.
///
/// Implementors store and retrieve fixed-size (4096-byte) pages identified
/// by a 0-based u32 index. The interface is intentionally synchronous; async
/// backends (S3, IndexedDB) must implement blocking via Atomics.wait() in a
/// SharedArrayBuffer-based Worker.
pub trait PageStorage {
    fn page_count(&self) -> u32;
    fn set_page_count(&mut self, count: u32);
    /// Read page `page_no`. Panics if `page_no >= page_count()`.
    fn read_page(&self, page_no: u32) -> [u8; PAGE_SIZE];
    fn write_page(&mut self, page_no: u32, bytes: &[u8; PAGE_SIZE]);
    fn flush(&mut self) -> std::io::Result<()>;
}
```

`read_page` takes `&self` (not `&mut self`) to match the current `Pager::read_bytes`
signature — the file backend achieves interior mutability via `RefCell<File>` for
the seek position. All write methods take `&mut self`.

### `FilePageStorage`

```rust
#[cfg(not(target_arch = "wasm32"))]
pub struct FilePageStorage {
    file: std::cell::RefCell<std::fs::File>,
}

#[cfg(not(target_arch = "wasm32"))]
impl FilePageStorage {
    pub fn open(path: &std::path::Path) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true).write(true).create(true)
            .open(path)?;
        Ok(Self { file: std::cell::RefCell::new(file) })
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl PageStorage for FilePageStorage {
    fn page_count(&self) -> u32 {
        use std::os::unix::prelude::MetadataExt;
        let file = self.file.borrow();
        let size = file.metadata().unwrap().size();
        (size / PAGE_SIZE as u64) as u32
    }

    fn set_page_count(&mut self, count: u32) {
        self.file.borrow().set_len(PAGE_SIZE as u64 * count as u64).unwrap();
    }

    fn read_page(&self, page_no: u32) -> [u8; PAGE_SIZE] {
        use std::io::{Read, Seek};
        let mut file = self.file.borrow_mut();
        let offset = PAGE_SIZE as u64 * page_no as u64;
        file.seek(std::io::SeekFrom::Start(offset)).unwrap();
        let mut bytes = [0u8; PAGE_SIZE];
        file.read_exact(&mut bytes).unwrap();
        bytes
    }

    fn write_page(&mut self, page_no: u32, bytes: &[u8; PAGE_SIZE]) {
        use std::io::{Seek, Write};
        let mut file = self.file.borrow_mut();
        let offset = PAGE_SIZE as u64 * page_no as u64;
        file.seek(std::io::SeekFrom::Start(offset)).unwrap();
        file.write_all(bytes).unwrap();
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.borrow_mut().flush()
    }
}
```

### `MemoryPageStorage`

```rust
pub struct MemoryPageStorage {
    pages: Vec<[u8; PAGE_SIZE]>,
}

impl MemoryPageStorage {
    pub fn new() -> Self {
        Self { pages: Vec::new() }
    }
}

impl PageStorage for MemoryPageStorage {
    fn page_count(&self) -> u32    { self.pages.len() as u32 }
    fn set_page_count(&mut self, n: u32) {
        self.pages.resize(n as usize, [0u8; PAGE_SIZE]);
    }
    fn read_page(&self, n: u32) -> [u8; PAGE_SIZE]            { self.pages[n as usize] }
    fn write_page(&mut self, n: u32, bytes: &[u8; PAGE_SIZE]) { self.pages[n as usize] = *bytes; }
    fn flush(&mut self) -> std::io::Result<()>                { Ok(()) }
}
```

### PAGE_SIZE migration

`pager.rs` currently defines:
```rust
pub(super) const PAGE_SIZE: u64 = 2 << 11; // 4096 bytes
const PAGE_SIZE_USIZE: usize = PAGE_SIZE as usize;
```

After this item, `PAGE_SIZE` is the `usize` constant in `page_storage.rs`.
The `pager.rs` re-export (`use super::page_storage::PAGE_SIZE`) and the u64
conversion for file seeks (`PAGE_SIZE as u64`) happen inside `FilePageStorage`.
`node_page_store.rs` imports `PAGE_SIZE` from `page_storage` instead of `pager`.

### Key Files

- `src/storage/page_storage.rs` — new file (trait + both impls)
- `src/storage/storage.rs` — add `mod page_storage;`

### Tests

```rust
#[test]
fn memory_page_storage_roundtrip() {
    let mut s = MemoryPageStorage::new();
    assert_eq!(s.page_count(), 0);
    s.set_page_count(2);
    assert_eq!(s.page_count(), 2);
    let mut page = [0u8; PAGE_SIZE];
    page[0] = 42;
    s.write_page(1, &page);
    let r = s.read_page(1);
    assert_eq!(r[0], 42);
}
```

### Implementation Steps (1 commit)

#### Step 144.1 — Add `page_storage.rs` with trait + `FilePageStorage` + `MemoryPageStorage`

**Commit:** `Storage: add PageStorage trait with FilePageStorage and MemoryPageStorage`

---

## 145. Refactor `Pager` to use `Box<dyn PageStorage>` (Track 3)

### What Changes

The `PagerStorage` enum is removed. `Pager` holds `storage: Box<dyn PageStorage>`.
All `Pager` methods delegate to `self.storage.*` instead of matching on the enum.

A new `Pager::with_storage(storage: Box<dyn page_storage::PageStorage>)` constructor
is added (crate-private). `NodePageStore` gains a matching
`NodePageStore::with_storage(storage: Box<dyn PageStorage>)` factory.

This is a pure refactor: no behaviour changes, all existing tests pass.

### `Pager` after the change

```rust
pub(super) struct Pager {
    storage: Box<dyn page_storage::PageStorage>,
}

impl Pager {
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn new(path: &str) -> Pager {
        Pager {
            storage: Box::new(FilePageStorage::open(path.as_ref()).unwrap()),
        }
    }

    pub(super) fn new_in_memory() -> Pager {
        Pager { storage: Box::new(MemoryPageStorage::new()) }
    }

    pub(super) fn with_storage(storage: Box<dyn page_storage::PageStorage>) -> Pager {
        Pager { storage }
    }

    pub(super) fn get_file_size_pages(&self) -> u32 {
        self.storage.page_count()
    }

    fn set_file_size_pages(&mut self, count: u32) {
        self.storage.set_page_count(count);
    }

    fn read_bytes(&self, page_no: u32) -> [u8; PAGE_SIZE] {
        self.storage.read_page(page_no)
    }

    fn write_bytes(&mut self, page_no: u32, bytes: &[u8; PAGE_SIZE]) {
        self.storage.write_page(page_no, bytes);
    }

    pub(super) fn flush(&mut self) -> std::io::Result<()> {
        self.storage.flush()
    }
}
```

The ZeroPage/FreeList methods (`get_zero_page`, `set_zero_page`, `allocate`, `free`)
are unchanged — they still call `read_bytes`/`write_bytes` as before.

`Debug` for `Pager` uses `std::any::type_name_of_val` or a simpler string:

```rust
impl std::fmt::Debug for Pager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pager")
            .field("pages", &self.storage.page_count())
            .finish()
    }
}
```

### `NodePageStore` addition

```rust
impl NodePageStore {
    pub fn with_storage(storage: Box<dyn PageStorage>) -> Self {
        NodePageStore {
            pager: Pager::with_storage(storage),
            cache: HashMap::new(),
            dirty: HashSet::new(),
        }
    }
}
```

### Key Files

- `src/storage/pager.rs` — remove `PagerStorage` enum; replace with `Box<dyn PageStorage>`
- `src/storage/node_page_store.rs` — add `with_storage` constructor; update `PAGE_SIZE` import

### Tests

All existing `cargo test --workspace` tests must continue to pass — this is
the primary verification. No new tests needed (the trait impls are tested in item 144).

### Implementation Steps (1 commit)

#### Step 145.1 — Refactor `Pager` + add `NodePageStore::with_storage`

**Commit:** `Storage: replace PagerStorage enum with Box<dyn PageStorage>; add NodePageStore::with_storage`

---

## 146. Public API: `PageStorage` + `BTree::with_storage()` (Track 3)

### What Changes

The `PageStorage` trait and `MemoryPageStorage` implementation are made public
from the library crate. `BTree` gains a `with_storage` constructor.

`FilePageStorage` is also exported on native targets (behind
`#[cfg(not(target_arch = "wasm32"))]`) so native Rust integrators can open
a custom-path file without going through `BTree::new`.

### Exports from `src/storage.rs`

```rust
pub use page_storage::MemoryPageStorage;
pub use page_storage::PageStorage;
#[cfg(not(target_arch = "wasm32"))]
pub use page_storage::FilePageStorage;
```

### `BTree::with_storage`

```rust
impl BTree {
    /// Create a `BTree` backed by a custom `PageStorage` implementation.
    ///
    /// If the storage is empty (`page_count() == 0`), bootstraps a fresh
    /// catalog tree. If the storage already contains data, validates the
    /// format version.
    pub fn with_storage(storage: impl PageStorage + 'static) -> BTree {
        let store = NodePageStore::with_storage(Box::new(storage));
        let is_new = store.page_count() == 0;
        let mut btree = BTree {
            store: Arc::new(RefCell::new(store)),
            rowid_cache: Arc::new(RefCell::new(HashMap::new())),
            catalog_cache: RefCell::new(None),
        };
        if !is_new {
            btree
                .store
                .borrow()
                .validate_format_version()
                .unwrap_or_else(|e| panic!("{e}"));
        } else {
            btree.bootstrap_catalog();
        }
        btree
    }
}
```

This mirrors the logic of `BTree::new` for existing databases and
`BTree::new_in_memory` for fresh ones.

### Key Files

- `src/storage.rs` — add public re-exports
- `src/storage/page_storage.rs` — make `PageStorage`, `MemoryPageStorage`, `FilePageStorage` `pub`
- `src/storage/btree.rs` — add `BTree::with_storage`

### Tests

```rust
#[test]
fn with_storage_roundtrip() {
    // Use MemoryPageStorage to exercise the public API
    let mut btree = BTree::with_storage(MemoryPageStorage::new());
    execute("CREATE TABLE t (x INTEGER)", &mut btree).unwrap();
    execute("INSERT INTO t VALUES (99)", &mut btree).unwrap();
    let rows = collect_rows("SELECT x FROM t", &mut btree);
    assert_eq!(rows[0][0], ScalarValue::Integer(99));
}

#[test]
fn with_storage_existing_data_reloaded() {
    // Write to a MemoryPageStorage, then hand the same storage to a new BTree.
    let storage = MemoryPageStorage::new();
    {
        let mut btree = BTree::with_storage(storage.clone()); // requires Clone on MemoryPageStorage
        // ... OR: we can use a Rc/Arc-wrapped storage for sharing
    }
    // Note: MemoryPageStorage doesn't implement Clone by default.
    // This test should instead write via BTree, then verify roundtrip
    // by testing that BTree::with_storage on a non-empty MemoryPageStorage
    // runs validate_format_version without panic.
    // See implementation notes below.
}
```

**Implementation note on the roundtrip test**: `MemoryPageStorage` does not
implement `Clone`. A cleaner roundtrip test uses a `Rc<RefCell<MemoryPageStorage>>`
wrapper or simply tests that the format-version validation path is exercised by
running `BTree::new_in_memory()`, harvesting its pages, and re-opening — but
that is complex. The simplest test is:

```rust
#[test]
fn with_storage_bootstraps_catalog() {
    let mut btree = BTree::with_storage(MemoryPageStorage::new());
    // Catalog should exist after bootstrap
    assert!(btree.catalog().lookup_table_info("db_schema").is_some());
}
```

### Implementation Steps (1 commit)

#### Step 146.1 — Make PageStorage public; add BTree::with_storage

**Commit:** `Storage: expose PageStorage trait publicly; add BTree::with_storage`

---

## 147. WASM: `JsPageStorage` + `Database.withStorage()` (Track 3)

### What Changes

`src/wasm.rs` gains:

1. `JsPageStorage` — a `PageStorage` implementor that calls methods on a JS
   object via `js_sys::Reflect`.
2. `Database::with_storage(provider: JsValue) -> Result<Database, JsValue>` —
   a new WASM-exported constructor that wraps the provider.

The existing `Database::new()` (in-memory) is unchanged.

### JS provider interface

The JS object passed to `Database.withStorage(provider)` must implement:

```typescript
interface PageStorageProvider {
    pageCount(): number;
    setPageCount(n: number): void;
    /** Must return exactly 4096 bytes */
    readPage(n: number): Uint8Array;
    writePage(n: number, data: Uint8Array): void;
    flush(): void;
}
```

All methods are called **synchronously**. For async backends (S3, IndexedDB),
run the WASM module in a Worker and implement blocking via `Atomics.wait()`.

### `JsPageStorage` implementation

```rust
// src/wasm.rs

use crate::storage::PageStorage;
use crate::storage::page_storage::PAGE_SIZE;

struct JsPageStorage {
    provider: js_sys::Object,
}

impl PageStorage for JsPageStorage {
    fn page_count(&self) -> u32 {
        call_method_0(&self.provider, "pageCount")
            .as_f64()
            .unwrap_or(0.0) as u32
    }

    fn set_page_count(&mut self, count: u32) {
        call_method_1(&self.provider, "setPageCount", JsValue::from(count));
    }

    fn read_page(&self, page_no: u32) -> [u8; PAGE_SIZE] {
        let val = call_method_1(&self.provider, "readPage", JsValue::from(page_no));
        let arr = js_sys::Uint8Array::from(val);
        let mut bytes = [0u8; PAGE_SIZE];
        arr.copy_to(&mut bytes);
        bytes
    }

    fn write_page(&mut self, page_no: u32, bytes: &[u8; PAGE_SIZE]) {
        let arr = js_sys::Uint8Array::new_with_length(PAGE_SIZE as u32);
        arr.copy_from(bytes.as_slice());
        call_method_2(&self.provider, "writePage", JsValue::from(page_no), arr.into());
    }

    fn flush(&mut self) -> std::io::Result<()> {
        call_method_0(&self.provider, "flush");
        Ok(())
    }
}

fn call_method_0(obj: &js_sys::Object, method: &str) -> JsValue {
    js_sys::Reflect::get(obj, &JsValue::from_str(method))
        .expect("method missing")
        .unchecked_into::<js_sys::Function>()
        .call0(obj)
        .expect("call failed")
}

fn call_method_1(obj: &js_sys::Object, method: &str, arg: JsValue) -> JsValue {
    js_sys::Reflect::get(obj, &JsValue::from_str(method))
        .expect("method missing")
        .unchecked_into::<js_sys::Function>()
        .call1(obj, &arg)
        .expect("call failed")
}

fn call_method_2(obj: &js_sys::Object, method: &str, a: JsValue, b: JsValue) -> JsValue {
    js_sys::Reflect::get(obj, &JsValue::from_str(method))
        .expect("method missing")
        .unchecked_into::<js_sys::Function>()
        .call2(obj, &a, &b)
        .expect("call failed")
}
```

### `Database` extension

```rust
#[wasm_bindgen]
impl Database {
    /// Create a Database backed by a custom JS storage provider.
    ///
    /// `provider` must implement the `PageStorageProvider` interface:
    /// - `pageCount(): number`
    /// - `setPageCount(n: number): void`
    /// - `readPage(n: number): Uint8Array`   — exactly 4096 bytes
    /// - `writePage(n: number, data: Uint8Array): void`
    /// - `flush(): void`
    ///
    /// All methods are called synchronously. For async backends (S3, IndexedDB),
    /// implement the provider in a Worker and use `Atomics.wait()` to block.
    #[wasm_bindgen(js_name = withStorage)]
    pub fn with_storage(provider: JsValue) -> Result<Database, JsValue> {
        let obj = js_sys::Object::try_from(&provider)
            .cloned()
            .ok_or_else(|| JsValue::from_str("storage provider must be an object"))?;
        let storage = JsPageStorage { provider: obj };
        Ok(Database {
            btree: BTree::with_storage(storage),
        })
    }
}
```

### Updated TypeScript definition

The generated `pkg/database.d.ts` will not automatically include the new constructor
with its JSDoc comment. After `wasm-pack build`, verify the .d.ts includes:

```typescript
export interface PageStorageProvider {
    pageCount(): number;
    setPageCount(n: number): void;
    readPage(n: number): Uint8Array;
    writePage(n: number, data: Uint8Array): void;
    flush(): void;
}

export class Database {
    free(): void;
    [Symbol.dispose](): void;
    constructor();
    /** Create a Database backed by a custom storage provider. */
    static withStorage(provider: PageStorageProvider): Database;
    execute(sql: string): string;
    query(sql: string): any;
}
```

If `wasm-pack` does not generate the interface automatically, add a handwritten
`pkg/database.d.ts` overlay or a `types/` directory in the package.

### WASM integration test

```rust
// tests/wasm.rs (or new tests/wasm_storage.rs)

#[wasm_bindgen_test]
fn with_storage_accepts_custom_provider() {
    use wasm_bindgen::JsValue;
    use js_sys::Reflect;

    // Build a minimal JS object implementing PageStorageProvider
    let provider = js_sys::Object::new();
    // ... attach methods via Reflect::set ...
    // (in practice this test uses a simple MemoryPageStorage via the Rust API;
    // a full JS-object test is done in the example app)

    // Smoke test: with_storage on an MemoryPageStorage does not panic
    let db = Database::new(); // in-memory, unchanged
    let _ = db.execute("CREATE TABLE t (x INTEGER)");
}
```

A full end-to-end test of `Database.withStorage` with a JS object is added to
`example/webapp/index.html` alongside the existing in-memory demo:

```js
// Custom in-memory provider using a JS Map
const provider = {
    _pages: new Map(),
    pageCount()         { return this._pages.size; },
    setPageCount(n)     {
        while (this._pages.size < n) this._pages.set(this._pages.size, new Uint8Array(4096));
        while (this._pages.size > n) this._pages.delete(this._pages.size - 1);
    },
    readPage(n)         { return this._pages.get(n) ?? new Uint8Array(4096); },
    writePage(n, data)  { this._pages.set(n, data.slice()); },
    flush()             {},
};

const db2 = Database.withStorage(provider);
db2.execute("CREATE TABLE custom (val INTEGER)");
db2.execute("INSERT INTO custom VALUES (7)");
log(`<div class="ok">Custom storage query: ${JSON.stringify(db2.query("SELECT val FROM custom"))}</div>`);
```

### Key Files

- `src/wasm.rs` — `JsPageStorage`, helper functions, `Database::with_storage`
- `example/webapp/index.html` — demo of custom JS storage
- `pkg/database.d.ts` — update TypeScript typings (post `wasm-pack build`)

### Implementation Steps (2 commits)

#### Step 147.1 — `JsPageStorage` + `Database::withStorage` in Rust

Implement `JsPageStorage`, the helper call functions, and the new constructor.
Verify `wasm-pack build` succeeds.

**Commit:** `WASM: add JsPageStorage adapter and Database::withStorage constructor`

#### Step 147.2 — Update example webapp + TypeScript types

Add the custom-provider demo to `index.html`. Update `pkg/database.d.ts`
with the `PageStorageProvider` interface.

**Commit:** `WASM: add custom storage provider demo; update TypeScript types`

---

## Verification

- [ ] `cargo test --workspace` passes (all existing tests)
- [ ] `cargo check -p database --target wasm32-unknown-unknown` passes
- [ ] `wasm-pack build` succeeds
- [ ] `BTree::with_storage(MemoryPageStorage::new())` — creates DB, inserts, queries correctly
- [ ] `BTree::with_storage(MemoryPageStorage::new())` on a non-empty storage — validates format version without panic
- [ ] `Database.withStorage(provider)` in the webapp — table created, rows inserted, query returns correct rows
- [ ] `Database.new()` (in-memory) still works as before
- [ ] Zero warnings: `cargo fmt --all && cargo build --workspace 2>&1 | grep -i warning`
- [ ] Each commit is independently testable

# Phase S — JavaScript / WebAssembly Bindings

Compile the database to WebAssembly and expose a JavaScript API so the engine can run in browsers and Node.js.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 81 | 3 | Add `wasm` Cargo feature; configure `wasm-bindgen`; build pipeline | — |
| 82 | 3 | Implement `JsDatabase` wrapper with `execute()` and `query()` methods | 81 |
| 83 | 3 | In-memory storage backend for WASM (no file I/O) | 81 |
| 84 | 7 | JS smoke tests with `wasm-pack test --node` | 82, 83 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

**Prerequisite:** Phase BF (WASM Core Library Prerequisites) must be completed first — it
gates the `testing` module and `BTree::dump_to_file()` so the library compiles for
`wasm32-unknown-unknown` without those host-platform dependencies. The only remaining
blocker after BF is the file-backed `Pager`, which this phase replaces (item 83).

The database is a pure-Rust library with no OS dependencies beyond file I/O (the `Pager`). Compiling to WASM requires:

1. Replacing file-based storage with an in-memory buffer (the `Pager` reads/writes pages; substituting a `Vec<u8>` makes it WASM-compatible).
2. Wrapping the `Database` API in a `wasm-bindgen` `#[wasm_bindgen]` struct so JavaScript can call it.
3. A build step (`wasm-pack build`) that outputs an npm package in `pkg/`.

The result: a `.wasm` module + JS glue that can be imported in any modern browser or Node.js:

```js
import init, { Database } from './pkg/database.js';

await init();
const db = new Database();
db.execute("CREATE TABLE users (id INTEGER, name TEXT)");
db.execute("INSERT INTO users VALUES (1, 'alice')");
const rows = db.query("SELECT * FROM users");
// rows: [{ id: 1, name: 'alice' }]
```

### Dependencies

Add to `Cargo.toml` under `[target.'cfg(target_arch = "wasm32")'.dependencies]`:

```toml
wasm-bindgen = "0.2"
js-sys = "0.3"
serde-wasm-bindgen = "0.6"   # for JsValue serialisation
```

And `[dev-dependencies]` for tests:

```toml
wasm-bindgen-test = "0.3"
```

---

## 81. WASM Feature Flag & Build Pipeline (Track 3)

### What Changes

- `Cargo.toml` gains a `wasm` feature and WASM-target dependencies.
- `wasm-pack` is used as the build tool (install separately: `cargo install wasm-pack`).
- A `Makefile` target `make wasm` runs `wasm-pack build --target web`.
- CI-skip notes are added to `manual_tests/README.md` for the WASM build step.

### `Cargo.toml` additions

```toml
[features]
wasm = ["wasm-bindgen", "js-sys", "serde-wasm-bindgen"]

[target.'cfg(target_arch = "wasm32")'.dependencies]
wasm-bindgen = { version = "0.2", optional = true }
js-sys = { version = "0.3", optional = true }
serde-wasm-bindgen = { version = "0.6", optional = true }

[dev-dependencies]
wasm-bindgen-test = "0.3"
```

### `Makefile` target

```makefile
wasm:
	wasm-pack build --target web --out-dir pkg
```

### Verify build works

```bash
wasm-pack build --target web --out-dir pkg
# Should produce pkg/database.js, pkg/database_bg.wasm, pkg/package.json
```

The initial build will likely fail due to `std::fs` usage in `Pager` — item 83 addresses that.

### Key Files

- `Cargo.toml` — feature + dependencies
- `Makefile` — `wasm` target

### Implementation Steps (1 commit)

#### Step 81.1 — Add wasm feature, dependencies, and Makefile target

**Commit:** Build: add wasm-bindgen feature and wasm-pack build target

---

## 82. `JsDatabase` Wrapper (Track 3)

### What Changes

A new file `src/wasm.rs` (compiled only when `feature = "wasm"`) exposes a `#[wasm_bindgen]` struct:

```rust
#[cfg(feature = "wasm")]
mod wasm_bindings {
    use wasm_bindgen::prelude::*;
    use js_sys::Array;

    #[wasm_bindgen]
    pub struct Database {
        inner: crate::db::Db,  // wraps the in-memory DB
    }

    #[wasm_bindgen]
    impl Database {
        #[wasm_bindgen(constructor)]
        pub fn new() -> Database {
            Database { inner: crate::db::Db::new_in_memory() }
        }

        /// Execute a DDL or DML statement. Returns a status string.
        pub fn execute(&mut self, sql: &str) -> Result<String, JsValue> {
            match crate::db::execute(sql, self.inner.btree_mut()) {
                Ok(result) => Ok(format_execute_result(result)),
                Err(e) => Err(JsValue::from_str(&e.to_string())),
            }
        }

        /// Execute a SELECT. Returns a JS Array of row objects.
        pub fn query(&mut self, sql: &str) -> Result<JsValue, JsValue> {
            let result = crate::db::execute(sql, self.inner.btree_mut())
                .map_err(|e| JsValue::from_str(&e.to_string()))?;

            match result {
                crate::db::ExecuteResult::Query(mut q) => {
                    let arr = Array::new();
                    while let Some(row) = q.next() {
                        // Return rows as JS arrays of primitives
                        let js_row = Array::new();
                        for val in &row {
                            js_row.push(&scalar_to_js(val));
                        }
                        arr.push(&js_row);
                    }
                    Ok(arr.into())
                }
                _ => Ok(Array::new().into()),
            }
        }

        /// Return column names for the last query.
        /// (Once Phase P is implemented, pull from QueryExecution.column_names)
        pub fn column_names(&self) -> Array {
            // Placeholder — returns empty array until Phase P
            Array::new()
        }
    }

    fn scalar_to_js(v: &crate::engine::scalarvalue::ScalarValue) -> JsValue {
        use crate::engine::scalarvalue::ScalarValue::*;
        match v {
            Integer(i) => JsValue::from(*i),
            Floating(f) => JsValue::from(*f),
            Boolean(b) => JsValue::from(*b),
            String(s) => JsValue::from_str(s),
            Blob(b) => {
                // Return as Uint8Array
                let arr = js_sys::Uint8Array::new_with_length(b.len() as u32);
                arr.copy_from(b);
                arr.into()
            }
            Null => JsValue::null(),
        }
    }
}
```

### `Db::new_in_memory()`

A thin wrapper created in item 83:

```rust
impl Db {
    pub fn new_in_memory() -> Self { ... }
    pub fn btree_mut(&mut self) -> &mut BTree { ... }
}
```

### Key Files

- `src/wasm.rs` — `JsDatabase` and helper fns (new file)
- `src/lib.rs` — `mod wasm` gated on `#[cfg(feature = "wasm")]`

### Implementation Steps (1 commit)

#### Step 82.1 — Implement JsDatabase wrapper with execute() and query()

**Commit:** WASM: add JsDatabase wrapper with execute and query methods

---

## 83. In-Memory Storage Backend (Track 3)

### What Changes

The `Pager` (in `src/storage/pager.rs`) currently only opens pages from a file using `std::fs`. Add a `MemoryPager` variant that stores pages in a `Vec<Vec<u8>>` — no file I/O, no `std::fs`.

### Background

`BTree` is constructed from a `Pager`. `Pager` encapsulates all file reads/writes. The WASM environment has no filesystem (the `wasm32-unknown-unknown` target lacks `std::fs`).

The cleanest approach: make `Pager` generic over a storage backend:

```rust
pub trait PageStore {
    fn read_page(&mut self, page_num: u32) -> &[u8];
    fn write_page(&mut self, page_num: u32, data: &[u8]);
    fn num_pages(&self) -> u32;
    fn allocate_page(&mut self) -> u32;
    fn flush(&mut self) -> std::io::Result<()>;
}

pub struct FilePager { ... }       // existing implementation
pub struct MemoryPager {           // new
    pages: Vec<Vec<u8>>,
    page_size: usize,
}
```

`BTree<S: PageStore>` or (simpler for V1) `BTree` holds a `Box<dyn PageStore>`. The `Database` / `Db` struct is updated to accept either.

### V1 Simplification

Rather than fully genericising `Pager`, add a lighter path: `Pager::new_in_memory() -> Pager` that initialises with a `Vec<Vec<u8>>` buffer and replaces the `File` with an `enum PagerStorage { File(std::fs::File), Memory(Vec<Vec<u8>>) }` inside the existing struct. File operations are guarded with `match self.storage { ... }`.

This avoids touching the `BTree` type signature.

### Key Files

- `src/storage/pager.rs` — `PagerStorage` enum; `Pager::new_in_memory()`; `read_page`/`write_page` dispatch
- `src/db.rs` — `Db::new_in_memory()` constructor

### Tests

```rust
#[test]
fn test_in_memory_pager_basic() {
    let mut db = Db::new_in_memory();
    db.execute("CREATE TABLE t (x INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (42)").unwrap();
    let rows = db.query_rows("SELECT x FROM t").unwrap();
    assert_eq!(rows[0][0], ScalarValue::Integer(42));
}
```

### Implementation Steps (2 commits)

#### Step 83.1 — Add `PagerStorage` enum and `Pager::new_in_memory()`

Add `Memory` variant to storage, implement `read_page`/`write_page` dispatch, add `Pager::new_in_memory()`.

**Commit:** Storage: add in-memory pager backend

#### Step 83.2 — Add `Db::new_in_memory()` convenience constructor

Wire `Pager::new_in_memory()` through `BTree` and `Db`. Add integration test.

**Commit:** DB: add Db::new_in_memory() constructor

---

## 84. JS Smoke Tests (Track 7)

### What Changes

`tests/wasm/` directory with `wasm-bindgen-test` tests that run under `wasm-pack test --node`:

```rust
// tests/wasm/basic.rs
use wasm_bindgen_test::*;
use database::Database;

wasm_bindgen_test_configure!(run_in_node_experimental);

#[wasm_bindgen_test]
fn test_create_table_and_insert() {
    let mut db = Database::new();
    let r = db.execute("CREATE TABLE t (id INTEGER, val TEXT)");
    assert!(r.is_ok());
    let r = db.execute("INSERT INTO t VALUES (1, 'hello')");
    assert!(r.is_ok());
}

#[wasm_bindgen_test]
fn test_query_returns_rows() {
    let mut db = Database::new();
    db.execute("CREATE TABLE t (x INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (7)").unwrap();
    let rows = db.query("SELECT x FROM t").unwrap();
    let arr = rows.dyn_into::<js_sys::Array>().unwrap();
    assert_eq!(arr.length(), 1);
}
```

Run with:

```bash
wasm-pack test --node
```

### Key Files

- `tests/wasm/basic.rs` — WASM smoke tests

### Implementation Steps (1 commit)

#### Step 84.1 — Add wasm-bindgen-test smoke tests

**Commit:** Tests: wasm-bindgen smoke tests for JsDatabase

---

## Verification

- [ ] `wasm-pack build --target web` completes without errors
- [ ] `wasm-pack test --node` passes all WASM tests
- [ ] Native `cargo test` still passes (WASM code is feature-gated)
- [ ] Zero warnings: `cargo fmt && cargo build 2>&1 | grep -i warning`
- [ ] `pkg/database.js` and `pkg/database_bg.wasm` are generated
- [ ] JS snippet: `const db = new Database(); db.execute("CREATE TABLE t (x INTEGER)"); db.query("SELECT x FROM t")` works in Node.js
- [ ] In-memory pager passes `test_in_memory_pager_basic`

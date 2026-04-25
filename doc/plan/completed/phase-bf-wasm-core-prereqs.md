# Phase BF — WASM Core Library Prerequisites

Gate host-platform code behind `cfg` guards so the core library compiles for
`wasm32-unknown-unknown` without modification to call sites.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 141 | 3 | Gate `testing` module for WASM — remove `println!` and `std::fs` from WASM builds | — |
| 142 | 3 | Gate `BTree::dump_to_file()` and its `std::fs` import for WASM | — |
| 143 | 3 | Verify `cargo build --target wasm32-unknown-unknown` compiles (minus Pager, handled by Phase S) | 141, 142 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

Phase S (JavaScript / WebAssembly Bindings) compiles the database to WASM and adds an
in-memory `Pager` backend. Before that work can proceed, the core library must compile
cleanly for `wasm32-unknown-unknown`. Two host-platform dependencies block compilation
today — neither is in the `Pager` (which Phase S replaces):

1. **`pub mod testing`** (`src/testing/sql_runner.rs`):
   Declared `pub mod testing;` in `lib.rs` with no cfg guard, so it is compiled
   unconditionally into the library. `sql_runner.rs` uses `std::fs`, `env!("CARGO_MANIFEST_DIR")`,
   and `println!`. All three are unavailable or purposeless in `wasm32-unknown-unknown`.

2. **`BTree::dump_to_file()`** (`src/storage/btree.rs:~956`):
   A public method that calls `std::fs::OpenOptions`. The WASM target has no filesystem,
   so this will fail to link even though callers never invoke it at runtime.

Everything else in the core library is already WASM-compatible:
- `probe::probe!` uses its no-op `default.rs` implementation on non-Linux targets.
- `std::io::Write` (used by `CountingWriter` in `node.rs` and `Display` impls) is present on WASM.
- All `println!` calls in `lexer.rs`, `parser.rs`, `node.rs`, `btree.rs`, and `compiler/nodes.rs`
  are inside `#[test]` functions and are not compiled into library builds.

The `Pager` uses `std::fs::File` and `os::unix::prelude::MetadataExt`, but Phase S item 83
replaces the file-backed pager with a `MemoryPager` enum variant — that work is left to
Phase S. This phase removes the remaining blockers so Phase S starts with a library that
already compiles.

**Phase S dependency:** Phase S should be updated to list Phase BF as a prerequisite.

---

## Stubs

None.

---

## 141. Gate `testing` module for WASM (Track 3)

### What Changes

`src/lib.rs` currently unconditionally exports:

```rust
pub mod testing;
```

This pulls `src/testing/sql_runner.rs` into every build, including WASM. `sql_runner.rs`
uses:
- `use std::fs;` — filesystem not available on WASM
- `env!("CARGO_MANIFEST_DIR")` — resolves at compile time, but `PathBuf::from(...)` calls
  `fs::read_to_string` at runtime which won't link
- `println!()` — no stdout on `wasm32-unknown-unknown`

### Implementation Approach

Gate the module declaration with a cfg attribute so it is excluded from WASM builds:

```rust
// src/lib.rs
#[cfg(not(target_arch = "wasm32"))]
pub mod testing;
```

This is a non-breaking change for native builds: the module remains `pub` and all existing
callers (`update-sql-tests` binary, test harness via `build.rs`) continue to work because
they are also gated to native targets implicitly (binaries and `#[test]` code are never
compiled for WASM).

No changes are needed inside `src/testing/` itself.

### Key Files

- `src/lib.rs` — add `#[cfg(not(target_arch = "wasm32"))]` to `pub mod testing;`

### Tests

Native `cargo test --workspace` continues to pass — the cfg guard only excludes WASM.
After the change, verify:

```bash
cargo test --workspace                          # must still pass
cargo check --target wasm32-unknown-unknown     # testing module no longer causes errors
```

### Implementation Steps (1 commit)

#### Step 141.1 — Gate `pub mod testing` for WASM

**Commit:** Build: exclude testing module from wasm32 builds

---

## 142. Gate `BTree::dump_to_file()` for WASM (Track 3)

### What Changes

`src/storage/btree.rs` contains a public method:

```rust
pub fn dump_to_file(&self, output_path: &std::path::Path) -> std::io::Result<()> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .append(false)
        .open(output_path)?;
    let mut writer = std::io::BufWriter::new(file);
    write!(writer, "{}", self)?;
    Ok(())
}
```

`std::fs::OpenOptions` does not exist on `wasm32-unknown-unknown`, so this method blocks
compilation.

### Implementation Approach

Gate the method and its `use std::path::Path;` import with cfg:

```rust
#[cfg(not(target_arch = "wasm32"))]
pub fn dump_to_file(&self, output_path: &std::path::Path) -> std::io::Result<()> {
    // ... unchanged
}
```

The `use std::path::Path;` at the top of `btree.rs` (line 5) is only used in the
`dump_to_file` signature; gate it too, or change the signature to use the inline
`std::path::Path` form as shown above (removing the top-level `use`). The inline path
(`&std::path::Path`) is clearer and avoids a dead-import warning on WASM.

`dump_to_file` is called only from the CLI's `btree dump <path>` command
(`crates/database-cli/src/repl/modes/btree.rs`). The CLI crate targets native only, so
the call site remains unaffected.

### Key Files

- `src/storage/btree.rs` — add `#[cfg(not(target_arch = "wasm32"))]` to `dump_to_file`
- `src/storage/btree.rs` — remove or gate `use std::path::Path;`

### Tests

```bash
cargo test --workspace                          # must still pass
cargo check --target wasm32-unknown-unknown     # dump_to_file no longer blocks compilation
```

### Implementation Steps (1 commit)

#### Step 142.1 — Gate `dump_to_file` for WASM

**Commit:** Build: gate BTree::dump_to_file for wasm32 target

---

## 143. Verify WASM compilation baseline (Track 3)

### What Changes

After items 141 and 142, the core library should compile for `wasm32-unknown-unknown`
with the only remaining blocker being the `Pager` (`std::fs::File`,
`os::unix::prelude::MetadataExt`), which Phase S item 83 replaces.

This item is a verification step, not a code change: attempt a WASM `cargo check` and
document what errors remain.

### Implementation Approach

```bash
# Install the target if not already present
rustup target add wasm32-unknown-unknown

# Check the core library (not the CLI crate, which is native-only)
cargo check -p database --target wasm32-unknown-unknown
```

Expected output after items 141 and 142: errors only from `src/storage/pager.rs` (the
file-backed Pager). All other modules should compile cleanly.

If additional unexpected blockers surface, address them in this item before closing the
phase.

### Key Files

- `src/storage/pager.rs` — document remaining errors for Phase S to resolve

### Tests

No new tests. The successful `cargo check` output is the verification.

### Implementation Steps (1 commit or no commit)

If the verification reveals no unexpected blockers, no commit is needed — close the item
with a note in the PR description. If unexpected blockers are found, fix them and commit:

**Commit (only if needed):** Build: fix unexpected wasm32 compilation blockers

---

## Verification

- [ ] `cargo test --workspace` passes (native builds unaffected)
- [ ] `cargo check --target wasm32-unknown-unknown -p database` produces errors only from `pager.rs`
- [ ] Zero warnings: `cargo fmt --all && cargo build --workspace 2>&1 | grep -i warning`
- [ ] Phase S plan updated to list Phase BF as a prerequisite

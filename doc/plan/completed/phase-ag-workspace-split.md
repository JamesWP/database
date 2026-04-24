# Phase AG — Workspace Split: Core Library + CLI Crate

Restructure the single-crate repository into a Cargo workspace with a standalone
`database` library crate and a separate `database-cli` crate for the REPL and
interactive tooling.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 119 | 6 | Convert root Cargo.toml to workspace root and create `crates/database-cli/` skeleton | — |
| 120 | 6 | Move `src/main.rs` + `src/repl/` into `crates/database-cli/` | 119 |
| 121 | 6 | Move REPL/TUI deps to CLI crate; promote test deps to `[dev-dependencies]` | 120 |
| 122 | 7 | Update CLAUDE.md and README.md for workspace layout | 121 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

The entire codebase lives in one Cargo package today.  A user who wants to embed
the database engine in their application would pull in `rustyline`, `ratatui`,
`crossterm`, and `ansi-to-tui` — none of which they need.  Similarly, `proptest`,
`rand`, and `tempfile` are test-only but listed as normal dependencies.

Splitting into a Cargo workspace fixes both problems:

- **`crates/database`** — the core library.  Zero interactive / TUI deps.  This is
  what an application author adds to their `Cargo.toml`.
- **`crates/database-cli`** — the interactive REPL and TUI debugger.  Depends on
  `database` and brings in UI deps.

The `tests/` directory, `build.rs`, and the `update-sql-tests` binary all belong to
the library crate (they test and support the engine, not the REPL).

**Embedding example after this phase:**

```toml
# application Cargo.toml
[dependencies]
database = { path = "../database/crates/database" }
# or once published:
# database = "0.1"
```

```rust
use database::db::Db;

fn main() {
    let mut db = Db::open("app.db").unwrap();
    db.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT, value TEXT)").unwrap();
    db.execute("INSERT INTO kv VALUES ('hello', 'world')").unwrap();
    let rows = db.query("SELECT key, value FROM kv").unwrap();
    println!("{:?}", rows);
}
```

---

## 119. Convert root to workspace + create CLI skeleton (Track 6)

### What Changes

1. Add a `[workspace]` section to the **root** `Cargo.toml` listing two members:
   - `.` (the existing `database` library package — stays at root)
   - `crates/database-cli`
2. Create `crates/database-cli/Cargo.toml` with:
   - `name = "database-cli"`
   - A dependency on `database = { path = "../.." }` (the root library)
   - An empty `src/main.rs` (`fn main() {}`) as a placeholder
3. Remove `default-run = "database"` from root `Cargo.toml` (no longer needed once
   the binary is gone from the root package).

After this commit `cargo build --workspace` compiles cleanly.  The existing
`database` binary still lives in the root package at this point.

### Background

A Cargo workspace allows multiple packages in one repository.  The root
`Cargo.toml` gains:

```toml
[workspace]
members = [".", "crates/database-cli"]
resolver = "2"
```

The root package (`[package]` block) stays unchanged — it is still the `database`
library crate.  Workspace membership is additive.

### Implementation Approach

Root `Cargo.toml` additions:

```toml
[workspace]
members = [".", "crates/database-cli"]
resolver = "2"
```

`crates/database-cli/Cargo.toml` (initial skeleton):

```toml
[package]
name = "database-cli"
version = "0.1.0"
edition = "2021"

[dependencies]
database = { path = "../.." }
```

`crates/database-cli/src/main.rs` (placeholder):

```rust
fn main() {}
```

### Key Files

- `Cargo.toml` — add `[workspace]` section, remove `default-run`
- `crates/database-cli/Cargo.toml` — new file
- `crates/database-cli/src/main.rs` — placeholder

### Tests

```bash
cargo build --workspace
cargo test --workspace  # all existing tests still pass
```

### Implementation Steps (1 commit)

#### Step 119.1 — Add workspace Cargo.toml and CLI skeleton

**Commit:** `Refactor: convert to Cargo workspace, add database-cli skeleton`

---

## 120. Move `src/main.rs` + `src/repl/` into `crates/database-cli/` (Track 6)

### What Changes

1. Copy `src/main.rs` → `crates/database-cli/src/main.rs` (replacing the placeholder),
   updating the `use database::...` paths (they already use the crate name, so most
   will work unchanged).
2. Move `src/repl/` → `crates/database-cli/src/repl/`.
3. Remove `src/main.rs` and `src/repl/` from the root package.
4. Remove `mod repl;` from what was the binary entry point (now gone from root).
5. The root package `Cargo.toml` no longer has a `[[bin]]` target for `database`;
   the CLI binary is now `database-cli`.

After this commit:
- `cargo run -p database-cli -- test.db` launches the REPL.
- `cargo test` (library tests) still passes.

### Background

`src/main.rs` today does:

```rust
mod repl;

use database::storage;
use repl::{Repl, SharedState};
```

The `mod repl;` resolves to `src/repl/mod.rs`.  After the move this becomes
`crates/database-cli/src/repl/mod.rs` and `mod repl;` in
`crates/database-cli/src/main.rs` resolves correctly.

The REPL modes reference library types via `use database::...` paths — these
already use the published crate name and require no changes.  The few
`use super::...` or `use crate::...` paths inside `src/repl/` will need updating
to `use database::...` where they reference library internals.

### Key Files

- `crates/database-cli/src/main.rs` — moved from `src/main.rs`
- `crates/database-cli/src/repl/` — moved from `src/repl/`
- `src/main.rs` — deleted
- `src/repl/` — deleted

### Tests

```bash
cargo build --workspace             # no errors
cargo run -p database-cli -- test.db sql "SELECT 1"
cargo test                          # library tests pass
```

### Implementation Steps (1 commit)

#### Step 120.1 — Move REPL source files to crates/database-cli/

**Commit:** `Refactor: move REPL and main.rs into crates/database-cli`

---

## 121. Move REPL/TUI deps to CLI; promote test deps to dev-dependencies (Track 6)

### What Changes

Move these deps from the **root** `Cargo.toml` to `crates/database-cli/Cargo.toml`:

| Dep | Reason |
|-----|--------|
| `rustyline` | REPL readline library |
| `ratatui` | TUI framework (used only in `tui_debugger.rs`) |
| `crossterm` | Terminal backend for ratatui |
| `ansi-to-tui` | ANSI escape → ratatui widget (used in TUI debugger) |

Move these to `[dev-dependencies]` in the root `Cargo.toml`:

| Dep | Reason |
|-----|--------|
| `proptest` | Property-based tests (`src/storage/btree.rs`, etc.) |
| `rand` | Random data generation in tests |
| `tempfile` | Temporary file creation in tests |

`colored` stays in the root library (`[dependencies]`) — it is used in
`src/storage/btree.rs`, `src/engine/program.rs`, and `src/engine/scalarvalue.rs`
for debug/display formatting.

`serde`, `serde_bytes`, `ciborium`, `peekmore` stay as normal library deps.

### Implementation Approach

Root `Cargo.toml` after changes:

```toml
[dependencies]
ciborium       = { version = "0.2", default-features = false, features = ["std"] }
colored        = "2"
peekmore       = "1.3.0"
serde          = { version = "1.0.158", features = ["derive"] }
serde_bytes    = "0.11"

[dev-dependencies]
proptest       = "1.1.0"
rand           = "0.8.5"
tempfile       = "3.24.0"
```

`crates/database-cli/Cargo.toml` after changes:

```toml
[dependencies]
database       = { path = "../.." }
ansi-to-tui    = "7"
colored        = "2"
crossterm      = "0.28"
ratatui        = "0.29"
rustyline      = "17.0.2"
```

### Key Files

- `Cargo.toml` — trim `[dependencies]`, add `[dev-dependencies]`
- `crates/database-cli/Cargo.toml` — add REPL/TUI deps

### Tests

```bash
cargo build --workspace
cargo test --workspace   # proptest, rand, tempfile available via dev-deps
```

### Implementation Steps (1 commit)

#### Step 121.1 — Redistribute dependencies between library and CLI crates

**Commit:** `Refactor: move REPL/TUI deps to CLI crate, test deps to dev-dependencies`

---

## 122. Update CLAUDE.md and README.md for workspace layout (Track 7)

### What Changes

Update documentation to reflect:

1. **CLAUDE.md**: Update build commands to use `--workspace` or `-p` flags where
   relevant; update the architecture section to describe the two-crate structure;
   update the interactive CLI section to use `cargo run -p database-cli`.
2. **README.md**: Add an "Embedding" section showing how to depend on `database`
   from an application; update the "Running" / "Usage" section for the new binary
   name/path.

### Key Files

- `CLAUDE.md`
- `README.md`

### Tests

n/a — documentation only.

### Implementation Steps (1 commit)

#### Step 122.1 — Update CLAUDE.md and README.md for workspace

**Commit:** `Docs: update CLAUDE.md and README for Cargo workspace layout`

---

## Verification

- [ ] `cargo build --workspace` — clean build, zero errors
- [ ] `cargo test --workspace` — all tests pass, no regressions
- [ ] `cargo fmt --all && cargo build --workspace 2>&1 | grep -i warning` — zero warnings
- [ ] `cargo run -p database-cli -- test.db` launches the interactive REPL
- [ ] `cargo run -p database-cli -- test.db sql "SELECT 1"` returns `1`
- [ ] A project that adds `database = { path = "..." }` compiles without pulling in `rustyline`, `ratatui`, `crossterm`, or `ansi-to-tui`
- [ ] Each commit is independently buildable

# Phase AY — Per-Query bpftrace Trace Log

Add a `trace-query.bt` bpftrace script that emits a sequential, per-query trace log showing the SQL text, each VM operation executed, and the user-space call stack for every page read or write.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 120 | 7 | Add `query_start` USDT probe to `db::execute()` carrying the SQL string | — |
| 121 | 7 | Add `engine_opcode` USDT probe to `engine::step()` carrying the opcode name | — |
| 122 | 7 | Add `scripts/trace-query.bt` and `make trace-query` Makefile target | 120, 121 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

The existing `trace-sakila.bt` script aggregates counters across an entire run. It answers "how many page reads happened?" but not "which page reads happened during *this* query, and what code caused them?".

`trace-query.bt` produces a sequential trace: each query prints its SQL, then every VM opcode it executes, then every page read/write it triggers together with the user-space stack at the call site. This is the right tool for diagnosing unexpected I/O, verifying that an index scan avoids a full table read, or understanding the bytecode path through a complex query.

### Why two new probes?

The existing `engine_program_run` probe fires once per `run()` call but carries no data. The existing `engine_step` probe fires per step but carries no data.

To emit a useful trace we need:

1. **`query_start(ptr, len)`** — fires at the entry point of `db::execute()` with the raw SQL string. The bpftrace script reads it with `str(arg0, arg1)` and labels the following trace output.

2. **`engine_opcode(ptr, len)`** — fires once per VM step carrying the opcode name as a static string. A new `Operation::name() -> &'static str` method provides the string without any allocation. This is a *separate* probe from `engine_step` so the existing statistics scripts remain unchanged.

### Stack traces and symbol resolution

`ustack()` in bpftrace walks the user-space call stack. Readable symbol names require debug symbols in the binary. The debug build (`cargo build`) includes full symbols. Release builds strip symbols by default, so `trace-query` targets `./target/debug/database` and the `make trace-query` target builds the debug binary.

To use with a release binary, rebuild with:
```toml
# Cargo.toml
[profile.release]
debug = 1
```
and update the binary path in the script.

---

## Stubs

None.

---

## 120. Add `query_start` USDT probe to `db::execute()` (Track 7)

### What Changes

A new `query_start` probe fires at the very start of `db::execute()`, before parsing, carrying the SQL string as a `(pointer, length)` pair.

### Background

The SQL string lives only in `db::execute()` as `sql: &str`. The address and length of the backing bytes are available without allocation. bpftrace reads the string with `str(arg0, arg1)`.

Firing before parsing means the probe captures the raw user-supplied SQL even if parsing fails — useful for tracing error cases.

### Implementation Approach

```rust
// src/db.rs — top of execute()
pub fn execute(sql: &str, catalog: &mut Catalog) -> Result<ExecuteResult, ExecuteError> {
    let sql_bytes = sql.as_bytes();
    probe!(database, query_start, sql_bytes.as_ptr() as usize, sql_bytes.len());
    let stmt = parse(sql).map_err(ExecuteError::Parse)?;
    ...
}
```

In bpftrace:
```
usdt:.../database:database:query_start {
    printf("\n[query] %s\n", str(arg0, arg1));
}
```

The `probe!` macro marks the call site as a NOP sled when no bpftrace is attached, so there is zero overhead in normal execution.

### Key Files

- `src/db.rs` — add `probe!(database, query_start, ...)` at the start of `execute()`

### Tests

No automated test needed — USDT probes are inert when bpftrace is not attached. Manual verification: run the script against `cargo run -- demo.db sql "SELECT 1"` and confirm the SQL line appears.

### Implementation Steps (1 commit)

#### Step 120.1 — Add `query_start` probe to `db::execute()`

Add `use probe::probe;` if not already imported. Insert the two-argument probe call before `parse(sql)`. Run `cargo build` and verify zero warnings.

**Commit:** `tracing: add query_start USDT probe carrying SQL string`

---

## 121. Add `engine_opcode` USDT probe to `engine::step()` (Track 7)

### What Changes

A new `engine_opcode(ptr, len)` probe fires once per VM step carrying the opcode name as a static string. A new `Operation::name() -> &'static str` method is added to `program.rs`.

### Background

`engine_step` fires per step but carries no data — bpftrace can count steps but cannot identify which operation fired. A separate `engine_opcode` probe that carries the name allows the trace script to print each operation in order without modifying the existing stats scripts.

`Operation::name()` is a simple match returning short, stable string literals. The match is exhaustive and will produce a compile error if a new operation variant is added without a name entry.

### Implementation Approach

**`src/engine/program.rs` — add `Operation::name()`:**

```rust
impl Operation {
    /// Return a short static name for use in USDT tracing.
    pub fn name(&self) -> &'static str {
        use Operation::*;
        match self {
            StoreValue(..)              => "Store",
            IncrementValue(..)          => "Inc",
            DecrementValue(..)          => "Dec",
            AddValue(..)                => "Add",
            SubtractValue(..)           => "Sub",
            MultiplyValue(..)           => "Mul",
            DivideValue(..)             => "Div",
            RemainderValue(..)          => "Rem",
            LessThanValue(..)           => "Lt",
            LessThanOrEqualValue(..)    => "Le",
            GreaterThanValue(..)        => "Gt",
            GreaterThanOrEqualValue(..) => "Ge",
            EqualsValue(..)             => "Eq",
            NotEqualsValue(..)          => "Ne",
            AndValue(..)                => "And",
            OrValue(..)                 => "Or",
            NotValue(..)                => "Not",
            NegateValue(..)             => "Neg",
            CopyValue(..)               => "Copy",
            IsNullValue(..)             => "IsNull",
            IsNotNullValue(..)          => "IsNotNull",
            LengthValue(..)             => "Length",
            UpperValue(..)              => "Upper",
            LowerValue(..)              => "Lower",
            AbsValue(..)                => "Abs",
            LikeValue(..)               => "Like",
            InitRowBuffer(..)           => "InitRowBuf",
            AppendToRowBuffer(..)       => "AppendRowBuf",
            SortRowBuffer(..)           => "SortRowBuf",
            RewindRowBuffer(..)         => "RewindRowBuf",
            NextFromRowBuffer(..)       => "NextRowBuf",
            InitGroupTable(..)          => "InitGroupTable",
            UpdateGroup(..)             => "UpdateGroup",
            YieldFromGroupTable(..)     => "YieldGroup",
            Open(..)                    => "Open",
            MoveCursor(..)              => "MoveCursor",
            ReadCursor(..)              => "ReadCursor",
            ReadKey(..)                 => "ReadKey",
            WriteCursor(..)             => "WriteCursor",
            WriteIndex(..)              => "WriteIndex",
            CheckUnique(..)             => "CheckUnique",
            DeleteIndex(..)             => "DeleteIndex",
            DeleteCursor(..)            => "DeleteCursor",
            CanReadCursor(..)           => "CanRead",
            EncodeIndexKey(..)          => "EncodeKey",
            ReadCurrentKey(..)          => "ReadCurKey",
            BlobStartsWith(..)          => "BlobStarts",
            BlobPrefixLt(..)            => "BlobPfxLt",
            BlobPrefixLe(..)            => "BlobPfxLe",
            BlobSliceTail(..)           => "BlobTail",
            BlobSliceLast(..)           => "BlobLast",
            BlobDropLast(..)            => "BlobDrop",
            DecodeU64Key(..)            => "DecodeU64",
            DecodeIndexColumns { .. }   => "DecodeIdxCols",
            Yield(..)                   => "Yield",
            GoTo(..)                    => "GoTo",
            GoToIfEqualValue(..)        => "GoToIfEq",
            GoToIfFalse(..)             => "GoToIfFalse",
            Halt                        => "Halt",
        }
    }
}
```

**`src/engine.rs` — fire `engine_opcode` in `step()`:**

```rust
pub fn step(&mut self) -> StepResult {
    probe!(database, engine_step);
    let op = self.program.advance();
    let opname = op.name().as_bytes();
    probe!(database, engine_opcode, opname.as_ptr() as usize, opname.len());
    match op { ... }
}
```

`engine_step` continues to fire first (for backward compatibility with `trace-sakila.bt` counters), then `engine_opcode` fires with the name.

In bpftrace:
```
usdt:.../database:database:engine_opcode {
    printf("  [op] %s\n", str(arg0, arg1));
}
```

### Key Files

- `src/engine/program.rs` — add `Operation::name() -> &'static str`
- `src/engine.rs` — add `engine_opcode` probe call in `step()` after `advance()`

### Tests

`cargo test` must pass. The `engine_opcode` probe is inert when bpftrace is not attached. Manual spot-check: `sudo bpftrace -e 'usdt:./target/debug/database:database:engine_opcode { printf("%s\n", str(arg0, arg1)); }' & cargo run -- demo.db sql "SELECT 1"` should print `Store`, `Yield`, `Halt` (or similar).

### Implementation Steps (1 commit)

#### Step 121.1 — Add `Operation::name()` and `engine_opcode` probe

Add `Operation::name()` to `program.rs`. In `engine.rs::step()`, call `op.name()` after `advance()` and fire the `engine_opcode` probe. Run `cargo build && cargo test`.

**Commit:** `tracing: add engine_opcode USDT probe with opcode name`

---

## 122. Add `scripts/trace-query.bt` and `make trace-query` (Track 7)

### What Changes

A new bpftrace script `scripts/trace-query.bt` that produces a per-query sequential trace, and a `make trace-query` Makefile target that builds the debug binary and launches the script.

### Background

The script attaches to the debug binary (for readable stack symbols) and prints:

```
[query] SELECT * FROM users WHERE id = 1

  [op] Open
  [op] MoveCursor
  [op] CanRead
  [op] GoToIfFalse
  [op] ReadCursor
  [op] Store
  [op] Lt
  [op] GoToIfFalse
    [page_read: leaf]  page=3
        database`storage::btree::BTree::read_leaf+0x42
        database`storage::btree::BTree::step_cursor+0x1e0
        database`engine::Engine::step+0x3f8
        database`db::execute+0x2c1
        ...
  [op] Yield
  [op] GoTo
  [op] Halt
```

Each `[query]` block starts with the SQL. The `[op]` lines show the bytecode sequence. Page read/write events include the page type and a user-space stack trace showing which code path triggered the I/O.

### Implementation Approach

```bpftrace
#!/usr/bin/env bpftrace
//
// Per-query trace log: SQL text, VM operations, and page I/O call stacks.
//
// Requires debug symbols — targets the debug binary by default.
// Build with: cargo build
//
// Usage:
//   sudo bpftrace scripts/trace-query.bt
//   # In another terminal:
//   cargo run -- <db_file> sql "<your query>"
//
// Or via: make trace-query
//   # Launches the script; run your query manually in a second terminal.
//
// For release binary (stripped): rebuild with debug = 1 in [profile.release]
// and change the binary path below to ./target/release/database.

// ─── Query start ─────────────────────────────────────────────────────────────

usdt:./target/debug/database:database:query_start {
    printf("\n[query] %s\n", str(arg0, arg1));
}

// ─── VM operations ───────────────────────────────────────────────────────────

usdt:./target/debug/database:database:engine_opcode {
    printf("  [op] %s\n", str(arg0, arg1));
}

// ─── Page reads by type ───────────────────────────────────────────────────────

usdt:./target/debug/database:database:page_read_leaf {
    printf("    [page_read: leaf]\n");
    printf("%s\n", ustack());
}

usdt:./target/debug/database:database:page_read_interior {
    printf("    [page_read: interior]\n");
    printf("%s\n", ustack());
}

usdt:./target/debug/database:database:page_read_overflow {
    printf("    [page_read: overflow]\n");
    printf("%s\n", ustack());
}

usdt:./target/debug/database:database:page_read_zero {
    printf("    [page_read: zero-page]\n");
    printf("%s\n", ustack());
}

usdt:./target/debug/database:database:page_read_freelist {
    printf("    [page_read: freelist]\n");
    printf("%s\n", ustack());
}

// ─── Page writes by type ──────────────────────────────────────────────────────

usdt:./target/debug/database:database:page_write_leaf {
    printf("    [page_write: leaf]\n");
    printf("%s\n", ustack());
}

usdt:./target/debug/database:database:page_write_interior {
    printf("    [page_write: interior]\n");
    printf("%s\n", ustack());
}

usdt:./target/debug/database:database:page_write_zero {
    printf("    [page_write: zero-page]\n");
    printf("%s\n", ustack());
}

// ─── Cache hits / misses (no stack — high frequency, informational only) ─────

usdt:./target/debug/database:database:page_read_cache_hit {
    printf("    [cache: HIT]\n");
}

usdt:./target/debug/database:database:page_read_cache_miss {
    printf("    [cache: MISS]\n");
}
```

**Makefile target:**

```makefile
# Trace a single query with per-operation and per-page-IO call stacks.
# Attaches to the debug binary (symbols required for readable stacks).
# Launch this target, then run your query in a second terminal.
trace-query: target/debug/database
	@echo "==> bpftrace attached to ./target/debug/database"
	@echo "==> Run your query in another terminal, e.g.:"
	@echo "      cargo run -- demo.db sql \"SELECT * FROM users\""
	@echo "==> Press Ctrl-C to stop."
	sudo bpftrace scripts/trace-query.bt
```

The target depends on `target/debug/database` so `cargo build` runs automatically if the binary is stale.

### Key Files

- `scripts/trace-query.bt` — new bpftrace script
- `Makefile` — add `trace-query` target after `trace-sql-tests`

### Tests

No automated tests. Manual verification:
1. `make trace-query` in one terminal (requires sudo/bpftrace).
2. In another terminal: `cargo run -- demo.db sql "SELECT * FROM users LIMIT 2"`.
3. Confirm output contains `[query] SELECT * FROM users LIMIT 2`, a sequence of `[op]` lines, and `[page_read: leaf]` entries with a stack trace showing `btree` and `engine` frames.

### Implementation Steps (1 commit)

#### Step 122.1 — Add `trace-query.bt` script and `make trace-query` target

Create `scripts/trace-query.bt` with the content above. Add the `trace-query` target to `Makefile` after `trace-sql-tests`. Run `make trace-query` and verify the script loads without parse errors (bpftrace will report probe attachment counts).

**Commit:** `tracing: add trace-query.bt per-query trace script and make target`

---

## Verification

- [ ] `cargo build && cargo test` — zero warnings, all tests pass
- [ ] `sudo bpftrace -l 'usdt:./target/debug/database:database:*'` — lists `query_start` and `engine_opcode` alongside existing probes
- [ ] `make trace-query` attaches without errors; a query in a second terminal produces `[query]`, `[op]`, and `[page_read/write]` lines
- [ ] Each commit is independently buildable and testable
- [ ] Existing `make trace-sakila` / `make trace-tests` / `make trace-sql-tests` unaffected

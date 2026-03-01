# Phase AB — REPL Polish & Parallel-Slice Refactor

Four developer-experience improvements: merge parallel index slices into a paired struct, add step-by-step bytecode execution to the engine REPL mode, overhaul the Graphviz dot exporter to show real cell data, and embed a demo GIF of the colorful bytecode listing in the README.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 99 | 7 | Refactor: merge `IndexMaintenanceInfo` + cursor `Reg` into `IndexWithCursor` | — |
| 100 | 7 | Engine REPL: `step`/`run` commands for executing compiled bytecode | — |
| 101 | 3 | Overhaul `btree_graph.rs`: per-tree subgraphs, decoded cell values, overflow links | — |
| 102 | 7 | README: add demo GIF of engine REPL compile listing | 100 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

Three unrelated quality-of-life improvements grouped into one phase.

**Item 99** fixes a code smell in `src/compiler/nodes.rs` where two parallel slices — `indexes: &[IndexMaintenanceInfo]` and `cursor_regs: &[Reg]` — are threaded through multiple functions and zipped inside each body. The fix: a single `IndexWithCursor` struct pairs the two and eliminates the zips. This is a pure refactor with no behaviour changes.

**Item 100** unlocks the "Future work" note in the engine REPL help text. The engine already has a `step()` method; this item exposes it in the REPL so developers can single-step through compiled bytecode and inspect register state after each instruction. Useful for debugging new compiler output.

**Item 101** records a GIF of the engine REPL in action (colorized bytecode listing) and embeds it in the README, making the project more approachable.

---

## 99. Refactor: `IndexWithCursor` (Track 7)

### What Changes

In `src/compiler/nodes.rs`, `open_index_cursors` returns `Vec<Reg>`. This is immediately zipped with the `indexes: &[IndexMaintenanceInfo]` parameter at every call site:

```rust
for (index, &cursor_reg) in indexes.iter().zip(cursor_regs) { ... }
```

The fix: introduce a `IndexWithCursor` struct that pairs both fields. Change `open_index_cursors` to return `Vec<IndexWithCursor>`, and update `emit_write_indexes` / `emit_delete_indexes` to accept `&[IndexWithCursor]`.

### Background

The parallel-slice pattern is fragile: the caller must maintain two `Vec`s in lockstep; passing them out of order silently produces wrong bytecode. Merging them into one type enforces the pairing at the type level.

Current call pattern (appears 3× in nodes.rs):

```rust
let index_cursor_regs = open_index_cursors(indexes, ctx);
// ...
emit_write_indexes(indexes, &index_cursor_regs, &row_regs, key_reg, ctx);
```

After refactor:

```rust
let index_cursors = open_index_cursors(indexes, ctx);
// ...
emit_write_indexes(&index_cursors, &row_regs, key_reg, ctx);
```

### Implementation Approach

Add a private struct near the top of `nodes.rs`:

```rust
struct IndexWithCursor {
    info: crate::planner::IndexMaintenanceInfo,
    cursor_reg: Reg,
}
```

Update `open_index_cursors`:

```rust
fn open_index_cursors(
    indexes: &[crate::planner::IndexMaintenanceInfo],
    ctx: &mut CodegenContext,
) -> Vec<IndexWithCursor> {
    indexes
        .iter()
        .map(|index| {
            let cursor_reg = ctx.registers.alloc();
            init!(ctx; Open(cursor_reg, index.root_page, OpenMode::ReadWrite));
            IndexWithCursor {
                info: index.clone(),
                cursor_reg,
            }
        })
        .collect()
}
```

Update `emit_write_indexes` and `emit_delete_indexes` signatures:

```rust
fn emit_write_indexes(
    index_cursors: &[IndexWithCursor],
    row_regs: &[Reg],
    key_reg: Reg,
    ctx: &mut CodegenContext,
) {
    for ic in index_cursors {
        let col_regs: Vec<Reg> = ic.info.column_idxs.iter().map(|&c| row_regs[c]).collect();
        ctx.body_emitter
            .emit(Operation::WriteIndex(ic.cursor_reg, col_regs, key_reg));
    }
}
```

Update the three call sites in `codegen_insert`, `codegen_update`, and `codegen_delete`.

### Key Files

- `src/compiler/nodes.rs` — struct definition, `open_index_cursors`, `emit_write_indexes`, `emit_delete_indexes`, and three call sites

### Tests

No new tests needed — this is a pure refactor. `cargo test` must pass identically before and after.

### Implementation Steps (1 commit)

#### Step 99.1 — Introduce `IndexWithCursor` and update all sites

Add the struct, update `open_index_cursors` return type, update both emit helpers, fix all call sites.

**Commit:** `Refactor: merge IndexMaintenanceInfo and cursor Reg into IndexWithCursor`

---

## 100. Engine REPL: step/run commands (Track 7)

### What Changes

The engine REPL mode gains two new commands:

- `run` — execute the loaded program to completion against the BTree, printing each yielded row and final register state
- `step` — execute one bytecode instruction, printing the instruction and register state after

The `EngineMode` stores an optional `StepState` alongside the compiled program:

```rust
struct StepState {
    engine: Engine,
    pc: usize,  // instruction index (for display)
}
```

### Background

The engine already exposes `Engine::new(registers, program)` and `Engine::step() -> StepResult` in `src/engine.rs`. The REPL help text currently says "Full VM execution requires btree integration (future work)" — this item ships the btree-less version, which is sufficient for inspecting the init+body control flow, StoreValue, GoTo, GoToIfFalse, IncrementValue, etc. Operations that touch the BTree (OpenCursor, ReadCursor, MoveCursor) will return an `Err` which the REPL displays as a step error.

### Implementation Approach

`StepState` holds an `Engine` initialised without a BTree (using `Engine::new`). The `step` command calls `engine.step()` once and pretty-prints the result:

```
engine> compile SELECT 1 + 2
Compiled: 4 operations, 3 registers
engine> step
  0  StoreValue(r0, 1)
     r0 = 1
engine> step
  1  StoreValue(r1, 2)
     r1 = 2
engine> step
  2  Add(r2, r0, r1)
     r2 = 3
engine> step
  3  Halt
     [halted]
```

Register display: iterate `0..num_registers` and show only non-null registers (skip `RegisterValue::Null` / `Uninitialized`).

New fields on `EngineMode`:

```rust
pub struct EngineMode {
    program: Option<CompiledProgram>,
    step_state: Option<StepState>,
}
```

`compile` clears `step_state`. `step` lazily initialises `step_state` from `program` on the first call. `reset`/`clear` clears both.

New command arms:

```rust
["step"] => { /* single step */ }
["run"] => { /* run to halt, print rows */ }
["registers"] | ["regs"] => { /* print all register values */ }
["restart"] => { /* reinitialise step_state from program */ }
```

### Key Files

- `src/repl/modes/engine.rs` — new commands, `StepState` struct, register display helper

### Tests

No automated tests needed for REPL mode (REPL has 0 tests currently). Verify manually:

```
engine> compile SELECT 1
engine> step          # shows instruction 0
engine> step          # shows instruction 1
engine> step          # shows Halt
engine> step          # shows "Program halted"
engine> restart
engine> run           # runs all steps, prints final state
```

### Implementation Steps (1 commit)

#### Step 100.1 — Add step/run/restart/regs commands to engine REPL mode

Add `StepState`, update `EngineMode`, implement the four command arms, update help text.

**Commit:** `Feature: engine REPL step/run commands for bytecode inspection`

---

## 101. Overhaul `btree_graph.rs` (Track 3)

### What Changes

`src/storage/btree_graph.rs` is largely commented-out dead code. The current `dump` function:
- Iterates pages 1..N blindly (no tree structure)
- Leaf nodes: entirely commented out — nothing renders
- Overflow pages: entirely commented out
- Interior nodes: render but with no knowledge of which tree they belong to
- Values: raw bytes, never decoded

The overhaul rewrites `dump` to produce a correct, useful Graphviz diagram:

1. **Named subgraphs** — one `subgraph cluster_N` per B-tree, labeled with the table/index name (catalog, user tables, secondary indexes)
2. **Tree-walk** — DFS from each known root page rather than blind page iteration
3. **Leaf nodes** — render each cell as `key | col0, col1, col2` with CBOR-decoded values
4. **Interior nodes** — render separator keys and child pointers (already partially working)
5. **Overflow pages** — render as a small stub node linked from the leaf cell with a dashed edge; label with byte count

### Background

The feature has existed since early development but was never completed after the CBOR serialization overhaul (Phase F). The Makefile already has `make <name>.svg` support — this item makes it actually useful.

Currently `dump` receives only `&Pager` and has no table metadata. After this change it receives `&BTree`, which provides:
- `btree.schema_root_page()` → `Option<u32>` for the catalog root
- `btree.scan_schema_entries()` → `Vec<(type, name, tbl_name, rootpage, sql)>` for all tables and indexes
- `CellReader::new(pager, page_idx, cell_idx).decode_as_array()` for decoding cell values

### Implementation Approach

Change the `BTree::fmt` Display impl and `dump_to_file` to pass `&self` (i.e. `&BTree`) to the graph function.

New top-level structure in `btree_graph.rs`:

```rust
pub fn dump<W: Write>(output: &mut W, btree: &BTree) -> Result {
    writeln!(output, "digraph Database {{")?;
    writeln!(output, "\tnode [shape=record fontname=\"monospace\"]")?;
    writeln!(output, "\trankdir=\"LR\";")?;

    // Catalog tree (treated as a table — rowid keys)
    if let Some(root) = btree.schema_root_page() {
        write_subgraph(output, btree, root, "db_schema (catalog)", TreeKind::Table)?;
    }

    // User tables and indexes
    for (kind, name, _tbl, rootpage, _sql) in btree.scan_schema_entries() {
        match kind.as_str() {
            "table" => {
                write_subgraph(output, btree, rootpage, &name, TreeKind::Table)?;
            }
            "index" => {
                // Retrieve column names from IndexInfo to decode composite keys
                let col_names = btree.lookup_indexes_for_table(&_tbl)
                    .into_iter()
                    .find(|i| i.index_name == name)
                    .map(|i| i.column_names)
                    .unwrap_or_default();
                write_subgraph(output, btree, rootpage, &format!("idx: {name}"),
                    TreeKind::Index { col_names: &col_names })?;
            }
            _ => {}
        }
    }

    writeln!(output, "}}")?;
    Ok(())
}
```

`write_subgraph` wraps a DFS in `subgraph cluster_N { label="..." }` and calls:
- `write_interior_node` for interior pages
- `write_leaf_node` for leaf pages
- `write_overflow_node` (stub) for overflow pages

**Leaf cell rendering** — use `CellReader` to decode values:

```rust
fn write_leaf_node<W: Write>(output: &mut W, pager: &Pager, page_idx: u32) -> Result {
    let node = pager.get_and_decode(page_idx);
    if let NodePage::Leaf(leaf) = node {
        write!(output, "\t{}", node_name_str(page_idx))?;
        let cells: Vec<String> = (0..leaf.num_items()).map(|i| {
            let cell = leaf.get_item_at_index(i).unwrap();
            let key_bytes = cell.key();
            let key_display = format_key(key_bytes);
            // Decode value via CellReader
            if let Some(mut reader) = CellReader::new(pager, page_idx, i) {
                let values = reader.decode_as_array();
                let vals: String = values.iter().map(|v| format!("{v}")).collect::<Vec<_>>().join(", ");
                format!("<c{i}>{key_display}: {vals}")
            } else {
                format!("<c{i}>{key_display}: (overflow)")
            }
        }).collect();
        let label = cells.join("|");
        writeln!(output, "[label=\"{label}\"]")?;
    }
    Ok(())
}
```

**Key formatting** — table and index B-trees use different key encodings; the graph must know which kind each subgraph is:

*Table B-tree keys* — plain big-endian `u64` rowid (encoded with `encode_u64_key`):

```rust
fn format_table_key(bytes: &[u8]) -> String {
    if bytes.len() == 8 {
        return decode_u64_key(bytes).to_string();
    }
    format!("0x{}", bytes.iter().map(|b| format!("{b:02x}")).collect::<String>())
}
```

*Index B-tree keys* — self-describing composite: N tagged column segments followed by an 8-byte `u64` rowid. Each column segment starts with a type tag that determines its length:

| Tag | Type | Consumed bytes |
|-----|------|----------------|
| `0x00` | NULL | 0 (tag only) |
| `0x01` | INTEGER | tag + 8 (sign-flipped big-endian i64) |
| `0x02` | REAL | tag + 8 (IEEE 754 sortable encoding) |
| `0x03` | TEXT | tag + UTF-8 bytes + `0x00` NUL terminator |

The key is fully self-describing so no schema is needed to parse it. After consuming all column segments, the remaining 8 bytes are the rowid.

```rust
fn format_index_key(bytes: &[u8], col_names: &[String]) -> String {
    let mut pos = 0;
    let mut parts: Vec<String> = Vec::new();

    for name in col_names {
        if pos >= bytes.len() { break; }
        let tag = bytes[pos]; pos += 1;
        let val = match tag {
            0x00 => { "NULL".to_string() }
            0x01 => {
                let v = decode_integer_key(&bytes[pos..pos+8]); pos += 8;
                format!("{name}={v}")
            }
            0x02 => {
                // reverse the sortable IEEE 754 encoding
                let bits = u64::from_be_bytes(bytes[pos..pos+8].try_into().unwrap());
                pos += 8;
                let bits = if bits >> 63 == 1 { bits ^ 0x8000_0000_0000_0000 }
                           else { bits ^ 0xFFFF_FFFF_FFFF_FFFF };
                format!("{name}={}", f64::from_bits(bits))
            }
            0x03 => {
                let end = bytes[pos..].iter().position(|&b| b == 0x00).unwrap_or(bytes.len() - pos);
                let s = std::str::from_utf8(&bytes[pos..pos+end]).unwrap_or("?");
                pos += end + 1; // skip NUL
                format!("{name}={s:?}")
            }
            _ => { format!("{name}=?") }
        };
        parts.push(val);
    }

    // trailing 8 bytes = rowid
    let rowid = if pos + 8 <= bytes.len() {
        decode_u64_key(&bytes[pos..pos+8])
    } else { 0 };
    parts.push(format!("rowid={rowid}"));
    parts.join(", ")
}
```

The `write_subgraph` function receives a `TreeKind` enum:

```rust
enum TreeKind<'a> {
    Table,
    Index { col_names: &'a [String] },
}
```

and dispatches to the appropriate key formatter when rendering interior separator keys and leaf cell keys.

**Overflow pages** — render as a rounded rectangle linked with a dashed edge from the leaf cell:

```
node_5_c2_overflow [label="overflow\n(N bytes)" shape=ellipse style=dashed]
node_5:c2 -> node_5_c2_overflow [style=dashed]
```

### Key Files

- `src/storage/btree_graph.rs` — full rewrite
- `src/storage/btree.rs` — change `Display::fmt` to call `btree_graph::dump(f, self)` passing `&BTree`

### Tests

No automated tests (graph output is visual). Verify manually:

```bash
cargo run -- test.db btree dump /tmp/test.dot
dot -Tsvg /tmp/test.dot -o /tmp/test.svg
# Open test.svg — should show catalog subgraph and user table subgraphs with decoded cell values
make test.svg   # should still work
```

Also verify an empty database (only catalog) produces a valid `.dot` file.

### Implementation Steps (2 commits)

#### Step 101.1 — Rewrite `btree_graph.rs` with tree-walk and decoded cell values

Implement `write_subgraph`, `write_interior_node`, `write_leaf_node`, `write_overflow_node`. Update `BTree::fmt` to pass `self`. Remove all commented-out code.

**Commit:** `Feature: overhaul btree_graph dot exporter with per-tree subgraphs and decoded cells`

#### Step 101.2 — Add overflow page stub rendering

Detect leaf cells whose inline value indicates overflow; emit dashed overflow stub nodes.

**Commit:** `Feature: btree dot exporter — render overflow page stubs`

---

## 102. README: demo GIF of engine REPL (Track 7)

### What Changes

Add an animated GIF to `README.md` showing the engine REPL in action (colorized bytecode listing). Follow the existing VHS infrastructure in `doc/screenshots/`:

1. Add `doc/screenshots/repl-engine.tape` — VHS script that records the session
2. Add `doc/screenshots/repl-engine.gif` — committed GIF output
3. Wire the tape into the `screenshots` Makefile target
4. Embed the GIF in `README.md`
5. Add `doc/screenshots/README.md` explaining the whole screenshots system

### Background

The project already has a `screenshots` Makefile target that uses [VHS](https://github.com/charmbracelet/vhs) to regenerate GIFs from tape scripts. The tapes live in `doc/screenshots/` alongside their output GIFs. Two tapes already exist: `repl-sql.tape` and `repl-index.tape`. The new engine tape slots in alongside them.

The bytecode listing uses `colored` crate ANSI escapes and is visually distinctive — a good addition to the README's "wow factor".

### Implementation Approach

**VHS tape** (`doc/screenshots/repl-engine.tape`):

```
Output doc/screenshots/repl-engine.gif

Set FontSize 14
Set Width 900
Set Height 480
Set Theme "Catppuccin Mocha"
Env PATH "/home/james/gits/database/.bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

Type "db demo-preloaded.db"
Enter
Sleep 1500ms

Type "enter engine"
Enter
Sleep 400ms

Type "compile SELECT id, name FROM users WHERE age > 27 ORDER BY name"
Enter
Sleep 500ms

Type "program"
Enter
Sleep 4s
```

(Uses `demo-preloaded.db` which the `screenshots` target already creates with the `users` table populated.)

**Makefile** — extend the `screenshots` target to also run the new tape:

```makefile
screenshots: $(PROG)
	rm -f doc/screenshots/demo.db doc/screenshots/demo-preloaded.db
	$(PROG) doc/screenshots/demo-preloaded.db sql "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)"
	$(PROG) doc/screenshots/demo-preloaded.db sql "INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 35), (4, 'dave', 28), (5, 'eve', 22), (6, 'frank', 31)"
	python3 doc/screenshots/check.py
	cp doc/screenshots/demo-preloaded.db demo-preloaded.db
	rm -f demo.db
	PATH="$(PWD)/.bin:$(PATH)" vhs doc/screenshots/repl-sql.tape
	PATH="$(PWD)/.bin:$(PATH)" vhs doc/screenshots/repl-index.tape
	PATH="$(PWD)/.bin:$(PATH)" vhs doc/screenshots/repl-engine.tape
	rm -f demo.db demo-preloaded.db doc/screenshots/demo.db doc/screenshots/demo-preloaded.db
```

**`doc/screenshots/README.md`** — explains the system:

```markdown
# Screenshots

Animated GIFs embedded in the README are generated reproducibly from
[VHS](https://github.com/charmbracelet/vhs) tape scripts.

## Regenerating

```bash
make screenshots
```

This rebuilds all GIFs. Requires `vhs` on PATH (or in `.bin/`).

## Files

| Tape | GIF | Shows |
|------|-----|-------|
| `repl-sql.tape` | `repl-sql.gif` | SQL mode: CREATE TABLE, INSERT, SELECT with WHERE and ORDER BY |
| `repl-index.tape` | `repl-index.gif` | Index mode: CREATE INDEX, indexed query |
| `repl-engine.tape` | `repl-engine.gif` | Engine mode: compile SQL → colorized bytecode listing |

## Adding a new GIF

1. Write a `.tape` file in this directory
2. Run `vhs <your>.tape` to test it
3. Add a line to the `screenshots` target in `Makefile`
4. Embed the output GIF in `README.md`
5. Commit both the tape and the GIF

## Dependencies

- `vhs` binary — place in `.bin/` or ensure it is on PATH
- `check.py` — validates that the demo database was created correctly before recording
```

**README placement** — after the existing GIFs in the "Interactive CLI" section, add:

```markdown
### Bytecode Compilation

![engine REPL bytecode listing](doc/screenshots/repl-engine.gif)
```

### Key Files

- `doc/screenshots/repl-engine.tape` — VHS recording script
- `doc/screenshots/repl-engine.gif` — committed GIF
- `doc/screenshots/README.md` — new: documents the screenshots system
- `Makefile` — add `vhs repl-engine.tape` line to `screenshots` target
- `README.md` — embed the GIF

### Tests

None — visual change. Verify `make screenshots` completes without error and the GIF renders on GitHub.

### Implementation Steps (3 commits)

#### Step 102.1 — Add `doc/screenshots/README.md`

Document the VHS tape system, file listing, and instructions for adding new GIFs.

**Commit:** `Docs: add screenshots/README.md explaining VHS tape system`

#### Step 102.2 — Add engine tape, record GIF, wire into Makefile

Write `repl-engine.tape`, run `vhs` to produce `repl-engine.gif`, extend `screenshots` Makefile target with the new tape line.

**Commit:** `Docs: add engine REPL demo tape and GIF; wire into screenshots target`

#### Step 102.3 — Embed GIF in README

Add the GIF reference and caption to `README.md`.

**Commit:** `Docs: embed engine REPL bytecode listing GIF in README`

---

## Verification

- [ ] `cargo test` — all tests pass
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` — zero warnings
- [ ] `IndexWithCursor` refactor: test count unchanged, no logic changes
- [ ] Engine REPL: `compile` → `step` → `step` → `step` shows each instruction then "halted"
- [ ] Engine REPL: `compile` → `run` prints each yielded row (for non-BTree programs)
- [ ] `cargo run -- test.db btree dump /tmp/out.dot && dot -Tsvg /tmp/out.dot -o /tmp/out.svg` — produces valid SVG with labeled subgraphs and decoded cell values
- [ ] `make test.svg` still works end-to-end
- [ ] README GIF renders in GitHub preview
- [ ] Each commit is independently testable

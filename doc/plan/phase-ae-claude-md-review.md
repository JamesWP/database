# Phase AE — CLAUDE.md Review

Documentation-only phase: verify every claim in CLAUDE.md against the actual source files and correct what is wrong or missing.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 107 | 7.1 | Fix architecture diagram and Planner layer description | — |
| 108 | 7.2 | Add missing Compiler layer; fix Engine layer description; add explain.rs | — |
| 109 | 7.3 | Fix REPL layer: add shared.rs; add sql mode; fix planner/engine command lists | — |
| 110 | 7.4 | Fix test organisation section; update test count in doc/plan/README.md; reframe CBOR section | — |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

CLAUDE.md has drifted from the codebase. The architecture diagram omits the Compiler layer entirely. The Planner is described as a single file with old node names. The `sql` REPL mode is undocumented. SQL test organisation references `.expected` files that no longer exist. Several sections carry historical framing ("V1 Restriction", "After CBOR Migration") that is now misleading.

This phase makes no code changes — only documentation edits, each verified against the source.

---

## 107. Architecture: diagram and Planner layer (Track 7.1)

### What Changes

- Pipeline diagram gains the missing `Compiler` step.
- Planner layer description updated: single file → directory, old node names → actual names.

### Current vs. Correct

**Pipeline diagram — current:**
```
SQL Input → Frontend (Lexer/Parser/AST) → Planner → Engine (VM) → Storage (BTree/Pager)
```

**Correct:**
```
SQL Input → Frontend (Lexer/Parser/AST) → Planner → Compiler → Engine (VM) → Storage (BTree/Pager)
```

**Planner layer — current:**
> `src/planner.rs` — Converts AST to query execution plans (TableScan, Select nodes)

**Correct:** The planner is a directory `src/planner/` with these files:
- `mod.rs` — `LogicalPlan` enum, `plan()` entry point, `PlanError`
- `select.rs` — SELECT planning
- `dml.rs` — INSERT, UPDATE, DELETE planning
- `ddl.rs` — CREATE TABLE / CREATE INDEX / DROP planning
- `optimizer.rs` — Optimisation passes (`optimize`, `fuse_projects`)
- `resolver.rs` — Column and expression resolution
- `schema.rs` — Schema types

Actual plan node names (not "TableScan, Select"): `Scan`, `Filter`, `Project`, `Sort`, `Limit`, `Count`, `Aggregate`, `Distinct`, `Values`, `Sequence`, `IndexScan`, `RowidLookup`, `Join`, `IndexJoin`, `Insert`, `Update`, `Delete`, `PopulateIndex`.

### Key Files

- `CLAUDE.md` — Architecture section
- `src/planner/mod.rs` — source of truth for node names and structure

### Implementation Steps (1 commit)

#### Step 107.1 — Fix pipeline diagram and Planner layer description

**Commit:** `docs: fix architecture diagram and Planner layer description in CLAUDE.md`

---

## 108. Add Compiler layer; fix Engine layer; add explain.rs (Track 7.2)

### What Changes

- Add `Compiler` (`src/compiler/`) as a named layer with its files listed.
- Fix `ScalarValue` variant list: currently says "int, float, bool"; actually Integer, Floating, Boolean, String, Blob, Null.
- Add `src/explain.rs` to the architecture overview — it is a public top-level module that is not mentioned anywhere.

### Compiler layer (missing entirely)

**Compiler** (`src/compiler/`): Converts `LogicalPlan` to bytecode programs.
- `mod.rs` — `compile()` entry point; `body!` / `init!` macros for ergonomic emit
- `nodes.rs` — Per-node codegen functions (`codegen_scan`, `codegen_filter`, …)
- `emitter.rs` — `BytecodeEmitter` with label and forward-jump support
- `expr.rs` — Expression compilation
- `registers.rs` — `RegisterAllocator` for VM register assignment

### Engine layer fix

Current description of `scalarvalue.rs`: "Scalar value types (int, float, bool)"

Correct: `ScalarValue` has six variants — `Integer(i64)`, `Floating(f64)`, `Boolean(bool)`, `String(String)`, `Blob(Vec<u8>)`, `Null`.

### explain.rs (missing)

Add brief mention: **Explain** (`src/explain.rs`) — walks a `LogicalPlan` tree and produces `(id, indented_text)` rows for `EXPLAIN` output, resolving column indices to names via `ExplainSchema`.

### Key Files

- `CLAUDE.md` — Architecture > Layers section
- `src/compiler/mod.rs` — source of truth
- `src/engine/scalarvalue.rs` — variant list
- `src/explain.rs` — public module

### Implementation Steps (1 commit)

#### Step 108.1 — Add Compiler layer, fix Engine description, add explain.rs

**Commit:** `docs: add Compiler layer, fix ScalarValue variants, add explain.rs to CLAUDE.md`

---

## 109. REPL: add shared.rs, sql mode, fix command lists (Track 7.3)

### What Changes

- Add `shared.rs` to the REPL layer file list.
- Add the `sql` mode (currently undocumented) to the Interactive CLI section.
- Add the `last` command to the planner mode.
- Add `show`/`list` aliases and `clear`/`reset` command to the engine mode.

### REPL layer fix

Current: lists `mod.rs`, `mode.rs`, `modes/`. Missing: `shared.rs` — holds `SharedState` (the `BTree` handle and `db_path` shared across all modes).

### sql mode (missing entirely)

There are five REPL modes; CLAUDE.md documents four. Add:

**sql** — Execute SQL statements directly
```
sql> CREATE TABLE users (id INTEGER, name TEXT)
sql> INSERT INTO users VALUES (1, 'alice')
sql> SELECT id, name FROM users
sql> EXPLAIN SELECT * FROM users WHERE id = 1
```

Also accessible non-interactively: `cargo run -- test.db sql "SELECT * FROM users"`

### planner mode — add `last` command

```
planner> last     # Re-display the plan for the last compiled query
```

### engine mode — add aliases and `clear`

```
engine> show / list     # Aliases for `program`
engine> clear / reset   # Discard the current compiled program
```

### Key Files

- `CLAUDE.md` — Interactive CLI section
- `src/repl/shared.rs` — SharedState
- `src/repl/modes/` — source of truth for commands

### Implementation Steps (1 commit)

#### Step 109.1 — Fix REPL documentation: shared.rs, sql mode, planner/engine commands

**Commit:** `docs: add sql mode, shared.rs, and missing REPL commands to CLAUDE.md`

---

## 110. Test organisation, test count, CBOR section (Track 7.4)

### What Changes

- Remove reference to `.expected` files from the test organisation section.
- Update the test count in `doc/plan/README.md` (says 161; actual is ~327).
- Reframe the "After CBOR Migration (Phase F)" debugging section — CBOR is now standard, not a migration state.

### Test organisation fix

**Current:** "Automated tests in `tests/sql/*.sql` with `.expected` files"

**Correct:** No `.expected` files exist. Expected output is embedded as `-- >` comment lines directly in the `.sql` files. Remove the `.expected` reference.

### Test count in doc/plan/README.md

The table header says "Current Test Coverage (161 tests)". Running `cargo test` now shows ~327 tests. Update the count and the per-subsystem table to reflect current reality (re-run `cargo test -- --list | wc -l` to get the precise count at implementation time).

### CBOR section reframe

The section heading "After CBOR Migration (Phase F)" treats CBOR as a historical transition. Rename it to "Cell Data Format" and drop the "Phase F" reference. Keep the useful debugging facts (CBOR-encoded `Vec<ScalarValue>`, `decoded=[...]` display, overflow chains).

### Key Files

- `CLAUDE.md` — Build & Test, Debugging sections
- `doc/plan/README.md` — test count table

### Implementation Steps (1 commit)

#### Step 110.1 — Fix test organisation, test count, and CBOR section framing

**Commit:** `docs: fix test organisation, update test count, reframe CBOR section in CLAUDE.md`

---

## Verification

- [ ] `cargo test` — all tests pass (no code changes, but confirm nothing broken)
- [ ] Each CLAUDE.md claim verified against source before committing
- [ ] No `.expected` file references remain in CLAUDE.md
- [ ] Architecture diagram matches the actual pipeline
- [ ] All five REPL modes documented

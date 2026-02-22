# Phase M — Register Consolidation

Simplify the VM's non-scalar register types by unifying RowBuffer's dual iteration modes and absorbing KeyList into RowBuffer, eliminating one register type and four operations.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 58 | 5 | Unify RowBuffer iteration: remove `YieldFromRowBuffer` | — |
| 59 | 5 | Absorb `KeyList` into `RowBuffer` | 58 |
| 60 | 5 | Post-consolidation cleanup | 59 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

The VM currently has three non-scalar register types and nine associated operations. Two of those types overlap in structure, and one of those types has internally inconsistent semantics.

### The two problems

**Problem 1 — RowBuffer has dual, incompatible iteration modes.**

`RowBuffer` is used in two completely different ways:

| Use case | Pattern |
|----------|---------|
| Sort     | `SortRowBuffer` (sorts **and reverses**) → `YieldFromRowBuffer` (destructive pop from end) |
| Join     | `RewindRowBuffer` → `NextFromRowBuffer` (non-destructive cursor advance) |

The reverse-then-pop trick exists solely so that `YieldFromRowBuffer` can pop cheaply. It is an implementation detail that leaks into the opcode design: `SortRowBuffer` has a hidden side-effect (reversal), and `YieldFromRowBuffer` is a weaker version of `NextFromRowBuffer` that happens to be destructive.

**Fix**: Make `SortRowBuffer` sort in ascending order only (no reverse). Replace `YieldFromRowBuffer` with `NextFromRowBuffer` in the Sort codegen path. Both Sort and Join now use the same cursor-based iteration.

**Problem 2 — KeyList is a single-column integer RowBuffer.**

`KeyList` (`Vec<u64>`) exists purely so DELETE can collect rowids in a first pass and delete them in a second pass (avoiding mutation during iteration). Its three operations (`InitKeyList`, `AppendKey`, `PopKey`) duplicate the semantics of `InitRowBuffer`, `AppendToRowBuffer`, `NextFromRowBuffer` for the special case of one `INTEGER` column.

**Fix**: Replace `KeyList` with a single-column `RowBuffer`. After item 58, the replacement for `PopKey` is already `NextFromRowBuffer`, so no additional iteration op is needed.

### Net reduction

| Before | After |
|--------|-------|
| 3 non-scalar types | 2 non-scalar types |
| `InitKeyList`, `AppendKey`, `PopKey`, `YieldFromRowBuffer` | removed (−4 ops) |
| `SortRowBuffer` reverses output | `SortRowBuffer` sorts only |
| Total RowBuffer ops: 6 | Total RowBuffer ops: 4 (`Init`, `Append`, `Sort`, `Rewind`, `Next`) |

---

## 58. Unify RowBuffer Iteration (Track 5)

### What Changes

- `SortRowBuffer` sorts rows in ascending order **without reversing**.
- `YieldFromRowBuffer` is removed.
- The Sort codegen in `compiler/nodes.rs` switches from `YieldFromRowBuffer` to `NextFromRowBuffer` (with a preceding `RewindRowBuffer` to position the cursor at 0 after sorting).
- Unit tests for the Sort path are updated.

### Background

`SortRowBuffer` currently calls `sort_by` then `reverse()`, producing a descending-by-pop-order vector so that `YieldFromRowBuffer` can `pop()` from the end in O(1) and get items in ascending order. This is a micro-optimisation that complicates every other reader of the code.

`NextFromRowBuffer` already advances `cursor` and returns the row at that position without removing it. After a sort, reading via cursor in index order is identical in semantics to the old pop-from-reversed-vec approach, with negligible cost difference (index access vs pop).

### Implementation Approach

**`src/engine.rs`** — remove the `reverse()` call from `SortRowBuffer`, remove the `YieldFromRowBuffer` match arm.

**`src/engine/program.rs`** — remove `YieldFromRowBuffer` variant from `Operation`.

**`src/compiler/nodes.rs`** — Sort codegen currently:

```rust
// before
ctx.body_emitter.emit(Operation::SortRowBuffer(buffer_reg, sort_key_specs));
ctx.body_emitter.emit(Operation::YieldFromRowBuffer(output_regs, buffer_reg, on_done));
ctx.body_emitter.emit(Operation::GoTo(...back to YieldFromRowBuffer...));
```

Becomes:

```rust
// after
ctx.body_emitter.emit(Operation::SortRowBuffer(buffer_reg, sort_key_specs));
ctx.body_emitter.emit(Operation::RewindRowBuffer(buffer_reg));  // cursor = 0
// loop:
ctx.body_emitter.emit(Operation::NextFromRowBuffer(output_regs, buffer_reg, on_done));
ctx.body_emitter.emit(Operation::GoTo(...back to NextFromRowBuffer...));
```

**`src/compiler/emitter.rs`** — remove `YieldFromRowBuffer` from the jump-resolution match arm and from the no-jump-target exhaustive arm.

### Key Files

- `src/engine.rs` — remove `reverse()` from `SortRowBuffer`, remove `YieldFromRowBuffer` arm
- `src/engine/program.rs` — remove `YieldFromRowBuffer` variant and its `Display` arm
- `src/compiler/nodes.rs` — update Sort codegen (`compile_sort`)
- `src/compiler/emitter.rs` — remove `YieldFromRowBuffer` from jump resolution

### Tests

The existing engine unit tests for Sort (`test_sort_row_buffer`, `test_sort_multi_column_row_buffer`) use `YieldFromRowBuffer` directly. Update them to use `RewindRowBuffer` + `NextFromRowBuffer` and verify sort order is preserved.

All SQL sort tests (`cargo test test_sql_order_by`) must continue to pass.

### Implementation Steps (2 commits)

#### Step 58.1 — Engine: remove reverse from SortRowBuffer, drop YieldFromRowBuffer

Remove `reverse()` from the `SortRowBuffer` handler. Remove the `YieldFromRowBuffer` match arm. Remove the variant from `Operation`. Remove it from the emitter's jump resolution. Update engine unit tests.

**Commit:** Engine: remove YieldFromRowBuffer; SortRowBuffer no longer reverses

#### Step 58.2 — Compiler: update Sort codegen to use NextFromRowBuffer

Update `compile_sort` in `compiler/nodes.rs` to emit `RewindRowBuffer` + `NextFromRowBuffer` instead of `YieldFromRowBuffer`. Run full test suite.

**Commit:** Compiler: Sort uses RewindRowBuffer + NextFromRowBuffer

---

## 59. Absorb KeyList into RowBuffer (Track 5)

### What Changes

- `RegisterValue::KeyList` is removed.
- `InitKeyList`, `AppendKey`, `PopKey` operations are removed.
- DELETE codegen (`compile_delete`) replaces these with `InitRowBuffer`, `AppendToRowBuffer` (1-element row), `NextFromRowBuffer`.
- The `key_list_mut` accessor on `RegisterValue` is removed.

### Background

DELETE uses a two-phase approach to avoid mutating the B-tree while a cursor is open on it:

```
Phase 1: scan table, collect rowids → KeyList
Phase 2: pop rowids from KeyList, delete each one
```

`KeyList` is structurally `Vec<u64>`, with `AppendKey` pushing and `PopKey` popping. After item 58, `RowBuffer` + `NextFromRowBuffer` provides identical semantics for a 1-column `INTEGER` buffer. The rowid is stored as `ScalarValue::Integer(rowid as i64)` (matching how `ReadCurrentKey` already stores it), and extracted back with `.integer()`.

There are two DELETE call sites in `compiler/nodes.rs` (plain DELETE and DELETE with a sub-plan). Both are updated the same way.

### Implementation Approach

**`src/engine/registers.rs`**:
- Remove `KeyList(Vec<u64>)` from `RegisterValue`
- Remove `key_list_mut()` accessor

**`src/engine/program.rs`**:
- Remove `InitKeyList(Reg)`, `AppendKey(Reg, Reg)`, `PopKey(Reg, Reg, JumpTarget)` from `Operation`
- Remove their `Display` arms

**`src/engine.rs`**:
- Remove the three match arms

**`src/compiler/nodes.rs`** — DELETE phase 1 (key collection):

```rust
// before
ctx.init_emitter.emit(Operation::InitKeyList(key_list_reg));
// ...in scan body:
ctx.body_emitter.emit(Operation::AppendKey(key_list_reg, key_reg));
```

```rust
// after — key_reg holds ScalarValue::Integer (rowid)
ctx.init_emitter.emit(Operation::InitRowBuffer(key_list_reg));
// ...in scan body:
ctx.body_emitter.emit(Operation::AppendToRowBuffer(key_list_reg, vec![key_reg]));
```

DELETE phase 2 (deletion loop):

```rust
// before
// loop:
Operation::PopKey(r_key, key_list_reg, done)
// delete using r_key
```

```rust
// after
Operation::RewindRowBuffer(key_list_reg),
// loop:
Operation::NextFromRowBuffer(vec![r_key], key_list_reg, done),
// delete using r_key
```

**`src/compiler/emitter.rs`** — remove `PopKey` from jump resolution; remove `InitKeyList` and `AppendKey` from the no-jump exhaustive arm.

### Key Files

- `src/engine/registers.rs` — remove `KeyList` variant and `key_list_mut`
- `src/engine/program.rs` — remove `InitKeyList`, `AppendKey`, `PopKey`
- `src/engine.rs` — remove three match arms; remove engine unit tests that use KeyList directly
- `src/compiler/nodes.rs` — update both DELETE call sites
- `src/compiler/emitter.rs` — remove from jump resolution

### Tests

Engine unit tests for the KeyList operations (`test_key_list`, `test_key_list_with_delete`) are removed (they test removed ops). Coverage is maintained by the SQL DELETE integration tests (`cargo test test_sql_delete`).

### Implementation Steps (2 commits)

#### Step 59.1 — Remove KeyList from registers, program, and engine

Remove the type, the three operations, and the engine match arms. Remove engine unit tests that directly construct KeyList programs.

**Commit:** Remove KeyList register type and associated operations

#### Step 59.2 — Compiler: DELETE uses RowBuffer

Update both DELETE codegen paths in `compile_delete` to use `InitRowBuffer`, `AppendToRowBuffer`, `RewindRowBuffer`, `NextFromRowBuffer`. Run `cargo test test_sql_delete` and full suite.

**Commit:** Compiler: DELETE uses RowBuffer instead of KeyList

---

## 60. Post-Consolidation Cleanup (Track 5)

### What Changes

Minor cleanup made possible by the consolidation:

1. **`RowBuffer` cursor field docs**: Update the struct-level comment to reflect that cursor is always used (no more destructive mode).
2. **`SortRowBuffer` comment in engine**: Remove any mention of "reverse" from comments/doc strings.
3. **`compile_sort` comment in nodes.rs**: Update the codegen comment block that describes the Sort bytecode pattern.
4. **`NextFromRowBuffer` comment in engine**: Note it is used for both Sort and Join iteration, and also for the DELETE key-drain pattern.
5. **Zero warnings check**: Ensure no dead `allow(dead_code)` annotations remain on removed accessors.

### Key Files

- `src/engine/registers.rs` — update `RowBuffer` doc comment
- `src/engine.rs` — update `SortRowBuffer` and `NextFromRowBuffer` handler comments
- `src/compiler/nodes.rs` — update Sort and Delete codegen comment blocks

### Tests

`cargo fmt && cargo build 2>&1 | grep -i warning` — zero warnings.

### Implementation Steps (1 commit)

#### Step 60.1 — Update comments and verify zero warnings

**Commit:** Cleanup: update comments after register consolidation

---

## Verification

- [ ] Tests pass: `cargo test`
- [ ] SQL sort tests: `cargo test test_sql_order_by`
- [ ] SQL delete tests: `cargo test test_sql_delete`
- [ ] SQL join tests: `cargo test test_sql_join`
- [ ] Zero warnings: `cargo fmt && cargo build 2>&1 | grep -i warning`
- [ ] Each commit is independently testable
- [ ] `Operation` enum has no `YieldFromRowBuffer`, `InitKeyList`, `AppendKey`, `PopKey` variants
- [ ] `RegisterValue` enum has no `KeyList` variant

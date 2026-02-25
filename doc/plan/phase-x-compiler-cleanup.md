# Phase X — Compiler Index Codegen Cleanup

Extract three shared helpers from `src/compiler/nodes.rs` to remove duplicated index-cursor boilerplate in `codegen_insert`, `codegen_delete`, and `codegen_update`.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 94 | 4 | Compiler: extract `open_index_cursors` / `emit_write_indexes` / `emit_delete_indexes` helpers | Phase U |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

After Phase U (UPDATE index maintenance), three codegen functions contain nearly identical blocks for opening index cursors and emitting `WriteIndex` / `DeleteIndex` operations. Extracting three small helpers removes the duplication and makes the pattern explicit.

This phase is independent of Phase W — it touches only `src/compiler/nodes.rs`.

---

## 94. Extract index cursor codegen helpers (Track 4)

### What Changes

Three codegen functions share two identical patterns:

**Pattern A — open index cursors** (in init phase):
```rust
// codegen_insert lines ~1131–1136
// codegen_delete lines ~1563–1570
// codegen_update (Phase U)
let mut index_cursor_regs = Vec::new();
for index in indexes {
    let reg = ctx.registers.alloc();
    ctx.init_emitter.emit(Operation::Open(reg, index.rootpage));
    index_cursor_regs.push(reg);
}
```

**Pattern B — emit WriteIndex / DeleteIndex** (in body phase):
```rust
for (i, index) in indexes.iter().enumerate() {
    let col_regs: Vec<_> = index.column_idxs.iter().map(|&c| row_regs[c]).collect();
    ctx.body_emitter.emit(Operation::WriteIndex(index_cursor_regs[i], col_regs, key_reg));
}
```

### Implementation Approach

Add three private helpers near the top of `src/compiler/nodes.rs` (before `codegen_insert`):

```rust
fn open_index_cursors(indexes: &[IndexMaintenanceInfo], ctx: &mut CodegenContext) -> Vec<Reg> {
    indexes.iter().map(|index| {
        let reg = ctx.registers.alloc();
        ctx.init_emitter.emit(Operation::Open(reg, index.rootpage));
        reg
    }).collect()
}

fn emit_write_indexes(
    indexes: &[IndexMaintenanceInfo],
    cursor_regs: &[Reg],
    row_regs: &[Reg],
    key_reg: Reg,
    ctx: &mut CodegenContext,
) {
    for (index, &cursor_reg) in indexes.iter().zip(cursor_regs) {
        let col_regs: Vec<Reg> = index.column_idxs.iter().map(|&c| row_regs[c]).collect();
        ctx.body_emitter.emit(Operation::WriteIndex(cursor_reg, col_regs, key_reg));
    }
}

fn emit_delete_indexes(
    indexes: &[IndexMaintenanceInfo],
    cursor_regs: &[Reg],
    row_regs: &[Reg],
    key_reg: Reg,
    ctx: &mut CodegenContext,
) {
    for (index, &cursor_reg) in indexes.iter().zip(cursor_regs) {
        let col_regs: Vec<Reg> = index.column_idxs.iter().map(|&c| row_regs[c]).collect();
        ctx.body_emitter.emit(Operation::DeleteIndex(cursor_reg, col_regs, key_reg));
    }
}
```

Replace inline blocks at each call site:

```rust
// codegen_insert
let index_cursor_regs = open_index_cursors(indexes, ctx);
// …
emit_write_indexes(indexes, &index_cursor_regs, &reordered_regs, key_reg, ctx);

// codegen_delete
let index_cursor_regs = open_index_cursors(indexes, ctx);
// …
emit_delete_indexes(indexes, &index_cursor_regs, &phase2_read_regs, key_reg, ctx);

// codegen_update (Phase U)
let index_cursor_regs = open_index_cursors(indexes, ctx);
// … after ReadCursor (old values):
emit_delete_indexes(indexes, &index_cursor_regs, &read_regs, key_reg, ctx);
// … after applying assignments:
emit_write_indexes(indexes, &index_cursor_regs, &new_values, key_reg, ctx);
```

### Key Files

- `src/compiler/nodes.rs` — three new helpers; `codegen_insert`, `codegen_delete`, `codegen_update` simplified

### Tests

All existing compiler and SQL integration tests cover these paths. No new tests needed.
Verify: `cargo test test_sql_index`, `cargo test test_sql_delete`, `cargo test test_sql_update`.

### Implementation Steps (1 commit)

#### Step 94.1 — Extract open_index_cursors / emit_write_indexes / emit_delete_indexes

**Commit:** Compiler: extract index cursor codegen helpers; remove duplication in insert/delete/update

---

## Verification

- [ ] Tests pass: `cargo test`
- [ ] Zero warnings: `cargo fmt && cargo build 2>&1 | grep -i warning`
- [ ] `open_index_cursors`, `emit_write_indexes`, `emit_delete_indexes` exist in `nodes.rs`
- [ ] No inline equivalents remain in the three codegen functions
- [ ] All index-related SQL tests pass: `cargo test test_sql_index`

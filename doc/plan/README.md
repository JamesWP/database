# Database Development Roadmap

## Context

The database currently supports CREATE TABLE, INSERT, and basic SELECT (with WHERE and LIMIT). The storage layer uses JSON serialization throughout and has no delete support. This roadmap organizes next features into 7 parallel tracks, each with small incremental steps.

## Tracks

### Track 1: SQL Language Coverage
Expanding the SQL dialect — SELECT \*, DELETE, UPDATE, DROP, ORDER BY, aggregates, DISTINCT.

### Track 2: Data Model
NULL support, IS NULL/IS NOT NULL, DEFAULT values, type affinity/coercion.

### Track 3: Storage & Serialization
B-tree delete, binary cell/page/record formats, page cache.

### Track 4: Query Capabilities
Expression functions, LIKE, CREATE INDEX, index scans, JOINs.

### Track 5: Overhaul — Cursor API
Fix CellReader unsafe pointer, complete cursor.last(), find() return value, stack invalidation after mutations.

### Track 6: Overhaul — B-tree & Pager
Cache file handle, handle serialization errors, replace panics with Results, ZeroPage free list, split balance.

### Track 7: Testing
Fill unit test gaps (Cell/CellReader have 0 tests, Pager has 2), build automated SQL test harness, expand property-based testing.

## Phases

| Phase | Focus | Items | File | Status |
|-------|-------|-------|------|--------|
| **A** | Critical fixes & test foundations | 6 | [phase-a-critical-fixes.md](completed/phase-a-critical-fixes.md) | Completed |
| **B** | Quick SQL wins + backfill tests | 7 | [phase-b-sql-wins.md](completed/phase-b-sql-wins.md) | Completed |
| **C** | API cleanup + more SQL | 5 | [phase-c-api-cleanup.md](completed/phase-c-api-cleanup.md) | Completed |
| **D** | Core CRUD | 5 | [phase-d-core-crud.md](completed/phase-d-core-crud.md) | Completed |
| **D2** | Refinements (cursor + functions) | 3 | [phase-d2-refinements.md](completed/phase-d2-refinements.md) | Completed |
| **D3** | Cursor Stability | 1 | [phase-d3-cursor-stability.md](completed/phase-d3-cursor-stability.md) | Completed |
| **E** | Query power | 4 | [phase-e-query-power.md](completed/phase-e-query-power.md) | Completed |
| **F** | Serialization overhaul | 4 | [phase-f-serialization.md](completed/phase-f-serialization.md) | Completed |
| **G** | Advanced capabilities | 8 | [phase-g-advanced.md](completed/phase-g-advanced.md) | Completed |
| **G4** | Advanced Indexing | 8 | [phase-g4-advanced-indexing.md](completed/phase-g4-advanced-indexing.md) | Completed |
| **G2** | Performance & Testing | 4 | [phase-g2-indexing-and-perf.md](completed/phase-g2-indexing-and-perf.md) | Completed |
| **H** | Compiler Type Safety | 1 | [phase-h-compiler-safety.md](completed/phase-h-compiler-safety.md) | Completed |
| **I** | Bug Fixes | 4 | [phase-i-bug-fixes.md](completed/phase-i-bug-fixes.md) | Completed |
| **J** | Cleanup and Refactor | 3 | [phase-j-cleanup-and-refactor.md](completed/phase-j-cleanup-and-refactor.md) | Completed |
| **K** | Inline SQL Test Expected Output | 4 | [phase-k-inline-sql-tests.md](completed/phase-k-inline-sql-tests.md) | Completed |
| **L** | EXPLAIN Query Plans | 4 | [phase-l-explain.md](completed/phase-l-explain.md) | Completed |
| **M** | Register Consolidation | 3 | [phase-m-register-consolidation.md](completed/phase-m-register-consolidation.md) | Completed |
| **N** | Index Maintenance & Sort Elision | 5 | [phase-n-index-maintenance.md](completed/phase-n-index-maintenance.md) | Completed |
| **O** | HAVING Clause | 4 | [phase-o-having.md](completed/phase-o-having.md) | Completed |
| **P** | Column Names in Query Output | 3 | [phase-p-column-names.md](completed/phase-p-column-names.md) | Completed |
| **Q** | Bytecode Emit Ergonomics | 2 | [phase-q-program-macro.md](completed/phase-q-program-macro.md) | Completed |
| **R** | PRIMARY KEY & UNIQUE Constraints | 6 | [phase-r-unique-constraints.md](completed/phase-r-unique-constraints.md) | Completed |
| **S** | JavaScript / WebAssembly Bindings | 4 | [phase-s-javascript.md](phase-s-javascript.md) | Planned |
| **T** | INSERT INTO … SELECT | 3 | [phase-t-insert-select.md](completed/phase-t-insert-select.md) | Completed |
| **U** | UPDATE Index Maintenance | 3 | [phase-u-update-index-maintenance.md](completed/phase-u-update-index-maintenance.md) | Completed |
| **V** | README Overhaul | 2 | [phase-v-readme-overhaul.md](completed/phase-v-readme-overhaul.md) | Completed |
| **W** | Planner Architecture | 3 | [phase-w-planner-architecture.md](completed/phase-w-planner-architecture.md) | Completed |
| **X** | Compiler Index Codegen Cleanup | 1 | [phase-x-compiler-cleanup.md](completed/phase-x-compiler-cleanup.md) | Completed |
| **Y** | Project Node Fusion | 2 | [phase-y-project-fusion.md](completed/phase-y-project-fusion.md) | Completed |
| **Z** | Unified SELECT Planner | 2 | [phase-z-unified-select.md](phase-z-unified-select.md) | Planned |
| **AA** | Composite Index Predicate Pushdown & Sort Elision | 2 | [phase-aa-composite-index-pushdown.md](phase-aa-composite-index-pushdown.md) | Planned |
| **AB** | REPL Polish & Parallel-Slice Refactor | 4 | [phase-ab-repl-and-refactor.md](phase-ab-repl-and-refactor.md) | Planned |
| **AC** | Non-Correlated Subqueries | 4 | [phase-ac-subqueries.md](phase-ac-subqueries.md) | Planned |

## Current Test Coverage (161 tests)

| Subsystem | Tests | Assessment |
|-----------|-------|------------|
| Planner | 33 | Excellent |
| Compiler nodes | 38 | Very good |
| Engine/VM | 24 | Good |
| B-tree | 15 + 1 proptest | Reasonable |
| Parser | 9 | Moderate |
| Compiler expr | 8 | Moderate |
| Compiler emitter | 7 | Good for scope |
| Node | 7 + 2 proptests | Good |
| DB integration | 6 | Good |
| ScalarValue | 6 | Moderate |
| Compiler registers | 5 | Good for scope |
| Lexer | 4 | Minimal |
| Pager | 2 | Minimal |
| Cell / CellReader | 0 | **None** |
| REPL | 0 | **None** |

## Verification (all phases)

Each feature should follow TDD per CLAUDE.md:
- Write failing test(s) first
- Implement the feature
- Run `cargo test --bin database` to confirm all tests pass
- `cargo fmt && cargo build 2>&1 | grep warning` before committing

## Example prompt

```txt
in a new branch
implement the plan in @doc/plan/phase-b-sql-wins.md
overall plan structure in @doc/plan/README.md       
of course, follow general project guidance in @CLAUDE.md especially Git Workflow
```
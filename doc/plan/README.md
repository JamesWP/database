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

| Phase | Focus | Items | File |
|-------|-------|-------|------|
| **A** | Critical fixes & test foundations | 6 | [phase-a-critical-fixes.md](phase-a-critical-fixes.md) |
| **B** | Quick SQL wins + backfill tests | 7 | [phase-b-sql-wins.md](phase-b-sql-wins.md) |
| **C** | API cleanup + more SQL | 5 | [phase-c-api-cleanup.md](phase-c-api-cleanup.md) |
| **D** | Core CRUD | 5 | [phase-d-core-crud.md](phase-d-core-crud.md) |
| **E** | Query power | 4 | [phase-e-query-power.md](phase-e-query-power.md) |
| **F** | Serialization overhaul | 4 | [phase-f-serialization.md](phase-f-serialization.md) |
| **G** | Advanced capabilities | 8 | [phase-g-advanced.md](phase-g-advanced.md) |

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

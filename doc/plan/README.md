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
| **Z** | Unified SELECT Planner | 2 | [phase-z-unified-select.md](completed/phase-z-unified-select.md) | Completed |
| **AA** | REPL Polish & Parallel-Slice Refactor | 4 | [phase-aa-repl-and-refactor.md](completed/phase-aa-repl-and-refactor.md) | Completed |
| **AB** | Non-Correlated Subqueries | 4 | [phase-ab-subqueries.md](phase-ab-subqueries.md) | Planned |
| **AC** | Join Improvements | 6 | [phase-ac-join-improvements.md](completed/phase-ac-join-improvements.md) | Completed |
| **AD** | Page-Geometry-Aware Overflow Thresholds | 4 | [phase-ad-overflow-thresholds.md](completed/phase-ad-overflow-thresholds.md) | Completed |
| **AE** | TUI Bytecode Debugger | 3 | [phase-ae-tui-debugger.md](completed/phase-ae-tui-debugger.md) | Completed |
| **AF** | Covering Indexes | 3 | [phase-af-covering-indexes.md](completed/phase-af-covering-indexes.md) | Completed |
| **AG** | Workspace Split: Core Library + CLI Crate | 4 | [phase-ag-workspace-split.md](phase-ag-workspace-split.md) | Planned |
| **AH** | Decouple `colored` from Core Library | 4 | [phase-ah-decouple-colored.md](phase-ah-decouple-colored.md) | Planned |
| **AI** | Move `inspect_page` to CLI | 2 | [phase-ai-inspect-to-cli.md](phase-ai-inspect-to-cli.md) | Planned |
| **AJ-1** | Extract Catalog Layer from BTree | 3 | [phase-aj1-catalog-layer.md](completed/phase-aj1-catalog-layer.md) | Completed |
| **AJ** | Type System & Schema Compatibility | 8 | [phase-aj-type-system.md](completed/phase-aj-type-system.md) | Completed |
| **AK** | String Operators & Functions | — | — | Backlog |
| **AL** | CASE Expressions | — | — | Backlog |
| **AM** | LEFT / RIGHT OUTER JOIN | — | — | Backlog |
| **AN** | Views | — | — | Backlog |
| **AO** | Triggers | — | — | Backlog |
| **AP** | Aggregate Enhancements & Text Indexes | — | — | Backlog |
| **AQ** | UNION / UNION ALL / INTERSECT / EXCEPT | — | — | Backlog |
| **AR** | Date / Time Functions | — | — | Backlog |
| **AS** | Transactions | — | — | Backlog |
| **AT** | Window Functions | — | — | Backlog |
| **AU** | CTEs (Common Table Expressions) | — | — | Backlog |
| **AV** | Foreign Key Enforcement | — | — | Backlog |
| **AW** | Decoded Page Cache | 2 | [phase-aw-page-cache.md](completed/phase-aw-page-cache.md) | Completed |
| **AX** | Fast Tests by Default | 3 | [phase-ax-fast-tests.md](completed/phase-ax-fast-tests.md) | Completed |
| **AY** | Per-Query bpftrace Trace Log | 3 | [phase-ay-query-trace.md](completed/phase-ay-query-trace.md) | Completed |
| **AZ** | INSERT Performance: Rowid Cache & Fused Unique Write | 2 | [phase-az-insert-perf.md](completed/phase-az-insert-perf.md) | Completed |
| **BA** | Eliminate Page Clones | 3 | [phase-ba-arc-page-cache.md](completed/phase-ba-arc-page-cache.md) | Superseded by BB |
| **BB** | Storage Layer Redesign: NodePageStore | 5 | [phase-bb-node-page-store.md](completed/phase-bb-node-page-store.md) | Completed |
| **BC** | Lexer Performance | 2 | [phase-bc-lexer-perf.md](completed/phase-bc-lexer-perf.md) | Completed |
## Sakila Compatibility

Phases AJ–AV are ordered to progressively support the [sqlite-sakila-db](https://github.com/jOOQ/sakila/tree/main/sqlite-sakila-db) benchmark schema. Each phase unlocks more of the schema/data/views:

| After Phase | Sakila milestone |
|-------------|-----------------|
| AJ | All 18 CREATE TABLEs execute; INSERT data loads |
| AK + AL + AM | View SELECT bodies work as standalone queries |
| AN | All 5 views queryable (customer_list, film_list, staff_list, sales_by_store, sales_by_film_category) |
| AB + AN | IN/subquery patterns + views |
| AO | Full schema loads without stripping triggers |
| AP | Text indexes optimized; GROUP_CONCAT queries |
| AQ | Set-operation queries |
| AR | Date/time expressions in triggers and queries |
| AS | Safe transactional bulk data loading |
| AT | Analytical / ranking queries |
| AU | CTE-based reporting |
| AV | Full referential integrity enforced |

## Stubbed Features

Features that are currently parsed or partially handled but not fully implemented. Updated at the end of each phase.

| Feature | Stub behaviour | Tracking |
|---------|---------------|----------|
| Partial unique-index rollback on INSERT | If a second unique index fails after a first has already been written, the first index write is not rolled back (no transaction support) | TODO phase-az |
| UPDATE unique constraint enforcement | UPDATE does not check uniqueness on the new values — a duplicate introduced by UPDATE is silently written | TODO phase-az |
| FOREIGN KEY constraints | Parsed and silently ignored (`skip_table_constraint`) | TODO phase-av |
| CHECK constraints | Parsed and silently ignored (`skip_table_constraint`) | TODO phase-av |
| NOT NULL enforcement for INSERT omitted columns | Omitted columns without a DEFAULT are filled with NULL rather than rejected | TODO phase-aj |
| Expression DEFAULTs e.g. `DEFAULT (DATETIME('now'))` | Parenthesised expression consumed and treated as no default | TODO phase-aj |

## Future

* support aggregation for joins
* multi-column joins — extend planner and JoinResolver to support multiple JOIN clauses and multi-column ON conditions (a.x = b.x AND a.y = b.y)
* perf oppertunities:
    - make the lexer faster (23% of execute for sakila inserts)
    - `database::planner::schema::resolve_table` is slow in the planner (17%)
    - `BTree`::clone is 4% in the sakila insert test (4%)
    
## Verification (all phases)

Each feature should follow TDD per CLAUDE.md:
- Write failing test(s) first
- Implement the feature
- Run `cargo test --bin database` to confirm all tests pass
- `cargo fmt && cargo build 2>&1 | grep warning` before committing

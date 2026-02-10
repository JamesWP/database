# Phase G — Advanced Capabilities

Phase G adds advanced SQL features, indexing, performance infrastructure, and expanded property-based testing.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 32 | 2.2 | IS NULL / IS NOT NULL | Phase B: NULL support |
| 33 | 4.5 | INNER JOIN | — |
| 34 | 4.3 | CREATE INDEX | — |
| 35 | 4.4 | Index scan in planner | 34 |
| 36 | 1.9 | DISTINCT | — |
| 37 | 3.5 | Page cache / buffer pool | — |
| 38 | 6.5 | Interior node split balance | — |
| 39 | 7.3 | Proptest expansion | — |

---

## 32. IS NULL / IS NOT NULL (Track 2.2)

### What Changes

Parser recognizes `expr IS NULL` and `expr IS NOT NULL`. New comparison ops in engine.

### Key Files

- `src/frontend/parser.rs` — parse IS [NOT] NULL after expression
- `src/frontend/ast.rs` — add IsNull/IsNotNull expression variants
- `src/engine/scalarvalue.rs` — implement is_null() check

### Tests

- `WHERE col IS NULL` / `WHERE col IS NOT NULL` / with inserted NULL values

---

## 33. INNER JOIN (Track 4.5)

### What Changes

Support `SELECT ... FROM a JOIN b ON condition` with nested loop join.

### Key Files

- `src/frontend/parser.rs` — parse JOIN ... ON syntax
- `src/planner.rs` — create Join plan node (partially exists)
- `src/compiler/nodes.rs` — nested loop join compilation

### Implementation Approach

Nested loop: outer scan on left table, for each row inner scan on right table, evaluate ON condition, yield matching pairs. Column references must handle two input sources.

### Tests

- INNER JOIN with ON condition / multi-column join / no matches / self-join

---

## 34. CREATE INDEX (Track 4.3)

### What Changes

New `CREATE INDEX idx ON table(col)` — creates secondary B-tree mapping index key → primary key.

### Key Files

- `src/frontend/parser.rs` — parse CREATE INDEX
- `src/frontend/ast.rs` — add CreateIndex statement
- `src/db.rs` — create index B-tree, add catalog entry
- On INSERT: also insert into index B-trees

### Tests

- Create index, verify catalog entry / insert data, verify index populated / index survives restart

---

## 35. Index Scan in Planner (Track 4.4)

### What Changes

Planner checks available indexes when planning WHERE. If WHERE matches an index, use index scan instead of full table scan.

### Key Files

- `src/planner.rs` — index-aware query planning

### Implementation Approach

When WHERE has `col = literal` and an index exists on `col`, plan an IndexScan: look up in index B-tree to get primary key, then fetch row from table by primary key.

### Tests

- Index scan faster than table scan for selective queries / correct results / planner chooses index when available

---

## 36. DISTINCT (Track 1.9)

### What Changes

New `LogicalPlan::Distinct` node. Dedup rows using HashSet or sort-based approach.

### Key Files

- `src/frontend/parser.rs` — parse SELECT DISTINCT
- `src/compiler/nodes.rs` — collect rows, dedup, yield

### Tests

- DISTINCT removes duplicates / with single column / multiple columns / all unique (no change)

---

## 37. Page Cache / Buffer Pool (Track 3.5)

### What Changes

Add in-memory page cache to Pager. Avoids re-reading pages from disk.

### Key Files

- `src/storage/pager.rs` — add `HashMap<u32, Page>` cache, dirty flags, LRU eviction

### Implementation Approach

On read: check cache first, return cached page or read from disk and cache. On write: update cache, mark dirty. On close: flush all dirty pages.

### Tests

- Cache hit avoids disk read / dirty pages flushed on close / eviction works / cache correct after mutations

---

## 38. Interior Node Split Balance (Track 6.5)

### What Changes

Fix asymmetric edge distribution in `src/storage/node.rs` lines 300-308 where `(len+1)/2` causes imbalance.

### Key Files

- `src/storage/node.rs`

### Tests

- Verify balanced split: both sides within 1 key of equal / proptest with random data

---

## 39. Proptest Expansion (Track 7.3)

### What Changes

Expand property-based testing beyond storage layer.

### New Proptests

- **Parser**: generate random valid SQL tokens, parse, verify no panic
- **Compiler**: generate random `LogicalPlan` trees, compile, verify valid bytecode (no unresolved labels, register bounds ok)
- **B-tree**: 1000+ random inserts, verify sorted order and verify() passes

### Key Files

- `src/frontend/parser.rs` — new proptest
- `src/compiler/nodes.rs` — new proptest
- `src/storage/btree.rs` — extend existing proptest

---

## Verification

For each item:
- [ ] Tests written first (TDD)
- [ ] All tests pass: `cargo test --bin database`
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning`

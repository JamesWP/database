# Phase G2 — Performance & Testing

Phase G2 focuses on query optimization, performance infrastructure, and expanded property-based testing.

**Note:** Items 34-35 (CREATE INDEX and Index Scan) have been split into their own dedicated phase: see `phase-g3-indexing.md`

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 36 | 1.9 | DISTINCT | — |
| 37 | 3.5 | Page cache / buffer pool | — |
| 38 | 6.5 | Interior node split balance | — |
| 39 | 7.3 | Proptest expansion | — |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

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

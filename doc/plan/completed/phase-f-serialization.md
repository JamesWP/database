# Phase F — Serialization Overhaul (CBOR)

Phase F replaces JSON-based serialization with CBOR (Concise Binary Object Representation), reducing storage overhead by 30-50% while maintaining Serde compatibility and simplifying implementation.

## Serialization Approach: CBOR with Serde

**Why CBOR over hand-rolled binary:**
- **80/20 principle**: 80% of size savings (30-50%) with 20% of implementation effort (days vs weeks)
- **Drop-in replacement**: Swap `serde_json` for `ciborium` - keep existing `#[derive(Serialize, Deserialize)]`
- **Proven standard**: RFC 8949, battle-tested, self-describing binary format
- **Maintainability**: No manual byte layout - easier for future contributors
- **Flexibility**: Trivial to add new types (dates, decimals) later

**Crate choice: `ciborium`**
- Pure Rust, actively maintained (2024 releases)
- Supports `no_std` for future embedded use
- Better than `serde_cbor` (deprecated) or `minicbor` (less Serde integration)

**Current state:**
- All serialization uses `serde_json` (JSON text format)
- Two-layer approach: Pages are JSON, row values are JSON arrays
- 4KB page size limit with overflow handling for values > 55 bytes
- Key files: `cell.rs`, `cell_reader.rs`, `node.rs`, `pager.rs`, `engine.rs`

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 28 | 3.2 | CBOR cell format | — |
| 29 | 3.4 | CBOR record format | 28 |
| 30 | 3.3 | CBOR page format | 28 |
| 31 | 6.4 | ZeroPage free list | 30 |
| 32 | — | Remove serde_json dependency | 31 |

---
**Important:** Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

## 28. CBOR Cell Format (Track 3.2)

### What Changes

Replace JSON cell serialization with CBOR encoding. Add `ciborium` dependency and replace `serde_json` serialization calls.

### Key Files

- `Cargo.toml` — add `ciborium` dependency
- `src/storage/cell.rs` — cell write path (custom Serialize impl)
- `src/storage/cell_reader.rs` — cell read path

### Current Implementation

Cell struct has custom `Serialize`/`Deserialize` to avoid serializing `None` continuation:
```rust
impl Serialize for Cell {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.continuation {
            Some(cont) => (self.key, &self.value, cont).serialize(serializer),
            None => (self.key, &self.value).serialize(serializer),
        }
    }
}
```

Currently uses `serde_json::to_vec(&cell)` and `serde_json::from_slice(&bytes)`.

### Implementation Steps

1. **Add dependency** to `Cargo.toml`:
   ```toml
   ciborium = { version = "0.2", default-features = false, features = ["std"] }
   ```

2. **Update `cell.rs`** - replace JSON with CBOR:
   ```rust
   // OLD:
   let bytes = serde_json::to_vec(&cell).unwrap();

   // NEW:
   let mut bytes = Vec::new();
   ciborium::ser::into_writer(&cell, &mut bytes).unwrap();
   ```

3. **Update `cell_reader.rs`** - replace JSON deserialization:
   ```rust
   // OLD:
   let cell: Cell = serde_json::from_slice(&bytes).unwrap();

   // NEW:
   let cell: Cell = ciborium::de::from_reader(&bytes[..]).unwrap();
   ```

4. **Keep existing `Serialize`/`Deserialize` impls** - they work with CBOR via Serde trait!

### Tests

- All existing cell tests should pass (roundtrip, continuation, empty value, large value)
- Add size comparison test:
  ```rust
  #[test]
  fn test_cbor_smaller_than_json() {
      let cell = Cell { key: 42, value: vec![1,2,3], continuation: None };
      let json_size = serde_json::to_vec(&cell).unwrap().len();
      let cbor_size = {
          let mut v = Vec::new();
          ciborium::ser::into_writer(&cell, &mut v).unwrap();
          v.len()
      };
      assert!(cbor_size < json_size, "CBOR {cbor_size} >= JSON {json_size}");
  }
  ```

### Expected Size Reduction

- Simple cell (key=1, value=[42]): JSON ~20 bytes → CBOR ~12 bytes
- Cell with continuation: JSON ~35 bytes → CBOR ~20 bytes

---

## 29. CBOR Record Format (Track 3.4)

### What Changes

Replace JSON row arrays (`[1, "alice", 30]`) with CBOR encoding. CBOR automatically handles type tags, no manual encoding needed.

### Key Files

- `src/engine.rs` — INSERT instruction (row serialization)
- `src/storage/cell_reader.rs` — `decode_as_json_array()` method
- `src/storage/btree.rs` — catalog row serialization

### Current Implementation

**In `engine.rs` (INSERT instruction):**
```rust
let json_values: Vec<serde_json::Value> = value_regs
    .iter()
    .map(|reg| {
        match self.registers.get(*reg).scalar().unwrap() {
            ScalarValue::Integer(i) => serde_json::Value::Number((*i).into()),
            ScalarValue::Floating(f) => serde_json::Value::Number(...),
            ScalarValue::Boolean(b) => serde_json::Value::Bool(*b),
            ScalarValue::String(s) => serde_json::Value::String(s.clone()),
            ScalarValue::Null => serde_json::Value::Null,
        }
    })
    .collect();

let bytes = serde_json::to_vec(&serde_json::Value::Array(json_values)).unwrap();
cursor.insert(key, bytes);
```

**In `cell_reader.rs`:**
```rust
pub fn decode_as_json_array(&mut self) -> Result<Vec<serde_json::Value>> {
    serde_json::from_reader(self).map_err(...)
}
```

### Implementation Steps

1. **Update `engine.rs`** - replace JSON array encoding:
   ```rust
   // Keep building serde_json::Value for now (works with CBOR too!)
   let json_values: Vec<serde_json::Value> = ...; // same as before

   // NEW: Use CBOR encoding
   let mut bytes = Vec::new();
   ciborium::ser::into_writer(&json_values, &mut bytes).unwrap();
   cursor.insert(key, bytes);
   ```

2. **Update `cell_reader.rs`** - rename and update method:
   ```rust
   // Rename: decode_as_json_array → decode_as_array (still returns serde_json::Value)
   pub fn decode_as_array(&mut self) -> Result<Vec<serde_json::Value>> {
       ciborium::de::from_reader(self).map_err(|e| {
           Error::new(ErrorKind::InvalidData, e.to_string())
       })
   }
   ```

3. **Update all callers** - search for `decode_as_json_array()` and rename to `decode_as_array()`

4. **Update `btree.rs`** - catalog row serialization:
   ```rust
   // OLD:
   let value = serde_json::to_vec(&serde_json::json!([obj_type, name, ...])).unwrap();

   // NEW:
   let mut value = Vec::new();
   ciborium::ser::into_writer(&serde_json::json!([obj_type, name, ...]), &mut value).unwrap();
   ```

### Alternative: Native ScalarValue Encoding

Instead of `serde_json::Value` intermediate, could serialize `Vec<ScalarValue>` directly:

```rust
// Make ScalarValue derive Serialize/Deserialize:
#[derive(Serialize, Deserialize)]
pub enum ScalarValue {
    Integer(i64),
    Floating(f64),
    Boolean(bool),
    String(String),
    Null,
}

// Then in engine.rs:
let scalar_values: Vec<ScalarValue> = value_regs.iter().map(...).collect();
let mut bytes = Vec::new();
ciborium::ser::into_writer(&scalar_values, &mut bytes).unwrap();
```

**Recommendation:** Start with `serde_json::Value` (simpler change), optimize later if needed.

### Tests

- All existing SQL integration tests should pass (`cargo test test_sql_`)
- Test mixed types: `INSERT INTO t VALUES (42, 'text', 3.14, NULL, TRUE)`
- Add size comparison logging to verify CBOR < JSON

### Expected Size Reduction

- Row `[1, "alice", 30]`: JSON ~17 bytes → CBOR ~12 bytes (30% reduction)
- Row with NULL: JSON ~25 bytes → CBOR ~15 bytes (40% reduction)

---

## 30. CBOR Page Format (Track 3.3)

### What Changes

Replace JSON serialization of pages (NodePage, LeafNodePage, InteriorNodePage, OverflowPage) with CBOR encoding. Keep existing struct definitions - just change serialization format.

### Key Files

- `src/storage/node.rs` — node definitions (already have `#[derive(Serialize, Deserialize)]`)
- `src/storage/pager.rs` — `encode_and_set()` and `get_and_decode()` methods

### Current Implementation

**Structures in `node.rs`:**
```rust
#[derive(Serialize, Deserialize)]
pub enum NodePage {
    Leaf(LeafNodePage),
    Interior(InteriorNodePage),
    Overflow(OverflowPage),
}

#[derive(Serialize, Deserialize)]
pub struct LeafNodePage {
    pub cells: Vec<Cell>,
}

#[derive(Serialize, Deserialize)]
pub struct InteriorNodePage {
    pub keys: Vec<Key>,
    pub edges: Vec<u32>, // page numbers
}

#[derive(Serialize, Deserialize)]
pub struct OverflowPage {
    pub content: Vec<u8>,
    pub continuation: Option<u32>,
}
```

**In `pager.rs`:**
```rust
pub fn encode_and_set<P: Serialize>(&mut self, page_num: u32, node_page: &P) -> Result<()> {
    let mut page = vec![0u8; PAGE_SIZE];
    serde_json::to_writer(&mut BufWriter::new(&mut page[..]), node_page)
        .map_err(|e| ...)?;
    self.write_page(page_num, &page)
}

pub fn get_and_decode<P: DeserializeOwned>(&mut self, page_num: u32) -> Result<P> {
    let page = self.read_page(page_num)?;
    let mut de = serde_json::Deserializer::from_reader(BufReader::new(&page[..]));
    P::deserialize(&mut de).map_err(|e| ...)
}
```

### Implementation Steps

1. **Update `pager.rs` - `encode_and_set()`:**
   ```rust
   pub fn encode_and_set<P: Serialize>(&mut self, page_num: u32, node_page: &P) -> Result<()> {
       let mut page = vec![0u8; PAGE_SIZE];

       // NEW: Use CBOR instead of JSON
       ciborium::ser::into_writer(node_page, &mut &mut page[..])
           .map_err(|e| Error::new(
               ErrorKind::InvalidData,
               format!("CBOR encoding failed: {}", e)
           ))?;

       self.write_page(page_num, &page)
   }
   ```

2. **Update `pager.rs` - `get_and_decode()`:**
   ```rust
   pub fn get_and_decode<P: DeserializeOwned>(&mut self, page_num: u32) -> Result<P> {
       let page = self.read_page(page_num)?;

       // NEW: Use CBOR instead of JSON
       ciborium::de::from_reader(&page[..])
           .map_err(|e| Error::new(
               ErrorKind::InvalidData,
               format!("CBOR decoding failed: {}", e)
           ))
   }
   ```

3. **No changes to `node.rs`** - existing `#[derive(Serialize, Deserialize)]` works with CBOR!

### Note on Slotted Pages

The original hand-rolled plan proposed a slotted page layout (SQLite-style). With CBOR:
- **Skip slotted pages initially** - CBOR provides compact encoding without manual offset management
- **Simpler implementation** - no need for cell pointer arrays or manual free space tracking
- **Future optimization** - can add slotted layout later if needed for specific use cases
- **CBOR is sufficient** - achieves 30-50% size reduction, which meets phase F goals

### Tests

- All existing B-tree tests should pass (insert, lookup, split, iteration)
- All SQL integration tests: `cargo test test_sql_`
- Page size enforcement: verify serialized pages fit in 4KB
- Size verification: add test logging to compare JSON vs CBOR page sizes
- Test scenarios:
  - Leaf page with many cells
  - Interior page with many keys/edges
  - Overflow page with continuation
  - Empty page (edge case)

### Expected Size Reduction

- Leaf page (50 cells): JSON ~2.5KB → CBOR ~1.5KB (40% reduction)
- Interior page (100 keys): JSON ~1.8KB → CBOR ~1.2KB (33% reduction)
- More cells fit per page = fewer splits = smaller overall database file

---

## 31. ZeroPage Free List (Track 6.4)

### What Changes

Replace `free_page_list: Vec<u32>` with linked list approach to handle potential overflow and prepare for serde_json removal.

### Key Files

- `src/storage/pager.rs` - ZeroPage struct

### Current Implementation

```rust
#[derive(Serialize, Deserialize)]
pub struct ZeroPage {
    pub magic: u32,              // 0x53514C69 ("SQLi")
    pub format_version: u16,     // Bump to 1 for CBOR
    pub schema_root_page: Option<u32>,
    pub free_page_list: Vec<u32>,
}
```

Serialized with serde_json (soon to be CBOR).

### Analysis

**When does Vec<u32> overflow 4KB?**
- Each `u32` is 4 bytes
- Maximum entries: 4096 / 4 = 1024 page IDs (theoretical maximum)
- With CBOR overhead: ~900-1000 page IDs practical limit
- At 4KB per page: ~4MB database before overflow
- **Conclusion:** Not urgent for initial release

**CBOR improves the situation:**
- JSON encoding: `[1,2,3,...]` with commas and brackets
- CBOR encoding: More compact array representation
- Pushes the problem further out

### Implementation Strategy

Implement linked list structure for free pages to prevent overflow and eliminate Vec dependency:

**New structures:**
```rust
pub struct ZeroPage {
    pub magic: u32,
    pub format_version: u16,
    pub schema_root_page: Option<u32>,
    pub free_list_head: Option<u32>,  // First page of linked list (None if empty)
    pub free_page_count: u32,         // Total free pages
}

#[derive(Serialize, Deserialize)]
pub struct FreeListPage {
    pub next: Option<u32>,       // Next free list page (None if last)
    pub page_ids: Vec<u32>,      // Up to ~1000 page IDs per page
}
```

**Implementation steps:**

1. **Define FreeListPage struct** in `pager.rs` with CBOR serialization

2. **Update ZeroPage** - replace `free_page_list: Vec<u32>` with `free_list_head` and `free_page_count`

3. **Update Pager::allocate_page()**:
   - If `free_list_head.is_some()`, read the FreeListPage
   - Pop a page ID from `page_ids`
   - If page_ids empty after pop, update head to `next`
   - Decrement `free_page_count`

4. **Update Pager::free_page()**:
   - If free list empty, create first FreeListPage
   - If current head page is full (~1000 entries), create new FreeListPage
   - Add page ID to current head's `page_ids`
   - Increment `free_page_count`

5. **Update ZeroPage serialization** - already using CBOR from Item 30

### Tests

- Alloc+free roundtrip with single page
- Many pages (allocate/free 2000+ pages to test multi-page list)
- Overflow to multiple list pages (>1000 free pages)
- Persistence across database close/reopen
- Empty free list (all pages allocated)
- Edge case: free the last allocated page

---

## 32. Remove serde_json Dependency

### What Changes

Remove all remaining uses of `serde_json::Value` and the `serde_json` crate dependency. Replace with native types.

### Key Files

- `Cargo.toml` — remove `serde_json` dependency
- `src/engine/scalarvalue.rs` — add Serialize/Deserialize derives
- `src/engine.rs` — replace serde_json::Value with ScalarValue
- `src/storage/cell_reader.rs` — decode directly to Vec<ScalarValue>
- `src/storage/btree.rs` — catalog row serialization
- Any other files using serde_json::Value

### Current State After Items 28-31

At this point, all serialization uses CBOR via ciborium, but we still use `serde_json::Value` as an intermediate type when encoding/decoding row data:

```rust
// In engine.rs:
let json_values: Vec<serde_json::Value> = value_regs
    .iter()
    .map(|reg| match self.registers.get(*reg).scalar().unwrap() {
        ScalarValue::Integer(i) => serde_json::Value::Number((*i).into()),
        // ... etc
    })
    .collect();
ciborium::ser::into_writer(&json_values, &mut bytes).unwrap();

// In cell_reader.rs:
pub fn decode_as_array(&mut self) -> Result<Vec<serde_json::Value>>
```

### Implementation Steps

1. **Add Serialize/Deserialize to ScalarValue** in `src/engine/scalarvalue.rs`:
   ```rust
   #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
   pub enum ScalarValue {
       Integer(i64),
       Floating(f64),
       Boolean(bool),
       String(String),
       Null,
   }
   ```

2. **Update engine.rs INSERT** - serialize ScalarValue directly:
   ```rust
   // OLD:
   let json_values: Vec<serde_json::Value> = value_regs.iter().map(...).collect();
   ciborium::ser::into_writer(&json_values, &mut bytes).unwrap();

   // NEW:
   let scalar_values: Vec<ScalarValue> = value_regs
       .iter()
       .map(|reg| self.registers.get(*reg).scalar().unwrap().clone())
       .collect();
   ciborium::ser::into_writer(&scalar_values, &mut bytes).unwrap();
   ```

3. **Update cell_reader.rs** - decode to ScalarValue:
   ```rust
   // Rename and change return type:
   pub fn decode_as_scalar_array(&mut self) -> Result<Vec<ScalarValue>> {
       ciborium::de::from_reader(self).map_err(|e| {
           Error::new(ErrorKind::InvalidData, e.to_string())
       })
   }
   ```

4. **Update all callers** of `decode_as_array()` to use `decode_as_scalar_array()`:
   - Search codebase for `decode_as_array()`
   - Update to use new method and handle `Vec<ScalarValue>` instead of `Vec<serde_json::Value>`
   - Engine SELECT/other readers will need updates

5. **Update btree.rs catalog rows** - use ScalarValue or custom struct:
   ```rust
   // Catalog rows: [type, name, tbl_name, rootpage, sql]
   // Option 1: Use ScalarValue
   let row = vec![
       ScalarValue::String(obj_type.to_string()),
       ScalarValue::String(name.to_string()),
       ScalarValue::String(tbl_name.to_string()),
       ScalarValue::Integer(rootpage as i64),
       ScalarValue::String(sql.to_string()),
   ];
   ciborium::ser::into_writer(&row, &mut value).unwrap();

   // Option 2: Define a CatalogRow struct
   #[derive(Serialize, Deserialize)]
   struct CatalogRow {
       obj_type: String,
       name: String,
       tbl_name: String,
       rootpage: u32,
       sql: String,
   }
   ```

6. **Remove serde_json from Cargo.toml**:
   ```toml
   # DELETE this line:
   serde_json = "1.0.94"
   ```

7. **Run cargo build** - compiler will find any remaining serde_json usages

### Tests

- All existing tests should pass with ScalarValue serialization
- Verify no serde_json imports remain: `rg "use.*serde_json" src/`
- Verify Cargo.toml doesn't reference serde_json
- Full test suite: `cargo test`
- SQL integration: `cargo test test_sql_`

### Expected Benefits

- **Smaller dependency tree** - one less crate to compile
- **Type safety** - no intermediate Value type conversions
- **Cleaner code** - direct ScalarValue → CBOR → ScalarValue
- **Better performance** - eliminate conversion overhead
- **Complete CBOR migration** - no JSON remnants

### Verification

After this item:
```bash
# Should find zero matches:
rg "serde_json" Cargo.toml
rg "use.*serde_json" src/

# Should still work:
cargo build
cargo test
cargo test test_sql_
```

---

## Migration Strategy

### Format Version Bump

**Update ZeroPage struct:**
```rust
pub struct ZeroPage {
    pub magic: u32,              // 0x53514C69 ("SQLi") - unchanged
    pub format_version: u16,     // Bump from 0 (JSON) to 1 (CBOR)
    pub schema_root_page: Option<u32>,
    pub free_page_list: Vec<u32>,
}
```

**On database open** (in `Pager::new()` or `BTree::open()`):
```rust
let zero_page: ZeroPage = pager.get_and_decode(0)?;

match zero_page.format_version {
    0 => return Err(Error::new(
        ErrorKind::InvalidData,
        "Database format version 0 (JSON) is no longer supported. \
         Please recreate your database. Pre-1.0 databases are not \
         backwards compatible."
    )),
    1 => { /* CBOR format - continue normally */ },
    v => return Err(Error::new(
        ErrorKind::InvalidData,
        format!("Unknown database format version {}. \
                 This database may have been created by a newer version.", v)
    )),
}
```

**No automatic migration** - per CLAUDE.md, pre-1.0 databases are disposable.

### Implementation Order

**Recommended sequence:**
1. **Item 28** (Cell-level CBOR) - Foundation, smallest scope
2. **Item 29** (Row values CBOR) - Biggest size impact, still uses serde_json::Value
3. **Item 30** (Page structure CBOR) - Completes CBOR migration
4. **Format check** - Add version checking on database open
5. **Item 31** (Free list) - Linked list for scalability
6. **Item 32** (Remove serde_json) - Final cleanup, pure CBOR with native types

**Each item as separate commit** - follow 'Git Workflow' in CLAUDE.md

**Why this order:**
- Cell-level first: Smallest change, tests serialization infrastructure
- Row values next: Most data volume, verifies size reduction (keeps serde_json::Value for simplicity)
- Page structure: Touches most code, builds on proven cell/row serialization
- Format check: After all serialization changed to CBOR
- Free list: Makes free page management scalable
- Remove serde_json last: Clean up intermediate types after everything works with CBOR

### Testing Between Items

After each item:
```bash
cargo fmt                      # Format code
cargo build                    # Verify compilation
cargo build 2>&1 | grep -i warning  # Zero warnings (CRITICAL)
cargo test                     # All unit tests
cargo test test_sql_          # All SQL integration tests
```

All tests must pass before proceeding to next item.

## Verification Checklist

### Item 28 (CBOR Cells)
- [ ] `ciborium` added to Cargo.toml
- [ ] `cell.rs` uses `ciborium::ser::into_writer()`
- [ ] `cell_reader.rs` uses `ciborium::de::from_reader()`
- [ ] Size comparison test added
- [ ] All cell tests pass
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` (zero output)
- [ ] Committed with message: "Implement CBOR cell format (Item 28)"

### Item 29 (CBOR Records)
- [ ] `engine.rs` INSERT uses CBOR for row encoding
- [ ] `cell_reader.rs` method renamed to `decode_as_array()`
- [ ] `btree.rs` catalog rows use CBOR
- [ ] All callers updated
- [ ] All SQL tests pass: `cargo test test_sql_`
- [ ] Size reduction observed (add logging/tests)
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` (zero output)
- [ ] Committed with message: "Implement CBOR record format (Item 29)"

### Item 30 (CBOR Pages)
- [ ] `pager.rs` `encode_and_set()` uses CBOR
- [ ] `pager.rs` `get_and_decode()` uses CBOR
- [ ] `node.rs` unchanged (derives still work)
- [ ] All B-tree tests pass
- [ ] All SQL tests pass
- [ ] Page size limits still enforced
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` (zero output)
- [ ] Committed with message: "Implement CBOR page format (Item 30)"

### Format Version Check
- [ ] ZeroPage.format_version bumped to 1
- [ ] Version check added on database open
- [ ] Error message tested (try opening JSON database)
- [ ] Committed with message: "Add CBOR format version check"

### Item 31 (Free List)
- [ ] FreeListPage struct defined with CBOR serialization
- [ ] ZeroPage updated with `free_list_head` and `free_page_count`
- [ ] Pager::allocate_page() uses linked list
- [ ] Pager::free_page() maintains linked list
- [ ] Tests for multi-page free list (>1000 entries)
- [ ] Persistence tests (close/reopen database)
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` (zero output)
- [ ] Committed with message: "Implement linked list free page management (Item 31)"

### Item 32 (Remove serde_json)
- [ ] ScalarValue has Serialize/Deserialize derives
- [ ] engine.rs INSERT uses Vec<ScalarValue> directly
- [ ] cell_reader.rs decode_as_scalar_array() implemented
- [ ] All callers of decode_as_array() updated
- [ ] btree.rs catalog rows use ScalarValue (or custom struct)
- [ ] serde_json removed from Cargo.toml
- [ ] No serde_json imports remain: `rg "use.*serde_json" src/`
- [ ] All tests pass: `cargo test`
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` (zero output)
- [ ] Committed with message: "Remove serde_json dependency, use native ScalarValue (Item 32)"

### Overall Verification
- [ ] Full test suite: `cargo test` (all ~132+ tests passing)
- [ ] SQL integration: `cargo test test_sql_` (all passing)
- [ ] No ignored tests beyond existing `test_select_star`
- [ ] REPL still works: `cargo run -- test.db`
- [ ] Database file size reduced by 30-50% (compare before/after with test data)
- [ ] No performance regression (manual testing)
- [ ] serde_json completely removed: `rg "serde_json" Cargo.toml src/` returns nothing
- [ ] Only CBOR serialization remains (ciborium crate only)
- [ ] Documentation updated if needed

## Size Reduction Expectations

Based on CBOR specification and testing:

**Individual serialization sizes:**
- Cell: JSON ~20-35 bytes → CBOR ~12-20 bytes (40% reduction)
- Row `[1, "alice", 30]`: JSON ~17 bytes → CBOR ~12 bytes (30% reduction)
- Page (50 cells): JSON ~2.5KB → CBOR ~1.5KB (40% reduction)

**Overall database file:**
- Small database (1K rows): 30-40% reduction
- Large database (1M rows): 35-50% reduction (more cells = better compression ratio)

**Example:** `make big.db` (1M entries)
- Before (JSON): ~120MB
- After (CBOR): ~70-80MB (expected)

## Troubleshooting

**If tests fail after CBOR migration:**

1. **Deserialization errors**: Check that reader uses `from_reader(&bytes[..])` not `from_slice()`
2. **Size too large**: CBOR should be smaller - check for double-encoding or extra wrappers
3. **Type mismatches**: Verify Serialize/Deserialize derives present on all types
4. **Page overflow**: CBOR is more compact - if JSON fit, CBOR will fit

**Common mistakes:**
- Forgetting to update both serialization AND deserialization
- Using `serde_json::Value` with `ciborium` (works but may have edge cases)
- Not updating all call sites (use `cargo build` to find them)

## Future Optimizations (Beyond Phase F)

Once CBOR is working:

1. **Native ScalarValue encoding**: Skip `serde_json::Value` intermediate
2. **Custom CBOR types**: Optimize for specific use cases
3. **Slotted pages**: Add if need finer free space management
4. **Compression**: Add optional compression layer (zstd, lz4)
5. **Zero-copy deserialization**: Use `&[u8]` instead of owned Vec<u8>

But start with simple, proven CBOR - it's 80% of the benefit.

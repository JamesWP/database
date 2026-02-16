# Phase G3 — CREATE INDEX and Index Scans

Split from phase-g2-indexing-and-perf.md (items 34-35) into a dedicated phase with detailed implementation steps.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 34 | 4.3 | CREATE INDEX | — |
| 35 | 4.4 | Index scan in planner | 34 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

**Secondary indexes** enable fast lookups on non-primary-key columns. Instead of scanning an entire table to find rows matching `WHERE age = 25`, an index on the `age` column provides a direct path to matching rows.

**Index structure:** Each index is a separate B-tree where:
- **Key**: The indexed column's value (e.g., age = 25)
- **Value**: The primary key of the row in the main table (e.g., rowid = 42)

**Why this is non-trivial:**
1. **Catalog integration**: Indexes must be persisted alongside tables in the `db_schema` catalog
2. **Multi-tree mutation**: INSERT/UPDATE/DELETE must maintain both the table B-tree and all index B-trees
3. **Query planning**: The planner must detect when an index can accelerate a query
4. **Index population**: CREATE INDEX must scan the existing table to populate the index
5. **Root page stability**: Like tables, index root pages can change during splits and must be tracked

## Background: Index Architecture

### Storage Layer

Each index is stored as a **separate B-tree** with its own root page:

```
Table B-tree:          Index B-tree (on age):
key → row             indexed_value → primary_key
1 → [1,'alice',30]    25 → 2
2 → [2,'bob',25]      30 → 1
3 → [3,'carol',30]    30 → 3
```

The index B-tree maps `indexed_column_value → primary_key`. When evaluating `WHERE age = 30`, the engine:
1. Looks up `30` in the index B-tree → finds keys [1, 3]
2. For each key, looks up the full row in the table B-tree

### Catalog Schema

The `db_schema` catalog stores indexes with `type='index'`:

```
[type,    name,         tbl_name,  rootpage, sql]
['table', 'users',      'users',   4,        'CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)']
['index', 'idx_age',    'users',   7,        'CREATE INDEX idx_age ON users(age)']
```

**Key fields:**
- `type`: 'index' (vs. 'table')
- `name`: Index name (e.g., 'idx_age')
- `tbl_name`: Table this index belongs to (e.g., 'users')
- `rootpage`: Root page of the index B-tree
- `sql`: Original CREATE INDEX statement

### Query Planning

The planner must:
1. Detect applicable indexes (e.g., `WHERE age = 25` can use an index on `age`)
2. Add a `lookup_indexes_for_table()` method to scan the catalog
3. Introduce `LogicalPlan::IndexScan { index_rootpage, search_value, table_rootpage, columns }`

### DML Maintenance

Every INSERT/UPDATE/DELETE must maintain all indexes:

**INSERT example:**
```rust
// After writing to table (line 889 in nodes.rs)
ctx.body_emitter.emit(Operation::WriteCursor(table_cursor, key_reg, reordered_regs));

// For each index on this table:
for index in indexes {
    // Extract indexed column value from row
    let index_value_reg = reordered_regs[index.column_idx];
    // Write to index B-tree: key=column_value, value=primary_key
    ctx.body_emitter.emit(Operation::WriteIndex(
        index_cursor,
        index_value_reg,  // Index key is the column value
        vec![key_reg],     // Index value is the primary key
    ));
}
```

### Index Key Encoding

**V1 Limitation: INTEGER columns only**

For V1, CREATE INDEX is restricted to INTEGER columns. The key encoding is straightforward:
- Positive integers: stored directly as u64
- Negative integers: use two's complement representation with sign bit flip (`(i as u64) ^ 0x8000000000000000`)

This preserves sort order: negative numbers sort before positive, and within each group the natural order is maintained.

**Index B-tree structure:**
- **Key**: Encoded integer value (maintains sort order)
- **Value**: `[primary_key]` as CBOR array

**Future enhancements** (not in V1):
- TEXT columns: concatenate bytes directly (UTF-8 ordering)
- REAL columns: encode IEEE 754 with sign bit adjustments
- Multi-column indexes: concatenate encodings with type prefixes
- NULL handling: reserve key 0x0000000000000000 for NULL values

---

## 34. CREATE INDEX (Track 4.3)

### Implementation Steps (7 commits)

Implementation is broken into incremental commits, each independently testable.

#### Step 34.1 — Lexer: INDEX keyword

**Commit:** Add INDEX keyword to lexer with tokenization test.

**Files:** `src/frontend/lexer.rs`

**Changes:**

1. Add `Index` variant to the `Type` enum (around line 24):
```rust
pub enum Type {
    // ... existing variants ...
    Inner,
    Index,  // NEW

    #[allow(dead_code)]
    Error(Error),
    // ...
}
```

2. In the keyword matching trie (around line 266), add the `'i'` branch:
```rust
'i' => {
    if ident == "insert" { Type::Insert }
    else if ident == "into" { Type::Into }
    else if ident == "is" { Type::Is }
    else if ident == "inner" { Type::Inner }
    else if ident == "index" { Type::Index }  // NEW
    else { Type::Identifier(ident.to_string()) }
}
```

**Tests:** Add unit test in lexer's `#[cfg(test)]` module:
```rust
#[test]
fn test_index_keyword() {
    let tokens = lex("CREATE INDEX idx ON users(age)");
    let types: Vec<Type> = tokens.iter().map(|t| t.tipe()).collect();
    assert!(matches!(types[0], Type::Create));
    assert!(matches!(types[1], Type::Index));
    // ... verify rest
}
```

**Verification:** `cargo test test_index_keyword`

---

#### Step 34.2 — AST: CreateIndexStatement

**Commit:** Add CreateIndex AST node. Parser not yet implemented.

**Files:** `src/frontend/ast.rs`

**Changes:**

1. Add `CreateIndex` variant to `Statement` enum (line 2):
```rust
#[derive(Debug)]
pub enum Statement {
    Select(SelectStatement),
    CreateTable(CreateTableStatement),
    CreateIndex(CreateIndexStatement),  // NEW
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Drop(DropTableStatement),
}
```

2. Add the struct definition (after `CreateTableStatement`):
```rust
#[derive(Debug)]
pub struct CreateIndexStatement {
    pub index_name: String,
    pub table_name: String,
    pub column_name: String,  // Single column for V1
}
```

**Verification:** `cargo build` (should compile with no warnings)

---

#### Step 34.3 — Parser: CREATE INDEX syntax

**Commit:** Parse CREATE INDEX. Planner not yet wired.

**Files:** `src/frontend/parser.rs`

**Changes:**

1. Add `Expect::Index` to the `Expect` enum (around line 130)

2. Add matching arm in `expect()` method:
```rust
(Expect::Index, lexer::Type::Index) => { self.advance(); Ok(()) }
```

3. In `parse_statement()` (around line 160), add CREATE INDEX dispatch:
```rust
pub fn parse_statement(&mut self) -> Result<Statement, ParseError> {
    match self.input.peek() {
        lexer::Type::Create => {
            self.input.advance(); // consume CREATE
            match self.input.peek() {
                lexer::Type::Table => {
                    self.input.advance();
                    Ok(Statement::CreateTable(self.parse_create_table()?))
                }
                lexer::Type::Index => {  // NEW
                    self.input.advance();
                    Ok(Statement::CreateIndex(self.parse_create_index()?))
                }
                _ => Err(ParseError::ExpectedTableOrIndex),  // NEW error
            }
        }
        // ... rest
    }
}
```

4. Add `parse_create_index()` method:
```rust
fn parse_create_index(&mut self) -> Result<ast::CreateIndexStatement, ParseError> {
    // CREATE INDEX idx_name ON table_name(column_name)

    let index_name = match self.input.peek() {
        lexer::Type::Identifier(name) => {
            let n = name.clone();
            self.input.advance();
            n
        }
        _ => return Err(ParseError::ExpectedIdentifier),
    };

    self.input.expect(Expect::On)?;

    let table_name = match self.input.peek() {
        lexer::Type::Identifier(name) => {
            let n = name.clone();
            self.input.advance();
            n
        }
        _ => return Err(ParseError::ExpectedIdentifier),
    };

    self.input.expect(Expect::LeftParen)?;

    let column_name = match self.input.peek() {
        lexer::Type::Identifier(name) => {
            let n = name.clone();
            self.input.advance();
            n
        }
        _ => return Err(ParseError::ExpectedIdentifier),
    };

    self.input.expect(Expect::RightParen)?;

    Ok(ast::CreateIndexStatement {
        index_name,
        table_name,
        column_name,
    })
}
```

5. Add `ParseError::ExpectedTableOrIndex` variant

**Tests:** Add unit test:
```rust
#[test]
fn test_parse_create_index() {
    let sql = "CREATE INDEX idx_age ON users(age)";
    let stmt = parse_statement_from_str(sql).unwrap();
    match stmt {
        Statement::CreateIndex(ci) => {
            assert_eq!(ci.index_name, "idx_age");
            assert_eq!(ci.table_name, "users");
            assert_eq!(ci.column_name, "age");
        }
        _ => panic!("Expected CreateIndex statement"),
    }
}
```

**Verification:** `cargo test test_parse_create_index`

---

#### Step 34.4 — Catalog: lookup_indexes_for_table()

**Commit:** Add catalog method to retrieve indexes for a table.

**Files:** `src/storage/btree.rs`

**Changes:**

1. Add struct for index metadata (around line 20):
```rust
/// Metadata for a single index from the catalog
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub index_name: String,
    pub column_name: String,
    pub rootpage: u32,
}
```

2. Add `lookup_indexes_for_table()` method (after `lookup_table()` around line 813):
```rust
/// Look up all indexes for a table by scanning db_schema.
pub fn lookup_indexes_for_table(&self, table_name: &str) -> Vec<IndexInfo> {
    let schema_root = match self.schema_root_page() {
        Some(root) => root,
        None => return vec![],
    };

    let mut indexes = Vec::new();
    let mut cursor = self.open(schema_root);
    let mut c = cursor.open_readonly();
    c.first();

    loop {
        let entry = c.get_entry();
        match entry {
            None => break,
            Some(mut reader) => {
                let values = reader.decode_as_array();
                // Row format: [type, name, tbl_name, rootpage, sql]
                if values.len() >= 5 {
                    let obj_type = values[0].as_str().unwrap_or("");
                    let tbl_name = values[2].as_str().unwrap_or("");

                    if obj_type == "index" && tbl_name == table_name {
                        let name = values[1].as_str().unwrap_or("");
                        let rootpage = values[3].as_u64().unwrap() as u32;
                        let sql = values[4].as_str().unwrap_or("");

                        let column_name = extract_column_from_index_sql(sql);

                        indexes.push(IndexInfo {
                            index_name: name.to_string(),
                            column_name,
                            rootpage,
                        });
                    }
                }
            }
        }
        c.next();
    }

    indexes
}

/// Extract column name from CREATE INDEX SQL
/// "CREATE INDEX idx ON table(col)" → "col"
fn extract_column_from_index_sql(sql: &str) -> String {
    if let Some(start) = sql.find('(') {
        if let Some(end) = sql[start..].find(')') {
            return sql[start + 1..start + end].trim().to_string();
        }
    }
    String::new()
}
```

**Tests:** Add unit tests in btree's `#[cfg(test)]` module:
```rust
#[test]
fn test_lookup_indexes_empty() {
    let test = TestDb::default();
    let indexes = test.btree.lookup_indexes_for_table("users");
    assert_eq!(indexes.len(), 0);
}

#[test]
fn test_lookup_indexes_for_table() {
    let test = TestDb::default();

    test.btree.insert_schema_entry(
        "table", "users", "users", 10,
        "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)",
    );
    test.btree.insert_schema_entry(
        "index", "idx_age", "users", 12,
        "CREATE INDEX idx_age ON users(age)",
    );

    let indexes = test.btree.lookup_indexes_for_table("users");
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].index_name, "idx_age");
    assert_eq!(indexes[0].column_name, "age");
    assert_eq!(indexes[0].rootpage, 12);
}
```

**Verification:** `cargo test test_lookup_indexes`

---

#### Step 34.5 — Execute: CREATE INDEX implementation (DDL, no planning)

**Commit:** Execute CREATE INDEX (DDL). Follows CREATE TABLE pattern - no planning, direct execution.

**Files:** `src/db.rs`, `src/frontend/parser.rs` (to get column type)

**Pattern:** Like CREATE TABLE and DROP TABLE (lines 66-103), handle CREATE INDEX directly in `db.rs::execute()` without going through planner/compiler/engine. This follows the established DDL pattern.

**Changes:**

1. Add `CreateIndex` variant to `ExecuteResult` enum (around line 10):
```rust
CreateIndex { index_name: String },
```

2. Add error variants to `ExecuteError` enum:
```rust
IndexAlreadyExists(String),
TableNotFound(String),
ColumnNotFound { table: String, column: String },
ColumnNotInteger { table: String, column: String },  // V1: INTEGER only
```

3. In `execute()` function (around line 62), add `Statement::CreateIndex` case (between CREATE TABLE and DROP):
```rust
Statement::CreateIndex(_) => {
    let ci = match stmt {
        Statement::CreateIndex(ci) => ci,
        _ => unreachable!(),
    };

    // 1. Resolve table and validate
    let (table_rootpage, ddl) = btree
        .lookup_table(&ci.table_name)
        .ok_or_else(|| ExecuteError::TableNotFound(ci.table_name.clone()))?;

    // 2. Check if index already exists
    let indexes = btree.lookup_indexes_for_table(&ci.table_name);
    for index in &indexes {
        if index.index_name == ci.index_name {
            return Err(ExecuteError::IndexAlreadyExists(ci.index_name.clone()));
        }
    }

    // 3. Parse DDL to get column info
    let parsed_ddl = parse(&ddl).map_err(ExecuteError::Parse)?;
    let create_table = match parsed_ddl {
        Statement::CreateTable(ct) => ct,
        _ => return Err(ExecuteError::Parse(ParseError::ExpectedTableOrIndex)),
    };

    // 4. Find column and verify it's INTEGER
    let column_def = create_table
        .columns
        .iter()
        .find(|col| col.name == ci.column_name)
        .ok_or_else(|| ExecuteError::ColumnNotFound {
            table: ci.table_name.clone(),
            column: ci.column_name.clone(),
        })?;

    // V1: Only allow INTEGER columns
    if !matches!(column_def.type_name, Some(DataType::Integer)) {
        return Err(ExecuteError::ColumnNotInteger {
            table: ci.table_name.clone(),
            column: ci.column_name.clone(),
        });
    }

    let column_idx = create_table
        .columns
        .iter()
        .position(|col| col.name == ci.column_name)
        .unwrap();

    // 5. Create the index B-tree
    let index_rootpage = btree.create_tree();

    // 6. Scan table and populate index
    let mut table_cursor = btree.open(table_rootpage);
    let mut tc = table_cursor.open_readonly();
    tc.first();

    let mut index_cursor = btree.open(index_rootpage);
    loop {
        let entry = tc.get_entry();
        match entry {
            None => break,
            Some(mut reader) => {
                let table_key = reader.key();
                let values = reader.decode_as_array();

                if column_idx < values.len() {
                    let index_key_value = &values[column_idx];

                    // Encode INTEGER to u64 key (preserves sort order)
                    let index_key = match index_key_value {
                        ScalarValue::Integer(i) => encode_integer_key(*i),
                        ScalarValue::Null => 0, // NULL sorts first
                        _ => continue, // Skip non-integer values (shouldn't happen)
                    };

                    // Encode primary key as index value
                    let index_value = vec![ScalarValue::Integer(table_key as i64)];
                    let mut encoded = Vec::new();
                    ciborium::ser::into_writer(&index_value, &mut encoded).unwrap();

                    index_cursor.open_readwrite().insert(index_key, encoded);
                }
            }
        }
        tc.next();
    }

    // 7. Add catalog entry
    btree.insert_schema_entry(
        "index",
        &ci.index_name,
        &ci.table_name,
        index_rootpage,
        sql,
    );

    Ok(ExecuteResult::CreateIndex {
        index_name: ci.index_name.clone(),
    })
}

/// Encode i64 to u64 preserving sort order
/// Flip sign bit so negative numbers sort before positive
fn encode_integer_key(i: i64) -> u64 {
    (i as u64) ^ 0x8000000000000000
}

/// Decode u64 back to i64
fn decode_integer_key(key: u64) -> i64 {
    (key ^ 0x8000000000000000) as i64
}
```

**Test:** Add `tests/sql/create_index.sql`:
```sql
CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)
INSERT INTO users VALUES (1, 'Alice', 30)
INSERT INTO users VALUES (2, 'Bob', 25)
INSERT INTO users VALUES (3, 'Charlie', 30)
CREATE INDEX idx_age ON users(age)
-- V1: Only INTEGER columns allowed
-- This should fail:
-- CREATE INDEX idx_name ON users(name)
```

And `tests/sql/create_index.expected`:
```
Table 'users' created
1 row inserted
1 row inserted
1 row inserted
Index 'idx_age' created
```

**Verification:** `cargo test test_sql_create_index`

---

#### Step 34.6 — Helper: encode/decode integer keys

**Commit:** Add utility functions for integer key encoding to maintain sort order.

**Files:** `src/storage/btree.rs` or new `src/storage/key_encoding.rs`

**Changes:**

Add public functions for encoding/decoding:

```rust
/// Encode i64 to u64 preserving sort order.
/// Flip the sign bit so negative numbers sort before positive.
///
/// Examples:
///   -100 → 0x7FFF_FFFF_FFFF_FF9C
///      0 → 0x8000_0000_0000_0000
///    100 → 0x8000_0000_0000_0064
pub fn encode_integer_key(i: i64) -> u64 {
    (i as u64) ^ 0x8000_0000_0000_0000
}

/// Decode u64 back to i64
pub fn decode_integer_key(key: u64) -> i64 {
    (key ^ 0x8000_0000_0000_0000) as i64
}
```

**Tests:**
```rust
#[test]
fn test_integer_key_encoding_order() {
    let keys = vec![-100, -1, 0, 1, 100];
    let encoded: Vec<u64> = keys.iter().map(|k| encode_integer_key(*k)).collect();

    // Verify encoded keys maintain sort order
    for i in 1..encoded.len() {
        assert!(encoded[i-1] < encoded[i],
            "Encoded keys should maintain order: {:?}", keys);
    }

    // Verify round-trip
    for k in keys {
        assert_eq!(decode_integer_key(encode_integer_key(k)), k);
    }
}
```

**Verification:** `cargo test test_integer_key_encoding`

---

#### Step 34.7 — DML: Maintain indexes on INSERT

**Commit:** Update INSERT compiler to write to all indexes after writing to table.

**Files:** `src/planner.rs`, `src/compiler/nodes.rs`, `src/engine/program.rs`, `src/engine.rs`

**Planner changes:**

1. Add struct (around line 86):
```rust
#[derive(Debug, Clone, PartialEq)]
pub struct IndexMaintenanceInfo {
    pub rootpage: u32,
    pub column_idx: usize,
}
```

2. Modify `LogicalPlan::Insert` to include `indexes: Vec<IndexMaintenanceInfo>` (line ~177)

3. Update `plan_insert()` (line ~634) to look up indexes and populate the vec:
```rust
// Look up indexes for this table
let index_infos = btree.lookup_indexes_for_table(&table_name);
let mut indexes = Vec::new();
for index_info in index_infos {
    // Find column index
    let col_idx = table
        .columns
        .iter()
        .position(|col| col.name == index_info.column_name)
        .unwrap();

    indexes.push(IndexMaintenanceInfo {
        rootpage: index_info.rootpage,
        column_idx: col_idx,
    });
}
```

**Compiler changes:**

1. Update `codegen_insert()` signature to accept `indexes: &[IndexMaintenanceInfo]` (line 810)

2. In INIT: open cursors for all indexes:
```rust
let mut index_cursors = Vec::new();
for index in indexes {
    let cursor_reg = ctx.registers.alloc();
    ctx.init_emitter.emit(Operation::Open(cursor_reg, index.rootpage));
    index_cursors.push(cursor_reg);
}
```

3. After table WriteCursor (line 889), emit WriteIndex for each index:
```rust
// Write to each index
for (i, index) in indexes.iter().enumerate() {
    let index_cursor = index_cursors[i];
    let indexed_column_reg = reordered_regs[index.column_idx];

    ctx.body_emitter.emit(Operation::WriteIndex(
        index_cursor,
        indexed_column_reg,  // Column value to index
        key_reg,             // Primary key
    ));
}
```

4. Update dispatch in `codegen()` to pass indexes (around line 1170)

**Engine changes:**

1. Add `Operation::WriteIndex(cursor_reg, value_reg, pk_reg)` to `src/engine/program.rs`

2. In `src/engine.rs`, execute WriteIndex (around line 640):
```rust
WriteIndex(cursor_reg, value_reg, pk_reg) => {
    // Encode the indexed value to u64 key
    let indexed_value = self.registers.get(*value_reg).scalar().unwrap();
    let index_key = match indexed_value {
        ScalarValue::Integer(i) => encode_integer_key(*i),
        ScalarValue::Null => 0,  // NULL sorts first
        _ => panic!("WriteIndex: only INTEGER columns supported in V1"),
    };

    // Encode primary key as index value
    let pk_value = self.registers.get(*pk_reg).scalar().unwrap();
    let index_value = vec![pk_value.clone()];
    let mut encoded = Vec::new();
    ciborium::ser::into_writer(&index_value, &mut encoded).unwrap();

    // Write to index B-tree
    let cursor = self.registers.get_mut(*cursor_reg).cursor_mut().unwrap();
    let mut c = cursor.open_readwrite();
    c.insert(index_key, encoded);
}
```

**Test:** Extend `tests/sql/create_index.sql`:
```sql
-- ... existing setup ...
CREATE INDEX idx_age ON users(age)
INSERT INTO users VALUES (4, 'Diana', 25)
INSERT INTO users VALUES (5, 'Eve', 35)
```

**Verification:** `cargo test test_sql_create_index`

---

## 35. Index Scan in Planner (Track 4.4)

### Implementation Steps (3 commits + docs)

#### Step 35.1 — Planner: IndexScan logical plan node

**Commit:** Add IndexScan plan node. Compiler not yet implemented.

**Files:** `src/planner.rs`

**Changes:**

1. Add `IndexScan` variant to `LogicalPlan` enum (around line 121):
```rust
/// Scan via an index
IndexScan {
    index_rootpage: u32,
    search_value: Literal,
    table_rootpage: u32,
    columns: Vec<usize>,
},
```

2. Modify `plan_select()` (line ~305) to detect index opportunities:
```rust
let mut plan = if let Some(ref filter) = select.filter {
    if let Some(index_scan) = try_plan_index_scan(filter, &table, btree)? {
        index_scan  // Use IndexScan
    } else {
        // Fall back to Scan + Filter
        let scan = LogicalPlan::Scan { ... };
        LogicalPlan::Filter { ... }
    }
} else {
    LogicalPlan::Scan { ... }
};
```

3. Add helper functions:
```rust
fn try_plan_index_scan(...) -> Result<Option<LogicalPlan>, PlanError>
fn extract_equality_filter(...) -> Option<(String, Literal)>
fn ast_scalar_to_literal(...) -> Option<Literal>
```

**Tests:** Unit test verifying IndexScan chosen when index exists

**Verification:** `cargo test test_plan_index_scan`

---

#### Step 35.2 — Compiler: IndexScan bytecode generation

**Commit:** Compile IndexScan to bytecode. Use proper key encoding.

**Files:** `src/compiler/nodes.rs`, `src/engine/program.rs`, `src/engine.rs`

**Changes:**

1. Add `codegen_index_scan()` function (after `codegen_scan()` around line 180):

```rust
pub fn codegen_index_scan(
    index_rootpage: u32,
    search_value: &Literal,
    table_rootpage: u32,
    columns: &[usize],
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    let index_cursor_reg = ctx.registers.alloc();
    let table_cursor_reg = ctx.registers.alloc();
    let search_key_reg = ctx.registers.alloc();
    let flag_reg = ctx.registers.alloc();
    let pk_reg = ctx.registers.alloc();

    let output_regs = ctx.registers.alloc_block(columns.len());

    // INIT: Open cursors and encode search key
    ctx.init_emitter.emit(Operation::Open(index_cursor_reg, index_rootpage));
    ctx.init_emitter.emit(Operation::Open(table_cursor_reg, table_rootpage));

    // Store search value and encode to index key
    let search_scalar = literal_to_scalar(search_value);
    ctx.init_emitter.emit(Operation::StoreValue(search_key_reg, search_scalar));
    ctx.init_emitter.emit(Operation::EncodeIndexKey(search_key_reg, search_key_reg));

    // Find first matching entry in index
    ctx.init_emitter.emit(Operation::MoveCursor(
        index_cursor_reg,
        MoveOperation::Find(search_key_reg),
    ));

    // BODY: Check if positioned on matching key
    let index_check = ctx.body_emitter.create_label();
    ctx.body_emitter.bind_label(index_check);

    ctx.body_emitter.emit(Operation::CanReadCursor(flag_reg, index_cursor_reg));
    ctx.body_emitter.emit_goto_if_false(cont.on_done, flag_reg);

    // Read primary key from index value
    ctx.body_emitter.emit(Operation::ReadCursor(vec![pk_reg], index_cursor_reg));

    // Look up row in table by primary key
    ctx.body_emitter.emit(Operation::MoveCursor(
        table_cursor_reg,
        MoveOperation::Find(pk_reg),
    ));

    // Read full row from table
    ctx.body_emitter.emit(Operation::ReadCursor(output_regs.clone(), table_cursor_reg));

    // Yield this row
    ctx.body_emitter.emit_goto(cont.on_tuple);

    // INDEX_NEXT: advance to next entry (may not match our search key anymore)
    let index_next = ctx.body_emitter.create_label();
    ctx.body_emitter.bind_label(index_next);
    ctx.body_emitter.emit(Operation::MoveCursor(index_cursor_reg, MoveOperation::Next));
    ctx.body_emitter.emit_goto(index_check);

    NodeOutput {
        next: index_next,
        output_regs,
    }
}
```

2. Add `Operation::EncodeIndexKey(dest_reg, src_reg)` to `src/engine/program.rs`:
```rust
/// Encode a ScalarValue to a u64 index key (preserves sort order)
EncodeIndexKey(Reg, Reg),
```

3. Execute EncodeIndexKey in `src/engine.rs`:
```rust
EncodeIndexKey(dest, src) => {
    let value = self.registers.get(*src).scalar().unwrap();
    let encoded_key = match value {
        ScalarValue::Integer(i) => encode_integer_key(*i),
        ScalarValue::Null => 0,
        _ => panic!("EncodeIndexKey: only INTEGER supported in V1"),
    };
    *self.registers.get_mut(*dest) = RegisterValue::ScalarValue(
        ScalarValue::Integer(encoded_key as i64)
    );
}
```

4. Update `codegen()` dispatch for IndexScan

**Note:** For V1 with INTEGER-only indexes, the index B-tree may have multiple entries with the same key (duplicates). The simple Find + iterate approach will work for exact matches.

**Verification:** `cargo build`

---

#### Step 35.3 — Integration tests & verification

**Commit:** End-to-end SQL tests for index scans.

**Files:** `tests/sql/index_scan.sql`, `tests/sql/index_scan.expected`

**Test coverage:**
- Query without index (table scan)
- Create index
- Query with index (same results)
- Index scan with ORDER BY
- Index scan with no matches
- INSERT after index creation
- Query again to verify maintenance

**Verification:** `cargo test test_sql_index_scan`

---

#### Step 35.4 — Documentation

**Commit:** Add comments, update CLAUDE.md with index usage patterns.

**Files:** `CLAUDE.md`

Add "## Indexes" section after "## Schema Catalog" explaining:
- Index structure (separate B-trees)
- CREATE INDEX syntax
- Index maintenance on DML
- Query optimization (IndexScan vs TableScan)
- V1 limitations (single-column, INTEGER only, equality only)
- Future enhancements (range scans, covering indexes, multi-column)

**Verification:** Review CLAUDE.md for clarity

---

## Verification

For each commit:
- [ ] Tests written first or alongside implementation (TDD)
- [ ] All tests pass: `cargo test --bin database`
- [ ] Zero warnings: `cargo fmt && cargo build 2>&1 | grep -i warning`
- [ ] Each commit is independently testable

**End-to-end verification:**
```bash
cargo test test_sql_create_index
cargo test test_sql_index_scan
cargo test test_lookup_indexes
cargo test test_plan_index_scan
```

**Performance impact:** Queries with `WHERE col = value` go from O(N) table scans to O(log N) index lookups + O(M) row fetches.

---

## Future Enhancements (Not in This Phase)

- Range scans (WHERE age > 25, BETWEEN)
- Multi-column indexes
- UPDATE/DELETE index maintenance (requires those features)
- DROP INDEX
- UNIQUE indexes with constraint enforcement
- Covering indexes (avoid table lookup)
- TEXT/REAL column support
- Proper collation for strings (currently only INTEGER preserves sort order)

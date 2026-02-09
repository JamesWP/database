# database

A single-file relational database library in Rust, similar to SQLite. Implements a complete database engine from scratch with SQL parsing, query planning, a bytecode virtual machine, and B-tree storage.

## Build & Run

```bash
cargo build              # Debug build
cargo build --release    # Release build
cargo test               # Run all tests
cargo run -- <db_file>   # Run interactive CLI
```

## Architecture

```
SQL Input -> Frontend (Lexer/Parser/AST) -> Planner -> Compiler -> Engine (VM) -> Storage (BTree/Pager)
```

- **Frontend**: SQL tokenization and parsing to AST
- **Planner**: Converts AST to logical query plans
- **Compiler**: Compiles logical plans to bytecode
- **Engine**: Register-based VM executing bytecode
- **Storage**: Persistent B-tree with page-based I/O

## Interactive CLI

The REPL uses a mode-based architecture exposing different subsystems:

```
$ cargo run -- test.db
db> modes
Available modes:
  btree    - B-tree storage operations
  parser   - SQL lexer and parser inspection
  planner  - Query planning and logical plans
  engine   - VM bytecode execution

db> enter btree
btree> create table users
btree> open users
btree:users> insert 1 alice
btree:users> insert 2 bob
btree:users> print data
Entry: key=1, len=5 value=alice
Entry: key=2, len=3 value=bob

db> enter parser
parser> parse SELECT id FROM users
AST:
Select(SelectStatement { ... })

db> enter planner
planner> mock schema
planner> plan SELECT id FROM users
LogicalPlan:
Project { input: Scan { table: "users", ... }, ... }

db> enter engine
engine> compile SELECT id FROM users
Compiled: 13 operations, 4 registers
engine> program
   0: Open(Reg(0), "users")
   1: MoveCursor(Reg(0), First)
   ...
```

## Manual Tests

Manual test scripts are located in the repository root. These complement automated tests by exercising the database through realistic usage scenarios.

### SQL Mode End-to-End Test

**Script**: `test_sql_mode.sql`
**Purpose**: Comprehensive test of SQL mode functionality

Creates a multi-table database with realistic data and exercises various SQL features:

```bash
# Run the test
cat test_sql_mode.sql | ./target/release/database test_sql.db

# Test includes:
# - 3 tables (users, products, orders)
# - 16 row insertions
# - 10 SELECT queries with various WHERE conditions
# - Arithmetic expressions in WHERE clauses
# - Multiple comparison operators (>, <, >=, etc.)
```

**What it tests**:
- Table creation (CREATE TABLE)
- Data insertion (INSERT INTO ... VALUES)
- Basic queries (SELECT ... FROM)
- Filtered queries (WHERE with comparison operators)
- Arithmetic expressions in predicates (age-20>10)
- Column alignment and output formatting

**Expected results**: See `test_results.md` for detailed output from each query.

### Adding New Manual Tests

When adding manual tests:
1. Create a `.sql` or command script file
2. Document it in this section with purpose and usage
3. Include expected results or reference documentation
4. Consider adding it to CI if it's stable and fast

## References

- B-tree design: https://cglab.ca/~abeinges/blah/rust-btree-case/
- File format: https://www.sqlite.org/fileformat.html

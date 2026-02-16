# database

A single-file relational database library in Rust, similar to SQLite. Implements a complete database engine from scratch with SQL parsing, query planning, a bytecode virtual machine, and B-tree storage.

## Build & Run

```bash
cargo build              # Debug build
cargo build --release    # Release build
cargo test               # Run all tests
cargo run -- <db_file>   # Run interactive CLI
```

## Testing

The project includes comprehensive automated tests:

- **Unit tests**: 201 tests embedded in source files
- **SQL integration tests**: End-to-end tests in `tests/sql/` with `.sql` scripts and `.expected` output files

**Run a single SQL test** (faster iteration during development):
```bash
SQL_TEST_FILE=where_clauses cargo test test_sql_scripts
```

**Test error cases**: The SQL test runner supports testing expected errors using `ERROR: <pattern>` syntax:
```sql
-- error_cases.sql
CREATE TABLE users (id INTEGER);
CREATE TABLE users (id INTEGER);  -- Should error
```
```
-- error_cases.expected
Table 'users' created
ERROR: already exists
```

**See also**: [`manual_tests/`](manual_tests/) for manual end-to-end test scripts

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

### Non-Interactive Mode

You can execute commands directly from the command line for scripting and debugging:

```bash
# List all tables in database
cargo run -- test.db btree tables

# Inspect raw page structure (useful for debugging CBOR serialization)
cargo run -- test.db btree inspect page 0
cargo run -- test.db btree inspect all

# Quick SQL queries
cargo run -- test.db sql "SELECT * FROM users"

# Parse SQL to see AST
cargo run -- test.db parser parse "SELECT id FROM users WHERE age > 18"

# Generate query plan
cargo run -- test.db planner plan "SELECT name FROM users WHERE id = 1"
```

**Debugging Commands** (useful during development):
```bash
# Check database format version and metadata
cargo run -- test.db btree inspect page 0

# View catalog structure
cargo run -- test.db btree open db_schema
cargo run -- test.db btree print data

# Inspect specific table's B-tree pages
cargo run -- test.db btree open users
cargo run -- test.db btree print data

# Verify B-tree integrity
cargo run -- test.db btree open users
cargo run -- test.db btree verify
cargo run -- test.db btree verify all
```

## Manual Tests

Manual test scripts complement automated tests by exercising the database through realistic end-to-end scenarios.

**Location**: [`manual_tests/`](manual_tests/)

**Available tests**:
- SQL mode end-to-end test (3 tables, 16 rows, 10 queries)
- More tests coming soon...

**See**: [`manual_tests/README.md`](manual_tests/README.md) for complete documentation, how to run tests, and guidelines for adding new tests.

## References

- B-tree design: https://cglab.ca/~abeinges/blah/rust-btree-case/
- File format: https://www.sqlite.org/fileformat.html

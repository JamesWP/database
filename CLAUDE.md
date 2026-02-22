# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A single-file relational database library in Rust, similar to SQLite. Implements a complete database engine from scratch with SQL parsing, query planning, a bytecode virtual machine, and B-tree storage.

## Build & Test Commands

```bash
cargo build              # Debug build
cargo build --release    # Release build
cargo test               # Run all tests (lib + integration + doctests) - THE BASELINE
cargo test test_sql_     # Run SQL integration tests
cargo test <test_name>   # Run single test
cargo run -- <db_file>   # Run interactive CLI
cargo fmt                # Format code
```

### Test Organization

- **Unit tests**: Embedded in source files with `#[cfg(test)]` modules
- **Integration tests**: Located in `tests/` directory
- **SQL tests**: Automated tests in `tests/sql/*.sql` with `.expected` files

**SQL Test Runner:**

Uses `build.rs` to auto-generate individual test functions for each `.sql` file. Each becomes a separate `#[test]` function with native cargo test features.

- Run all SQL tests: `cargo test test_sql_`
- Run specific test: `cargo test test_sql_where_clauses`
- Update inline expected output: `cargo run --bin update-sql-tests [test_names...]`
- Error testing: Use `ERROR: pattern` in `-- >` lines for case-insensitive substring matching

Each SQL file contains inline expected output as `-- >` comment lines:

```sql
-- tests/sql/error_cases.sql
CREATE TABLE users (id INTEGER, name TEXT)
-- > Table 'users' created
CREATE TABLE users (id INTEGER, name TEXT)
-- > ERROR: already exists
```

**Manual Tests:** See `manual_tests/README.md` for end-to-end scenarios and REPL tests.

## Development Workflow

**TDD Process:**
1. Run `cargo test` before changes (establish baseline)
2. Write tests alongside or before implementation
3. Run `cargo test` after changes (verify nothing broke)
4. Add regression tests when fixing bugs

**Code Quality (before committing):**
```bash
cargo fmt                        # Format code
cargo build 2>&1 | grep warning  # Zero warnings policy - fix all warnings
cargo test                       # All tests must pass
```

**Task Tracking:** Use TaskCreate/TaskUpdate to track multi-step work. Update status: pending → in_progress → completed.

## Git & Commits

**Commit Strategy:**
- Small, focused commits with clear messages (imperative mood)
- Stage and review before committing: `git add <files> && git diff --cached --stat`
- For multi-item phases: commit each item separately, infrastructure changes first
- Wait for user approval after staging, before committing

**Standard Workflow:**
```bash
cargo fmt && cargo build && cargo test  # Verify before changes
# ... make changes ...
cargo fmt && cargo build && cargo test  # Verify after changes
git add <files>
git diff --cached --stat                # Show for review
git commit -m "message"                 # After approval
```

**Phase Completion:** Run final verification, show summary (`git log --oneline -N`), update documentation.

## Architecture

The database follows a layered architecture:

```
SQL Input → Frontend (Lexer/Parser/AST) → Planner → Engine (VM) → Storage (BTree/Pager)
```

### Layers

**Frontend** (`src/frontend/`): SQL tokenization and parsing
- `lexer.rs` - Tokenizes SQL strings
- `parser.rs` - Produces AST from tokens
- `ast.rs` - AST node definitions

**Planner** (`src/planner.rs`): Converts AST to query execution plans (TableScan, Select nodes)

**Engine** (`src/engine.rs`, `src/engine/`): Register-based virtual machine executing bytecode
- `program.rs` - Bytecode instruction definitions (StoreValue, Open, MoveCursor, ReadCursor, Yield, GoTo, Halt, etc.)
- `registers.rs` - Register management for VM state
- `scalarvalue.rs` - Scalar value types (int, float, bool)

**Storage** (`src/storage/`): Persistent B-tree with page-based I/O
- `btree.rs` - B-tree implementation with cursor-based access
- `pager.rs` - Page manager (4KB pages), handles file I/O
- `node.rs` - Leaf and interior node structures
- `cell.rs`, `cell_reader.rs` - Key-value cell storage with overflow support for large values

**REPL** (`src/repl/`): Mode-based interactive CLI
- `mod.rs` - Main REPL loop and mode switching
- `mode.rs` - Mode trait and types
- `modes/` - Individual mode implementations (btree, parser, planner, engine)

### Key References

- B-tree design inspired by: https://cglab.ca/~abeinges/blah/rust-btree-case/
- File format based on: https://www.sqlite.org/fileformat.html

## Interactive CLI

The REPL uses a mode-based architecture. Run with `cargo run -- <db_file>`:

```
db> modes              # List available modes
db> enter <mode>       # Enter a mode
db> back               # Return to root mode
db> exit               # Exit REPL
```

### Modes

**btree** - B-tree storage operations
```
btree> create table <name>     # Create a new table
btree> open <name>             # Open cursor on table
btree> insert <key> <value>    # Insert key-value pair
btree> first/next/prev/find    # Navigate cursor
btree> print / print data      # Print current/all entries
btree> verify                  # Verify B-tree integrity
btree> dump <path>             # Export as graphviz dot
```

**parser** - SQL lexer and parser inspection
```
parser> tokenize <sql>         # Show lexer tokens
parser> parse <sql>            # Show AST
parser> both <sql>             # Show tokens and AST
```

**planner** - Query planning
```
planner> mock schema           # Create test schema (users table)
planner> schema                # Show current schema
planner> plan <sql>            # Show logical plan
```

**engine** - Bytecode compilation
```
engine> compile <sql>          # Compile SQL to bytecode
engine> program                # Show bytecode listing
```

### Non-Interactive Mode

Commands can be executed directly from the command line (useful for debugging and CI):

```bash
# General format
cargo run -- <db_file> <mode> <command...>

# List all tables
cargo run -- test.db btree tables

# Inspect page structure (debugging CBOR serialization)
cargo run -- test.db btree inspect page 0
cargo run -- test.db btree inspect all

# SQL queries
cargo run -- test.db sql "SELECT * FROM users"

# Parse and plan
cargo run -- test.db parser parse "SELECT * FROM users"
cargo run -- test.db planner plan "SELECT * FROM users"
```

## Debugging Commands

When working on storage, serialization, or fixing bugs, these commands are essential:

**Inspect Database Format:**
```bash
# Check ZeroPage (magic number, version, free list, schema root)
cargo run -- test.db btree inspect page 0

# View entire database structure
cargo run -- test.db btree inspect all
```

**Examine Catalog:**
```bash
# List all tables with root pages
cargo run -- test.db btree tables

# Inspect catalog entries (schema stored as CBOR Vec<ScalarValue>)
cargo run -- test.db btree open db_schema
cargo run -- test.db btree print data
```

**Debug Specific Tables:**
```bash
# View table data with CBOR decoding
cargo run -- test.db btree open users
cargo run -- test.db btree print data

# Verify B-tree integrity
cargo run -- test.db btree open users
cargo run -- test.db btree verify
cargo run -- test.db btree verify all
```

**After CBOR Migration (Phase F):**
- Use `inspect page` to see continuation pointers and overflow chains
- Cell values are CBOR-encoded Vec<ScalarValue> - shown as `decoded=[...]`
- Look for `continuation=<page_num> (overflow)` in magenta for large values
- Verify catalog keys are sequential (0, 1, 2...) not hashes

## Indexes

Each secondary index is a separate B-tree with its own root page.
- **V1 Restriction**: Single-column, `INTEGER` only, equality filters only.
- **Index Catalog**: Stored in `db_schema` with `type='index'`.
- **Key Encoding**: Composite key: `[encoded_column_value][encoded_rowid]`.
  - Column Value: Big-endian `i64` with sign bit flipped (preserves sort order).
  - Rowid: Big-endian `u64`.
- **Value Encoding**: Stored as `[primary_key]` in the index B-tree value (for simplicity).
- **Maintenance**: INSERT updates all indexes for the table.
- **Query Optimization**: Planner detects applicable indexes for `WHERE col = literal` and generates `IndexScan` bytecode.
- **Prefix Matching**: Uses `MoveCursor(Find)` to position at the first candidate and `KeyMatchesPrefix` to verify.

## Makefile Targets

```bash
make big.db    # Create 1M-entry test database
make <name>.svg  # Generate B-tree visualization from .db file
```

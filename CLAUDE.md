# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A single-file relational database library in Rust, similar to SQLite. Implements a complete database engine from scratch with SQL parsing, query planning, a bytecode virtual machine, and B-tree storage.

## Build & Test Commands

```bash
cargo build              # Debug build
cargo build --release    # Release build
cargo test               # Run all tests
cargo test <test_name>   # Run single test
cargo test -- --nocapture  # Run tests with output
cargo run -- <db_file>   # Run interactive CLI
cargo fmt                # Format code
cargo fmt -- --check     # Check if code is formatted
```

## Development Practices

### Test-Driven Development (TDD)

- **Always use TDD where possible**: Write tests before or alongside implementation
- **Run tests before making changes**: Establish a baseline with `cargo test --bin database`
- **Run tests after making changes**: Verify nothing broke with `cargo test --bin database`
- **Add regression tests**: When fixing bugs, add tests that would have caught the bug

### Manual Testing

**Purpose**: Manual tests complement automated unit/integration tests by exercising the database through realistic end-to-end scenarios.

**Location**: Manual test scripts live in the repository root (e.g., `test_sql_mode.sql`)

**When to create manual tests**:
- End-to-end workflows that exercise multiple subsystems
- Interactive REPL scenarios that are hard to unit test
- Performance or stress tests that need visual inspection
- Regression tests for bugs that involve complex interactions
- Demonstrative examples that also serve as documentation

**Guidelines**:
1. **Script format**: Use `.sql` files for SQL mode tests, or shell scripts for complex scenarios
2. **Documentation**: Document each test in README.md's "Manual Tests" section
3. **Expected output**: Create or reference result files (e.g., `test_results.md`)
4. **Naming**: Use descriptive names like `test_sql_mode.sql`, `test_btree_stress.sql`
5. **Maintenance**: Keep manual tests working as features evolve
6. **Extension**: Add new tests as you implement features or find bugs

**Example manual test workflow**:
```bash
# Create a test script
vim test_new_feature.sql

# Run the test
cat test_new_feature.sql | ./target/release/database test.db > actual_output.txt

# Verify results (inspect output)
cat actual_output.txt

# Document in README.md
# Add expected results to version control if useful
```

**Existing manual tests**:
- `test_sql_mode.sql` - Comprehensive SQL mode test with 3 tables, 16 rows, 10 queries
- See README.md for complete list and usage

### Code Formatting

**IMPORTANT**: Always format code before committing to keep diffs clean and focused.

1. **Before starting work**: Run `cargo fmt -- --check` to verify code is formatted
   - If not formatted, run `cargo fmt` first and check the diff
   - This ensures your commits only contain relevant changes, not formatting noise

2. **Before committing**: Always run `cargo fmt` to format your changes
   - This keeps the codebase consistently formatted
   - Prevents formatting changes from polluting functional commits

### Git Workflow

**Small, focused commits**: Break work into logical, self-contained commits.

**Commit message style**: Use imperative mood, be concise but descriptive:
```
Fix infinite loop in lexer
Add support for WHERE clauses
Refactor expression compiler
```

**Fixup commits**: When you need to fix or update something from a previous unpushed commit:
- Consider using `git commit --fixup <commit-hash>`
- Then squash with `git rebase -i --autosquash` before pushing
- This keeps history clean without losing development context

**Standard workflow**:
```bash
cargo fmt -- --check        # Check formatting first
cargo test --bin database   # Run tests before changes
# ... make changes ...
cargo fmt                   # Format code
cargo test --bin database   # Run tests after changes
git add <files>
git commit -m "message"
```

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

## Makefile Targets

```bash
make big.db    # Create 1M-entry test database
make <name>.svg  # Generate B-tree visualization from .db file
```

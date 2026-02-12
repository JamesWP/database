# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A single-file relational database library in Rust, similar to SQLite. Implements a complete database engine from scratch with SQL parsing, query planning, a bytecode virtual machine, and B-tree storage.

## Build & Test Commands

```bash
cargo build              # Debug build
cargo build --release    # Release build
cargo test               # Run all tests (lib + integration + doctests) - THE BASELINE
cargo test --lib         # Run library unit tests only
cargo test --test sql_runner  # Run SQL integration tests
cargo test <test_name>   # Run single test
cargo test -- --nocapture  # Run tests with output
cargo run -- <db_file>   # Run interactive CLI
cargo fmt                # Format code
cargo fmt -- --check     # Check if code is formatted
```

### Test Organization

- **Unit tests**: Embedded in source files with `#[cfg(test)]` modules
- **Integration tests**: Located in `tests/` directory
- **SQL tests**: Automated end-to-end tests in `tests/sql/*.sql` with `.expected` files

## Development Practices

### Working Through Implementation Phases

**Task Tracking:**
- Use TaskCreate/TaskUpdate to track progress through phase items
- Mark dependencies between tasks (e.g., tests depend on implementation)
- Update task status: pending → in_progress → completed
- Check TaskList to see what's blocked and what's ready

**Verification at Each Step:**
```bash
# Before starting a task
cargo test  # Baseline: all tests (unit + integration + doctests)

# After completing implementation
cargo test  # Verify all tests pass
cargo build 2>&1 | grep -i warning  # Check warnings

# Before committing
cargo fmt  # Format code
git add <files>
git diff --cached --stat  # User review
# Wait for approval, then commit
```

**Pattern:** Code → Test → Format → Stage → Review → Commit → Next Item

### Test-Driven Development (TDD)

- **Always use TDD where possible**: Write tests before or alongside implementation
- **Run tests before making changes**: Establish a baseline with `cargo test`
- **Run tests after making changes**: Verify nothing broke with `cargo test`
- **Add regression tests**: When fixing bugs, add tests that would have caught the bug
- **Full test suite**: `cargo test` runs unit tests, integration tests, and doctests - all must pass

### Manual Testing

**Purpose**: Manual tests complement automated unit/integration tests by exercising the database through realistic end-to-end scenarios.

**Location**: Manual test scripts live in `manual_tests/` directory

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
vim manual_tests/test_new_feature.sql

# Run the test
cat manual_tests/test_new_feature.sql | ./target/release/database test.db > actual_output.txt

# Verify results (inspect output)
cat actual_output.txt

# Document in manual_tests/README.md
# Add expected results to version control if useful
```

**Existing manual tests**:
- See `manual_tests/README.md` for complete list and usage
- Example: `test_sql_mode.sql` - SQL mode test with 3 tables, 16 rows, 10 queries

### Code Formatting

**IMPORTANT**: Always format code before committing to keep diffs clean and focused.

1. **Before starting work**: Run `cargo fmt -- --check` to verify code is formatted
   - If not formatted, run `cargo fmt` first and check the diff
   - This ensures your commits only contain relevant changes, not formatting noise

2. **Before committing**: Always run `cargo fmt` to format your changes
   - This keeps the codebase consistently formatted
   - Prevents formatting changes from polluting functional commits

### Compiler Warnings

**CRITICAL**: Maintain a warning-free codebase at all times.

**Zero warnings policy**: The project must compile without warnings. Warnings indicate potential bugs, code smells, or technical debt.

**Before committing**:
```bash
# Check for warnings in debug build
cargo build 2>&1 | grep -i warning

# Check for warnings in release build
cargo build --release 2>&1 | grep -i warning

# If any warnings exist, fix them before committing
```

**Common warning types and how to handle them**:

1. **Unused variables**: Remove them or prefix with underscore if intentionally unused
   ```rust
   // Bad
   let result = compute();

   // Good - remove if truly unused
   compute();

   // Good - keep if needed for clarity
   let _result = compute();
   ```

2. **Unused imports**: Remove them
   ```rust
   // Bad
   use std::collections::HashMap;  // unused

   // Good - remove unused imports
   ```

3. **Dead code**: Remove unused functions/methods or add `#[allow(dead_code)]` if keeping for future use
   ```rust
   // If keeping for future, document why
   #[allow(dead_code)]
   fn helper_for_future_feature() { }
   ```

4. **Deprecated API usage**: Update to use the replacement API

**When introducing new code**:
- Write code that compiles cleanly from the start
- If you must temporarily allow warnings, add a TODO comment and issue
- Never commit code with new warnings

**Benefits**:
- Warnings often indicate bugs before they manifest
- Clean builds make real issues easier to spot
- Maintains high code quality standards
- Prevents warning fatigue

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
cargo build                 # Check for warnings before changes
cargo test                  # Run all tests before changes
# ... make changes ...
cargo fmt                   # Format code
cargo build                 # Check for new warnings
cargo test                  # Run all tests after changes
git add <files>
git commit -m "message"
```

### Multi-Item Phase Workflow

When working through a phase with multiple items (like Phase A, B, C, etc.):

**1. Commit Strategy**
- Commit each item separately with clear, descriptive messages
- If infrastructure changes (module exports, lib.rs changes) are needed, commit those first as a separate commit
- Keep commits focused and atomic - one logical change per commit

**2. Pre-Commit Review Process**
```bash
# Stage files for review BEFORE committing
git add <files>

# Show what's staged so user can review
git diff --cached --stat
git diff --cached <file>  # Show detailed changes

# Wait for user approval before committing
# User can review staged changes in IDE or with git diff

# After approval, commit
git commit -m "message"
```

**3. Phase Completion**
At the end of each phase:
- Run final verification: `cargo test` + `cargo build` (check warnings)
- Show phase summary: `git log --oneline -N` (all phase commits)
- Show total diff: `git diff master --stat`
- Update `doc/plan/README.md` test coverage section with new counts
- Suggest any relevant CLAUDE.md updates based on patterns learned

**Example Phase A Workflow:**
```bash
# Infrastructure changes first
git add src/lib.rs src/frontend.rs src/main.rs src/repl/
git diff --cached --stat  # Review
git commit -m "Refactor module structure..."

# Item 1: SQL test harness
git add tests/
git diff --cached --stat  # Review
git commit -m "Add SQL test harness..."

# Item 2: Safety fix
git add src/storage/cell_reader.rs
git diff --cached --stat  # Review
git commit -m "Fix CellReader unsafe pointer..."

# Item 3: Tests for Item 2
git add src/storage/cell.rs
git diff --cached --stat  # Review
git commit -m "Add comprehensive tests for Cell and CellReader..."

# ... continue for remaining items
```

**Benefits:**
- User can review changes before they're committed
- Clean, focused commit history
- Easy to understand what changed in each step
- Easy to cherry-pick or revert individual changes if needed

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

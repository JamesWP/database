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

# Phase V — README Overhaul

Rewrite `README.md` to reflect the current project state: accurate SQL feature coverage, architecture, test stats, and annotated terminal screenshots showing the REPL in action.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 91 | 7 | Capture terminal screenshots of key REPL sessions | — |
| 92 | 7 | Rewrite README with current features, architecture, and screenshots | 91 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

The current `README.md` was written early in the project and no longer reflects what the database can do. It references old test-runner syntax (removed in Phase K), omits SQL features added in phases D–U, and has no visual demonstration of the interactive REPL.

A prospective user or contributor reading the README today would not know that the database supports:

- Secondary indexes and index-accelerated queries
- GROUP BY / HAVING / ORDER BY / DISTINCT / LIMIT
- DELETE with WHERE, UPDATE with WHERE
- EXPLAIN query plans
- An interactive mode-based REPL with btree/parser/planner/engine debug modes
- 200+ passing tests and an inline SQL test harness

This phase produces a README that accurately describes the project, shows real terminal output, and serves as the project's public face.

---

## 91. Capture Terminal Screenshots (Track 7)

### What Changes

A new directory `doc/screenshots/` holds PNG (or SVG) captures of REPL sessions. Screenshots are referenced from the README. At minimum, capture:

| File | Content |
|------|---------|
| `doc/screenshots/repl-sql.png` | Basic SQL session: CREATE TABLE, INSERT, SELECT with column headers |
| `doc/screenshots/repl-where-index.png` | SELECT with WHERE using an index (EXPLAIN shows IndexScan) |
| `doc/screenshots/repl-explain.png` | EXPLAIN output for a SELECT with GROUP BY |
| `doc/screenshots/repl-modes.png` | `modes` command listing btree / parser / planner / engine |
| `doc/screenshots/repl-btree.png` | btree mode: create table, insert, print data |

### Background

Screenshots show colour output (column headers bold-white, divider lines dimmed) that cannot be conveyed in plain-text code blocks. They give the README a polished appearance and let readers immediately see what the REPL looks like.

### Implementation Approach

Use a terminal emulator that supports 256-colour ANSI (e.g. iTerm2, Kitty, Windows Terminal) and a screenshot tool:

**Option A — `vhs` (recommended)**: [VHS](https://github.com/charmbracelet/vhs) is a terminal GIF/PNG recorder driven by a `.tape` script. Write one tape file per screenshot, commit the `.tape` files alongside the PNG output so screenshots can be regenerated.

```bash
# Install vhs (once)
brew install vhs          # macOS
# or download release binary for Linux

# Record a session
vhs doc/screenshots/sql-session.tape
```

Example tape file (`doc/screenshots/sql-session.tape`):

```
Output doc/screenshots/repl-sql.png

Set FontSize 14
Set Width 900
Set Height 500
Set Theme "Catppuccin Mocha"

Type "cargo run -- demo.db"
Sleep 500ms
Enter
Sleep 1s

Type "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)"
Enter
Sleep 500ms

Type "INSERT INTO users VALUES (1, 'alice', 30)"
Enter
Sleep 300ms

Type "INSERT INTO users VALUES (2, 'bob', 25)"
Enter
Sleep 300ms

Type "SELECT * FROM users WHERE age > 20"
Enter
Sleep 500ms

Type "exit"
Enter
Sleep 300ms
```

**Option B — manual screenshot**: Run `cargo run -- demo.db`, execute the session manually, capture a screenshot of the terminal window. Crop to the relevant area.

Either approach is acceptable. Commit the resulting PNG files under `doc/screenshots/`.

### Key Files

- `doc/screenshots/` — new directory
- `doc/screenshots/*.tape` — VHS tape scripts (if using VHS)
- `doc/screenshots/*.png` — captured images

### Tests

No automated tests. Visual review: confirm each screenshot shows the output described in the table above.

### Implementation Steps (1 commit)

#### Step 91.1 — Add terminal screenshots

Run the REPL sessions, capture PNGs, commit to `doc/screenshots/`.

**Commit:** Docs: add terminal screenshots for README

---

## 92. Rewrite README (Track 7)

### What Changes

`README.md` is rewritten top to bottom. Below is the target structure and content. The implementer should fill in accurate numbers (test count, supported SQL) by checking the current state of the codebase before writing.

### Target Structure

```markdown
# database

> A single-file relational database engine written in Rust, built from scratch.

![REPL screenshot](doc/screenshots/repl-sql.png)

One-paragraph description: what it is, what it implements (SQL parser, query
planner, bytecode VM, B-tree storage), what it is similar to (SQLite), current
status.

## Features

### SQL

| Feature | Status |
|---------|--------|
| CREATE TABLE | ✓ |
| INSERT INTO … VALUES | ✓ |
| INSERT INTO … SELECT | ✓ |
| SELECT with column list / SELECT * | ✓ |
| WHERE (comparison, AND, OR, NOT) | ✓ |
| ORDER BY (ASC / DESC, multi-column) | ✓ |
| GROUP BY | ✓ |
| HAVING | ✓ |
| DISTINCT | ✓ |
| LIMIT | ✓ |
| COUNT(*) / COUNT(col) aggregate | ✓ |
| DELETE FROM … WHERE | ✓ |
| UPDATE … SET … WHERE | ✓ |
| CREATE INDEX (single-column INTEGER) | ✓ |
| Index-accelerated equality scans | ✓ |
| EXPLAIN | ✓ |
| DROP TABLE | — |
| JOINs | — |
| NULL / IS NULL | — |
| Subqueries | — |

### Storage

- Custom B-tree engine with CBOR-encoded rows
- Page-based file format (4 KB pages), overflow chains for large values
- Secondary indexes as separate B-trees
- Persistent storage — survives process restart

### Testing

- NNN unit and integration tests (`cargo test`)
- Inline SQL test harness: `.sql` files with `-- >` expected output lines
- Property-based tests for B-tree invariants (proptest)

## Quick Start

\`\`\`bash
git clone https://github.com/…/database
cd database
cargo run -- mydb.db
\`\`\`

\`\`\`
db> CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)
Table 'users' created
db> INSERT INTO users VALUES (1, 'alice', 30)
1 row inserted
db> INSERT INTO users VALUES (2, 'bob', 25)
1 row inserted
db> SELECT * FROM users WHERE age > 20 ORDER BY name
┌────┬───────┬─────┐
│ id │ name  │ age │
├────┼───────┼─────┤
│ 1  │ alice │ 30  │
│ 2  │ bob   │ 25  │
└────┴───────┴─────┘
\`\`\`

## Build & Test

\`\`\`bash
cargo build              # Debug build
cargo build --release    # Release build
cargo test               # All NNN tests
cargo test test_sql_     # SQL integration tests only
cargo run -- <db_file>   # Interactive REPL
\`\`\`

## Architecture

\`\`\`
SQL Text
  │
  ▼
Lexer / Parser  (src/frontend/)   ── produces AST
  │
  ▼
Planner         (src/planner.rs)  ── produces LogicalPlan
  │
  ▼
Compiler        (src/compiler/)   ── produces bytecode Program
  │
  ▼
Engine / VM     (src/engine/)     ── executes bytecode
  │
  ▼
B-tree / Pager  (src/storage/)    ── reads/writes .db file
\`\`\`

Brief paragraph on each layer (2-3 sentences).

## Interactive REPL

The REPL exposes several debug modes in addition to the default SQL mode.

### SQL mode (default)

![SQL session](doc/screenshots/repl-sql.png)

\`\`\`
db> SELECT name, age FROM users WHERE age > 20
…
\`\`\`

### EXPLAIN

![EXPLAIN output](doc/screenshots/repl-explain.png)

\`\`\`
db> EXPLAIN SELECT name FROM users WHERE age = 30
…
\`\`\`

### Inspection modes

![Mode list](doc/screenshots/repl-modes.png)

| Mode | Purpose |
|------|---------|
| `btree` | Inspect B-tree pages, cursors, raw data |
| `parser` | Tokenize and parse SQL; show AST |
| `planner` | Show logical plan for a query |
| `engine` | Compile to bytecode and inspect program |

![btree mode](doc/screenshots/repl-btree.png)

### Non-interactive usage

\`\`\`bash
cargo run -- mydb.db sql "SELECT * FROM users"
cargo run -- mydb.db parser parse "SELECT id FROM users WHERE age > 18"
cargo run -- mydb.db planner plan "SELECT name FROM users WHERE id = 1"
cargo run -- mydb.db btree inspect all
\`\`\`

## References

- B-tree design: https://cglab.ca/~abeinges/blah/rust-btree-case/
- SQLite file format: https://www.sqlite.org/fileformat.html
```

### Background

The key improvements over the current README:

1. **Feature table** — readers can quickly see what SQL is and isn't supported.
2. **Screenshots** — visual proof that the REPL works and looks polished.
3. **Architecture diagram** — text-art pipeline replaces the one-liner.
4. **Accurate test count** — updated after each phase; currently 200+.
5. **Removed stale content** — old `SQL_TEST_FILE` syntax, references to `.expected` files, outdated REPL output.

### Implementation Approach

1. Check actual test count: `cargo test 2>&1 | tail -5`.
2. Verify which SQL features exist by skimming `src/frontend/parser.rs` keyword coverage and `tests/sql/` filenames.
3. Confirm table rendering exists (Phase P) — use real REPL output for the Quick Start block.
4. Write the README following the target structure above.
5. Reference screenshots committed in item 91.

### Key Files

- `README.md` — full rewrite
- `doc/screenshots/*.png` — referenced images (from item 91)

### Tests

No automated tests. Manual review checklist:
- All SQL feature table rows reflect actual implementation status.
- Quick Start code block uses real REPL output (no fabricated output).
- All screenshot references point to files that exist in `doc/screenshots/`.
- `cargo test` count in the Testing section matches `cargo test 2>&1 | tail -5`.

### Implementation Steps (1 commit)

#### Step 92.1 — Rewrite README.md

Replace `README.md` with the new content. Verify all screenshot links resolve.

**Commit:** Docs: rewrite README with current features, architecture, and screenshots

---

## Verification

- [ ] `doc/screenshots/` exists and contains all referenced PNG files
- [ ] Every `![…](doc/screenshots/…)` link in README.md resolves to an existing file
- [ ] SQL feature table accurately reflects the codebase (spot-check 3–5 features)
- [ ] Test count in README matches `cargo test 2>&1 | tail -5`
- [ ] Quick Start REPL output is real output from `cargo run -- demo.db`, not fabricated
- [ ] `cargo build` — zero warnings (README changes don't affect Rust)

# Phase AH — Decouple `colored` from Core Library

Remove the `colored` dependency from the `database` library crate by making all
`Display` implementations plain text and moving colored formatting into newtype
wrappers in `crates/database-cli`.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 123 | 6 | Plain `ScalarValue::Display`; `ColoredScalarValue` wrapper in CLI | Phase AG |
| 124 | 6 | Plain `Reg`, `JumpTarget`, `Operation::Display`; colored wrappers in CLI; update engine mode and TUI debugger | 123 |
| 125 | 6 | Remove `colored` from `btree.rs` inspect output | 124 |
| 126 | 6 | Drop `colored` from library `Cargo.toml`; verify clean build | 125 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

After Phase AG, the `database` library crate still depends on `colored` because
three core files use it for terminal output:

| File | Usage |
|------|-------|
| `src/engine/scalarvalue.rs` | `ScalarValue::Display` — colorizes integers, strings, etc. |
| `src/engine/program.rs` | `Display` for `Reg`, `JumpTarget`, `Operation` — colorized bytecode listing |
| `src/storage/btree.rs` | `inspect`/`print_page` diagnostic output — coloured page dump |

An application that embeds `database` does not need terminal color output at all.
Colors are a presentation concern and belong in the CLI layer.

**The approach**: make every `Display` impl in the library produce plain text.
Add a `crates/database-cli/src/display.rs` module with newtype wrappers that
re-implement the colored formatting for the REPL and TUI debugger.

```
Before:  format!("{op}")              → ANSI-colored string from library Display
After:   format!("{op}")              → plain text
         format!("{}", ColoredOp(op)) → ANSI-colored string from CLI wrapper
```

The TUI debugger uses `ansi-to-tui` to convert ANSI strings to ratatui widgets.
After this phase it continues to work: it calls `format!("{}", ColoredOp(op))`
instead of `format!("{op}")`, producing the same ANSI output.

---

## 123. Plain `ScalarValue::Display`; `ColoredScalarValue` wrapper in CLI (Track 6)

### What Changes

1. **`src/engine/scalarvalue.rs`**: Remove `use colored::Colorize;` from the
   `Display` impl.  Make it plain text:

   ```rust
   // Before
   ScalarValue::Integer(i)  => write!(f, "{}", i.to_string().green()),
   ScalarValue::String(s)   => write!(f, "{}", format!("\"{}\"", s).green()),
   ScalarValue::Floating(f) => write!(f, "{}", fl.to_string().green()),
   ScalarValue::Boolean(b)  => write!(f, "{}", b.to_string().green()),
   ScalarValue::Blob(b)     => write!(f, "{}", format!("Blob({})", b.len()).green()),
   ScalarValue::Null        => write!(f, "NULL"),

   // After
   ScalarValue::Integer(i)  => write!(f, "{i}"),
   ScalarValue::String(s)   => write!(f, "\"{s}\""),
   ScalarValue::Floating(fl)=> write!(f, "{fl}"),
   ScalarValue::Boolean(b)  => write!(f, "{b}"),
   ScalarValue::Blob(b)     => write!(f, "Blob({})", b.len()),
   ScalarValue::Null        => write!(f, "NULL"),
   ```

2. **`crates/database-cli/src/display.rs`** (new file): Add `ColoredScalarValue`
   wrapper with the original colored formatting:

   ```rust
   use colored::Colorize as _;
   use database::engine::scalarvalue::ScalarValue;
   use std::fmt;

   pub struct ColoredScalarValue<'a>(pub &'a ScalarValue);

   impl fmt::Display for ColoredScalarValue<'_> {
       fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
           use ScalarValue::*;
           match self.0 {
               Integer(i)  => write!(f, "{}", i.to_string().green()),
               Floating(fl)=> write!(f, "{}", fl.to_string().green()),
               Boolean(b)  => write!(f, "{}", b.to_string().green()),
               String(s)   => write!(f, "{}", format!("\"{}\"", s).green()),
               Blob(b)     => write!(f, "{}", format!("Blob({})", b.len()).green()),
               Null        => write!(f, "NULL"),
           }
       }
   }
   ```

3. Update usages in `crates/database-cli/src/repl/modes/engine.rs` and
   `crates/database-cli/src/repl/tui_debugger.rs` to use `ColoredScalarValue(&v)`
   wherever `format!("{v}")` was emitting colored scalar output.

   In `tui_debugger.rs` the register pane (around line 294) does:
   ```rust
   // Before
   RegisterValue::ScalarValue(s) => format!("  r{i} = {s}"),
   // After
   RegisterValue::ScalarValue(s) => format!("  r{i} = {}", ColoredScalarValue(s)),
   ```

   In `engine.rs` mode (around line 67 and 177):
   ```rust
   // Before
   RegisterValue::ScalarValue(s) => format!("{s}"),
   // After
   RegisterValue::ScalarValue(s) => format!("{}", ColoredScalarValue(s)),
   ```

### Key Files

- `src/engine/scalarvalue.rs` — plain Display
- `crates/database-cli/src/display.rs` — new, `ColoredScalarValue`
- `crates/database-cli/src/repl/modes/engine.rs` — use wrapper
- `crates/database-cli/src/repl/tui_debugger.rs` — use wrapper
- `crates/database-cli/src/main.rs` (or `lib.rs`) — `mod display;`

### Tests

```bash
cargo build --workspace
cargo test --workspace
# Verify scalar values still print correctly in REPL and TUI
cargo run -p database-cli -- test.db sql "SELECT 1, 'hello', 3.14"
```

### Implementation Steps (1 commit)

#### Step 123.1 — Plain ScalarValue Display; ColoredScalarValue in CLI

**Commit:** `Refactor: plain ScalarValue::Display, move colored formatting to CLI wrapper`

---

## 124. Plain `Reg`, `JumpTarget`, `Operation::Display`; colored wrappers; update REPL and TUI (Track 6)

### What Changes

1. **`src/engine/program.rs`**: Remove `use colored::Colorize;` from all three
   `Display` impls and strip color calls:

   ```rust
   // Reg — Before
   write!(f, "{}", format!("R{}", self.0).yellow())
   // After
   write!(f, "R{}", self.0)

   // JumpTarget — Before
   JumpTarget::Resolved(addr) => write!(f, "{}", format!("@{}", addr).magenta()),
   JumpTarget::Unresolved(label) => write!(f, "{}", format!("?L{}", label.0).red()),
   // After
   JumpTarget::Resolved(addr) => write!(f, "@{addr}"),
   JumpTarget::Unresolved(label) => write!(f, "?L{}", label.0),

   // Operation — Before (example)
   StoreValue(r, v) => write!(f, "{:10} {}, {}", "Store".cyan().bold(), r, v),
   // After
   StoreValue(r, v) => write!(f, "{:10} {}, {}", "Store", r, v),
   ```

   All color calls in the `Operation::Display` match arm are removed — leave the
   mnemonic strings and structure intact, just without `.cyan().bold()` etc.

2. **`crates/database-cli/src/display.rs`**: Add `ColoredReg`, `ColoredJumpTarget`,
   `ColoredOperation` wrappers that mirror the colored formatting:

   ```rust
   pub struct ColoredReg(pub Reg);
   impl fmt::Display for ColoredReg {
       fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
           write!(f, "{}", format!("R{}", self.0.0).yellow())
       }
   }

   pub struct ColoredJumpTarget<'a>(pub &'a JumpTarget);
   impl fmt::Display for ColoredJumpTarget<'_> {
       fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
           match self.0 {
               JumpTarget::Resolved(addr) =>
                   write!(f, "{}", format!("@{addr}").magenta()),
               JumpTarget::Unresolved(label) =>
                   write!(f, "{}", format!("?L{}", label.0).red()),
           }
       }
   }

   pub struct ColoredOperation<'a>(pub &'a Operation);
   impl fmt::Display for ColoredOperation<'_> {
       fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
           use Operation::*;
           match self.0 {
               StoreValue(r, v) =>
                   write!(f, "{:10} {}, {}", "Store".cyan().bold(),
                          ColoredReg(*r), v),
               // ... one arm per Operation variant, mirroring program.rs ...
           }
       }
   }
   ```

   `ColoredOperation` internally uses `ColoredReg` and `ColoredJumpTarget` so that
   registers and jump targets within an instruction listing are still coloured.

3. **Update CLI usages**:

   In `crates/database-cli/src/repl/modes/engine.rs`:
   ```rust
   // Before (program listing)
   output += &format!("{}  {}\n", format!("{:4}", i).dimmed(), op);
   // After
   output += &format!("{}  {}\n", format!("{:4}", i).dimmed(), ColoredOperation(op));

   // Before (step output)
   .map(|o| format!("{o}"))
   // After
   .map(|o| format!("{}", ColoredOperation(o)))
   ```

   In `crates/database-cli/src/repl/tui_debugger.rs`:
   ```rust
   // Before (program pane, line ~264)
   let ansi_str = format!("{prefix}{i:4}  {op}");
   // After
   let ansi_str = format!("{prefix}{i:4}  {}", ColoredOperation(op));
   ```

   The `ansi_str.into_text()` call on the next line continues to work unchanged
   because `ColoredOperation` still emits ANSI escape codes via `colored`.

### Key Files

- `src/engine/program.rs` — plain Display for `Reg`, `JumpTarget`, `Operation`
- `crates/database-cli/src/display.rs` — add `ColoredReg`, `ColoredJumpTarget`, `ColoredOperation`
- `crates/database-cli/src/repl/modes/engine.rs` — use wrappers
- `crates/database-cli/src/repl/tui_debugger.rs` — use wrappers

### Tests

```bash
cargo build --workspace
cargo test --workspace
# REPL engine mode should still show colored bytecode
cargo run -p database-cli -- test.db engine compile "SELECT 1"
cargo run -p database-cli -- test.db engine program
# TUI debugger should still show colored register and program panes
cargo run -p database-cli -- test.db  # then: enter engine, compile ..., tui
```

### Implementation Steps (1 commit)

#### Step 124.1 — Plain program.rs Display; colored wrappers; update engine mode and TUI

**Commit:** `Refactor: plain Operation/Reg/JumpTarget Display, move colored formatting to CLI wrappers`

---

## 125. Remove `colored` from `btree.rs` inspect output (Track 6)

### What Changes

**`src/storage/btree.rs`**: The `print_page` / inspect method contains ~30 lines
of `println!` calls using `colored`.  Replace all color calls with plain text:

```rust
// Before
println!("{}: {}", "Type".yellow(), "ZeroPage".green());
println!("    {}={}", "key".cyan(), key_hex);
println!("    {}={}", "continuation".cyan(), "None".bright_black());

// After
println!("Type: ZeroPage");
println!("    key={key_hex}");
println!("    continuation=None");
```

Remove `use colored::Colorize;` from `btree.rs`.

**Note**: The inspect functionality lives entirely within `BTree` as a method, not
in the REPL layer.  Moving it to the CLI is a worthwhile long-term goal (it's a
diagnostic tool, not a library concern) but is out of scope for this phase.  Plain
text output is a sufficient improvement for now.

### Key Files

- `src/storage/btree.rs` — remove colored, plain `println!`

### Tests

```bash
cargo build --workspace
cargo run -p database-cli -- test.db btree inspect page 0
cargo run -p database-cli -- test.db btree inspect all
```

Output should still be readable; just no ANSI colours.

### Implementation Steps (1 commit)

#### Step 125.1 — Remove colored from btree.rs inspect output

**Commit:** `Refactor: remove colored from btree inspect — plain text output`

---

## 126. Drop `colored` from library `Cargo.toml`; verify clean build (Track 6)

### What Changes

**`Cargo.toml`** (library root): Remove the `colored` line from `[dependencies]`.

```toml
# Remove:
colored = "2"
```

`colored` remains in `crates/database-cli/Cargo.toml` — it was already added
there in Phase AG (Item 121).

After this change, `cargo build -p database` (library only) must succeed with
zero warnings and without pulling in any terminal-color dependency.

### Key Files

- `Cargo.toml` — remove `colored`

### Tests

```bash
cargo build -p database           # library only — must succeed
cargo build --workspace           # both crates
cargo test --workspace            # all tests pass
# Confirm no colored dep in library build graph:
cargo tree -p database | grep colored  # should print nothing
```

### Implementation Steps (1 commit)

#### Step 126.1 — Remove colored from library Cargo.toml

**Commit:** `Refactor: remove colored dependency from database library crate`

---

## Verification

- [ ] `cargo build -p database` — zero errors, zero warnings
- [ ] `cargo tree -p database | grep colored` — empty (no colored in library graph)
- [ ] `cargo build --workspace` — clean
- [ ] `cargo test --workspace` — all tests pass, no regressions
- [ ] `cargo fmt --all && cargo build --workspace 2>&1 | grep -i warning` — zero warnings
- [ ] REPL engine mode shows colored bytecode listing (`compile <sql>` + `program`)
- [ ] TUI debugger shows colored program and register panes
- [ ] `btree inspect page 0` output is readable plain text
- [ ] Each commit is independently buildable

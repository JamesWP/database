# Phase Q — Macro-Based Bytecode Program Builder

Replace manual `vec![Operation::...]` construction in engine tests with a concise `program!` declarative macro, making test programs readable and reducing boilerplate.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 73 | 7 | Define `program!` macro in a test-only module | — |
| 74 | 7 | Port existing engine unit tests to use the macro | 73 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

Engine unit tests build bytecode programs by hand:

```rust
let mut harness = TestHarness::new(
    &[
        Operation::StoreValue(Reg::new(0), ScalarValue::Integer(42)),
        Operation::AddValue(Reg::new(2), Reg::new(0), Reg::new(1)),
        Operation::Yield(vec![Reg::new(2)]),
        Operation::Halt,
    ],
    3,
);
```

This is verbose and noisy. A declarative macro cuts the ceremony:

```rust
let mut harness = program! {
    store r0 = 42;
    add r2 = r0 + r1;
    yield r2;
    halt;
};
```

The macro handles:
- `r<N>` → `Reg::new(N)` conversion
- `ScalarValue` literal inference from Rust literals
- `num_registers` counting (max register index + 1)
- Expansion to `TestHarness::new(&[...], N)`

The macro lives in `src/engine.rs` under `#[cfg(test)]` — no separate crate, no proc macro complexity. A standard `macro_rules!` macro is sufficient.

---

## 73. Define `program!` Macro (Track 7)

### What Changes

A `macro_rules! program` is added inside the `#[cfg(test)]` block of `src/engine.rs` (or a new `src/engine/test_macros.rs` imported only under `#[cfg(test)]`).

### Macro Grammar

```
program! {
    <stmt>;
    ...
}
```

Where `<stmt>` is one of:

| Syntax | Expands to |
|--------|-----------|
| `store rN = <int>` | `Operation::StoreValue(r(N), ScalarValue::Integer(<int>))` |
| `store rN = <float>f` | `Operation::StoreValue(r(N), ScalarValue::Floating(<float>))` |
| `store rN = "<str>"` | `Operation::StoreValue(r(N), ScalarValue::String("<str>".into()))` |
| `store rN = null` | `Operation::StoreValue(r(N), ScalarValue::Null)` |
| `add rN = rA + rB` | `Operation::AddValue(r(N), r(A), r(B))` |
| `sub rN = rA - rB` | `Operation::SubtractValue(r(N), r(A), r(B))` |
| `mul rN = rA * rB` | `Operation::MultiplyValue(r(N), r(A), r(B))` |
| `div rN = rA / rB` | `Operation::DivideValue(r(N), r(A), r(B))` |
| `eq rN = rA == rB` | `Operation::EqualsValue(r(N), r(A), r(B))` |
| `lt rN = rA < rB` | `Operation::LessThanValue(r(N), r(A), r(B))` |
| `gt rN = rA > rB` | `Operation::GreaterThanValue(r(N), r(A), r(B))` |
| `and rN = rA && rB` | `Operation::AndValue(r(N), r(A), r(B))` |
| `not rN = rA` | `Operation::NotValue(r(N), r(A))` |
| `copy rN = rA` | `Operation::CopyValue(r(N), r(A))` |
| `yield rA rB ...` | `Operation::Yield(vec![r(A), r(B), ...])` |
| `halt` | `Operation::Halt` |

`num_registers` is computed as `max_register_index + 1` by scanning the expanded operations.

### Implementation Approach

Since computing `num_registers` at macro expansion time requires knowing all registers mentioned, the macro first collects all operations into a `Vec`, then scans for the maximum `Reg` index:

```rust
macro_rules! program {
    ($($stmt:tt)*) => {{
        let ops: Vec<crate::engine::program::Operation> = vec![
            $(program_op!($stmt)),*
        ];
        let num_regs = ops.iter()
            .flat_map(|op| op.registers())   // helper that yields all Reg refs
            .map(|r| r.index() + 1)
            .max()
            .unwrap_or(1);
        TestHarness::new_owned(ops, num_regs)
    }};
}
```

`operation.registers()` is a helper method added to `Operation` that yields an iterator of all `Reg` values contained in a given operation. This avoids duplicating the register-scanning logic.

Helper sub-macro `program_op!` handles individual statement forms:

```rust
macro_rules! program_op {
    (store $dst:ident = $val:literal) => {
        Operation::StoreValue(reg!($dst), ScalarValue::Integer($val))
    };
    // ... other forms ...
    (halt) => { Operation::Halt };
}
```

And `reg!` converts `r0`, `r1`, etc.:

```rust
macro_rules! reg {
    (r0) => { Reg::new(0) };
    (r1) => { Reg::new(1) };
    // etc. up to r15 or use a tt → literal trick
}
```

A simpler alternative to the `reg!` helper: require callers to write `r!(0)` and define `macro_rules! r { ($n:expr) => { Reg::new($n) } }`. This is less pretty but trivially correct and avoids the `r0`/`r1` token-to-number mapping.

For the initial implementation, use `r!(N)` syntax and focus on correctness:

```rust
let mut harness = program! {
    store r!(0) = 42i64;
    yield r!(0);
    halt;
};
```

### Key Files

- `src/engine.rs` — add inside `#[cfg(test)]` module

### Tests

The macro is self-tested by compiling (macro expansion errors → compile errors). Add one explicit test:

```rust
#[test]
fn test_program_macro_basic() {
    let mut harness = program! {
        store r!(0) = 7i64;
        store r!(1) = 3i64;
        add r!(2) = r!(0) + r!(1);
        yield r!(2);
        halt;
    };
    let rows = harness.run_to_completion();
    assert_eq!(rows[0][0], ScalarValue::Integer(10));
}
```

### Implementation Steps (1 commit)

#### Step 73.1 — Define `program!` macro with basic operations

Implement `program!`, `program_op!`, and `r!` macros in `src/engine.rs` under `#[cfg(test)]`. Add `Operation::registers()` helper. Add `TestHarness::new_owned` variant if needed. Add smoke test.

**Commit:** Tests: add program! macro for building engine test programs

---

## 74. Port Existing Engine Tests to `program!` (Track 7)

### What Changes

All engine unit tests in `src/engine.rs` that build `Operation` slices manually are converted to use `program!`. Each test is converted one-for-one; no logic changes.

### Background

The engine unit tests (around lines 815–1100 in `src/engine.rs`) cover:

- `StoreValue`, `AddValue`, `SubtractValue`, arithmetic operations
- `EqualsValue`, comparison chains
- `GoToIfFalse` loops
- `RowBuffer` / sort operations
- `GroupTable` / aggregate operations
- `WriteCursor` / `ReadCursor` / cursor operations

Cursor and RowBuffer operations are more complex (multi-register, enum args) and may not map cleanly to simple macro tokens. For those, keep the manual `vec!` form or extend the macro. The goal is to reduce boilerplate for the simple arithmetic/value tests, which make up the majority.

### Conversion Example

Before:

```rust
#[test]
fn test_add_integers() {
    let mut harness = TestHarness::new(
        &[
            Operation::StoreValue(Reg::new(0), ScalarValue::Integer(3)),
            Operation::StoreValue(Reg::new(1), ScalarValue::Integer(4)),
            Operation::AddValue(Reg::new(2), Reg::new(0), Reg::new(1)),
            Operation::Yield(vec![Reg::new(2)]),
            Operation::Halt,
        ],
        3,
    );
    // ...
}
```

After:

```rust
#[test]
fn test_add_integers() {
    let mut harness = program! {
        store r!(0) = 3i64;
        store r!(1) = 4i64;
        add   r!(2) = r!(0) + r!(1);
        yield r!(2);
        halt;
    };
    // ...
}
```

Tests involving cursor ops, `MoveOperation` enums, or `AggregateSpec` structs are left in manual form with a comment (`// complex operation — manual construction`).

### Key Files

- `src/engine.rs` — `#[cfg(test)]` block, all test functions

### Tests

`cargo test` must pass identically before and after. No test logic is changed.

### Implementation Steps (1 commit)

#### Step 74.1 — Port engine unit tests to program! macro

Convert all simple arithmetic/value tests. Leave complex cursor/aggregate tests in manual form. Verify `cargo test` passes.

**Commit:** Tests: port engine unit tests to program! macro

---

## Verification

- [ ] Tests pass: `cargo test`
- [ ] Zero warnings: `cargo fmt && cargo build 2>&1 | grep -i warning`
- [ ] Each commit is independently testable
- [ ] `program!` macro compiles with no warnings
- [ ] Test count is unchanged (no tests removed, only reformatted)
- [ ] `test_program_macro_basic` exercises arithmetic through the macro end-to-end

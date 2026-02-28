# Phase Q — Bytecode Emit Ergonomics

Reduce boilerplate in `nodes.rs` by adding `label_here`/`bind_here` helpers to `BytecodeEmitter` and `init!`/`body!` macros that eliminate the `emit(Operation::` wrapper at every call site.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 73 | 7 | Add `label_here` and `bind_here` to `BytecodeEmitter` | — |
| 74 | 7 | Add `init!` and `body!` macros; port `nodes.rs` | 73 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

`src/compiler/nodes.rs` builds bytecode by calling `emitter.emit(Operation::Foo(...))` at every step. Two kinds of noise dominate:

**Label management** — creating and immediately binding a label takes two lines:
```rust
let check_label = ctx.body_emitter.create_label();
ctx.body_emitter.bind_label(check_label);
```

**Emit wrapper** — every operation is wrapped in `emit(Operation::...)`:
```rust
ctx.body_emitter.emit(Operation::CanReadCursor(flag_reg, cursor_reg));
ctx.body_emitter.emit(Operation::MoveCursor(cursor_reg, MoveOperation::Next));
ctx.body_emitter.emit_goto_if_false(cont.on_done, flag_reg);
```

Two small additions eliminate most of this:

1. `label_here()` / `bind_here()` on `BytecodeEmitter` — one-line label create-and-bind.
2. `init!` / `body!` macros — collapse consecutive emit calls into a single block, with built-in support for goto ops and inline label binding.

No Operation variants are renamed. No macro_rules complexity beyond straightforward recursive arm matching.

---

## 73. `label_here` and `bind_here` on `BytecodeEmitter` (Track 7)

### What Changes

Two new methods on `BytecodeEmitter` in `src/compiler/emitter.rs`:

```rust
/// Create a label and immediately bind it at the current position.
pub fn label_here(&mut self) -> Label {
    let label = self.create_label();
    self.bind_label(label);
    label
}

/// Bind a label at the current position (alias for bind_label with clearer intent).
pub fn bind_here(&mut self, label: Label) {
    self.bind_label(label);
}
```

`bind_here` is a simple rename-alias. It reads better at call sites where the intent is "bind this label *here*, right now", as opposed to `bind_label` which sounds like it could take a position argument.

### Background

The current pattern in `nodes.rs` for an immediate bind appears ~8 times:

```rust
let check_label = ctx.body_emitter.create_label();
ctx.body_emitter.bind_label(check_label);
```

With `label_here()` this becomes one line:

```rust
let check_label = ctx.body_emitter.label_here();
```

Forward-ref labels (created before `codegen()` is called, bound later) keep the two-step `create_label` + `bind_here` pattern — no change needed there.

### Key Files

- `src/compiler/emitter.rs` — add two methods to `BytecodeEmitter`

### Tests

Existing emitter unit tests cover `create_label` + `bind_label`. Add two tests:

```rust
#[test]
fn test_label_here() {
    let mut emitter = BytecodeEmitter::new();
    emitter.emit(Operation::Halt); // op 0
    let label = emitter.label_here(); // should point to op 1
    emitter.emit(Operation::Halt); // op 1
    let ops = emitter.finalize();
    assert_eq!(ops.len(), 2);
    // label_here binds at position 1 — verify by using it as a goto target
}

#[test]
fn test_bind_here() {
    let mut emitter = BytecodeEmitter::new();
    let label = emitter.create_label();
    emitter.emit(Operation::Halt);
    emitter.bind_here(label); // bind at position 1
    // verify same as bind_label
}
```

### Implementation Steps (1 commit)

#### Step 73.1 — Add `label_here` and `bind_here` to `BytecodeEmitter`

Add both methods. Add tests. Run `cargo test`.

**Commit:** `Refactor: add label_here and bind_here to BytecodeEmitter`

---

## 74. `init!` / `body!` Macros and Port `nodes.rs` (Track 7)

### What Changes

Two `macro_rules!` macros added to `src/compiler/mod.rs` (or a new `src/compiler/macros.rs` re-exported from `mod.rs`):

- `init!(ctx; Op(args); Op(args); ...)` — emits to `ctx.init_emitter`
- `body!(ctx; Op(args); Op(args); ...)` — emits to `ctx.body_emitter`

Both support goto ops and inline label binding via `Bind(label)`.

All codegen functions in `src/compiler/nodes.rs` are ported to use the macros where consecutive emits appear.

### Implementation Approach

The macros use recursive `macro_rules!` arms. Each arm matches one statement form, emits it, then recurses on the tail:

```rust
macro_rules! body {
    // Base case
    ($ctx:expr $(;)?) => {};

    // Inline label bind: Bind(label)
    ($ctx:expr; Bind($label:expr) $(; $($rest:tt)*)?) => {
        $ctx.body_emitter.bind_here($label);
        $(body!($ctx; $($rest)*);)?
    };

    // GoTo — routes to emit_goto for label resolution
    ($ctx:expr; GoTo($label:expr) $(; $($rest:tt)*)?) => {
        $ctx.body_emitter.emit_goto($label);
        $(body!($ctx; $($rest)*);)?
    };

    // GoToIfFalse
    ($ctx:expr; GoToIfFalse($label:expr, $reg:expr) $(; $($rest:tt)*)?) => {
        $ctx.body_emitter.emit_goto_if_false($label, $reg);
        $(body!($ctx; $($rest)*);)?
    };

    // GoToIfEqualValue
    ($ctx:expr; GoToIfEqualValue($label:expr, $a:expr, $b:expr) $(; $($rest:tt)*)?) => {
        $ctx.body_emitter.emit_goto_if_equal($label, $a, $b);
        $(body!($ctx; $($rest)*);)?
    };

    // General Operation
    ($ctx:expr; $op:ident($($arg:expr),*) $(; $($rest:tt)*)?) => {
        $ctx.body_emitter.emit(Operation::$op($($arg),*));
        $(body!($ctx; $($rest)*);)?
    };
}
```

`init!` is identical with `init_emitter` substituted for `body_emitter` (and no goto arms, since init code never contains jumps).

The goto arms must appear **before** the general `$op:ident(...)` arm — `macro_rules!` matches top-to-bottom and `GoTo` would otherwise be swallowed by the general arm.

### Conversion Example

`codegen_scan` body section, before:

```rust
let check_label = ctx.body_emitter.create_label();
ctx.body_emitter.bind_label(check_label);
ctx.body_emitter.emit(Operation::CanReadCursor(flag_reg, cursor_reg));
ctx.body_emitter.emit_goto_if_false(cont.on_done, flag_reg);
ctx.body_emitter.emit(Operation::ReadCursor(all_regs.clone(), cursor_reg));
if let Some(kr) = key_reg {
    ctx.body_emitter.emit(Operation::ReadKey(kr, cursor_reg));
}
ctx.body_emitter.emit(Operation::MoveCursor(cursor_reg, MoveOperation::Next));
ctx.body_emitter.emit_goto(cont.on_tuple);
```

After:

```rust
let check_label = ctx.body_emitter.label_here();
body!(ctx;
    CanReadCursor(flag_reg, cursor_reg);
    GoToIfFalse(cont.on_done, flag_reg);
    ReadCursor(all_regs.clone(), cursor_reg)
);
if let Some(kr) = key_reg {
    body!(ctx; ReadKey(kr, cursor_reg));
}
body!(ctx;
    MoveCursor(cursor_reg, MoveOperation::Next);
    GoTo(cont.on_tuple)
);
```

`codegen_count` body section, before:

```rust
ctx.body_emitter.bind_label(child_on_tuple);
ctx.body_emitter.emit(Operation::IncrementValue(counter_reg));
ctx.body_emitter.emit_goto(child_output.next);
ctx.body_emitter.bind_label(child_on_done);
ctx.body_emitter.emit_goto(cont.on_tuple);
let count_next = ctx.body_emitter.create_label();
ctx.body_emitter.bind_label(count_next);
ctx.body_emitter.emit_goto(cont.on_done);
```

After:

```rust
body!(ctx;
    Bind(child_on_tuple);
    IncrementValue(counter_reg);
    GoTo(child_output.next);
    Bind(child_on_done);
    GoTo(cont.on_tuple)
);
let count_next = ctx.body_emitter.label_here();
body!(ctx; GoTo(cont.on_done));
```

### Key Files

- `src/compiler/mod.rs` — add `init!` and `body!` macros (or new `src/compiler/macros.rs`)
- `src/compiler/nodes.rs` — port all codegen functions
- `src/compiler/emitter.rs` — already updated by item 73

### Tests

No behaviour changes — `cargo test` must pass identically before and after. The macro correctness is verified by the existing compiler node tests (38 tests). Add one explicit macro smoke test:

```rust
#[test]
fn test_body_macro_basic() {
    let mut ctx = CodegenContext::new();
    let on_done = ctx.body_emitter.create_label();
    let r0 = ctx.registers.alloc();
    let r1 = ctx.registers.alloc();
    init!(ctx; StoreValue(r0, ScalarValue::Integer(1)));
    body!(ctx;
        GoToIfFalse(on_done, r0);
        StoreValue(r1, ScalarValue::Integer(42));
        Bind(on_done)
    );
    let ops = ctx.finalize();
    assert_eq!(ops.len(), 4); // GoTo(body) + GoToIfFalse + Store + Halt-ish
}
```

### Implementation Steps (2 commits)

#### Step 74.1 — Define `init!` and `body!` macros

Add macros to `src/compiler/mod.rs`. Add smoke test. Verify `cargo test` passes.

**Commit:** `Refactor: add init! and body! macros to compiler`

#### Step 74.2 — Port `nodes.rs` to use macros and `label_here`/`bind_here`

Convert all codegen functions in `src/compiler/nodes.rs`. No logic changes. Verify `cargo test` passes identically.

**Commit:** `Refactor: port nodes.rs to init!/body! macros and label_here/bind_here`

---

## Verification

- [ ] Tests pass: `cargo test`
- [ ] Zero warnings: `cargo fmt && cargo build 2>&1 | grep -i warning`
- [ ] Each commit is independently testable
- [ ] Test count unchanged — no tests removed, only implementation reformatted
- [ ] `body!` macro smoke test exercises `Bind`, `GoTo`, `GoToIfFalse`, and general ops

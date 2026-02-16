# Phase H — Compiler Type Safety

Phase H hardens the compiler to make entire classes of bugs impossible by construction, leveraging Rust's type system for compile-time enforcement.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 40 | 8.1 | Exhaustive jump target handling | — |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

## 40. Exhaustive Jump Target Handling (Track 8.1)

### Background: The Bug

During ORDER BY implementation, we added `YieldFromRowBuffer` with a `JumpTarget` parameter. The compiler's `adjust_jump_targets()` function had a catch-all pattern `other => other`, which silently passed the operation through without adjusting its jump target by the init/body offset. This caused:

- Infinite loop at runtime (jumped to wrong address)
- No compiler error or warning
- Silent incorrect behavior that required debugging to discover

**Root cause:** Catch-all patterns in match statements prevent the compiler from enforcing exhaustive handling when new variants are added.

### What Changes

Remove catch-all patterns from jump-target-handling code, forcing compiler errors when new operations with jump targets are added.

### Key Files

- `src/compiler/nodes.rs` — `adjust_jump_targets()` function
- `src/compiler/emitter.rs` — `finalize()` function
- `src/engine/program.rs` — Operation enum (documentation update)

### Implementation Approach

**Option 1: Exhaustive Matching (Recommended)**

Remove the `other => other` catch-all and explicitly list every operation:

```rust
fn adjust_jump_targets(op: Operation, offset: usize) -> Operation {
    match op {
        // Operations with jump targets - MUST adjust
        Operation::GoTo(JumpTarget::Resolved(addr)) => {
            Operation::GoTo(JumpTarget::Resolved(addr + offset))
        }
        Operation::GoToIfFalse(JumpTarget::Resolved(addr), reg) => {
            Operation::GoToIfFalse(JumpTarget::Resolved(addr + offset), reg)
        }
        Operation::GoToIfEqualValue(JumpTarget::Resolved(addr), l, r) => {
            Operation::GoToIfEqualValue(JumpTarget::Resolved(addr + offset), l, r)
        }
        Operation::PopKey(dest, list, JumpTarget::Resolved(addr)) => {
            Operation::PopKey(dest, list, JumpTarget::Resolved(addr + offset))
        }
        Operation::YieldFromRowBuffer(regs, buf, JumpTarget::Resolved(addr)) => {
            Operation::YieldFromRowBuffer(regs, buf, JumpTarget::Resolved(addr + offset))
        }

        // Unresolved jump targets - panic (should have been resolved in finalize)
        Operation::GoTo(JumpTarget::Unresolved(_))
        | Operation::GoToIfFalse(JumpTarget::Unresolved(_), _)
        | Operation::GoToIfEqualValue(JumpTarget::Unresolved(_), _, _)
        | Operation::PopKey(_, _, JumpTarget::Unresolved(_))
        | Operation::YieldFromRowBuffer(_, _, JumpTarget::Unresolved(_)) => {
            panic!("Unresolved jump target after finalize")
        }

        // Operations without jump targets - explicit passthrough
        Operation::StoreValue(r, v) => Operation::StoreValue(r, v),
        Operation::IncrementValue(r) => Operation::IncrementValue(r),
        Operation::DecrementValue(r) => Operation::DecrementValue(r),
        Operation::AddValue(d, l, r) => Operation::AddValue(d, l, r),
        // ... ALL ~50 operations listed explicitly ...
        Operation::Halt => Operation::Halt,
    }
}
```

**Why this works:**
- Compiler errors if we add a new `Operation` variant and don't handle it
- Forces conscious decision: "does this need offset adjustment?"
- Self-documenting: operations with jump targets are grouped together

**Trade-offs:**
- ✅ Compiler enforced correctness
- ✅ No refactoring of Operation enum needed
- ✅ Clear separation of jump vs non-jump operations
- ❌ Verbose (~60 lines of simple passthrough cases)
- ❌ Each new operation adds 1 line

**Alternative Options Considered:**

1. **Split Operation Enum** - Create separate `JumpOperation` and `SimpleOperation` enums
   - Pros: Type system enforces separation, smaller match
   - Cons: Large refactor, more verbose operation construction

2. **Method on Operation** - `op.adjust_jump_targets(offset)`
   - Pros: Better encapsulation, cleaner API
   - Cons: Still needs catch-all for operations without targets

3. **Extract to Wrapper** - `struct Instruction { op, jump_target: Option<JumpTarget> }`
   - Pros: Trivial adjustment logic
   - Cons: Massive refactor, loses type safety (GoTo always has target, but now optional)

See `/tmp/jump_target_safety.md` for detailed analysis of alternatives.

### Implementation Steps

1. **Remove catch-all in `adjust_jump_targets()`**
   - Run `cargo build` - compiler will error on missing patterns
   - Add cases for all operations without jump targets
   - Group operations logically (value ops, cursor ops, control flow, etc.)

2. **Similarly update `emitter.rs::finalize()`**
   - Currently has catch-all in operation matching
   - Make exhaustive to catch new operations with unresolved jump targets

3. **Update Operation enum documentation**
   - Remove the TODO comment (compiler enforces this now)
   - Add doc comment explaining the pattern

4. **Verify with tests**
   - All existing tests should pass
   - No new test needed (this is compile-time enforcement)

### Tests

- No new runtime tests needed
- Verification: `cargo build` succeeds with no warnings
- Future verification: Adding a new operation with `JumpTarget` will cause compiler error until `adjust_jump_targets()` is updated

### Benefits

**Prevents this bug class:**
- Cannot forget to adjust jump targets
- Cannot forget to resolve unresolved targets
- Compiler enforces correctness at build time

**Improves maintainability:**
- Self-documenting: operations grouped by whether they have jump targets
- Explicit: no hidden behavior in catch-all
- Safe refactoring: renaming/changing operations causes compiler errors at all usage sites

**Sets precedent:**
- Demonstrates using Rust's type system for correctness
- Pattern can be applied to other match statements (register type handling, etc.)

### Future Extensions

If verbosity becomes a problem (unlikely with only ~5 jump operations out of 60 total):

- Add macro to reduce passthrough boilerplate:
  ```rust
  macro_rules! passthrough {
      ($($op:pat),* $(,)?) => {
          $($op => $op,)*
      }
  }
  ```

- Consider splitting Operation enum if we add many more jump operations

---

## Verification

- [ ] `adjust_jump_targets()` has no catch-all pattern
- [ ] `emitter.rs::finalize()` has no catch-all pattern
- [ ] `cargo build` succeeds with zero warnings
- [ ] All tests pass: `cargo test`
- [ ] Try adding a test operation with JumpTarget - verify compiler error

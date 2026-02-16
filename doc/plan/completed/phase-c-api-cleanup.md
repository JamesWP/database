# Phase C — API Cleanup + More SQL

Phase C hardens the codebase by replacing panics with proper error handling, adds quality-of-life SQL features (column aliases, expression functions), and fills test coverage gaps for the frontend and engine.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 14 | 6.3 | Replace panics with Result returns | — |
| 15 | 1.5 | Column aliases (AS) | — |
| 16 | 4.1 | Expression functions (LENGTH, UPPER, LOWER, ABS) | — |
| 17 | 7.1 | Lexer/parser error tests | — |
| 18 | 7.1 | Engine tests | — |

---
Important: Each item should be committed seperately, follow 'Git Workflow' in CLAUDE.md

## 14. Replace Panics with Result Returns (Track 6.3)

### What Changes

Multiple `panic!()` calls for OverflowPage in traversal. Convert to `Result<T, BTreeError>`.

### Key Files

- `src/storage/btree.rs` — lines 247, 268, 295
- `src/storage/node.rs` — lines 19, 55, 63, 71

### Implementation Approach

1. Define `BTreeError` enum (OverflowPageNotSupported, PageOutOfBounds, InvalidNodeFormat, IoError).
2. Convert each `panic!()` to `Err(BTreeError::...)`.
3. Update method signatures bottom-up: node parsing → cell reading → cursor methods → BTree public API.
4. In the engine, map `BTreeError` to user-visible error strings.

### Tests

- Existing tests pass (signatures change, happy paths same).
- Test that encountering an unexpected node type returns error, not panic.

---

## 15. Column Aliases (Track 1.5)

### What Changes

Wire AS aliases through planner to output. Parser already supports AS in the AST.

### Key Files

- `src/frontend/ast.rs` — verify alias field exists on ColumnExpression
- `src/planner.rs` — propagate alias to plan output metadata
- `src/engine.rs` — carry column name metadata alongside Yield results

### Implementation Approach

1. Attach alias (or default column name) to each output column in the plan.
2. Store `column_names: Vec<String>` on the compiled program.
3. Use column names as output headers in REPL/query results.

### Tests

- `SELECT x AS foo FROM t` — output column named "foo"
- `SELECT x+1 AS total, y FROM t` — columns "total" and "y"
- `SELECT x FROM t` (no alias) — column named "x"

---

## 16. Expression Functions (Track 4.1)

### What Changes

Add LENGTH(), UPPER(), LOWER(), ABS() scalar functions.

### Key Files

- `src/frontend/parser.rs` — recognize `identifier(expr)` as function call
- `src/frontend/ast.rs` — add `Expression::FunctionCall { name, args }`
- `src/planner.rs` — add `PlanExpr::Function`, validate function names
- `src/engine/scalarvalue.rs` — implement length(), to_uppercase(), to_lowercase(), abs()

### Implementation Approach

1. Parser: when current token is identifier and next is `(`, parse as function call.
2. Planner: validate function name is known, validate arg count (all take 1 arg).
3. Compiler: emit a `CallBuiltin` opcode or inline the logic.
4. ScalarValue: implement the four operations with NULL propagation.

### Tests

- `SELECT LENGTH('hello')` → 5
- `SELECT UPPER('hello')` → "HELLO"
- `SELECT LOWER('HELLO')` → "hello"
- `SELECT ABS(-42)` → 42
- `SELECT LENGTH(name) FROM users` — works with column references
- Error: unknown function, wrong argument count

---

## 17. Lexer/Parser Error Tests (Track 7.1)

### Tests to Add

**Lexer** (`src/frontend/lexer.rs`):
- Unterminated string: `SELECT 'hello`
- Unknown characters: `SELECT @foo`
- Empty input
- Number at i64 boundary: `9223372036854775807` vs `9223372036854775808`

**Parser** (`src/frontend/parser.rs`):
- Missing FROM: `SELECT x` (behavior check)
- Unclosed parens: `SELECT (1 + 2`
- Trailing garbage: `SELECT 1 GARBAGE`
- Deeply nested: `((((((1))))))`
- Keywords as identifiers: `SELECT select FROM from`

---

## 18. Engine Tests (Track 7.1)

### Tests to Add

**ScalarValue** (`src/engine/scalarvalue.rs`):
- Division by zero (integer and float)
- Integer overflow: i64::MAX + 1
- Type mismatch: `true + 42`
- String concatenation: `"hello" + " world"`

**VM** (`src/engine.rs`):
- ReadCursor on empty table → zero rows
- Multiple Yield calls → multiple result rows
- Open cursor on non-existent root page → error

---

## Suggested Ordering

```
17 + 18  (test-only, reveals panics, can be parallel)
14       (fix panics found by 17/18)
15 + 16  (feature work, parallel with each other)
```

## Verification

For each item:
- [ ] Tests written first (TDD)
- [ ] All tests pass: `cargo test --bin database`
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning`

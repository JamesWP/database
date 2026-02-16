# Phase I — Bug Fixes (Matt's Findings)

Phase I addresses bugs discovered during manual testing of SQL functionality.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 1 | 1.2 | INSERT float literal parsing | — |
| 2 | 3.1 | ORDER BY with function expressions | — |
| 3 | 3.2 | GROUP BY extra column bug | — |
| 4 | 4.1 | REPL compile output register formatting | — |

---

## 1. INSERT Float Literal Parsing (Track 1.2)

### What Changes

`INSERT INTO table VALUES (4.5)` produces `UnexpectedToken(Identifier, IntegerNumber(0))` when inserting a float literal. The parser doesn't correctly handle float literals in INSERT VALUE lists.

### Reproduction

```sql
CREATE TABLE sprocket (shape_id integer, side_count integer, name text, matt_score_tm real)
INSERT INTO sprocket VALUES (4.5)  -- Error: UnexpectedToken
```

### Key Files

- `src/frontend/parser.rs` — INSERT VALUES parsing
- `src/frontend/lexer.rs` — float literal tokenization

### Implementation Approach

1. Verify the lexer correctly tokenizes `4.5` as a FloatNumber token (not IntegerNumber).
2. In the parser's `parse_insert` function, ensure the VALUES clause accepts float literals in the expression parser.
3. Check if the issue is in how the parser handles the VALUES list — it may be expecting only integers.
4. Add proper error handling for type mismatches between schema and inserted values.

### Tests

- `test_insert_float_literal` — INSERT with float value into REAL column
- `test_insert_mixed_types` — INSERT with mix of integers, floats, strings
- SQL test: `tests/sql/insert_floats.sql` with various float formats (4.5, .5, 5., 1e-3)

---

## 2. ORDER BY with Function Expressions (Track 3.1)

### What Changes

`SELECT shape_id, side_count, upper(name) FROM sprocket ORDER BY matt_score_tm` causes:
```
thread 'main' panicked at src/engine.rs:311:43:
index out of bounds: the len is 3 but the index is 3
```

The issue appears to be with ORDER BY referencing a column that's not in the SELECT list when the SELECT list contains function expressions.

### Reproduction

```sql
CREATE TABLE sprocket (shape_id integer, side_count integer, name text, matt_score_tm real)
-- Insert data...
SELECT shape_id, side_count, upper(name) FROM sprocket ORDER BY matt_score_tm
-- Panic: index 3 out of bounds for length 3
```

### Key Files

- `src/engine.rs` — line 311 (panic location)
- `src/compiler/nodes.rs` — ORDER BY compilation
- `src/planner.rs` — ORDER BY planning with column resolution

### Implementation Approach

1. The SELECT list has 3 columns (indices 0, 1, 2), but ORDER BY is trying to access index 3.
2. The planner should resolve `matt_score_tm` to the correct column index from the source table (likely index 3 in the table schema).
3. Check if ORDER BY column resolution is incorrectly using SELECT list indices instead of source table column indices.
4. Fix the column resolution logic to handle:
   - ORDER BY columns present in SELECT list
   - ORDER BY columns NOT present in SELECT list (SQL standard allows this)
   - ORDER BY with expressions vs simple column references

### Tests

- `test_order_by_non_selected_column` — ORDER BY a column not in SELECT list
- `test_order_by_with_functions_in_select` — SELECT with upper(), ORDER BY different column
- `test_order_by_select_star` — SELECT *, ORDER BY specific column
- SQL test: `tests/sql/order_by_variations.sql`

---

## 3. GROUP BY Extra Column Bug (Track 3.2)

### What Changes

`SELECT max(shape_id), max(matt_score_tm) FROM sprocket GROUP BY name` produces 3 columns instead of 2:

```
Expected (2 columns):
3 | 8
2 | 4
1 | 9

Actual (3 columns):
"pentaboi" | 3 | 8
"square"   | 2 | 4
"triangle" | 1 | 9
```

The GROUP BY column (`name`) is being included in the output when it shouldn't be.

### Key Files

- `src/compiler/nodes.rs` — GROUP BY compilation
- `src/engine.rs` — GROUP BY execution (Yield instruction)
- `src/planner.rs` — GROUP BY planning

### Implementation Approach

1. The compiler is likely including the GROUP BY key column in the output registers.
2. Check the `compile_select_node` logic for GROUP BY:
   - It should only emit Yield instructions for columns in the SELECT list (the two max() results)
   - The GROUP BY column is used for grouping logic but should NOT be in the output unless explicitly selected
3. Verify the Yield instruction is emitting the correct register range.
4. Check if the grouping logic is storing the key in a register that's being incorrectly yielded.

### Tests

- `test_group_by_without_key_in_select` — GROUP BY col1, SELECT only aggregates, verify col1 not in output
- `test_group_by_with_key_in_select` — GROUP BY col1, SELECT col1, max(col2), verify col1 IS in output
- `test_multiple_aggregates_group_by` — Multiple aggregate functions with GROUP BY
- SQL test: `tests/sql/group_by_column_count.sql`

---

## 4. REPL Compile Output Register Formatting (Track 4.1)

### What Changes

The REPL's `engine> program` or `compile` command displays registers in debug format (`Reg(10)`) instead of the clean format (`R10`) used elsewhere.

### Reproduction

```
engine> compile SELECT * FROM users
...
14  YieldGrp   [[Reg(10), Reg(11), Reg(12)]], R0, @19
                  ^^^^^^^ Should be R10, R11, R12
```

Notice the inconsistency: the instruction uses `R0` and `@19` formatting but `Reg(10)` for the register array.

### Key Files

- `src/repl/modes/engine.rs` — REPL engine mode program listing
- `src/engine/program.rs` — Instruction Display implementation
- `src/engine/registers.rs` — Register Display implementation

### Implementation Approach

1. Locate where the program listing is formatted (likely in `engine.rs` or when calling `Display` on instructions).
2. Check if registers in complex instruction arguments (like `YieldGrp`'s register arrays) are using `Debug` format (`{:?}`) instead of `Display` format (`{}`).
3. Ensure all register formatting uses the `Display` trait which should output `R{n}` format.
4. If the issue is in nested structures (Vec of Reg), implement custom formatting or use iterators with `Display`.

### Tests

- Manual test: Compile a query with GROUP BY and verify registers display as `R10, R11, R12`
- Manual test: Verify all instruction types with register arguments use consistent formatting
- Add comments or examples in `manual_tests/test_repl_modes.md` showing expected output format

---

## Verification

For each item, before considering it done:
- [ ] Tests written first (TDD) or reproduction test created
- [ ] All new tests pass: `cargo test`
- [ ] All existing tests still pass
- [ ] Code formatted: `cargo fmt`
- [ ] No compiler warnings: `cargo build 2>&1 | grep -i warning`
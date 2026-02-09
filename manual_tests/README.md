# Manual Tests

This directory contains manual test scripts that complement automated unit tests by exercising the database through realistic end-to-end scenarios.

## Purpose

Manual tests are useful for:
- End-to-end workflows that exercise multiple subsystems
- Interactive REPL scenarios that are hard to unit test
- Performance or stress tests that need visual inspection
- Regression tests for bugs that involve complex interactions
- Demonstrative examples that also serve as documentation

## Running Manual Tests

Manual tests are typically run by piping commands to the database CLI:

```bash
# Build release binary first
cargo build --release

# Run a test script
cat manual_tests/<test_name>.sql | ./target/release/database <db_file>
```

## Available Tests

### SQL Mode End-to-End Test

**Files**:
- `test_sql_mode.sql` - Test commands
- `test_results.md` - Expected results

**Purpose**: Comprehensive test of SQL mode functionality

**What it tests**:
- Table creation (CREATE TABLE)
- Data insertion (INSERT INTO ... VALUES)
- Basic queries (SELECT ... FROM)
- Filtered queries (WHERE with comparison operators)
- Arithmetic expressions in predicates (age-20>10)
- Column alignment and output formatting

**Schema**:
- `users` table: id, name, age (4 rows)
- `products` table: id, name, price (5 rows)
- `orders` table: id, user_id, product_id, quantity (5 rows)

**How to run**:
```bash
cargo build --release
rm -f test_sql.db  # Start fresh
cat manual_tests/test_sql_mode.sql | ./target/release/database test_sql.db
```

**Expected results**:
- 3 tables created successfully
- 16 rows inserted (returns "1" for each insert)
- 10 SELECT queries execute with correct results
- See `test_results.md` for detailed output

**Key test cases**:
1. Select all users → 4 rows
2. Select users WHERE age>28 → 2 rows (Alice, Charlie)
3. Select all products → 5 rows
4. Select products WHERE price<100 → 2 rows (Mouse, Keyboard)
5. Select products WHERE price>=300 → 2 rows (Laptop, Monitor)
6. Select all orders → 5 rows
7. Select orders WHERE quantity>1 → 1 row
8. Select users WHERE age<30 → 2 rows (Bob, Diana)
9. Select products WHERE price-50>0 → 4 rows (tests arithmetic)
10. Select users WHERE age-20>10 → 1 row (regression test for lexer bug)

## Adding New Tests

When creating a new manual test:

1. **Create the test script** in this directory:
   ```bash
   vim manual_tests/test_new_feature.sql
   ```

2. **Run and capture results**:
   ```bash
   cat manual_tests/test_new_feature.sql | ./target/release/database test.db > output.txt
   ```

3. **Document the test** in this README.md:
   - Purpose and what it tests
   - How to run it
   - Expected results or reference to results file

4. **Commit everything**:
   ```bash
   git add manual_tests/test_new_feature.sql
   git add manual_tests/README.md  # if updated
   git commit -m "Add manual test for X feature"
   ```

## Test File Format

### SQL Mode Tests

SQL mode tests should:
- Start with `enter sql` to enter SQL mode
- End with `exit` to exit
- Use one SQL statement per line
- Omit semicolons (not required in REPL mode)

Example:
```
enter sql
CREATE TABLE users (id INTEGER, name TEXT)
INSERT INTO users (id, name) VALUES (1, 'Alice')
SELECT id, name FROM users
exit
```

### Other Mode Tests

Tests for other modes (btree, parser, planner, engine) follow similar patterns:
```
enter <mode>
<mode commands>
back  # or exit
```

## Maintenance

- Keep tests working as features evolve
- Update expected results when behavior changes intentionally
- Remove or update obsolete tests
- Consider promoting stable manual tests to automated tests if feasible

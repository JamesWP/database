CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)
-- > Table 'users' created
INSERT INTO users VALUES (1, 'Alice', 30)
-- > 1
INSERT INTO users VALUES (2, 'Bob', 25)
-- > 1
INSERT INTO users VALUES (3, 'Charlie', 30)
-- > 1
INSERT INTO users VALUES (4, 'Diana', 25)
-- > 1
CREATE INDEX idx_age ON users(age)
-- > Index 'idx_age' created
-- Verify the planner chooses an index scan for equality predicate
EXPLAIN SELECT id, name FROM users WHERE age = 30
-- > 0, "Project [id:0, name:1]"
-- > 1, "  RowidLookup users [cols: id, name, age]"
-- > 2, "    IndexScan via idx_age [= 30]"
-- Should use index scan
SELECT id, name FROM users WHERE age = 30 ORDER BY id
-- > 1, "Alice"
-- > 3, "Charlie"
SELECT id, name FROM users WHERE age = 25 ORDER BY id
-- > 2, "Bob"
-- > 4, "Diana"
-- No matches
SELECT id, name FROM users WHERE age = 40
-- > OK
-- INSERT after index creation
INSERT INTO users VALUES (5, 'Eve', 25)
-- > 1
SELECT id, name FROM users WHERE age = 25 ORDER BY id
-- > 2, "Bob"
-- > 4, "Diana"
-- > 5, "Eve"

CREATE TABLE users (id INTEGER, last TEXT, first TEXT, age INTEGER)
-- > Table 'users' created
INSERT INTO users VALUES (1, 'Smith', 'Alice', 30)
-- > 1
INSERT INTO users VALUES (2, 'Smith', 'Bob', 25)
-- > 1
INSERT INTO users VALUES (3, 'Jones', 'Charlie', 30)
-- > 1
INSERT INTO users VALUES (4, 'Adams', 'Diana', 25)
-- > 1
CREATE INDEX idx_age_id ON users(age, id)
-- > Index 'idx_age_id' created
-- Verify the planner chooses an index scan using the first column of a multi-column index
EXPLAIN SELECT id FROM users WHERE age = 30
-- > 0, "Project [id:0]"
-- > 1, "  RowidLookup users [cols: id, age]"
-- > 2, "    IndexScan via idx_age_id [= 30]"
-- Use index via first column equality (prefix match)
SELECT id FROM users WHERE age = 30 ORDER BY id
-- > 1
-- > 3
SELECT id FROM users WHERE age = 25 ORDER BY id
-- > 2
-- > 4
-- Range scan on first column of multi-column index
SELECT id FROM users WHERE age > 25 ORDER BY id
-- > 1
-- > 3

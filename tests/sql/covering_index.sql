CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)
-- > Table 'users' created
INSERT INTO users VALUES (1, 'Alice', 30)
-- > 1
INSERT INTO users VALUES (2, 'Bob', 25)
-- > 1
INSERT INTO users VALUES (3, 'Charlie', 30)
-- > 1

CREATE INDEX idx_age ON users(age)
-- > Index 'idx_age' created
CREATE INDEX idx_name ON users(name)
-- > Index 'idx_name' created

-- 1. Covered: select only the indexed INTEGER column
EXPLAIN SELECT age FROM users WHERE age = 30
-- > 0, "Project [age:0]"
-- > 1, "  IndexScan via idx_age [= 30] [age]"

-- 2. NOT covered: also selects 'name', not in idx_age
EXPLAIN SELECT age, name FROM users WHERE age = 30
-- > 0, "Project [age:1, name:0]"
-- > 1, "  RowidLookup users [cols: name, age]"
-- > 2, "    IndexScan via idx_age [= 30]"

-- 3. Covered: select only the indexed TEXT column
EXPLAIN SELECT name FROM users WHERE name = 'Alice'
-- > 0, "Project [name:0]"
-- > 1, "  IndexScan via idx_name [= 'Alice'] [name]"

-- 4. NOT covered: also selects 'id', not in idx_name
EXPLAIN SELECT name, id FROM users WHERE name = 'Alice'
-- > 0, "Project [name:1, id:0]"
-- > 1, "  RowidLookup users [cols: id, name]"
-- > 2, "    IndexScan via idx_name [= 'Alice']"

-- 5. Covered range scan
EXPLAIN SELECT age FROM users WHERE age > 25
-- > 0, "Project [age:0]"
-- > 1, "  IndexScan via idx_age [> 25] [age]"

-- Verify result correctness after optimization
SELECT age FROM users WHERE age = 30 ORDER BY age
-- > 30
-- > 30
SELECT name FROM users WHERE name = 'Alice'
-- > "Alice"

-- Multi-column index
CREATE TABLE orders (id INTEGER, status TEXT, priority INTEGER, note TEXT)
-- > Table 'orders' created
INSERT INTO orders VALUES (1, 'open', 1, 'rush')
-- > 1
INSERT INTO orders VALUES (2, 'closed', 2, 'normal')
-- > 1
INSERT INTO orders VALUES (3, 'open', 3, 'low')
-- > 1

CREATE INDEX idx_status_priority ON orders(status, priority)
-- > Index 'idx_status_priority' created

-- 6. Covered: both columns of multi-column index
EXPLAIN SELECT status, priority FROM orders WHERE status = 'open'
-- > 0, "Project [status:0, priority:1]"
-- > 1, "  IndexScan via idx_status_priority [= 'open'] [status, priority]"

-- 7. Covered: only the leading column
EXPLAIN SELECT status FROM orders WHERE status = 'open'
-- > 0, "Project [status:0]"
-- > 1, "  IndexScan via idx_status_priority [= 'open'] [status]"

-- 8. NOT covered: 'note' is not in the index
EXPLAIN SELECT status, note FROM orders WHERE status = 'open'
-- > 0, "Project [status:0, note:1]"
-- > 1, "  RowidLookup orders [cols: status, note]"
-- > 2, "    IndexScan via idx_status_priority [= 'open']"

-- Verify result correctness for multi-column covering
SELECT status, priority FROM orders WHERE status = 'open' ORDER BY priority
-- > "open", 1
-- > "open", 3

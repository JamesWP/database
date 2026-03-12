-- Multi-column index: basic prefix equality scan
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

-- Planner must choose the multi-column index via its first column
EXPLAIN SELECT id FROM users WHERE age = 30
-- > 0, "Project [id:0]"
-- > 1, "  IndexScan via idx_age_id [= 30] [id, age]"

-- Equality prefix scan
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
SELECT id FROM users WHERE age >= 25 ORDER BY id
-- > 1
-- > 2
-- > 3
-- > 4
SELECT id FROM users WHERE age < 30 ORDER BY id
-- > 2
-- > 4
SELECT id FROM users WHERE age <= 30 ORDER BY id
-- > 1
-- > 2
-- > 3
-- > 4

-- Bounded range on first column
SELECT id FROM users WHERE age >= 25 AND age <= 30 ORDER BY id
-- > 1
-- > 2
-- > 3
-- > 4

-- INSERT after index creation maintains index correctly
INSERT INTO users VALUES (5, 'Lee', 'Eve', 30)
-- > 1
INSERT INTO users VALUES (6, 'Park', 'Frank', 25)
-- > 1
SELECT id FROM users WHERE age = 30 ORDER BY id
-- > 1
-- > 3
-- > 5
SELECT id FROM users WHERE age = 25 ORDER BY id
-- > 2
-- > 4
-- > 6

-- DELETE maintains multi-column index
DELETE FROM users WHERE id = 3
-- > 1
SELECT id FROM users WHERE age = 30 ORDER BY id
-- > 1
-- > 5

-- UPDATE maintains multi-column index (change indexed column)
UPDATE users SET age = 28 WHERE id = 5
-- > 1
SELECT id FROM users WHERE age = 30 ORDER BY id
-- > 1
SELECT id FROM users WHERE age = 28 ORDER BY id
-- > 5

-- No matches returns OK
SELECT id FROM users WHERE age = 99
-- > OK

-- Three-column index: planner uses first-column prefix
CREATE TABLE orders (id INTEGER, status TEXT, priority INTEGER, amount INTEGER)
-- > Table 'orders' created
INSERT INTO orders VALUES (1, 'open', 1, 100)
-- > 1
INSERT INTO orders VALUES (2, 'open', 2, 200)
-- > 1
INSERT INTO orders VALUES (3, 'closed', 1, 150)
-- > 1
INSERT INTO orders VALUES (4, 'closed', 3, 300)
-- > 1
INSERT INTO orders VALUES (5, 'open', 1, 50)
-- > 1
CREATE INDEX idx_status_priority_id ON orders(status, priority, id)
-- > Index 'idx_status_priority_id' created

-- Planner picks first column of three-column index
EXPLAIN SELECT id FROM orders WHERE status = 'open'
-- > 0, "Project [id:0]"
-- > 1, "  IndexScan via idx_status_priority_id [= 'open'] [id, status]"

SELECT id FROM orders WHERE status = 'open' ORDER BY id
-- > 1
-- > 2
-- > 5
SELECT id FROM orders WHERE status = 'closed' ORDER BY id
-- > 3
-- > 4

-- Two indexes on same table: planner picks the right one
CREATE TABLE products (id INTEGER, category INTEGER, price INTEGER, stock INTEGER)
-- > Table 'products' created
INSERT INTO products VALUES (1, 10, 500, 20)
-- > 1
INSERT INTO products VALUES (2, 10, 300, 5)
-- > 1
INSERT INTO products VALUES (3, 20, 700, 10)
-- > 1
INSERT INTO products VALUES (4, 20, 100, 50)
-- > 1
CREATE INDEX idx_cat_price ON products(category, price)
-- > Index 'idx_cat_price' created
CREATE INDEX idx_price_stock ON products(price, stock)
-- > Index 'idx_price_stock' created

-- Filter on category → must use idx_cat_price
EXPLAIN SELECT id FROM products WHERE category = 10
-- > 0, "Project [id:0]"
-- > 1, "  RowidLookup products [cols: id, category]"
-- > 2, "    IndexScan via idx_cat_price [= 10]"

SELECT id FROM products WHERE category = 10 ORDER BY id
-- > 1
-- > 2

-- Filter on price → must use idx_price_stock
EXPLAIN SELECT id FROM products WHERE price = 700
-- > 0, "Project [id:0]"
-- > 1, "  RowidLookup products [cols: id, price]"
-- > 2, "    IndexScan via idx_price_stock [= 700]"

SELECT id FROM products WHERE price = 700
-- > 3
SELECT id FROM products WHERE price < 400 ORDER BY id
-- > 2
-- > 4

-- Full table scan when no index covers the predicate column
EXPLAIN SELECT id FROM products WHERE stock = 50
-- > 0, "Project [id:0]"
-- > 1, "  Filter [stock:1 = 50]"
-- > 2, "    Scan products [cols: id, stock]"

SELECT id FROM products WHERE stock = 50
-- > 4

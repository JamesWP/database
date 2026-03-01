-- IndexJoin: join with an index on the right table's join column
-- Verifies that IndexJoin produces the same results as a plain Join.

CREATE TABLE users (id INTEGER, name TEXT)
-- > Table 'users' created
CREATE INDEX idx_users_id ON users (id)
-- > Index 'idx_users_id' created
CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER)
-- > Table 'orders' created

INSERT INTO users VALUES (1, 'alice')
-- > 1
INSERT INTO users VALUES (2, 'bob')
-- > 1
INSERT INTO users VALUES (3, 'carol')
-- > 1

INSERT INTO orders VALUES (10, 1, 50)
-- > 1
INSERT INTO orders VALUES (11, 2, 30)
-- > 1
INSERT INTO orders VALUES (12, 1, 20)
-- > 1
INSERT INTO orders VALUES (13, 3, 40)
-- > 1

-- Basic IndexJoin query
SELECT orders.id, users.name FROM orders JOIN users ON orders.user_id = users.id ORDER BY orders.id
-- > 10, "alice"
-- > 11, "bob"
-- > 12, "alice"
-- > 13, "carol"

-- The plan should contain IndexJoin (not plain Join)
EXPLAIN SELECT orders.id, users.name FROM orders JOIN users ON orders.user_id = users.id ORDER BY orders.id
-- > 0, "Sort [id:0:0 ASC]"
-- > 1, "  Project [id:0, name:4]"
-- > 2, "    IndexJoin users via idx_users_id [left_key=1]"
-- > 3, "      Scan orders [cols: id, user_id, amount]"

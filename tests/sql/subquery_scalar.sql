CREATE TABLE users (id INTEGER, name TEXT)
-- > Table 'users' created
CREATE TABLE orders (id INTEGER, user_id INTEGER)
-- > Table 'orders' created
INSERT INTO users VALUES (1, 'alice'), (2, 'bob')
-- > 2
INSERT INTO orders VALUES (10, 1), (11, 1), (12, 2)
-- > 3

SELECT name, (SELECT COUNT(*) FROM orders) AS total_orders FROM users ORDER BY name
-- > "alice", 3
-- > "bob", 3

SELECT name FROM users WHERE id = (SELECT MAX(user_id) FROM orders)
-- > "bob"

SELECT name FROM users WHERE id = (SELECT id FROM users WHERE id = 999)
-- > OK

SELECT name FROM users WHERE id = (SELECT id, name FROM users LIMIT 1)
-- > ERROR: scalar subquery must return exactly one column

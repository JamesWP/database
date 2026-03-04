CREATE TABLE users (id INTEGER, name TEXT)
-- > Table 'users' created
CREATE TABLE orders (user_id INTEGER, amount INTEGER)
-- > Table 'orders' created
CREATE INDEX idx_orders_user ON orders (user_id)
-- > Index 'idx_orders_user' created
INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')
-- > 3
INSERT INTO orders VALUES (1, 100), (1, 200), (2, 300)
-- > 3

SELECT users.name, orders.amount FROM users JOIN orders ON orders.user_id = users.id ORDER BY users.name, orders.amount
-- > "alice", 100
-- > "alice", 200
-- > "bob", 300

EXPLAIN SELECT users.name, orders.amount FROM users JOIN orders ON orders.user_id = users.id
-- > 0, "Project [name:1, amount:3]"
-- > 1, "  Join [NestedLoop | true]"
-- > 2, "    Scan users [cols: id, name]"
-- > 3, "    RowidLookup orders [cols: user_id, amount]"
-- > 4, "      IndexProbe [index_col=user_id, key=col:0]"

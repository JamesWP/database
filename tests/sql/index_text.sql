CREATE TABLE products (id INTEGER, name TEXT, price INTEGER)
-- > Table 'products' created
INSERT INTO products VALUES (1, 'apple', 100)
-- > 1
INSERT INTO products VALUES (2, 'banana', 150)
-- > 1
INSERT INTO products VALUES (3, 'cherry', 200)
-- > 1
INSERT INTO products VALUES (4, 'date', 250)
-- > 1
CREATE INDEX idx_name ON products(name)
-- > Index 'idx_name' created
-- Equality scan on TEXT index
SELECT id FROM products WHERE name = 'banana'
-- > 2
SELECT id FROM products WHERE name = 'cherry'
-- > 3
-- No match
SELECT id FROM products WHERE name = 'elderberry'
-- > OK
-- Range scan on TEXT index
SELECT id FROM products WHERE name > 'banana' ORDER BY id
-- > 3
-- > 4
SELECT id FROM products WHERE name < 'cherry' ORDER BY id
-- > 1
-- > 2
-- INSERT after index creation
INSERT INTO products VALUES (5, 'apricot', 120)
-- > 1
SELECT id FROM products WHERE name = 'apricot'
-- > 5

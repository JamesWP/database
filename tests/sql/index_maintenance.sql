CREATE TABLE products (id INTEGER, price INTEGER)
-- > Table 'products' created

CREATE INDEX idx_price ON products(price)
-- > Index 'idx_price' created

INSERT INTO products VALUES (1, 100)
-- > 1
INSERT INTO products VALUES (2, 200)
-- > 1
INSERT INTO products VALUES (3, 100)
-- > 1

DELETE FROM products WHERE id = 1
-- > 1

-- Query via index — should not see deleted row
SELECT id FROM products WHERE price = 100
-- > 3

-- Full scan — must agree
SELECT id FROM products ORDER BY id
-- > 2
-- > 3

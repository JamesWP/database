CREATE TABLE products (id INTEGER, price INTEGER, name TEXT)
-- > Table 'products' created

CREATE INDEX idx_price ON products(price)
-- > Index 'idx_price' created

INSERT INTO products VALUES (1, 100, 'apple')
-- > 1
INSERT INTO products VALUES (2, 200, 'banana')
-- > 1
INSERT INTO products VALUES (3, 100, 'cherry')
-- > 1

-- Update price of product 1: index must reflect new value
UPDATE products SET price = 150 WHERE id = 1
-- > 1

-- Verify the planner uses the index for price equality lookups
EXPLAIN SELECT id FROM products WHERE price = 100
-- > 0, "Project [id:0]"
-- > 1, "  RowidLookup products [cols: id, price]"
-- > 2, "    IndexScan via idx_price [= 100]"

-- Old price must yield only the non-updated rows
SELECT id FROM products WHERE price = 100 ORDER BY id
-- > 3

-- New price must be findable
SELECT id FROM products WHERE price = 150
-- > 1

-- Unaffected row still findable at original price
SELECT id FROM products WHERE price = 200
-- > 2

-- Full scan agrees with index results
SELECT id FROM products ORDER BY id
-- > 1
-- > 2
-- > 3

-- Update non-indexed column: index must be unchanged
UPDATE products SET name = 'avocado' WHERE id = 1
-- > 1

SELECT id FROM products WHERE price = 150
-- > 1

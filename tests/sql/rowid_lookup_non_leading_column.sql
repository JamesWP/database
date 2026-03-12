-- Regression tests for RowidLookup fetching non-leading table columns.
--
-- When the indexed column is not the first column in the table,
-- RowidLookup must return the correct value (not the first-column value).
-- Also verifies that ORDER BY on a column not in the index is applied correctly.

CREATE TABLE products (id INTEGER, sku TEXT, price INTEGER, stock INTEGER)
-- > Table 'products' created
INSERT INTO products VALUES (1, 'AAA', 100, 50)
-- > 1
INSERT INTO products VALUES (2, 'BBB', 200, 30)
-- > 1
INSERT INTO products VALUES (3, 'CCC', 150, 10)
-- > 1

CREATE INDEX idx_price ON products(price)
-- > Index 'idx_price' created

-- 'price' is the 3rd column (table index 2). RowidLookup must return 200, not the rowid.
SELECT price FROM products WHERE price = 200
-- > 200

-- Selecting a non-leading column via RowidLookup with ORDER BY the same column.
SELECT price FROM products WHERE price > 100 ORDER BY price
-- > 150
-- > 200

-- ORDER BY a column that is NOT the indexed column must not be elided.
-- (stock is at table index 3; the index is on price at table index 2)
SELECT price, stock FROM products WHERE price > 100 ORDER BY stock
-- > 150, 10
-- > 200, 30

CREATE TABLE products (id INTEGER, name TEXT, price INTEGER)
-- > Table 'products' created

-- No index: should use Scan + Filter
EXPLAIN SELECT id, name FROM products WHERE price = 100
-- > 0, "Project [col:0, col:1]"
-- > 1, "  Filter [col:2 = 100]"
-- > 2, "    Scan products [cols: id, name, price]"

-- Add index on price
CREATE INDEX idx_price ON products(price)
-- > Index 'idx_price' created

-- With index: equality should use IndexScan
EXPLAIN SELECT id FROM products WHERE price = 100
-- > 0, "Project [col:0]"
-- > 1, "  RowidLookup products [cols: id, price]"
-- > 2, "    IndexScan via idx_price [= 100]"

-- Range predicate: should use IndexScan with > bound
EXPLAIN SELECT id FROM products WHERE price > 50
-- > 0, "Project [col:0]"
-- > 1, "  RowidLookup products [cols: id, price]"
-- > 2, "    IndexScan via idx_price [> 50]"

-- LIMIT (no filter): should use Scan with Limit wrapper
EXPLAIN SELECT id FROM products LIMIT 5
-- > 0, "Limit [5]"
-- > 1, "  Project [col:0]"
-- > 2, "    Scan products [cols: id]"

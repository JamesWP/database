-- Setup
CREATE TABLE products (id INTEGER, sku TEXT, price INTEGER, stock INTEGER)
-- > Table 'products' created
INSERT INTO products VALUES (1, 'AAA', 100, 50)
-- > 1
INSERT INTO products VALUES (2, 'BBB', 200, 30)
-- > 1
INSERT INTO products VALUES (3, 'CCC', 150, 10)
-- > 1
INSERT INTO products VALUES (4, 'DDD', 200, 0)
-- > 1
INSERT INTO products VALUES (5, 'EEE', 100, 20)
-- > 1

CREATE INDEX idx_price ON products(price)
-- > Index 'idx_price' created
CREATE INDEX idx_sku ON products(sku)
-- > Index 'idx_sku' created

-- 1. Select only the indexed INTEGER column (covering candidate)
SELECT price FROM products WHERE price = 200 ORDER BY price
-- > 200
-- > 200

-- 2. Select only the indexed TEXT column (covering candidate)
SELECT sku FROM products WHERE sku = 'AAA'
-- > "AAA"

-- 3. Select a non-indexed column alongside the indexed one (NOT covering — needs RowidLookup)
SELECT sku, stock FROM products WHERE sku = 'AAA'
-- > "AAA", 50

-- 4. Range scan: select only the indexed column
SELECT price FROM products WHERE price > 100 ORDER BY price
-- > 150
-- > 200
-- > 200

-- 5. Range scan: select indexed + non-indexed (NOT covering)
SELECT price, stock FROM products WHERE price > 100 ORDER BY price
-- > 150, 10
-- > 200, 30
-- > 200, 0

-- 6. Equality with ORDER BY: select indexed column only
SELECT price FROM products WHERE price = 100 ORDER BY price
-- > 100
-- > 100

-- Multi-column index
CREATE TABLE events (id INTEGER, category TEXT, priority INTEGER, label TEXT)
-- > Table 'events' created
INSERT INTO events VALUES (1, 'work', 1, 'meeting')
-- > 1
INSERT INTO events VALUES (2, 'work', 2, 'deadline')
-- > 1
INSERT INTO events VALUES (3, 'home', 1, 'chores')
-- > 1
INSERT INTO events VALUES (4, 'work', 1, 'standup')
-- > 1

CREATE INDEX idx_cat_pri ON events(category, priority)
-- > Index 'idx_cat_pri' created

-- 7. Select both columns of a multi-column index (covering candidate)
SELECT category, priority FROM events WHERE category = 'work' ORDER BY priority
-- > "work", 1
-- > "work", 1
-- > "work", 2

-- 8. Select only the first column of the multi-column index (covering candidate)
SELECT category FROM events WHERE category = 'home'
-- > "home"

-- 9. Select an index column plus a non-index column (NOT covering)
SELECT category, label FROM events WHERE category = 'work' ORDER BY label
-- > "work", "deadline"
-- > "work", "meeting"
-- > "work", "standup"

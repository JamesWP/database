-- Index with NULL values: IS NULL uses IndexScan
CREATE TABLE data (id INTEGER, value INTEGER)
-- > Table 'data' created
CREATE INDEX idx_value ON data(value)
-- > Index 'idx_value' created
INSERT INTO data VALUES (1, NULL)
-- > 1
INSERT INTO data VALUES (2, 10)
-- > 1
INSERT INTO data VALUES (3, 20)
-- > 1
INSERT INTO data VALUES (4, NULL)
-- > 1

-- Planner chooses IndexScan for IS NULL
EXPLAIN SELECT id FROM data WHERE value IS NULL
-- > 0, "Project [id:0]"
-- > 1, "  RowidLookup data [cols: id, value]"
-- > 2, "    IndexScan via idx_value [= NULL]"

-- IS NULL returns correct rows
SELECT id FROM data WHERE value IS NULL ORDER BY id
-- > 1
-- > 4

-- IS NOT NULL returns correct rows
SELECT id FROM data WHERE value IS NOT NULL ORDER BY id
-- > 2
-- > 3

-- Non-NULL equality and range still work
SELECT id FROM data WHERE value = 10
-- > 2
SELECT id FROM data WHERE value > 10
-- > 3

-- NULL sorts first (ASC)
SELECT id FROM data ORDER BY value
-- > 1
-- > 4
-- > 2
-- > 3

-- NULL sorts last (DESC)
SELECT id FROM data ORDER BY value DESC
-- > 3
-- > 2
-- > 1
-- > 4

-- INSERT NULL after index creation
INSERT INTO data VALUES (5, NULL)
-- > 1
SELECT id FROM data WHERE value IS NULL ORDER BY id
-- > 1
-- > 4
-- > 5

-- DELETE NULL rows maintains index
DELETE FROM data WHERE value IS NULL
-- > 3
SELECT id FROM data WHERE value IS NULL
-- > OK
SELECT id FROM data ORDER BY id
-- > 2
-- > 3

-- UPDATE to NULL maintains index
INSERT INTO data VALUES (6, 30)
-- > 1
UPDATE data SET value = NULL WHERE id = 6
-- > 1
SELECT id FROM data WHERE value IS NULL
-- > 6
SELECT id FROM data WHERE value IS NOT NULL ORDER BY id
-- > 2
-- > 3

-- Multi-column index: NULL in first column
CREATE TABLE events (id INTEGER, category INTEGER, score INTEGER)
-- > Table 'events' created
CREATE INDEX idx_cat_score ON events(category, score)
-- > Index 'idx_cat_score' created
INSERT INTO events VALUES (1, NULL, 100)
-- > 1
INSERT INTO events VALUES (2, 10, 200)
-- > 1
INSERT INTO events VALUES (3, NULL, 300)
-- > 1
INSERT INTO events VALUES (4, 10, 400)
-- > 1

-- IS NULL on first column of multi-column index
EXPLAIN SELECT id FROM events WHERE category IS NULL
-- > 0, "Project [id:0]"
-- > 1, "  RowidLookup events [cols: id, category]"
-- > 2, "    IndexScan via idx_cat_score [= NULL]"

SELECT id FROM events WHERE category IS NULL ORDER BY id
-- > 1
-- > 3
SELECT id FROM events WHERE category IS NOT NULL ORDER BY id
-- > 2
-- > 4

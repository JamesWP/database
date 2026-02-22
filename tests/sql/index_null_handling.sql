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
-- Verify the planner chooses an index scan for IS NULL predicate
EXPLAIN SELECT id FROM data WHERE value IS NULL
-- > 0, "Project [id:0]"
-- > 1, "  RowidLookup data [cols: id, value]"
-- > 2, "    IndexScan via idx_value [= NULL]"
-- NULL values appear in index, IS NULL uses index
SELECT id FROM data WHERE value IS NULL ORDER BY id
-- > 1
-- > 4
-- Non-null values still work
SELECT id FROM data WHERE value = 10
-- > 2
SELECT id FROM data WHERE value > 10
-- > 3
-- NULL sorts before all other values in index
SELECT id FROM data ORDER BY value
-- > 1
-- > 4
-- > 2
-- > 3

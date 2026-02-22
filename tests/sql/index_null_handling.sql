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

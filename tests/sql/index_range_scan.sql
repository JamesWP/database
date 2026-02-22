CREATE TABLE data (id INTEGER, value INTEGER)
-- > Table 'data' created
INSERT INTO data VALUES (1, 10)
-- > 1
INSERT INTO data VALUES (2, 20)
-- > 1
INSERT INTO data VALUES (3, 30)
-- > 1
INSERT INTO data VALUES (4, 40)
-- > 1
INSERT INTO data VALUES (5, 50)
-- > 1
CREATE INDEX idx_value ON data(value)
-- > Index 'idx_value' created

-- Greater than
SELECT id FROM data WHERE value > 20 ORDER BY id
-- > 3
-- > 4
-- > 5

-- Greater than or equal
SELECT id FROM data WHERE value >= 20 ORDER BY id
-- > 2
-- > 3
-- > 4
-- > 5

-- Less than
SELECT id FROM data WHERE value < 40 ORDER BY id
-- > 1
-- > 2
-- > 3

-- Less than or equal
SELECT id FROM data WHERE value <= 40 ORDER BY id
-- > 1
-- > 2
-- > 3
-- > 4

-- Range: lower and upper bound (AND)
SELECT id FROM data WHERE value > 10 AND value < 50 ORDER BY id
-- > 2
-- > 3
-- > 4

-- Range: inclusive both sides
SELECT id FROM data WHERE value >= 20 AND value <= 40 ORDER BY id
-- > 2
-- > 3
-- > 4

-- No matches (empty range)
SELECT id FROM data WHERE value > 50
-- > OK

-- Single match
SELECT id FROM data WHERE value >= 30 AND value <= 30
-- > 3

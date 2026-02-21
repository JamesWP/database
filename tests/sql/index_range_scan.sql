CREATE TABLE data (id INTEGER, value INTEGER)
INSERT INTO data VALUES (1, 10)
INSERT INTO data VALUES (2, 20)
INSERT INTO data VALUES (3, 30)
INSERT INTO data VALUES (4, 40)
INSERT INTO data VALUES (5, 50)
CREATE INDEX idx_value ON data(value)

-- Greater than
SELECT id FROM data WHERE value > 20 ORDER BY id

-- Greater than or equal
SELECT id FROM data WHERE value >= 20 ORDER BY id

-- Less than
SELECT id FROM data WHERE value < 40 ORDER BY id

-- Less than or equal
SELECT id FROM data WHERE value <= 40 ORDER BY id

-- Range: lower and upper bound (AND)
SELECT id FROM data WHERE value > 10 AND value < 50 ORDER BY id

-- Range: inclusive both sides
SELECT id FROM data WHERE value >= 20 AND value <= 40 ORDER BY id

-- No matches (empty range)
SELECT id FROM data WHERE value > 50

-- Single match
SELECT id FROM data WHERE value >= 30 AND value <= 30

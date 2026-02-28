-- Basic IS NULL / IS NOT NULL
CREATE TABLE t (id INTEGER, name TEXT, age INTEGER)
-- > Table 't' created
INSERT INTO t VALUES (1, 'Alice', 30)
-- > 1
INSERT INTO t VALUES (2, NULL, 25)
-- > 1
INSERT INTO t VALUES (3, 'Charlie', NULL)
-- > 1
INSERT INTO t VALUES (4, NULL, NULL)
-- > 1

SELECT id FROM t WHERE name IS NULL ORDER BY id
-- > 2
-- > 4
SELECT id FROM t WHERE name IS NOT NULL ORDER BY id
-- > 1
-- > 3
SELECT id FROM t WHERE age IS NULL ORDER BY id
-- > 3
-- > 4
SELECT id FROM t WHERE age IS NOT NULL ORDER BY id
-- > 1
-- > 2

-- Combined AND
SELECT id FROM t WHERE name IS NULL AND age IS NOT NULL
-- > 2

-- Combined OR
SELECT id FROM t WHERE name IS NULL OR age IS NULL ORDER BY id
-- > 2
-- > 3
-- > 4

-- Both columns NULL
SELECT id FROM t WHERE name IS NULL AND age IS NULL
-- > 4

-- Neither column NULL
SELECT id FROM t WHERE name IS NOT NULL AND age IS NOT NULL
-- > 1

-- NULL sorts before non-NULL values (ASC)
SELECT id, age FROM t ORDER BY age
-- > 3, NULL
-- > 4, NULL
-- > 2, 25
-- > 1, 30

-- NULL sorts after non-NULL values (DESC)
SELECT id, age FROM t ORDER BY age DESC
-- > 1, 30
-- > 2, 25
-- > 3, NULL
-- > 4, NULL

-- COUNT(*) includes NULLs; COUNT(col) excludes them
SELECT COUNT(*) FROM t
-- > 4
SELECT COUNT(age) FROM t
-- > 2
SELECT COUNT(name) FROM t
-- > 2

-- GROUP BY treats NULLs as equal (one NULL group)
SELECT age, COUNT(*) FROM t GROUP BY age ORDER BY age
-- > NULL, 2
-- > 25, 1
-- > 30, 1

-- DISTINCT treats NULLs as equal
SELECT DISTINCT age FROM t ORDER BY age
-- > NULL
-- > 25
-- > 30

-- UPDATE to NULL
UPDATE t SET age = NULL WHERE id = 2
-- > 1
SELECT id FROM t WHERE age IS NULL ORDER BY id
-- > 2
-- > 3
-- > 4
SELECT id FROM t WHERE age IS NOT NULL ORDER BY id
-- > 1

-- DELETE WHERE IS NULL
DELETE FROM t WHERE name IS NULL
-- > 2
SELECT id FROM t ORDER BY id
-- > 1
-- > 3

-- SELECT shows NULL as NULL in output
INSERT INTO t VALUES (5, NULL, NULL)
-- > 1
SELECT id, name, age FROM t ORDER BY id
-- > 1, "Alice", 30
-- > 3, "Charlie", NULL
-- > 5, NULL, NULL

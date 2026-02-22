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
SELECT id FROM t WHERE name IS NULL
-- > 2
-- > 4
SELECT id FROM t WHERE name IS NOT NULL
-- > 1
-- > 3
SELECT id FROM t WHERE age IS NULL
-- > 3
-- > 4
SELECT id FROM t WHERE age IS NOT NULL
-- > 1
-- > 2
SELECT id FROM t WHERE name IS NULL AND age IS NOT NULL
-- > 2

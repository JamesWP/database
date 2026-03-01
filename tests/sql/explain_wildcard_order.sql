CREATE TABLE t (id INTEGER, name TEXT, age INTEGER)
-- > Table 't' created
INSERT INTO t VALUES (1, 'alice', 30), (2, 'bob', 25)
-- > 2
CREATE INDEX idx_age ON t (age)
-- > Index 'idx_age' created

-- Wildcard ORDER BY with index: should produce exactly one Project node
EXPLAIN SELECT * FROM t WHERE age = 30 ORDER BY age
-- > 0, "Project [id:0, name:1, age:2]"
-- > 1, "  RowidLookup t [cols: id, name, age]"
-- > 2, "    IndexScan via idx_age [= 30]"

-- Wildcard ORDER BY without index: Sort above single Project
EXPLAIN SELECT * FROM t ORDER BY name
-- > 0, "Sort [name:1:1 ASC]"
-- > 1, "  Project [id:0, name:1, age:2]"
-- > 2, "    Scan t [cols: id, name, age]"

-- Non-wildcard SELECT with ORDER BY column not in select: extra column added and trimmed
EXPLAIN SELECT name FROM t ORDER BY age
-- > 0, "Project [name:0:0]"
-- > 1, "  Sort [age:1:1 ASC]"
-- > 2, "    Project [name:0, age:1]"
-- > 3, "      Scan t [cols: name, age]"

-- Simplest possible GROUP BY test
CREATE TABLE t (x INTEGER);
-- > Table 't' created
INSERT INTO t VALUES (1);
-- > 1
INSERT INTO t VALUES (1);
-- > 1

-- Just group by, one group, should output: 1, 2
SELECT x, COUNT(*) FROM t GROUP BY x;
-- > 1	2

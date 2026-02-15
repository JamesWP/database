-- Simplest possible GROUP BY test
CREATE TABLE t (x INTEGER);
INSERT INTO t VALUES (1);
INSERT INTO t VALUES (1);

-- Just group by, one group, should output: 1, 2
SELECT x, COUNT(*) FROM t GROUP BY x;

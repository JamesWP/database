CREATE TABLE t (id INTEGER NOT NULL, label TEXT DEFAULT 'unknown', score REAL DEFAULT 0.0)
-- > Table 't' created

INSERT INTO t (id) VALUES (1)
-- > 1

SELECT id, label, score FROM t
-- > 1, "unknown", 0

INSERT INTO t (id, label) VALUES (2, 'hello')
-- > 1

SELECT id, label, score FROM t WHERE id = 2
-- > 2, "hello", 0

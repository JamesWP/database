CREATE TABLE t (id INTEGER, rate REAL, label TEXT)
-- > Table 't' created

INSERT INTO t (id, rate, label) VALUES ('1', '4.99', 42)
-- > 1

SELECT id, rate FROM t
-- > 1, 4.99

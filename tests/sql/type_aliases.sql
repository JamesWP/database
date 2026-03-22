CREATE TABLE t1 (a VARCHAR(45) NOT NULL, b CHAR(1), c INT NOT NULL, d SMALLINT, e DECIMAL(4,2), f TIMESTAMP, g DATETIME, h BLOB SUB_TYPE TEXT)
-- > Table 't1' created

INSERT INTO t1 VALUES ('hello', 'Y', 1, 2, 3.14, '2024-01-01', '2024-01-01', 'data')
-- > 1

SELECT a, c FROM t1
-- > "hello", 1

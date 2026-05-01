CREATE TABLE numbers (id INTEGER, val INTEGER)
-- > Table 'numbers' created
INSERT INTO numbers VALUES (1, 10), (2, 20), (3, 30)
-- > 3

SELECT COUNT(*) FROM numbers WHERE RANDOM() IS NOT NULL
-- > 3

SELECT COUNT(*) FROM numbers WHERE RANDOM() != 0
-- > 3

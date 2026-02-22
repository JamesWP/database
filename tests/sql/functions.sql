-- Test LENGTH function
CREATE TABLE test_strings (id INTEGER, str TEXT);
-- > Table 'test_strings' created
INSERT INTO test_strings VALUES (1, 'hello'), (2, 'world'), (3, ''), (4, NULL);
-- > 4
SELECT id, LENGTH(str) FROM test_strings;
-- > 1, 5
-- > 2, 5
-- > 3, 0
-- > 4, NULL

-- Test UPPER function
SELECT id, UPPER(str) FROM test_strings;
-- > 1, "HELLO"
-- > 2, "WORLD"
-- > 3, ""
-- > 4, NULL

-- Test LOWER function
INSERT INTO test_strings VALUES (5, 'HELLO'), (6, 'WoRLd');
-- > 2
SELECT id, LOWER(str) FROM test_strings;
-- > 1, "hello"
-- > 2, "world"
-- > 3, ""
-- > 4, NULL
-- > 5, "hello"
-- > 6, "world"

-- Test ABS function with integers
CREATE TABLE test_numbers (id INTEGER, num INTEGER);
-- > Table 'test_numbers' created
INSERT INTO test_numbers VALUES (1, 5), (2, 10), (3, 100), (4, 0);
-- > 4
INSERT INTO test_numbers VALUES (5, NULL);
-- > 1
SELECT id, ABS(num) FROM test_numbers;
-- > 1, 5
-- > 2, 10
-- > 3, 100
-- > 4, 0
-- > 5, NULL

-- Test ABS with computed expressions (tests negative results)
SELECT id, num, ABS(num - 20) FROM test_numbers WHERE id <= 3;
-- > 1, 5, 15
-- > 2, 10, 10
-- > 3, 100, 80

-- Test ABS with negative computed value
SELECT id, num, ABS(5 - num) FROM test_numbers WHERE id <= 3;
-- > 1, 5, 0
-- > 2, 10, 5
-- > 3, 100, 95

-- Test LENGTH on non-string types (should convert)
SELECT id, LENGTH(num) FROM test_numbers WHERE id <= 3;
-- > 1, 1
-- > 2, 2
-- > 3, 3

-- Test UPPER on non-string types (should convert)
SELECT id, UPPER(num) FROM test_numbers WHERE id <= 2;
-- > 1, "5"
-- > 2, "10"

-- Test functions in WHERE clause
SELECT id, str FROM test_strings WHERE LENGTH(str) > 4;
-- > 1, "hello"
-- > 2, "world"
-- > 5, "HELLO"
-- > 6, "WoRLd"

-- Test nested function calls
SELECT id, LENGTH(UPPER(str)) FROM test_strings WHERE id <= 3;
-- > 1, 5
-- > 2, 5
-- > 3, 0

-- Test functions with computed expressions
CREATE TABLE test_calc (x INTEGER, y INTEGER);
-- > Table 'test_calc' created
INSERT INTO test_calc VALUES (3, 7), (2, 5), (8, 4);
-- > 3
SELECT x, y, ABS(x - y) FROM test_calc;
-- > 3, 7, 4
-- > 2, 5, 3
-- > 8, 4, 4

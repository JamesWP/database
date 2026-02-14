-- Test LENGTH function
CREATE TABLE test_strings (id INTEGER, str TEXT);
INSERT INTO test_strings VALUES (1, 'hello'), (2, 'world'), (3, ''), (4, NULL);
SELECT id, LENGTH(str) FROM test_strings;

-- Test UPPER function
SELECT id, UPPER(str) FROM test_strings;

-- Test LOWER function
INSERT INTO test_strings VALUES (5, 'HELLO'), (6, 'WoRLd');
SELECT id, LOWER(str) FROM test_strings;

-- Test ABS function with integers
CREATE TABLE test_numbers (id INTEGER, num INTEGER);
INSERT INTO test_numbers VALUES (1, 5), (2, 10), (3, 100), (4, 0);
INSERT INTO test_numbers VALUES (5, NULL);
SELECT id, ABS(num) FROM test_numbers;

-- Test ABS with computed expressions (tests negative results)
SELECT id, num, ABS(num - 20) FROM test_numbers WHERE id <= 3;

-- Test ABS with negative computed value
SELECT id, num, ABS(5 - num) FROM test_numbers WHERE id <= 3;

-- Test LENGTH on non-string types (should convert)
SELECT id, LENGTH(num) FROM test_numbers WHERE id <= 3;

-- Test UPPER on non-string types (should convert)
SELECT id, UPPER(num) FROM test_numbers WHERE id <= 2;

-- Test functions in WHERE clause
SELECT id, str FROM test_strings WHERE LENGTH(str) > 4;

-- Test nested function calls
SELECT id, LENGTH(UPPER(str)) FROM test_strings WHERE id <= 3;

-- Test functions with computed expressions
CREATE TABLE test_calc (x INTEGER, y INTEGER);
INSERT INTO test_calc VALUES (3, 7), (2, 5), (8, 4);
SELECT x, y, ABS(x - y) FROM test_calc;

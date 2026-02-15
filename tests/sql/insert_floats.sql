-- Test INSERT with various float literal formats
CREATE TABLE test_floats (id INTEGER, value REAL, name TEXT);

-- Regular float: 4.5
INSERT INTO test_floats VALUES (1, 4.5, 'regular');

-- Leading dot: .5
INSERT INTO test_floats VALUES (2, .5, 'leading_dot');

-- Trailing dot: 5.
INSERT INTO test_floats VALUES (3, 5., 'trailing_dot');

-- Scientific notation: 1e-3
INSERT INTO test_floats VALUES (4, 1e-3, 'scientific');

-- Scientific with plus: 2.5E+2
INSERT INTO test_floats VALUES (5, 2.5E+2, 'sci_plus');

-- Multiple floats in one INSERT
INSERT INTO test_floats VALUES (6, 3.14159, 'pi'), (7, 2.71828, 'e');

-- Verify all insertions
SELECT * FROM test_floats ORDER BY id;

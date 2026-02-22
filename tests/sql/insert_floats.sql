-- Test INSERT with various float literal formats
CREATE TABLE test_floats (id INTEGER, value REAL, name TEXT);
-- > Table 'test_floats' created

-- Regular float: 4.5
INSERT INTO test_floats VALUES (1, 4.5, 'regular');
-- > 1

-- Leading dot: .5
INSERT INTO test_floats VALUES (2, .5, 'leading_dot');
-- > 1

-- Trailing dot: 5.
INSERT INTO test_floats VALUES (3, 5., 'trailing_dot');
-- > 1

-- Scientific notation: 1e-3
INSERT INTO test_floats VALUES (4, 1e-3, 'scientific');
-- > 1

-- Scientific with plus: 2.5E+2
INSERT INTO test_floats VALUES (5, 2.5E+2, 'sci_plus');
-- > 1

-- Multiple floats in one INSERT
INSERT INTO test_floats VALUES (6, 3.14159, 'pi'), (7, 2.71828, 'e');
-- > 2

-- Verify all insertions
SELECT * FROM test_floats ORDER BY id;
-- > 1, 4.5, "regular"
-- > 2, 0.5, "leading_dot"
-- > 3, 5, "trailing_dot"
-- > 4, 0.001, "scientific"
-- > 5, 250, "sci_plus"
-- > 6, 3.14159, "pi"
-- > 7, 2.71828, "e"

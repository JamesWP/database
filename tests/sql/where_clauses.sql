CREATE TABLE numbers (id INTEGER, value INTEGER, name TEXT)
-- > Table 'numbers' created
INSERT INTO numbers VALUES (1, 100, 'one')
-- > 1
INSERT INTO numbers VALUES (2, 200, 'two')
-- > 1
INSERT INTO numbers VALUES (3, 100, 'three')
-- > 1
INSERT INTO numbers VALUES (4, 300, 'four')
-- > 1
INSERT INTO numbers VALUES (5, 150, 'five')
-- > 1
SELECT id, value FROM numbers WHERE value=100
-- > 1, 100
-- > 3, 100
SELECT id, value FROM numbers WHERE value!=100
-- > 2, 200
-- > 4, 300
-- > 5, 150
SELECT id, value FROM numbers WHERE value<200
-- > 1, 100
-- > 3, 100
-- > 5, 150
SELECT id, value FROM numbers WHERE value>100
-- > 2, 200
-- > 4, 300
-- > 5, 150
SELECT id, value FROM numbers WHERE value<=100
-- > 1, 100
-- > 3, 100
SELECT id, value FROM numbers WHERE value>=200
-- > 2, 200
-- > 4, 300
SELECT id, name FROM numbers WHERE value>100 AND value<300
-- > 2, "two"
-- > 5, "five"
SELECT id, name FROM numbers WHERE value=100 OR value=300
-- > 1, "one"
-- > 3, "three"
-- > 4, "four"
SELECT id, name FROM numbers WHERE value<150 OR value>250
-- > 1, "one"
-- > 3, "three"
-- > 4, "four"
SELECT id, name FROM numbers WHERE value>=100 AND value<=200
-- > 1, "one"
-- > 2, "two"
-- > 3, "three"
-- > 5, "five"

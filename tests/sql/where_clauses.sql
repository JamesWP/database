CREATE TABLE numbers (id INTEGER, value INTEGER, name TEXT)
INSERT INTO numbers VALUES (1, 100, 'one')
INSERT INTO numbers VALUES (2, 200, 'two')
INSERT INTO numbers VALUES (3, 100, 'three')
INSERT INTO numbers VALUES (4, 300, 'four')
INSERT INTO numbers VALUES (5, 150, 'five')
SELECT id, value FROM numbers WHERE value=100
SELECT id, value FROM numbers WHERE value!=100
SELECT id, value FROM numbers WHERE value<200
SELECT id, value FROM numbers WHERE value>100
SELECT id, value FROM numbers WHERE value<=100
SELECT id, value FROM numbers WHERE value>=200
SELECT id, name FROM numbers WHERE value>100 AND value<300
SELECT id, name FROM numbers WHERE value=100 OR value=300
SELECT id, name FROM numbers WHERE value<150 OR value>250
SELECT id, name FROM numbers WHERE value>=100 AND value<=200

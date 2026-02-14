-- Test COUNT(*) functionality
CREATE TABLE users (id INTEGER, name TEXT);
INSERT INTO users VALUES (1, 'alice');
INSERT INTO users VALUES (2, 'bob');
INSERT INTO users VALUES (3, 'charlie');

-- COUNT(*) all rows
SELECT COUNT(*) FROM users;

-- COUNT(*) with WHERE
SELECT COUNT(*) FROM users WHERE id > 1;

-- COUNT(*) empty table
CREATE TABLE empty (val INTEGER);
SELECT COUNT(*) FROM empty;

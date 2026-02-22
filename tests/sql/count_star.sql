-- Test COUNT(*) functionality
CREATE TABLE users (id INTEGER, name TEXT);
-- > Table 'users' created
INSERT INTO users VALUES (1, 'alice');
-- > 1
INSERT INTO users VALUES (2, 'bob');
-- > 1
INSERT INTO users VALUES (3, 'charlie');
-- > 1

-- COUNT(*) all rows
SELECT COUNT(*) FROM users;
-- > 3

-- COUNT(*) with WHERE
SELECT COUNT(*) FROM users WHERE id > 1;
-- > 2

-- COUNT(*) empty table
CREATE TABLE empty (val INTEGER);
-- > Table 'empty' created
SELECT COUNT(*) FROM empty;
-- > 0

-- Test LIKE operator

CREATE TABLE users (id INTEGER, name TEXT, email TEXT);
-- > Table 'users' created
INSERT INTO users VALUES (1, 'alice', 'alice@example.com');
-- > 1
INSERT INTO users VALUES (2, 'bob', 'bob@test.com');
-- > 1
INSERT INTO users VALUES (3, 'charlie', 'charlie@example.com');
-- > 1
INSERT INTO users VALUES (4, 'alice smith', 'asmith@company.org');
-- > 1

-- Exact match
SELECT name FROM users WHERE name LIKE 'alice';
-- > "alice"

-- Prefix match with %
SELECT name FROM users WHERE name LIKE 'ali%';
-- > "alice"
-- > "alice smith"

-- Suffix match with %
SELECT name FROM users WHERE email LIKE '%example.com';
-- > "alice"
-- > "charlie"

-- Contains with %
SELECT name FROM users WHERE name LIKE '%li%';
-- > "alice"
-- > "charlie"
-- > "alice smith"

-- Single character match with _
SELECT name FROM users WHERE name LIKE 'bo_';
-- > "bob"

-- Multiple wildcards
SELECT name FROM users WHERE name LIKE 'a%e';
-- > "alice"

-- % matches everything
SELECT name FROM users WHERE name LIKE '%';
-- > "alice"
-- > "bob"
-- > "charlie"
-- > "alice smith"

-- No matches
SELECT name FROM users WHERE name LIKE 'xyz%';
-- > OK

-- Test LIKE operator

CREATE TABLE users (id INTEGER, name TEXT, email TEXT);
INSERT INTO users VALUES (1, 'alice', 'alice@example.com');
INSERT INTO users VALUES (2, 'bob', 'bob@test.com');
INSERT INTO users VALUES (3, 'charlie', 'charlie@example.com');
INSERT INTO users VALUES (4, 'alice smith', 'asmith@company.org');

-- Exact match
SELECT name FROM users WHERE name LIKE 'alice';

-- Prefix match with %
SELECT name FROM users WHERE name LIKE 'ali%';

-- Suffix match with %
SELECT name FROM users WHERE email LIKE '%example.com';

-- Contains with %
SELECT name FROM users WHERE name LIKE '%li%';

-- Single character match with _
SELECT name FROM users WHERE name LIKE 'bo_';

-- Multiple wildcards
SELECT name FROM users WHERE name LIKE 'a%e';

-- % matches everything
SELECT name FROM users WHERE name LIKE '%';

-- No matches
SELECT name FROM users WHERE name LIKE 'xyz%';

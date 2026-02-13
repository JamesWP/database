-- Test UPDATE single row
CREATE TABLE users (id INTEGER, name TEXT, age INTEGER);
INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'charlie', 35);
UPDATE users SET age = 31 WHERE id = 1;
SELECT * FROM users WHERE id = 1;

-- Test UPDATE all rows
UPDATE users SET age = 40;
SELECT id, age FROM users;

-- Test UPDATE no match
UPDATE users SET name = 'nobody' WHERE id = 999;

-- Test UPDATE multiple columns
UPDATE users SET name = 'robert', age = 26 WHERE id = 2;
SELECT * FROM users WHERE id = 2;

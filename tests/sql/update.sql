-- Test UPDATE single row
CREATE TABLE users (id INTEGER, name TEXT, age INTEGER);
-- > Table 'users' created
INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'charlie', 35);
-- > 3
UPDATE users SET age = 31 WHERE id = 1;
-- > 1
SELECT * FROM users WHERE id = 1;
-- > 1	alice	31

-- Test UPDATE all rows
UPDATE users SET age = 40;
-- > 3
SELECT id, age FROM users;
-- > 1	40
-- > 2	40
-- > 3	40

-- Test UPDATE no match
UPDATE users SET name = 'nobody' WHERE id = 999;
-- > 0

-- Test UPDATE multiple columns
UPDATE users SET name = 'robert', age = 26 WHERE id = 2;
-- > 1
SELECT * FROM users WHERE id = 2;
-- > 2	robert	26

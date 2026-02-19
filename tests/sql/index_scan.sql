CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)
INSERT INTO users VALUES (1, 'Alice', 30)
INSERT INTO users VALUES (2, 'Bob', 25)
INSERT INTO users VALUES (3, 'Charlie', 30)
INSERT INTO users VALUES (4, 'Diana', 25)
CREATE INDEX idx_age ON users(age)
-- Should use index scan
SELECT id, name FROM users WHERE age = 30 ORDER BY id
SELECT id, name FROM users WHERE age = 25 ORDER BY id
-- No matches
SELECT id, name FROM users WHERE age = 40
-- INSERT after index creation
INSERT INTO users VALUES (5, 'Eve', 25)
SELECT id, name FROM users WHERE age = 25 ORDER BY id

CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)
CREATE INDEX idx_age ON users(age)
INSERT INTO users VALUES (2, 'Bob', 25)
INSERT INTO users VALUES (4, 'Diana', 25)
INSERT INTO users VALUES (3, 'Charlie', 30)
INSERT INTO users VALUES (1, 'Alice', 30)
SELECT id, name, age FROM users ORDER BY id

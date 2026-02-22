CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)
-- > Table 'users' created
INSERT INTO users VALUES (1, 'Alice', 30)
-- > 1
INSERT INTO users VALUES (2, 'Bob', 25)
-- > 1
INSERT INTO users VALUES (3, 'Charlie', 30)
-- > 1
CREATE INDEX idx_age ON users(age)
-- > Index 'idx_age' created

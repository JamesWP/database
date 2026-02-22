CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)
-- > Table 'users' created
CREATE INDEX idx_age ON users(age)
-- > Index 'idx_age' created
INSERT INTO users VALUES (2, 'Bob', 25)
-- > 1
INSERT INTO users VALUES (4, 'Diana', 25)
-- > 1
INSERT INTO users VALUES (3, 'Charlie', 30)
-- > 1
INSERT INTO users VALUES (1, 'Alice', 30)
-- > 1
SELECT id, name, age FROM users ORDER BY id
-- > 1	Alice	30
-- > 2	Bob	25
-- > 3	Charlie	30
-- > 4	Diana	25

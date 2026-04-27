CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)
-- > Table 'users' created
INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 35)
-- > 3

SELECT name FROM (SELECT id, name FROM users WHERE age > 28) AS young ORDER BY name
-- > "alice"
-- > "carol"

SELECT name FROM (SELECT id, name FROM users) AS u WHERE u.id > 1 ORDER BY name
-- > "bob"
-- > "carol"

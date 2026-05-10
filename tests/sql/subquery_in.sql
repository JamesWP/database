CREATE TABLE users (id INTEGER, name TEXT)
-- > Table 'users' created
CREATE TABLE admins (user_id INTEGER)
-- > Table 'admins' created
INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')
-- > 3
INSERT INTO admins VALUES (1), (3)
-- > 2

SELECT name FROM users WHERE id IN (1, 3) ORDER BY name
-- > "alice"
-- > "carol"

SELECT name FROM users WHERE id NOT IN (1, 3) ORDER BY name
-- > "bob"

SELECT name FROM users WHERE id IN (2) ORDER BY name
-- > "bob"

SELECT name FROM users WHERE id IN (99, 100)
-- > OK

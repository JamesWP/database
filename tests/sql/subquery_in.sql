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

SELECT name FROM users WHERE id IN (1) ORDER BY name
-- > "alice"

SELECT name FROM users WHERE id NOT IN (1, 2, 3) ORDER BY name
-- > OK

SELECT name FROM users WHERE id IN (SELECT user_id FROM admins) ORDER BY name
-- > "alice"
-- > "carol"

SELECT name FROM users WHERE id NOT IN (SELECT user_id FROM admins) ORDER BY name
-- > "bob"

SELECT name FROM users WHERE id IN (SELECT user_id FROM admins WHERE user_id > 100)
-- > OK

EXPLAIN SELECT name FROM users WHERE id IN (SELECT user_id FROM admins)
-- > 0, "Project [name:1]"
-- > 1, "  Join [Semi | id:0]"
-- > 2, "    Scan users [cols: id, name]"
-- > 3, "    Project [user_id:0]"
-- > 4, "      Scan admins [cols: user_id]"

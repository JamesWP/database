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

SELECT name FROM users WHERE id IN (SELECT user_id FROM admins) ORDER BY name
-- > "alice"
-- > "carol"

SELECT name FROM users WHERE id NOT IN (SELECT user_id FROM admins) ORDER BY name
-- > "bob"

SELECT name FROM users WHERE id IN (SELECT user_id FROM admins WHERE user_id > 100)
-- > OK

SELECT name FROM users WHERE id NOT IN (SELECT user_id FROM admins WHERE user_id > 100) ORDER BY name
-- > "alice"
-- > "bob"
-- > "carol"

EXPLAIN SELECT name FROM users WHERE id IN (SELECT user_id FROM admins)
-- > 0, "Project [name:1]"
-- > 1, "  Join [Semi] on id:0 = user_id:0:2"
-- > 2, "    Scan users [cols: id, name]"
-- > 3, "    Materialize"
-- > 4, "      Project [user_id:0]"
-- > 5, "        Scan admins [cols: user_id]"

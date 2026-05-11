-- Test DROP TABLE removes from catalog
CREATE TABLE users (id INTEGER, name TEXT);
-- > Table 'users' created
INSERT INTO users VALUES (1, 'alice'), (2, 'bob');
-- > 2
SELECT * FROM users;
-- > 1, "alice"
-- > 2, "bob"
DROP TABLE users;
-- > Table 'users' dropped
-- After DROP, SELECT should fail
SELECT * FROM users;
-- > ERROR: Planning error: table 'users' not found

-- Test DROP nonexistent table
DROP TABLE nonexistent;
-- > ERROR: Table 'nonexistent' not found

-- Test DROP and recreate same name
CREATE TABLE items (id INTEGER);
-- > Table 'items' created
INSERT INTO items VALUES (1);
-- > 1
DROP TABLE items;
-- > Table 'items' dropped
CREATE TABLE items (id INTEGER, name TEXT);
-- > Table 'items' created
INSERT INTO items VALUES (2, 'item2');
-- > 1
SELECT * FROM items;
-- > 2, "item2"

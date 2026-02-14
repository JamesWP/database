-- Test DROP TABLE removes from catalog
CREATE TABLE users (id INTEGER, name TEXT);
INSERT INTO users VALUES (1, 'alice'), (2, 'bob');
SELECT * FROM users;
DROP TABLE users;
-- After DROP, SELECT should fail
SELECT * FROM users;

-- Test DROP nonexistent table
DROP TABLE nonexistent;

-- Test DROP and recreate same name
CREATE TABLE items (id INTEGER);
INSERT INTO items VALUES (1);
DROP TABLE items;
CREATE TABLE items (id INTEGER, name TEXT);
INSERT INTO items VALUES (2, 'item2');
SELECT * FROM items;

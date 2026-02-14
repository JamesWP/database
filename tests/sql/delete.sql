-- Test DELETE single row
CREATE TABLE users (id INTEGER, name TEXT, age INTEGER);
INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'charlie', 35);
DELETE FROM users WHERE id = 1;
SELECT * FROM users;

-- Test DELETE all rows
CREATE TABLE temp (id INTEGER, value TEXT);
INSERT INTO temp VALUES (1, 'a'), (2, 'b'), (3, 'c');
DELETE FROM temp;
SELECT * FROM temp;

-- Test DELETE no match
CREATE TABLE items (id INTEGER, name TEXT);
INSERT INTO items VALUES (1, 'foo'), (2, 'bar');
DELETE FROM items WHERE id = 999;
SELECT * FROM items;

-- Test DELETE then insert
DELETE FROM items WHERE id = 1;
INSERT INTO items VALUES (1, 'new_foo');
SELECT * FROM items;

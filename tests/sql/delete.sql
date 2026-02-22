-- Test DELETE single row
CREATE TABLE users (id INTEGER, name TEXT, age INTEGER);
-- > Table 'users' created
INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'charlie', 35);
-- > 3
DELETE FROM users WHERE id = 1;
-- > 1
SELECT * FROM users;
-- > 2	bob	25
-- > 3	charlie	35

-- Test DELETE all rows
CREATE TABLE temp (id INTEGER, value TEXT);
-- > Table 'temp' created
INSERT INTO temp VALUES (1, 'a'), (2, 'b'), (3, 'c');
-- > 3
DELETE FROM temp;
-- > 3
SELECT * FROM temp;
-- > OK

-- Test DELETE no match
CREATE TABLE items (id INTEGER, name TEXT);
-- > Table 'items' created
INSERT INTO items VALUES (1, 'foo'), (2, 'bar');
-- > 2
DELETE FROM items WHERE id = 999;
-- > 0
SELECT * FROM items;
-- > 1	foo
-- > 2	bar

-- Test DELETE then insert
DELETE FROM items WHERE id = 1;
-- > 1
INSERT INTO items VALUES (1, 'new_foo');
-- > 1
SELECT * FROM items;
-- > 2	bar
-- > 1	new_foo

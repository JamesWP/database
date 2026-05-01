CREATE TABLE items (id INTEGER, active INTEGER)
-- > Table 'items' created
INSERT INTO items VALUES (1, 1), (2, 0), (3, 1)
-- > 3

SELECT id FROM items WHERE NOT (active = 1) ORDER BY id
-- > 2

SELECT id FROM items WHERE NOT (id = 1 OR id = 3) ORDER BY id
-- > 2

SELECT id FROM items WHERE NOT (id = 99) ORDER BY id
-- > 1
-- > 2
-- > 3

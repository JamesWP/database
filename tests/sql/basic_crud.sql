CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)
-- > Table 'users' created
CREATE TABLE products (id INTEGER, name TEXT, price INTEGER)
-- > Table 'products' created
CREATE TABLE orders (id INTEGER, user_id INTEGER, product_id INTEGER, quantity INTEGER)
-- > Table 'orders' created
INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)
-- > 1
INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)
-- > 1
INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)
-- > 1
INSERT INTO users (id, name, age) VALUES (4, 'Diana', 28)
-- > 1
INSERT INTO products (id, name, price) VALUES (1, 'Laptop', 1200)
-- > 1
INSERT INTO products (id, name, price) VALUES (2, 'Mouse', 25)
-- > 1
INSERT INTO products (id, name, price) VALUES (3, 'Keyboard', 75)
-- > 1
INSERT INTO products (id, name, price) VALUES (4, 'Monitor', 300)
-- > 1
INSERT INTO products (id, name, price) VALUES (5, 'Headphones', 150)
-- > 1
INSERT INTO orders (id, user_id, product_id, quantity) VALUES (1, 1, 1, 1)
-- > 1
INSERT INTO orders (id, user_id, product_id, quantity) VALUES (2, 1, 2, 2)
-- > 1
INSERT INTO orders (id, user_id, product_id, quantity) VALUES (3, 2, 3, 1)
-- > 1
INSERT INTO orders (id, user_id, product_id, quantity) VALUES (4, 3, 1, 1)
-- > 1
INSERT INTO orders (id, user_id, product_id, quantity) VALUES (5, 4, 5, 1)
-- > 1
SELECT id, name, age FROM users
-- > 1	Alice	30
-- > 2	Bob	25
-- > 3	Charlie	35
-- > 4	Diana	28
SELECT name, age FROM users WHERE age>28
-- > Alice	30
-- > Charlie	35
SELECT id, name, price FROM products
-- > 1	Laptop	1200
-- > 2	Mouse	25
-- > 3	Keyboard	75
-- > 4	Monitor	300
-- > 5	Headphones	150
SELECT name, price FROM products WHERE price<100
-- > Mouse	25
-- > Keyboard	75
SELECT name, price FROM products WHERE price>=300
-- > Laptop	1200
-- > Monitor	300
SELECT id, user_id, product_id, quantity FROM orders
-- > 1	1	1	1
-- > 2	1	2	2
-- > 3	2	3	1
-- > 4	3	1	1
-- > 5	4	5	1
SELECT id, user_id, product_id, quantity FROM orders WHERE quantity>1
-- > 2	1	2	2
SELECT name, age FROM users WHERE age<30
-- > Bob	25
-- > Diana	28
SELECT name, price FROM products WHERE price-50>0
-- > Laptop	1200
-- > Keyboard	75
-- > Monitor	300
-- > Headphones	150
SELECT name, age FROM users WHERE age-20>10
-- > Charlie	35

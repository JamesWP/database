enter sql
CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)
CREATE TABLE products (id INTEGER, name TEXT, price INTEGER)
CREATE TABLE orders (id INTEGER, user_id INTEGER, product_id INTEGER, quantity INTEGER)
INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)
INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)
INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)
INSERT INTO users (id, name, age) VALUES (4, 'Diana', 28)
INSERT INTO products (id, name, price) VALUES (1, 'Laptop', 1200)
INSERT INTO products (id, name, price) VALUES (2, 'Mouse', 25)
INSERT INTO products (id, name, price) VALUES (3, 'Keyboard', 75)
INSERT INTO products (id, name, price) VALUES (4, 'Monitor', 300)
INSERT INTO products (id, name, price) VALUES (5, 'Headphones', 150)
INSERT INTO orders (id, user_id, product_id, quantity) VALUES (1, 1, 1, 1)
INSERT INTO orders (id, user_id, product_id, quantity) VALUES (2, 1, 2, 2)
INSERT INTO orders (id, user_id, product_id, quantity) VALUES (3, 2, 3, 1)
INSERT INTO orders (id, user_id, product_id, quantity) VALUES (4, 3, 1, 1)
INSERT INTO orders (id, user_id, product_id, quantity) VALUES (5, 4, 5, 1)
SELECT id, name, age FROM users
SELECT name, age FROM users WHERE age>28
SELECT id, name, price FROM products
SELECT name, price FROM products WHERE price<100
SELECT name, price FROM products WHERE price>=300
SELECT id, user_id, product_id, quantity FROM orders
SELECT id, user_id, product_id, quantity FROM orders WHERE quantity>1
SELECT name, age FROM users WHERE age<30
SELECT name, price FROM products WHERE price-50>0
SELECT name, age FROM users WHERE age-20>10
exit

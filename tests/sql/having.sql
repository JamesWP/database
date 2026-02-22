CREATE TABLE orders (customer TEXT, amount INTEGER)
-- > Table 'orders' created

INSERT INTO orders VALUES ('alice', 100)
-- > 1
INSERT INTO orders VALUES ('alice', 200)
-- > 1
INSERT INTO orders VALUES ('bob', 50)
-- > 1
INSERT INTO orders VALUES ('carol', 300)
-- > 1
INSERT INTO orders VALUES ('carol', 400)
-- > 1

-- HAVING COUNT(*) >= 2: alice and carol
SELECT customer, COUNT(*) FROM orders GROUP BY customer HAVING COUNT(*) >= 2
-- > "alice", 2
-- > "carol", 2

-- HAVING SUM > threshold: only carol
SELECT customer, SUM(amount) FROM orders GROUP BY customer HAVING SUM(amount) > 500
-- > "carol", 700

-- HAVING with WHERE (WHERE filters rows first, then HAVING filters groups)
SELECT customer, COUNT(*) FROM orders WHERE amount >= 100 GROUP BY customer HAVING COUNT(*) = 1
-- > OK

-- HAVING MIN
SELECT customer, MIN(amount) FROM orders GROUP BY customer HAVING MIN(amount) < 100
-- > "bob", 50

-- No groups pass HAVING -- empty result
SELECT customer FROM orders GROUP BY customer HAVING COUNT(*) > 100
-- > OK

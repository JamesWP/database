-- Test GROUP BY with aggregates

-- Setup test data
CREATE TABLE employees (id INTEGER, dept TEXT, name TEXT, salary INTEGER);
-- > Table 'employees' created
INSERT INTO employees VALUES (1, 'eng', 'alice', 100);
-- > 1
INSERT INTO employees VALUES (2, 'eng', 'bob', 120);
-- > 1
INSERT INTO employees VALUES (3, 'sales', 'carol', 90);
-- > 1

-- Single group key with COUNT(*)
SELECT dept, COUNT(*) FROM employees GROUP BY dept;
-- > eng	2
-- > sales	1

-- Multiple aggregates (SUM, AVG, MIN, MAX)
SELECT dept, SUM(salary) FROM employees GROUP BY dept;
-- > eng	220
-- > sales	90
SELECT dept, AVG(salary) FROM employees GROUP BY dept;
-- > eng	110
-- > sales	90
SELECT dept, MIN(salary), MAX(salary) FROM employees GROUP BY dept;
-- > eng	100	120
-- > sales	90	90

-- Aggregates without GROUP BY (one big group)
SELECT COUNT(*), SUM(salary) FROM employees;
-- > 3	310

-- GROUP BY with WHERE clause
SELECT dept, COUNT(*) FROM employees WHERE salary > 100 GROUP BY dept;
-- > eng	1

-- GROUP BY with ORDER BY
SELECT dept, COUNT(*) FROM employees GROUP BY dept ORDER BY dept;
-- > eng	2
-- > sales	1

-- ORDER BY with alias
SELECT dept, COUNT(*) as cnt FROM employees GROUP BY dept ORDER BY cnt DESC;
-- > ERROR: Planning error: ColumnNotFound { table: "employees", column: "cnt" }

-- Multiple group keys
CREATE TABLE sales (region TEXT, product TEXT, amount INTEGER);
-- > Table 'sales' created
INSERT INTO sales VALUES ('west', 'widget', 100);
-- > 1
INSERT INTO sales VALUES ('west', 'widget', 150);
-- > 1
INSERT INTO sales VALUES ('east', 'widget', 120);
-- > 1

SELECT region, product, COUNT(*), SUM(amount) FROM sales GROUP BY region, product;
-- > east	widget	1	120
-- > west	widget	2	250

-- NULL handling
CREATE TABLE nulltest (category TEXT, value INTEGER);
-- > Table 'nulltest' created
INSERT INTO nulltest VALUES ('a', 10);
-- > 1
INSERT INTO nulltest VALUES ('a', 20);
-- > 1
INSERT INTO nulltest VALUES ('a', NULL);
-- > 1
INSERT INTO nulltest VALUES (NULL, 30);
-- > 1
INSERT INTO nulltest VALUES (NULL, 40);
-- > 1

SELECT category, COUNT(*), COUNT(value), SUM(value) FROM nulltest GROUP BY category;
-- > NULL	2	2	70
-- > a	3	2	30

-- Expression-based grouping
SELECT salary + 10, COUNT(*) FROM employees GROUP BY salary + 10;
-- > 100	1
-- > 110	1
-- > 130	1

-- Empty result
SELECT dept, COUNT(*) FROM employees WHERE salary > 1000 GROUP BY dept;
-- > OK

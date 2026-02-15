-- Test GROUP BY with aggregates

-- Setup test data
CREATE TABLE employees (id INTEGER, dept TEXT, name TEXT, salary INTEGER);
INSERT INTO employees VALUES (1, 'eng', 'alice', 100);
INSERT INTO employees VALUES (2, 'eng', 'bob', 120);
INSERT INTO employees VALUES (3, 'sales', 'carol', 90);

-- Single group key with COUNT(*)
SELECT dept, COUNT(*) FROM employees GROUP BY dept;

-- Multiple aggregates (SUM, AVG, MIN, MAX)
SELECT dept, SUM(salary) FROM employees GROUP BY dept;
SELECT dept, AVG(salary) FROM employees GROUP BY dept;
SELECT dept, MIN(salary), MAX(salary) FROM employees GROUP BY dept;

-- Aggregates without GROUP BY (one big group)
SELECT COUNT(*), SUM(salary) FROM employees;

-- GROUP BY with WHERE clause
SELECT dept, COUNT(*) FROM employees WHERE salary > 100 GROUP BY dept;

-- GROUP BY with ORDER BY
SELECT dept, COUNT(*) FROM employees GROUP BY dept ORDER BY dept;

-- ORDER BY with alias
SELECT dept, COUNT(*) as cnt FROM employees GROUP BY dept ORDER BY cnt DESC;

-- Multiple group keys
CREATE TABLE sales (region TEXT, product TEXT, amount INTEGER);
INSERT INTO sales VALUES ('west', 'widget', 100);
INSERT INTO sales VALUES ('west', 'widget', 150);
INSERT INTO sales VALUES ('east', 'widget', 120);

SELECT region, product, COUNT(*), SUM(amount) FROM sales GROUP BY region, product;

-- NULL handling
CREATE TABLE nulltest (category TEXT, value INTEGER);
INSERT INTO nulltest VALUES ('a', 10);
INSERT INTO nulltest VALUES ('a', 20);
INSERT INTO nulltest VALUES ('a', NULL);
INSERT INTO nulltest VALUES (NULL, 30);
INSERT INTO nulltest VALUES (NULL, 40);

SELECT category, COUNT(*), COUNT(value), SUM(value) FROM nulltest GROUP BY category;

-- Expression-based grouping
SELECT salary + 10, COUNT(*) FROM employees GROUP BY salary + 10;

-- Empty result
SELECT dept, COUNT(*) FROM employees WHERE salary > 1000 GROUP BY dept;

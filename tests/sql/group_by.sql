-- Test GROUP BY with aggregates

-- Setup test data
CREATE TABLE employees (id INTEGER, dept TEXT, name TEXT, salary INTEGER);
INSERT INTO employees VALUES (1, 'eng', 'alice', 100);
INSERT INTO employees VALUES (2, 'eng', 'bob', 120);
INSERT INTO employees VALUES (3, 'sales', 'carol', 90);

-- Single group key with COUNT(*)
SELECT dept, COUNT(*) FROM employees GROUP BY dept;

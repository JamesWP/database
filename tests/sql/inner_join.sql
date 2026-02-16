-- Setup tables
CREATE TABLE departments (id INTEGER, name TEXT)
CREATE TABLE employees (id INTEGER, name TEXT, dept_id INTEGER)
INSERT INTO departments VALUES (1, 'Engineering')
INSERT INTO departments VALUES (2, 'Sales')
INSERT INTO departments VALUES (3, 'Marketing')
INSERT INTO employees VALUES (100, 'Alice', 1)
INSERT INTO employees VALUES (101, 'Bob', 2)
INSERT INTO employees VALUES (102, 'Charlie', 1)
INSERT INTO employees VALUES (103, 'Diana', 2)

-- Basic INNER JOIN with qualified column names
SELECT employees.name, departments.name FROM employees JOIN departments ON employees.dept_id = departments.id

-- INNER JOIN keyword variant
SELECT employees.name, departments.name FROM employees INNER JOIN departments ON employees.dept_id = departments.id

-- JOIN with table aliases
SELECT e.name, d.name FROM employees AS e JOIN departments AS d ON e.dept_id = d.id

-- JOIN with WHERE clause
SELECT e.name, d.name FROM employees AS e JOIN departments AS d ON e.dept_id = d.id WHERE d.name = 'Engineering'

-- JOIN with no matches (department 3 has no employees)
SELECT d.name, e.name FROM departments AS d JOIN employees AS e ON d.id = e.dept_id WHERE d.id = 3

-- Self-join
CREATE TABLE people (id INTEGER, name TEXT, dept INTEGER)
INSERT INTO people VALUES (1, 'Alice', 10)
INSERT INTO people VALUES (2, 'Bob', 10)
INSERT INTO people VALUES (3, 'Charlie', 20)
SELECT a.name, b.name FROM people AS a JOIN people AS b ON a.dept = b.dept WHERE a.id < b.id

-- SELECT * with JOIN (all columns from both tables)
SELECT * FROM employees AS e JOIN departments AS d ON e.dept_id = d.id WHERE e.id = 100

-- ORDER BY with JOIN
SELECT e.name, d.name FROM employees AS e JOIN departments AS d ON e.dept_id = d.id ORDER BY e.name

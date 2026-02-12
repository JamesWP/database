CREATE TABLE departments (id INTEGER, name TEXT)
CREATE TABLE employees (id INTEGER, name TEXT, dept_id INTEGER, salary INTEGER)
CREATE TABLE projects (id INTEGER, title TEXT)
INSERT INTO departments VALUES (1, 'Engineering')
INSERT INTO departments VALUES (2, 'Sales')
INSERT INTO employees VALUES (100, 'Alice', 1, 90000)
INSERT INTO employees VALUES (101, 'Bob', 2, 80000)
INSERT INTO employees VALUES (102, 'Charlie', 1, 95000)
INSERT INTO projects VALUES (1, 'Database')
INSERT INTO projects VALUES (2, 'Frontend')
SELECT id, name FROM departments
SELECT id, name, salary FROM employees WHERE dept_id=1
SELECT title FROM projects
SELECT name FROM employees WHERE salary>85000
SELECT id, name FROM departments WHERE id=2

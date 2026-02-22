CREATE TABLE departments (id INTEGER, name TEXT)
-- > Table 'departments' created
CREATE TABLE employees (id INTEGER, name TEXT, dept_id INTEGER, salary INTEGER)
-- > Table 'employees' created
CREATE TABLE projects (id INTEGER, title TEXT)
-- > Table 'projects' created
INSERT INTO departments VALUES (1, 'Engineering')
-- > 1
INSERT INTO departments VALUES (2, 'Sales')
-- > 1
INSERT INTO employees VALUES (100, 'Alice', 1, 90000)
-- > 1
INSERT INTO employees VALUES (101, 'Bob', 2, 80000)
-- > 1
INSERT INTO employees VALUES (102, 'Charlie', 1, 95000)
-- > 1
INSERT INTO projects VALUES (1, 'Database')
-- > 1
INSERT INTO projects VALUES (2, 'Frontend')
-- > 1
SELECT id, name FROM departments
-- > 1	Engineering
-- > 2	Sales
SELECT id, name, salary FROM employees WHERE dept_id=1
-- > 100	Alice	90000
-- > 102	Charlie	95000
SELECT title FROM projects
-- > Database
-- > Frontend
SELECT name FROM employees WHERE salary>85000
-- > Alice
-- > Charlie
SELECT id, name FROM departments WHERE id=2
-- > 2	Sales

CREATE TABLE src (id INTEGER, name TEXT)
-- > Table 'src' created

INSERT INTO src VALUES (1, 'alice')
-- > 1
INSERT INTO src VALUES (2, 'bob')
-- > 1
INSERT INTO src VALUES (3, 'carol')
-- > 1

CREATE TABLE dst (id INTEGER, name TEXT)
-- > Table 'dst' created

-- Copy all rows
INSERT INTO dst SELECT id, name FROM src
-- > 3

SELECT id FROM dst ORDER BY id
-- > 1
-- > 2
-- > 3

-- Copy filtered rows
CREATE TABLE seniors (id INTEGER, name TEXT)
-- > Table 'seniors' created

INSERT INTO seniors SELECT id, name FROM src WHERE id >= 2
-- > 2

SELECT id FROM seniors ORDER BY id
-- > 2
-- > 3

-- Insert aggregate results
CREATE TABLE counts (dept TEXT, n INTEGER)
-- > Table 'counts' created

CREATE TABLE employees (dept TEXT, name TEXT)
-- > Table 'employees' created

INSERT INTO employees VALUES ('eng', 'alice')
-- > 1
INSERT INTO employees VALUES ('eng', 'bob')
-- > 1
INSERT INTO employees VALUES ('hr', 'carol')
-- > 1

INSERT INTO counts SELECT dept, COUNT(*) FROM employees GROUP BY dept
-- > 2

SELECT dept, n FROM counts ORDER BY dept
-- > "eng", 2
-- > "hr", 1

-- Column count mismatch — must error
INSERT INTO dst SELECT id FROM src
-- > ERROR: Planning error: column count mismatch: expected 2, got 1

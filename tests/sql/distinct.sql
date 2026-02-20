CREATE TABLE colors (id INTEGER, name TEXT, category TEXT)
INSERT INTO colors VALUES (1, 'red', 'warm')
INSERT INTO colors VALUES (2, 'blue', 'cool')
INSERT INTO colors VALUES (3, 'green', 'cool')
INSERT INTO colors VALUES (4, 'orange', 'warm')
INSERT INTO colors VALUES (5, 'pink', 'warm')

-- DISTINCT on single column with duplicates
SELECT DISTINCT category FROM colors

-- DISTINCT on single column, all unique
SELECT DISTINCT name FROM colors

-- DISTINCT on multiple columns
SELECT DISTINCT category, name FROM colors

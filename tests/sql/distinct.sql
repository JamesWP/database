CREATE TABLE colors (id INTEGER, name TEXT, category TEXT)
-- > Table 'colors' created
INSERT INTO colors VALUES (1, 'red', 'warm')
-- > 1
INSERT INTO colors VALUES (2, 'blue', 'cool')
-- > 1
INSERT INTO colors VALUES (3, 'green', 'cool')
-- > 1
INSERT INTO colors VALUES (4, 'orange', 'warm')
-- > 1
INSERT INTO colors VALUES (5, 'pink', 'warm')
-- > 1

-- DISTINCT on single column with duplicates
SELECT DISTINCT category FROM colors
-- > cool
-- > warm

-- DISTINCT on single column, all unique
SELECT DISTINCT name FROM colors
-- > blue
-- > green
-- > orange
-- > pink
-- > red

-- DISTINCT on multiple columns
SELECT DISTINCT category, name FROM colors
-- > cool	blue
-- > cool	green
-- > warm	orange
-- > warm	pink
-- > warm	red

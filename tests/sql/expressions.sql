CREATE TABLE calc (id INTEGER, x INTEGER, y INTEGER)
-- > Table 'calc' created
INSERT INTO calc VALUES (1, 10, 5)
-- > 1
INSERT INTO calc VALUES (2, 20, 8)
-- > 1
INSERT INTO calc VALUES (3, 15, 3)
-- > 1
SELECT id, x+y FROM calc
-- > 1, 15
-- > 2, 28
-- > 3, 18
SELECT id, x-y FROM calc
-- > 1, 5
-- > 2, 12
-- > 3, 12
SELECT id, x*y FROM calc
-- > 1, 50
-- > 2, 160
-- > 3, 45
SELECT id, x/y FROM calc
-- > 1, 2
-- > 2, 2
-- > 3, 5

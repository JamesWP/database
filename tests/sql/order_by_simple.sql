-- Simple ORDER BY test
CREATE TABLE nums (val INTEGER);
-- > Table 'nums' created
INSERT INTO nums VALUES (3);
-- > 1
INSERT INTO nums VALUES (1);
-- > 1
INSERT INTO nums VALUES (2);
-- > 1
SELECT val FROM nums ORDER BY val ASC;
-- > 1
-- > 2
-- > 3

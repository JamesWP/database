-- Test ORDER BY with various scenarios

CREATE TABLE scores (id INTEGER, name TEXT, score INTEGER);
-- > Table 'scores' created
INSERT INTO scores VALUES (1, 'alice', 85);
-- > 1
INSERT INTO scores VALUES (2, 'bob', 92);
-- > 1
INSERT INTO scores VALUES (3, 'charlie', 78);
-- > 1
INSERT INTO scores VALUES (4, 'diana', 92);
-- > 1
INSERT INTO scores VALUES (5, 'eve', 88);
-- > 1

-- ORDER BY ASC (default)
SELECT name, score FROM scores ORDER BY score;
-- > "charlie", 78
-- > "alice", 85
-- > "eve", 88
-- > "bob", 92
-- > "diana", 92

-- ORDER BY ASC (explicit)
SELECT name, score FROM scores ORDER BY score ASC;
-- > "charlie", 78
-- > "alice", 85
-- > "eve", 88
-- > "bob", 92
-- > "diana", 92

-- ORDER BY DESC
SELECT name, score FROM scores ORDER BY score DESC;
-- > "bob", 92
-- > "diana", 92
-- > "eve", 88
-- > "alice", 85
-- > "charlie", 78

-- ORDER BY with WHERE
SELECT name, score FROM scores WHERE score > 80 ORDER BY score DESC;
-- > "bob", 92
-- > "diana", 92
-- > "eve", 88
-- > "alice", 85

-- ORDER BY with LIMIT
SELECT name, score FROM scores ORDER BY score DESC LIMIT 3;
-- > "bob", 92
-- > "diana", 92
-- > "eve", 88

-- ORDER BY multiple columns (tie-breaking)
SELECT name, score FROM scores ORDER BY score DESC, name ASC;
-- > "bob", 92
-- > "diana", 92
-- > "eve", 88
-- > "alice", 85
-- > "charlie", 78

-- ORDER BY first column
SELECT id, name FROM scores ORDER BY id;
-- > 1, "alice"
-- > 2, "bob"
-- > 3, "charlie"
-- > 4, "diana"
-- > 5, "eve"

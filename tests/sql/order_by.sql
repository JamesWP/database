-- Test ORDER BY with various scenarios

CREATE TABLE scores (id INTEGER, name TEXT, score INTEGER);
INSERT INTO scores VALUES (1, 'alice', 85);
INSERT INTO scores VALUES (2, 'bob', 92);
INSERT INTO scores VALUES (3, 'charlie', 78);
INSERT INTO scores VALUES (4, 'diana', 92);
INSERT INTO scores VALUES (5, 'eve', 88);

-- ORDER BY ASC (default)
SELECT name, score FROM scores ORDER BY score;

-- ORDER BY ASC (explicit)
SELECT name, score FROM scores ORDER BY score ASC;

-- ORDER BY DESC
SELECT name, score FROM scores ORDER BY score DESC;

-- ORDER BY with WHERE
SELECT name, score FROM scores WHERE score > 80 ORDER BY score DESC;

-- ORDER BY with LIMIT
SELECT name, score FROM scores ORDER BY score DESC LIMIT 3;

-- ORDER BY multiple columns (tie-breaking)
SELECT name, score FROM scores ORDER BY score DESC, name ASC;

-- ORDER BY first column
SELECT id, name FROM scores ORDER BY id;

CREATE TABLE votes (id INTEGER PRIMARY KEY AUTOINCREMENT, winner_id INTEGER, loser_id INTEGER, voted_at INTEGER)
-- > Table 'votes' created

INSERT INTO votes (winner_id, loser_id, voted_at) VALUES (1, 2, 1000)
-- > 1
INSERT INTO votes (winner_id, loser_id, voted_at) VALUES (3, 4, 2000)
-- > 1
INSERT INTO votes (winner_id, loser_id, voted_at) VALUES (5, 6, 3000)
-- > 1

SELECT id, winner_id, loser_id FROM votes ORDER BY id
-- > 1, 1, 2
-- > 2, 3, 4
-- > 3, 5, 6

CREATE TABLE manual (id INTEGER PRIMARY KEY, name TEXT)
-- > Table 'manual' created
INSERT INTO manual VALUES (42, 'alice')
-- > 1
SELECT id, name FROM manual
-- > 42, "alice"

INSERT INTO manual VALUES (42, 'bob')
-- > ERROR: constraint violation

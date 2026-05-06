-- INTEGER PRIMARY KEY as rowid alias
CREATE TABLE votes (id INTEGER PRIMARY KEY, label TEXT)
-- > Table 'votes' created

INSERT INTO votes VALUES (10, 'yes')
-- > 1

INSERT INTO votes VALUES (20, 'no')
-- > 1

INSERT INTO votes VALUES (5, 'maybe')
-- > 1

-- SELECT returns rows in B-tree key order
SELECT id, label FROM votes ORDER BY id
-- > 5, "maybe"
-- > 10, "yes"
-- > 20, "no"

-- Duplicate PK is rejected
INSERT INTO votes VALUES (10, 'duplicate')
-- > ERROR: constraint violation

-- rowid() returns the PK value for rowid-alias tables
SELECT id, rowid() FROM votes ORDER BY id
-- > 5, 5
-- > 10, 10
-- > 20, 20

-- PK column at a non-zero schema position
CREATE TABLE events (ts INTEGER, kind TEXT, id INTEGER PRIMARY KEY)
-- > Table 'events' created

INSERT INTO events VALUES (1000, 'click', 7)
-- > 1

SELECT id, kind, ts FROM events
-- > 7, "click", 1000

-- Omitting PK auto-assigns max(rowid)+1
INSERT INTO votes (label) VALUES ('auto')
-- > 1

SELECT id, label FROM votes ORDER BY id DESC LIMIT 1
-- > 21, "auto"

-- rowid() on a table with no INTEGER PK returns the internal rowid
CREATE TABLE items (name TEXT)
-- > Table 'items' created

INSERT INTO items VALUES ('a')
-- > 1

INSERT INTO items VALUES ('b')
-- > 1

SELECT name, rowid() FROM items ORDER BY name
-- > "a", 1
-- > "b", 2

-- TEXT PRIMARY KEY tables still use implicit unique index (unchanged behaviour)
CREATE TABLE tags (name TEXT PRIMARY KEY, count INTEGER)
-- > Table 'tags' created

INSERT INTO tags VALUES ('rust', 3)
-- > 1

INSERT INTO tags VALUES ('rust', 5)
-- > ERROR: constraint violation

INSERT INTO tags VALUES ('go', 1)
-- > 1

SELECT name, count FROM tags ORDER BY name
-- > "go", 1
-- > "rust", 3

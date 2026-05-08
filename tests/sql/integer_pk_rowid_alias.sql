-- Tests for INTEGER PRIMARY KEY as rowid alias (Phase BK)

-- Basic rowid-alias insert and select
CREATE TABLE votes (id INTEGER PRIMARY KEY, label TEXT)
-- > Table 'votes' created

INSERT INTO votes VALUES (10, 'yes')
-- > 1
INSERT INTO votes VALUES (20, 'no')
-- > 1
INSERT INTO votes VALUES (5, 'maybe')
-- > 1

-- SELECT returns all three; ORDER BY id uses natural B-tree order
SELECT id, label FROM votes ORDER BY id
-- > 5, "maybe"
-- > 10, "yes"
-- > 20, "no"

-- Duplicate PK is rejected
INSERT INTO votes VALUES (10, 'duplicate')
-- > ERROR: constraint violation

-- Non-integer PK tables still use implicit index (TEXT PK → unchanged behaviour)
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

-- PK column at a non-zero schema position
CREATE TABLE events (ts INTEGER, kind TEXT, id INTEGER PRIMARY KEY)
-- > Table 'events' created

INSERT INTO events VALUES (1000, 'click', 7)
-- > 1

SELECT id, kind, ts FROM events
-- > 7, "click", 1000

-- SELECT just the PK column
SELECT id FROM votes ORDER BY id
-- > 5
-- > 10
-- > 20

-- WHERE clause on PK column
SELECT label FROM votes WHERE id = 10
-- > "yes"

-- Omitting PK column auto-assigns max(rowid)+1
INSERT INTO votes (label) VALUES ('auto')
-- > 1

SELECT id, label FROM votes ORDER BY id DESC LIMIT 1
-- > 21, "auto"

-- After explicit high PK insert, next auto-assign continues from max+1
INSERT INTO votes VALUES (100, 'high')
-- > 1

INSERT INTO votes (label) VALUES ('next')
-- > 1

SELECT id, label FROM votes WHERE id >= 100 ORDER BY id
-- > 100, "high"
-- > 101, "next"

-- rowid() returns the B-tree key on a non-PK table
CREATE TABLE items (name TEXT)
-- > Table 'items' created

INSERT INTO items VALUES ('a')
-- > 1
INSERT INTO items VALUES ('b')
-- > 1

SELECT name, rowid() FROM items ORDER BY rowid()
-- > "a", 1
-- > "b", 2

-- rowid() on a rowid-alias table equals the PK column
SELECT id, rowid() FROM votes ORDER BY id LIMIT 1
-- > 5, 5

-- rowid() in WHERE
SELECT name FROM items WHERE rowid() = 1
-- > "a"

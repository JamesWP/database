CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)
-- > Table 'users' created

INSERT INTO users VALUES (1, 'alice')
-- > 1

INSERT INTO users VALUES (2, 'bob')
-- > 1

-- Duplicate primary key — must error
INSERT INTO users VALUES (1, 'duplicate')
-- > ERROR: constraint violation: unique constraint violated

-- Non-duplicate succeeds
INSERT INTO users VALUES (3, 'carol')
-- > 1

SELECT id FROM users ORDER BY id
-- > 1
-- > 2
-- > 3

-- UNIQUE column (not pk)
CREATE TABLE emails (id INTEGER, addr TEXT UNIQUE)
-- > Table 'emails' created

INSERT INTO emails VALUES (1, 'a@example.com')
-- > 1

INSERT INTO emails VALUES (2, 'b@example.com')
-- > 1

INSERT INTO emails VALUES (3, 'a@example.com')
-- > ERROR: constraint violation: unique constraint violated

SELECT id FROM emails ORDER BY id
-- > 1
-- > 2

-- Test error handling in SQL runner

-- First, create a table successfully
CREATE TABLE users (id INTEGER, name TEXT, age INTEGER);
-- > Table 'users' created

-- Try to create the same table again (should error)
CREATE TABLE users (id INTEGER, name TEXT);
-- > ERROR: Table 'users' already exists

-- Try to select from a nonexistent table
SELECT id FROM nonexistent;
-- > ERROR: Planning error: table 'nonexistent' not found

-- Insert a valid row first
INSERT INTO users VALUES (1, 'alice', 30);
-- > 1

-- Try to insert with wrong column count (too few columns)
INSERT INTO users VALUES (2, 'bob');
-- > ERROR: Planning error: column count mismatch: expected 3, got 2

-- Try to insert with wrong column count (too many columns)
INSERT INTO users VALUES (3, 'charlie', 25, 'extra');
-- > ERROR: Planning error: column count mismatch: expected 3, got 4

-- Malformed SQL (incomplete statement)
SELECT id FROM
-- > ERROR: Parse error: UnexpectedToken(Identifier, Eof)

-- Malformed SQL (invalid syntax)
CREATE INVALID
-- > ERROR: Parse error: UnexpectedToken(Table, Identifier("invalid"))

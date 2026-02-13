-- Test error handling in SQL runner

-- First, create a table successfully
CREATE TABLE users (id INTEGER, name TEXT, age INTEGER);

-- Try to create the same table again (should error)
CREATE TABLE users (id INTEGER, name TEXT);

-- Try to select from a nonexistent table
SELECT id FROM nonexistent;

-- Insert a valid row first
INSERT INTO users VALUES (1, 'alice', 30);

-- Try to insert with wrong column count (too few columns)
INSERT INTO users VALUES (2, 'bob');

-- Try to insert with wrong column count (too many columns)
INSERT INTO users VALUES (3, 'charlie', 25, 'extra');

-- Malformed SQL (incomplete statement)
SELECT id FROM

-- Malformed SQL (invalid syntax)
CREATE INVALID

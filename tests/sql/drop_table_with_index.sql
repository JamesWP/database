CREATE TABLE users (id INTEGER, name TEXT)
CREATE INDEX idx_id ON users(id)
INSERT INTO users VALUES (1, 'Alice')
DROP TABLE users
-- Should fail because table is gone
SELECT * FROM users
-- Catalog should be empty
CREATE TABLE users (id INTEGER, name TEXT)
-- This should work if catalog entry for idx_id was removed
CREATE INDEX idx_id ON users(id)
INSERT INTO users VALUES (2, 'Bob')
SELECT * FROM users

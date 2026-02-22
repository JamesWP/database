CREATE TABLE users (id INTEGER, name TEXT)
-- > Table 'users' created
CREATE INDEX idx_id ON users(id)
-- > Index 'idx_id' created
INSERT INTO users VALUES (1, 'Alice')
-- > 1
DROP TABLE users
-- > Table 'users' dropped
-- Should fail because table is gone
SELECT * FROM users
-- > ERROR: Planning error: TableNotFound("users")
-- Catalog should be empty
CREATE TABLE users (id INTEGER, name TEXT)
-- > Table 'users' created
-- This should work if catalog entry for idx_id was removed
CREATE INDEX idx_id ON users(id)
-- > Index 'idx_id' created
INSERT INTO users VALUES (2, 'Bob')
-- > 1
SELECT * FROM users
-- > 2, "Bob"

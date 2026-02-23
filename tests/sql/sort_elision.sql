CREATE TABLE events (id INTEGER, ts INTEGER)
-- > Table 'events' created

CREATE INDEX idx_ts ON events(ts)
-- > Index 'idx_ts' created

INSERT INTO events VALUES (1, 300)
-- > 1
INSERT INTO events VALUES (2, 100)
-- > 1
INSERT INTO events VALUES (3, 200)
-- > 1

-- Should come back in ts order (index order) without a Sort node
SELECT id FROM events WHERE ts > 50 ORDER BY ts
-- > 2
-- > 3
-- > 1

-- EXPLAIN must show no Sort node
EXPLAIN SELECT id FROM events WHERE ts > 50 ORDER BY ts
-- > 0, "Project [id:0:0]"
-- > 1, "  Project [id:0, ts:1]"
-- > 2, "    RowidLookup events [cols: id, ts]"
-- > 3, "      IndexScan via idx_ts [> 50]"

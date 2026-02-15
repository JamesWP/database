-- Test ORDER BY with column not in SELECT list when SELECT has function expressions
CREATE TABLE sprocket (shape_id INTEGER, side_count INTEGER, name TEXT, matt_score_tm REAL);

INSERT INTO sprocket VALUES (1, 3, 'triangle', 9.0);
INSERT INTO sprocket VALUES (2, 4, 'square', 4.0);
INSERT INTO sprocket VALUES (3, 5, 'pentaboi', 8.0);

-- This should work: ORDER BY column that's not in SELECT list
SELECT shape_id, side_count, upper(name) FROM sprocket ORDER BY matt_score_tm;

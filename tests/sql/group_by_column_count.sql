-- Test GROUP BY column count bug
CREATE TABLE sprocket (shape_id INTEGER, side_count INTEGER, name TEXT, matt_score_tm REAL);
-- > Table 'sprocket' created

INSERT INTO sprocket VALUES (1, 3, 'triangle', 9.0);
-- > 1
INSERT INTO sprocket VALUES (2, 4, 'square', 4.0);
-- > 1
INSERT INTO sprocket VALUES (3, 5, 'pentaboi', 8.0);
-- > 1

-- This should return 2 columns (the two max() results), not 3
SELECT max(shape_id), max(matt_score_tm) FROM sprocket GROUP BY name;
-- > 3, 8
-- > 2, 4
-- > 1, 9

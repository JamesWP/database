CREATE TABLE rental (id INTEGER, rental_date TEXT, inventory_id INTEGER, customer_id INTEGER)
-- > Table 'rental' created

CREATE UNIQUE INDEX idx_rental_uq ON rental (rental_date, inventory_id, customer_id)
-- > Index 'idx_rental_uq' created

INSERT INTO rental VALUES (1, '2005-05-24', 367, 130)
-- > 1

INSERT INTO rental VALUES (2, '2005-05-24', 367, 130)
-- > ERROR: unique constraint violated

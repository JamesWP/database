CREATE TABLE city (city_id INTEGER NOT NULL, city VARCHAR(50) NOT NULL, country_id INT NOT NULL, last_update TIMESTAMP NOT NULL, PRIMARY KEY (city_id), CONSTRAINT fk_city_country FOREIGN KEY (country_id) REFERENCES country (country_id) ON DELETE NO ACTION ON UPDATE CASCADE)
-- > Table 'city' created

CREATE TABLE film (film_id INTEGER NOT NULL, title VARCHAR(255) NOT NULL, special_features VARCHAR(100), CONSTRAINT CHECK_special CHECK(special_features IS NULL))
-- > Table 'film' created

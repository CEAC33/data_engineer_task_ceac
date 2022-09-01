\c netflix_titles;

-- VALIDATIONS for show_types table
COPY (SELECT * FROM show_types WHERE NOT (show_types IS NOT NULL)) TO '/tmp/null_values_in_show_types_table.csv'  WITH DELIMITER ',' CSV HEADER;

-- VALIDATIONS for people_roles table
COPY (SELECT * FROM people_roles WHERE NOT (people_roles IS NOT NULL)) TO '/tmp/null_values_in_people_roles_table.csv'  WITH DELIMITER ',' CSV HEADER;

-- VALIDATIONS for people table
COPY (SELECT * FROM people WHERE NOT (people IS NOT NULL)) TO '/tmp/null_values_in_people_table.csv'  WITH DELIMITER ',' CSV HEADER;
-- unknown gender
COPY (SELECT * FROM people WHERE gender='u') TO '/tmp/people_with_unknown_gender.csv'  WITH DELIMITER ',' CSV HEADER;

-- VALIDATIONS for countries table
COPY (SELECT * FROM countries WHERE NOT (countries IS NOT NULL)) TO '/tmp/null_values_in_countries_table.csv'  WITH DELIMITER ',' CSV HEADER;

-- VALIDATIONS for ratings table
COPY (SELECT * FROM ratings WHERE NOT (ratings IS NOT NULL)) TO '/tmp/null_values_in_ratings_table.csv'  WITH DELIMITER ',' CSV HEADER;

-- VALIDATIONS for categories table
COPY (SELECT * FROM categories WHERE NOT (categories IS NOT NULL)) TO '/tmp/null_values_in_categories_table.csv'  WITH DELIMITER ',' CSV HEADER;

-- VALIDATIONS for shows table
COPY (SELECT * FROM shows WHERE NOT (shows IS NOT NULL)) TO '/tmp/null_values_in_shows_table.csv'  WITH DELIMITER ',' CSV HEADER;

-- VALIDATIONS for show_categories table
COPY (SELECT * FROM show_categories WHERE NOT (show_categories IS NOT NULL)) TO '/tmp/null_values_in_show_categories_table.csv'  WITH DELIMITER ',' CSV HEADER;
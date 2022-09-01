CREATE DATABASE netflix_titles;
\c netflix_titles;

-- Creation of show_types table
CREATE TABLE IF NOT EXISTS show_types (
    type_id SERIAL,
    type varchar(20),
    PRIMARY KEY (type_id)
); 

-- Creation of people_roles table
CREATE TABLE IF NOT EXISTS people_roles (
    role_id SERIAL,
    role varchar(20),
    PRIMARY KEY (role_id)
); 

-- Creation of people table
CREATE TABLE IF NOT EXISTS people (
    people_id SERIAL,
    role_id INT NOT NULL,
    name TEXT,
    gender varchar(1),
    PRIMARY KEY (people_id),
    CONSTRAINT fk_role
      FOREIGN KEY(role_id) 
	  REFERENCES people_roles(role_id)
); 

-- Creation of countries table
CREATE TABLE IF NOT EXISTS countries (
    country_id SERIAL,
    country TEXT,
    PRIMARY KEY (country_id)
); 

-- Creation of ratings table
CREATE TABLE IF NOT EXISTS ratings (
    rating_id SERIAL,
    rating TEXT,
    PRIMARY KEY (rating_id)
); 

-- Creation of categories table
CREATE TABLE IF NOT EXISTS categories (
    category_id SERIAL,
    category TEXT,
    PRIMARY KEY (category_id)
); 

-- Creation of shows table
CREATE TABLE IF NOT EXISTS shows (
    id SERIAL,
    show_id varchar(20),
    type_id INT,
    title varchar(250),
    director_id INT,
    country_id INT,
    date_added DATE, 
    release_year INT,
    rating_id INT,
    duration_amount INT,
    duration_unit varchar(20),
    description TEXT,
    PRIMARY KEY (id),
    CONSTRAINT fk_type
      FOREIGN KEY(type_id) 
	  REFERENCES show_types(type_id),
    CONSTRAINT fk_director
      FOREIGN KEY(director_id) 
	  REFERENCES people(people_id),
    CONSTRAINT fk_country
      FOREIGN KEY(country_id) 
	  REFERENCES countries(country_id),
    CONSTRAINT fk_rating
      FOREIGN KEY(rating_id) 
	  REFERENCES ratings(rating_id)
);

-- Creation of show_categories table
CREATE TABLE IF NOT EXISTS show_categories (
    category_id INT NOT NULL,
    show_id varchar(20) NOT NULL
); 

-- Creation of show_cast table
CREATE TABLE IF NOT EXISTS show_cast_members (
    show_id varchar(20) NOT NULL,
    people_id INT NOT NULL
); 

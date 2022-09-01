\c netflix_titles;

-- What is the most common first name among actors and actresses?
SELECT split_part(name, ' ', 1) as first_name, COUNT(*) AS name_count
FROM people
WHERE role_id = 2 -- cast
GROUP BY first_name
ORDER BY name_count DESC
LIMIT 1;

-- Which Movie had the longest timespan from release to appearing on Netflix?
SELECT show_id, 
title, 
to_date(release_year::text, 'YYYY') AS release_year_date, 
date_added,
date_added - to_date(release_year::text, 'YYYY') AS date_difference_days
FROM shows
WHERE type_id = 2 -- Movie
AND date_added - to_date(release_year::text, 'YYYY') > 0 -- Avoid nulls
ORDER BY date_difference_days DESC
LIMIT 1;

-- Which Month of the year had the most new releases historically?
SELECT EXTRACT(MONTH FROM date_added) AS month_added,
COUNT(*) as monthly_releases
FROM shows
GROUP BY month_added
ORDER BY monthly_releases DESC
LIMIT 1;

-- Which year had the largest increase year on year (percentage wise) for TV Shows?
SELECT * FROM
(
    SELECT t.year_added,
    t.yearly_releases,
    lag(t.yearly_releases, 1) over (order by t.year_added) as previous_year_releases,
    (100 * (t.yearly_releases - lag(t.yearly_releases, 1) over (order by t.year_added)) / lag(t.yearly_releases, 1) over 
    (order by t.year_added)) as percentage_growth
    FROM 
    (
        SELECT EXTRACT(YEAR FROM date_added) AS year_added,
        COUNT(*) as yearly_releases
        FROM shows
        WHERE type_id = 1 -- TV Shows
        GROUP BY year_added
    ) t
) results
WHERE percentage_growth IS NOT NULL
ORDER BY percentage_growth DESC
LIMIT 1;


-- List the actresses that have appeared in a movie with Woody Harrelson more than once.
SELECT * 
FROM people
WHERE gender = 'f' -- actresses
AND people_id IN
(
    SELECT people_id -- People that appeared more than once with Woody Harrelson
    FROM show_cast_members
    WHERE show_id IN
    (
        SELECT show_id -- Woody Harrelson shows
        FROM show_cast_members
        WHERE people_id = ( -- Woody Harrelson people_id
            SELECT people_id 
            FROM people
            WHERE name = 'Woody Harrelson'
        )
    )
    GROUP BY people_id
    HAVING count(*) > 1
)
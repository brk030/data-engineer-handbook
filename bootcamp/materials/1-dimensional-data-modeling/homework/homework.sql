-- DDL for actors table 
CREATE TYPE film AS (
        film TEXT,
        votes INTEGER,
        rating DECIMAL
        );
        
CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');

DROP TABLE IF EXISTS actors;        
CREATE TABLE actors (
        actor TEXT,
        films film[],
        quality_class quality_class,
        is_active BOOLEAN
        );
        
-- Cumulative Table Generation Query
WITH 

cte_yesterday AS (
        SELECT * FROM actors WHERE year=1969
),

cte_today AS (
        SELECT * FROM actor_films WHERE year=1970
)
        
        
SELECT 
        actor, 
        year, 
        array_agg[ROW(af.film, af.votes, af.rating)::film] 
FROM actor_films af WHERE actor='Lauren Bacall' GROUP BY actor, year
        
        
  
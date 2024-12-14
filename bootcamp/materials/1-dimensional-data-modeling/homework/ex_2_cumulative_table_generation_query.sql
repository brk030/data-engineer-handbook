DROP TABLE IF EXISTS actors;

CREATE TABLE actors (
        actor TEXT,
        year INTEGER,
        films film[],
        quality_class quality_class,
        is_active BOOLEAN,
        PRIMARY KEY (actor, year)
        );

DO $$
declare
i integer := 1970;

BEGIN

WHILE i <= 2021 loop

        raise notice 'Run number %',i;

        INSERT INTO actors
        
        WITH 
        cte_yesterday AS (
                SELECT * FROM actors WHERE year = i-1
        ),
        
        cte_today AS (
                SELECT 
                actor, 
                year, 
                array_agg(ROW(af.film, af.votes, af.rating)::film) as films,
                AVG(rating) AS avg_rating,
                TRUE AS is_active 
                FROM actor_films af 
                WHERE year=i
                GROUP BY actor, year
        )
                
                
        SELECT 
                COALESCE(t.actor, y.actor) AS actor,
                COALESCE(t.year, y.year+1) as year,
                CASE 
                        WHEN y.films IS NULL THEN t.films
                        WHEN t.films IS NOT NULL THEN y.films || t.films
                        ELSE y.films END AS films,
                CASE
                        WHEN t.avg_rating > 8 THEN 'star'
                        WHEN t.avg_rating > 7 THEN 'good'
                        WHEN t.avg_rating > 6 THEN 'average' ELSE 'bad' END::quality_class,
                CASE WHEN t.is_active IS TRUE THEN TRUE ELSE FALSE END AS is_active
        FROM cte_today t
        FULL OUTER JOIN cte_yesterday y
                ON t.actor=y.actor;
        raise notice 'Run finished';
        i := i + 1; 
end loop; 
END $$;
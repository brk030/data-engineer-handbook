-- DDL for actors table 
/*
CREATE TYPE film AS (
        film TEXT,
        votes INTEGER,
        rating DECIMAL
        );
*/        

--CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');

/*
DROP TABLE IF EXISTS actors;        
CREATE TABLE actors (
        actor TEXT,
        year INTEGER,
        films film[],
        quality_class quality_class,
        is_active BOOLEAN,
        PRIMARY KEY (actor, year)
        );
*/


-- Cumulative Table Generation Query
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
    
-- DDL for table 'actors_history_scd'
/*
DROP TABLE IF EXISTS actors_history_scd;
CREATE TABLE actors_history_scd (
        actor TEXT,
        quality_class quality_class,
        is_active BOOLEAN,
        current_year INTEGER,
        start_year INTEGER,
        end_year INTEGER,
        PRIMARY KEY (actor, start_date, end_date)
        );

*/


-- 4. Backfill query
/*
insert into actors_history_scd
with

cte_with_previous as (
	select
		actor,
		year,
		quality_class,
		LAG(quality_class, 1) over (partition by actor order by year) previous_quality_class,
		is_active,
		LAG(is_active, 1) over (partition by actor order by year) as previous_is_active
	from actors
	where year <= 2020
	order by actor, year
),

cte_with_change_indicator as (
	select
		*,
		case when quality_class <> previous_quality_class then 1
			 when is_active <> previous_is_active then 1 else 0 end as change_indicator
	from cte_with_previous
),

cte_with_streaks as (
	select
		*,
		SUM(change_indicator) over (partition by actor order by year) as streak_identifier
	from cte_with_change_indicator
)

select
	actor,
	quality_class,
	is_active,
	2020 as current_year,
	MIN(year) as start_year,
	MAX(year) as end_year
from cte_with_streaks
group by
	actor, streak_identifier, quality_class, is_active
;
*/


-- 5. Incremental query for actors_history_scd
/*
 CREATE TYPE scd_type_actors AS (
        quality_class quality_class,
        is_active BOOLEAN,
        start_season INTEGER,
        end_season INTEGER);
*/

WITH

historical_year_scd AS (
    SELECT
        actor,
        quality_class,
        is_active,
        start_year,
        end_year
    FROM actors_history_scd
    WHERE current_year = 2020
        AND end_year < 2020
),

last_year_scd AS (
    SELECT *
    FROM actors_history_scd
    WHERE current_year = 2020 and end_year=2020
),

this_year_data as (
	select *
	from actors a
	where year=2021
),

unchanged_records as (
	select
		ty.actor,
		ly.quality_class,
		ly.is_active,
		ly.start_year,
		ty.year as end_year
	from this_year_data ty
	join last_year_scd ly
		on ty.actor=ly.actor
	where ty.quality_class = ly.quality_class and ty.is_active = ly.is_active
),

changed_records as (
	select
		ty.actor,

		unnest(
			ARRAY[
				row(ty.quality_class, ty.is_active, ty.year, ty.year)::scd_type_actors,
				row(ly.quality_class, ly.is_active, ly.start_year, ly.current_year)::scd_type_actors
			]
		) as records

	from this_year_data ty
	join last_year_scd ly
		on ty.actor=ly.actor
	where 1=1
		and (ty.quality_class <> ly.quality_class OR ty.is_active <> ly.is_active)
		and ly.actor is not null
),

unnested_changed_records as (
	select
		actor,
		(records::scd_type_actors).*
	from changed_records
),

new_records as (
	select
		ty.actor,
        ty.quality_class,
        ty.is_active,
        ty.year as start_year,
        ty.year as end_year
	from this_year_data ty
	join last_year_scd ly
		on ty.actor=ly.actor
	where ly.actor is null
)

select * from historical_year_scd

union all

select * from unchanged_records

union all

select * from unnested_changed_records

union all

select * from new_records
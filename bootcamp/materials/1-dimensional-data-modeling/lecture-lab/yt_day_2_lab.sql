/*
DROP TABLE IF EXISTS players_scd;
CREATE TABLE players_scd (
        player_name TEXT,
        scoring_class scoring_class,
        is_active BOOLEAN,
        current_season INTEGER,
        start_season INTEGER,
        end_season INTEGER,
        PRIMARY KEY (player_name, start_season)     
);
*/

/*
CREATE TYPE scd_type AS (
        scoring_class scoring_class,
        is_active BOOLEAN,
        start_season INTEGER,
        end_season INTEGER 
);
*/

/*
-- Create the players_scd table, which functions as the current table including history
INSERT INTO players_scd  

WITH 
CTE_with_previous AS (
        SELECT
                player_name, 
                current_season,
                scoring_class,
                is_active,
                LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
                LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active
        FROM players
        WHERE current_season <= 2021 -- would be a parameter you inject in Airflow
),

CTE_with_indicators AS (
        SELECT *,
                CASE 
                        WHEN scoring_class <> previous_scoring_class THEN 1 
                        WHEN is_active <> previous_is_active THEN 1 ELSE 0 END AS change_indicator
        FROM CTE_with_previous
),

CTE_with_streaks AS (
        SELECT *,
                SUM(change_indicator) OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier 
        FROM CTE_with_indicators
)

SELECT 
        player_name, 
        scoring_class,
        is_active,
        2021 as current_season,
        MIN(current_season) as start_season,
        MAX(current_season) as end_season
FROM CTE_with_streaks
GROUP BY player_name, streak_identifier, is_active, scoring_class
ORDER BY player_name, start_season;
*/

WITH 

historical_season_scd AS (
        SELECT 
                player_name,
                scoring_class,
                is_active,
                start_season,
                end_season
        FROM players_scd 
        WHERE 1=1
                AND current_season = 2021
                AND end_season < 2021
),

last_season_scd AS (
        SELECT * 
        FROM players_scd 
        WHERE 1=1
                AND current_season = 2021
                AND end_season = 2021       
),

this_season_data AS (
        SELECT * 
        FROM players
        WHERE current_season = 2022
),

unchanged_records AS (
        SELECT 
                ts.player_name,
                ls.scoring_class, 
                ts.is_active,
                ls.start_season, 
                ts.current_season as end_season
        FROM this_season_data ts
        JOIN last_season_scd ls
                ON ts.player_name=ls.player_name
        WHERE 1=1
                AND ts.scoring_class = ls.scoring_class
                AND ts.is_active = ls.is_active
),

changed_records AS (
        SELECT 
                ts.player_name,
                UNNEST(ARRAY[
                        ROW(
                                ls.scoring_class, 
                                ls.is_active,
                                ls.start_season, 
                                ls.current_season)::scd_type,
                        ROW(
                                ts.scoring_class, 
                                ts.is_active,
                                ts.current_season, 
                                ts.current_season)::scd_type
                ]) AS records
        FROM this_season_data ts
        LEFT JOIN last_season_scd ls
                ON ts.player_name=ls.player_name
        WHERE (ts.scoring_class <> ls.scoring_class OR ts.is_active <> ls.is_active)
           AND  ls.player_name IS NOT NULL
),

unnested_changed_records AS (
        SELECT
                player_name,
                (records::scd_type).*
        FROM changed_records
),

new_records AS (
        SELECT 
                ts.player_name,
                ts.scoring_class,
                ts.is_active,
                ts.current_season AS start_season,
                ts.current_season AS end_season
        FROM this_season_data ts
        LEFT JOIN last_season_scd ls
                ON ts.player_name=ls.player_name
        WHERE ls.player_name IS NULL  
)

SELECT * FROM historical_season_scd

UNION ALL

SELECT * FROM unchanged_records

UNION ALL

SELECT * FROM  unnested_changed_records

UNION ALL

SELECT * FROM new_records

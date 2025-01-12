/*
select 
	gd.game_id,
	gd.team_id,
	gd.player_id,
	g.game_date_est,
	COUNT(*)
from game_details gd
join games g 
	on gd.game_id=g.game_id
group by 1, 2, 3, 4
having COUNT(*) > 1;
*/

-- Create filter to dedupe records which have been saved twice

with deduped as (
	select 
		g.game_date_est,
		g.season, 
		g.home_team_id,
		gd.*,
		-- usually use order by in the window function; this dataset does not have any timestamp
		ROW_NUMBER() over (partition by gd.game_id, team_id, player_id order by game_date_est) as row_num
	from game_details gd 
	join games g 
		on gd.game_id=g.game_id
)

select 
	game_date_est,
	season,
	team_id = home_team_id as dim_is_playing_at_home,
	player_id,
	player_name,
	start_position, 
	coalesce(position('DNP' in comment), 0) > 0 as dim_did_not_play,
	coalesce(position('DND' in comment), 0) > 0 as dim_did_not_dress,
	coalesce(position('NWT' in comment), 0) > 0 as dim_not_with_team,
	CAST(split_part(min, ':', 1) as REAL) + CAST(split_part(min, ':', 2) as REAL) / 60 as minutes,
	fgm, -- field goal mades
	fga, -- field goals attempted 
	fg3m,
	fg3a,
	ftm,
	fta,
	oreb,
	dreb,
	reb,
	ast,
	stl,
	blk,
	"TO" as turnovers,
	pf,
	pts,
	plus_minus
from deduped 
where 1=1
	and row_num = 1	
;


-- CREATE DDL
drop table if exists fct_game_details;
create table fct_game_details (
	-- dim columns are the one which you should filter/ group by
	dim_game_date DATE,
	dim_season INTEGER,
	dim_team_id INTEGER,
	dim_player_id INTEGER,
	dim_player_name TEXT,
	dim_is_playing_at_home BOOLEAN,
	dim_start_position TEXT,
	dim_did_not_play BOOLEAN,
	dim_did_not_dress BOOLEAN,
	dim_not_with_team BOOLEAN,
	-- measure columns should be used for aggregation; do math on
	m_minutes real,
	m_fgm INTEGER,
	m_fga INTEGER,
	m_fg3m INTEGER,
	m_fg3a INTEGER,
	m_ftm INTEGER,
	m_fta INTEGER,
	m_oreb INTEGER,
	m_dreb INTEGER,
	m_reb INTEGER,
	m_ast INTEGER,
	m_stl INTEGER,
	m_blk INTEGER,
	m_turnovers INTEGER,
	m_pf INTEGER,
	m_pts INTEGER,
	m_plus_minus INTEGER,
	primary key (dim_game_date, dim_team_id, dim_player_id)
);

-- Rewrite above query to match for the table 'fct_game_details'
insert into fct_game_details

with deduped as (
	select 
		g.game_date_est,
		g.season, 
		g.home_team_id,
		gd.*,
		-- usually use order by in the window function; this dataset does not have any timestamp
		ROW_NUMBER() over (partition by gd.game_id, team_id, player_id order by game_date_est) as row_num
	from game_details gd 
	join games g 
		on gd.game_id=g.game_id
)

select 
	game_date_est as dim_game_date,
	season as dim_season,
	team_id as dim_team_id,
	player_id as dim_player_id,
	player_name as dim_player_name,
	team_id = home_team_id as dim_is_playing_at_home,
	start_position as dim_start_position, 
	coalesce(position('DNP' in comment), 0) > 0 as dim_did_not_play,
	coalesce(position('DND' in comment), 0) > 0 as dim_did_not_dress,
	coalesce(position('NWT' in comment), 0) > 0 as dim_not_with_team,
	CAST(split_part(min, ':', 1) as REAL) + CAST(split_part(min, ':', 2) as REAL) / 60 as m_minutes,
	fgm as m_fgm, -- field goal mades
	fga as m_fga, -- field goals attempted 
	fg3m as m_fg3m,
	fg3a as m_fg3a,
	ftm as m_ftm,
	fta as m_fta,
	oreb as m_oreb,
	dreb as m_dreb,
	reb as m_reb,
	ast as m_ast,
	stl as m_stl,
	blk as m_blk,
	"TO" as m_turnovers,
	pf as m_pf,
	pts as m_pts,
	plus_minus as m_plus_minus
from deduped 
where 1=1
	and row_num = 1	
;

select * from fct_game_details;
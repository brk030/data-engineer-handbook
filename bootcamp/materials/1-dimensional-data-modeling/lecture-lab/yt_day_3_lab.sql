-- create type vertex_type as ENUM ('player', 'team', 'game');

/*
create table vertices(
	identifier TEXT,
	type vertex_type,
	properties JSON,
	primary key(identifier, type)
);
*/

-- create type edge_type as enum ('plays_against', 'shares_team', 'plays_in', 'plays_on');

/*
create table edges(
	subject_identifier text,
	subject_type vertex_type,
	object_identifier text,
	object_type vertex_type,
	edge_type edge_type,
	properties JSON,
	primary key(subject_identifier, subject_type, object_identifier, object_type, edge_type)
);
*/

-- create input for the table 'vertices'
/*
insert into vertices
select 
	game_id as identifier,
	'game'::vertex_type,
	json_build_object(
		'pts_home', pts_home,
		'pts_away', pts_away,
		'winning_team', case when home_team_wins=1 then home_team_id else visitor_team_id end 
	) as properties
from games;

insert into vertices 
with player_agg as (
	select 
		player_id as identifier,
		MAX(player_name) as player_name,
		COUNT(1) as number_of_games,
		SUM(pts) as total_points,
		ARRAY_AGG(distinct team_id) as teams
	from game_details gd 
	group by player_id 
 )
select 
	identifier,
	'player'::vertex_type,
	json_build_object(
		'player_name', player_name,
		'number_of_games', number_of_games,
		'total_points', total_points,
		'teams', teams
	)
from player_agg;


insert into vertices 

with 
teams_deduped as (
	select *,
	row_number() over(partition by team_id) as row_num
	from teams
)

select 
	team_id as identifier,
	'team'::vertex_type,
	json_build_object(
		'abbreviation', abbreviation,
		'nickname', nickname,
		'city', city,
		'arena', arena,
		'year_founded', yearfounded
	) 
from teams_deduped
where row_num=1;
*/


-- Create input for the table 'edges'
INSERT INTO edges
with 
deduped as (
	select *,
		row_number() over (partition by player_id, game_id) as row_num
	from game_details
)

select 
	player_id as subject_identifier,
	'player'::vertex_type as subject_type,
	game_id as object_identifier,
	'game'::vertex_type as object_type,
	'plays_in'::edge_type as edge_type,
	json_build_object(
		'start_position', start_position,
		'pts', pts,
		'team_id', team_id,
		'team_abbreviation', team_abbreviation
	) as properties
from deduped
where row_num=1;



INSERT INTO edges
with 
deduped as (
	select * 
	from (
		select *,
			row_number() over (partition by player_id, game_id) as row_num
		from game_details
	) filtered
),
aggregated as (
select 
	d1.player_id as subject_player_id,
	d2.player_id as object_player_id,
	case when d1.team_abbreviation = d2.team_abbreviation then 'shares_team'::edge_type else 'plays_against'::edge_type end as edge_type,
	MAX(d1.player_name) as subject_player_id_player_name,
	MAX(d2.player_name) as object_player_name,
	COUNT(1) as num_games,
	SUM(d1.pts) as subject_points,
	SUM(d2.pts) as object_points
from deduped d1
join deduped d2
	on   d1.game_id=d2.game_id
	 and d1.player_name <> d2.player_name
 where d1.player_id > d2.player_id  -- remove double edges; only keep single edges
 group by 
 	d1.player_id,
	d2.player_id,
	case when d1.team_abbreviation = d2.team_abbreviation then 'shares_team'::edge_type else 'plays_against'::edge_type end
)

select 
	subject_player_id as subject_identifier,
	'player'::vertex_type as subject_type,
	object_player_id as object_identifier,
	'player'::vertex_type as object_type,
	edge_type as edge_type,
	json_build_object(
		'num_games', num_games,
		'subject_points', subject_points,
		'object_points', object_points
	) 
from aggregated;


select 
	v.properties->>'player_name',
	cast(v.properties->>'number_of_games' as real) / 
	case when cast(v.properties->>'total_points' as REAL) = 0 then 1 else cast(v.properties->>'total_points' as REAL) end as points_per_game
from vertices v 
join edges e
	on v.identifier = e.subject_identifier
	 and v.type = e.subject_type
 where e.object_type = 'player'::vertex_type
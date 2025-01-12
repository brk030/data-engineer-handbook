/*
DROP TABLE IF EXISTS users_cumulated;
create table users_cumulated (
	user_id TEXT,  -- type INT has a limit in the billions
	dates_active DATE [],  -- the list of dates in the past where the user was active
	date DATE, -- current date for the user
	primary key (user_id, date)
);
*/

insert into users_cumulated 

with 
yesterday as (
	select * 
	from users_cumulated 
	where date = DATE('2023-01-30')
),

today as (
	select 
		cast(user_id as text),
		DATE(CAST(event_time as TIMESTAMP)) as date_active
	from events
	where DATE(CAST(event_time as TIMESTAMP)) = DATE('2023-01-31')
		and user_id is not null
	group by user_id, DATE(CAST(event_time as TIMESTAMP))
)

select 
	coalesce(t.user_id, y.user_id) as user_id,
	case 
		when y.date is null then ARRAY[t.date_active]
		when t.date_active is null then y.dates_active -- so not an array of null will be filled in 
		else ARRAY[t.date_active] || y.dates_active 
	end as dates_active,	
	coalesce(t.date_active, y.date + interval '1 day')
from today t
full outer join yesterday y
	on t.user_id = y.user_id;


with 
users as (
	select * 
	from users_cumulated uc 
	where date = DATE('2023-01-31')
),

series as (
	select * from generate_series('2023-01-01', '2023-01-31', interval '1 day') as series_date
),

-- bit masks
placeholder_ints as (
select 
	*,
	case 
		when dates_active @> array [DATE(series_date)] then cast(POW(2, 32 - (date - DATE(series_date))) as BIGINT) -- find out wherer the date ('series_date') is in the arrays 'dates_active'
		else 0 end as placeholder_int_value
from users
cross join series
)

select 
	user_id,
	cast(cast(SUM(placeholder_int_value) as BIGINT) as BIT(32)),
	BIT_COUNT(cast(cast(SUM(placeholder_int_value) as BIGINT) as BIT(32))) as days_active,
	BIT_COUNT(cast(cast(SUM(placeholder_int_value) as BIGINT) as BIT(32))) > 0 as dim_is_monthly_active,
	-- can also be done on a weekly basis but therefore is a bitwise operation needed; only 1 and 1 will be 1 otherwise 0(false)
	
	BIT_COUNT(CAST('11111110000000000000000000000000' as bit(32)) & 
	cast(cast(SUM(placeholder_int_value) as BIGINT) as BIT(32))) > 0 as dim_is_weekly_active
from placeholder_ints
group by user_id;





/*
DROP TABLE IF EXISTS array_metrics; 
create table array_metrics (
	user_id NUMERIC,
	month_start DATE,
	metric_name TEXT,
	metric_array REAL[],
	primary key (user_id, month_start, metric_name)
);
*/

insert into array_metrics
with 
daily_aggregate as (
	select 
		user_id,
		DATE(event_time) as date,
		COUNT(1) as num_site_hits
	from events e 
	where DATE(event_time)=DATE('2023-01-03')
		and user_id is not null
	group by user_id, DATE(event_time)
),

yesterday_array as (
	select *
	from array_metrics
	where month_start=DATE('2023-01-01')
)


select 
	coalesce(da.user_id, ya.user_id) as user_id,
	coalesce (ya.month_start, date_trunc('month', da.date)),
	'site_hits' as metric_name,
	case 
		when ya.metric_array is not null then ya.metric_array || ARRAY[coalesce(da.num_site_hits, 0)]
		when ya.metric_array is null then ARRAY_FILL(0, array[coalesce(da.date - DATE(date_trunc('month', da.date)), 0)]) || ARRAY[coalesce(da.num_site_hits, 0)]
	end as metric_array
from daily_aggregate da 
full outer join yesterday_array ya
	on da.user_id = ya.user_id
on conflict (user_id, month_start, metric_name)
do update set metric_array = excluded.metric_array;

with 
agg as (

	select 
		metric_name,
		month_start,
		array [
			sum(metric_array[1]), 
			sum(metric_array[2]), 
			sum(metric_array[3])
		] as summed_array
	from array_metrics
	group by metric_name, month_start
)



select 
	metric_name,
	month_start + cast((index-1) as int) as date,
	month_start + cast(cast(index - 1 as TEXT) || ' day' as interval) as date2,
	elem as value
from agg
cross join UNNEST(agg.summed_array) 
with ordinality as a(elem, index);
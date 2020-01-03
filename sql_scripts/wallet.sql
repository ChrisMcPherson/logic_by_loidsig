select * from wallet.cobinhood;

select * from wallet.binance;


---- Binance Daily balances
with access_totals as (
	select sum(usd_value::float) as total
		, access_datetime
		, (access_datetime::timestamp)::date as balance_day
	from wallet.binance
	group by access_datetime
		, (access_datetime::timestamp)::date
),
daily_totals as (
	select balance_day
		, first_value(total)
			over(partition by balance_day
			order by access_datetime desc
			rows between unbounded preceding and unbounded following) as daily_total
	from access_totals
)
select balance_day
	, max(daily_total) as daily_total
from daily_totals
group by balance_day
order by balance_day desc;


---- Cobinhood Daily balances
with access_totals as (
	select sum(usd_value::float) as total
		, access_datetime
		, (access_datetime::timestamp)::date as balance_day
	from wallet.cobinhood
	group by access_datetime
		, (access_datetime::timestamp)::date
),
daily_totals as (
	select balance_day
		, first_value(total)
			over(partition by balance_day
			order by access_datetime desc
			rows between unbounded preceding and unbounded following) as daily_total
	from access_totals
)
select balance_day
	, max(daily_total) as daily_total
from daily_totals
group by balance_day
order by balance_day desc;
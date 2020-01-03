select count(*) 
from the_logic.scoring_results;

select *
from the_logic.scoring_results
order by trade_datetime desc
limit 2000;


-- Inspect scoring results
select *
from the_logic.scoring_results
where trade_datetime > '2019-01-01 04:00:43'
order by trade_datetime asc
limit 2000;


-- Trade rows
select * 
from the_logic.scoring_results
where is_trade = true
order by trade_datetime desc;


-- Model versions
SELECT *
FROM the_logic.model_versions;

-- Trade data
select model_version, trade_datetime, predicted_return, trade_duration
	, ((sell_price - buy_price) / buy_price) * 100 as actual_return_perc
	, sell_price*sell_quantity - buy_price*buy_quantity as raw_return_dollars
	, (sell_price*sell_quantity - buy_price*buy_quantity) - (sell_price*sell_quantity * .0005) - (buy_price*buy_quantity * .0005) as return_dollars_post_fees
	, model_version
	, scoring_close_price
	, buy_price_pre_order
	, buy_price
	, sell_price_pre_order
	, sell_price
from the_logic.scoring_results
where is_trade = true
and model_version > 3.01
order by trade_datetime desc;


--- fix bad model version
update the_logic.scoring_results
set model_version = 3.0
where trade_datetime > '2018-11-20 21:06:20';


-- Total return for Binance Trading
with first_last_trade as (
	select min(trade_minute) as min_trade_min, max(trade_minute) as max_trade_min, model_version, target_coin
	from the_logic.scoring_results
	group by model_version, target_coin, is_trade
	having is_trade = true
),
first_trade_price as (
	select buy_price as first_price, sr.model_version, sr.target_coin
	from the_logic.scoring_results sr
	inner join first_last_trade flt on flt.model_version = sr.model_version and flt.target_coin = sr.target_coin and flt.min_trade_min = sr.trade_minute
	group by sr.model_version, sr.target_coin, sr.is_trade, sr.trade_minute, buy_price
	having is_trade = true
),
last_trade_price as (
	select sell_price as last_price, sr.model_version, sr.target_coin
	from the_logic.scoring_results sr
	inner join first_last_trade flt on flt.model_version = sr.model_version and flt.target_coin = sr.target_coin and flt.max_trade_min = sr.trade_minute
	group by sr.model_version, sr.target_coin, sr.is_trade, sr.trade_minute, sell_price
	having is_trade = true
)
select scoring_results.model_version, scoring_results.target_coin
	, count(*) as trade_count
	, count(distinct trade_datetime::date) as days_active
	, count(*) / count(distinct trade_datetime::date)::float as avg_trades_per_day
	, sum(sell_price*sell_quantity) - sum(buy_price*buy_quantity) as raw_return_dollars
	, sum((sell_price*sell_quantity - buy_price*buy_quantity) - (sell_price * sell_quantity * .0005) - (buy_price * buy_quantity * .0005)) as return_dollars_post_fees
	, sum(((sell_price - buy_price) / buy_price) * 100) as total_return_perc
	, sum((((sell_price - (sell_price * .0005) - (buy_price * .0005)) - buy_price) / buy_price) * 100) as total_return_perc_after_binance
	, ((sum(sell_price) - sum(buy_price)) / sum(buy_price)) * 100 as avg_return_perc
	, (((sum(sell_price) - sum(buy_price)) / sum(buy_price)) * 100) - (.001 * count(distinct trade_minute)) as avg_return_perc_after_binance
	, ((((sum(sell_price) - sum(buy_price)) / sum(buy_price)) * 100) - (.001 * count(distinct trade_minute))) / count(*) as avg_return_per_trade
	, ((ltp.last_price - ftp.first_price) / ftp.first_price) * 100 as long_term_position_return_perc
	, ftp.first_price
	, ltp.last_price
from the_logic.scoring_results
inner join first_trade_price ftp on scoring_results.model_version = ftp.model_version and scoring_results.target_coin = ftp.target_coin
inner join last_trade_price ltp on scoring_results.model_version = ltp.model_version and scoring_results.target_coin = ltp.target_coin
group by scoring_results.model_version, scoring_results.is_trade, scoring_results.target_coin, ftp.first_price, ltp.last_price
having is_trade = true
and scoring_results.model_version > 3.01
;


, report as (select scoring_results.model_version, scoring_results.target_coin
	, count(*) as trade_count
	, count(distinct trade_datetime::date) as days_active
	, count(*) / count(distinct trade_datetime::date)::float as avg_trades_per_day
	--, sum(sell_price*sell_quantity) - sum(buy_price*buy_quantity) as raw_return_dollars
	--, sum((sell_price*sell_quantity - buy_price*buy_quantity) - (sell_price * sell_quantity * .0005) - (buy_price * buy_quantity * .0005)) as return_dollars_post_fees
	, sum(((sell_price - buy_price) / buy_price) * 100) as total_return_perc
	, sum((((sell_price - (sell_price * .0005) - (buy_price * .0005)) - buy_price) / buy_price) * 100) as total_return_after_fees
	--, ((sum(sell_price) - sum(buy_price)) / sum(buy_price)) * 100 as avg_return_perc
	--, (((sum(sell_price) - sum(buy_price)) / sum(buy_price)) * 100) - (.001 * count(distinct trade_minute)) as avg_return_perc_after_binance
	--, ((((sum(sell_price) - sum(buy_price)) / sum(buy_price)) * 100) - (.001 * count(distinct trade_minute))) / count(*) as avg_return_per_trade
	, ((ltp.last_price - ftp.first_price) / ftp.first_price) * 100 as long_term_position_return_perc
	--, ftp.first_price
	--, ltp.last_price
from the_logic.scoring_results
inner join first_trade_price ftp on scoring_results.model_version = ftp.model_version and scoring_results.target_coin = ftp.target_coin
inner join last_trade_price ltp on scoring_results.model_version = ltp.model_version and scoring_results.target_coin = ltp.target_coin
group by scoring_results.model_version, scoring_results.is_trade, scoring_results.target_coin, ftp.first_price, ltp.last_price
having is_trade = true and scoring_results.model_version >= 1.2
)
select sum(trade_count) as trade_count, sum(days_active) as days_active
, sum(total_return_perc) as total_return_percentage
, sum(total_return_after_fees) as total_return_percentage_, sum(long_term_position_return_perc) as long_term_position_return_comparison
from report;



---- Cobinhood results
with first_last_trade as (
	select min(trade_minute) as min_trade_min, max(trade_minute) as max_trade_min, model_version, target_coin
	from the_logic.scoring_results
	group by model_version, target_coin, is_trade
	having is_trade = true
),
first_trade_price as (
	select buy_price as first_price, sr.model_version, sr.target_coin
	from the_logic.scoring_results sr
	inner join first_last_trade flt on flt.model_version = sr.model_version and flt.target_coin = sr.target_coin and flt.min_trade_min = sr.trade_minute
	group by sr.model_version, sr.target_coin, sr.is_trade, sr.trade_minute, buy_price
	having is_trade = true
),
last_trade_price as (
	select sell_price as last_price, sr.model_version, sr.target_coin
	from the_logic.scoring_results sr
	inner join first_last_trade flt on flt.model_version = sr.model_version and flt.target_coin = sr.target_coin and flt.max_trade_min = sr.trade_minute
	group by sr.model_version, sr.target_coin, sr.is_trade, sr.trade_minute, sell_price
	having is_trade = true
)
select scoring_results.model_version, scoring_results.target_coin
	, count(*) as trade_count
	, count(distinct trade_datetime::date) as days_active
	, count(*) / count(distinct trade_datetime::date)::float as avg_trades_per_day
	, sum(sell_price*sell_quantity - buy_price*buy_quantity) as return_dollars
	, sum(((sell_price - buy_price) / buy_price) * 100) as total_return_perc
	, ((sum(sell_price) - sum(buy_price)) / sum(buy_price)) * 100 as avg_return_perc
	, ((((sum(sell_price) - sum(buy_price)) / sum(buy_price)) * 100) - (.001 * count(distinct trade_minute))) / count(*) as avg_return_per_trade
	, ((ltp.last_price - ftp.first_price) / ftp.first_price) * 100 as long_term_position_return_perc
	, ftp.first_price
	, ltp.last_price
from the_logic.scoring_results
inner join first_trade_price ftp on scoring_results.model_version = ftp.model_version and scoring_results.target_coin = ftp.target_coin
inner join last_trade_price ltp on scoring_results.model_version = ltp.model_version and scoring_results.target_coin = ltp.target_coin
group by scoring_results.model_version, scoring_results.is_trade, scoring_results.target_coin, ftp.first_price, ltp.last_price
having is_trade = true;


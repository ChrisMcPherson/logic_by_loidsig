SELECT count(*) 
FROM binance.orderbook o;

--8,316,135 
--

SELECT *
FROM binance.orderbook o
limit 200;

-- Number of Days
SELECT count(distinct to_timestamp(trade_minute * 60)::date) as days_active
FROM binance.orderbook o
where coin_pair = 'btcusdt';
     
           
-- Find gaps in trade_minute
with gaps as (
select trade_minute as gap_start, 
       next_nr as gap_end,
       next_nr - trade_minute as gap_size
from (
  select trade_minute, 
         lead(trade_minute) over (order by trade_minute) as next_nr
  from binance.orderbook
  where coin_pair = 'btceth'
) nr
where trade_minute + 1 <> next_nr
)
select *
from gaps
where gap_size > 2
order by gap_size desc;


-- Maintenance
VACUUM ANALYZE binance.orderbook;
show autovacuum;


-- Find long running queries
SELECT
  pid,
  now() - pg_stat_activity.query_start AS duration,
  query,
  state
FROM pg_stat_activity
WHERE (now() - pg_stat_activity.query_start) > interval '5 minutes';

SELECT pid, age(clock_timestamp(), query_start), usename, query 
FROM pg_stat_activity 
WHERE query != '<IDLE>' AND query NOT ILIKE '%pg_stat_activity%' 
ORDER BY query_start desc;

-- cancel
SELECT pg_cancel_backend(__pid__);
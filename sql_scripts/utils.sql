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


-- 
SELECT to_timestamp(trade_minute * 60) AS trade_datetime, *
FROM binance.orderbook o
order by trade_minute DESC
limit 2000;        
           
-- Find gaps in trade_minute
with gaps as (
select trade_minute as gap_start, 
       next_nr as gap_end,
       next_nr - trade_minute as gap_size
from (
  select trade_minute, 
         lead(trade_minute) over (order by trade_minute) as next_nr
  from binance.orderbook
  where coin_pair = 'btcusdt'
) nr
where trade_minute + 1 <> next_nr
)
select *
from gaps
where gap_size > 2
order by gap_size desc;


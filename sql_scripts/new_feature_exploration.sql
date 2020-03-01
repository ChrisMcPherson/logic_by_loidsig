SELECT *
FROM binance.candledicks 
WHERE coin_pair = 'ethusdt'
ORDER BY trade_minute DESC 
LIMIT 2000
;

SELECT file_name
	, count(*)
FROM binance.candledicks c 
GROUP BY file_name
ORDER BY count(*) ASC 
;

SELECT CONCAT('binance/historic_candlesticks/', file_name)
FROM binance.candledicks c 
GROUP BY file_name
HAVING count(*) = 1440
;

SELECT array_agg(CONCAT('binance/historic_candlesticks/', file_name))
FROM binance.candledicks c 
GROUP BY file_name
HAVING count(*) = 1440
;

'binance/historic_candlesticks/bnbusdt/2018-01-01.csv'

SELECT max(trade_minute)
FROM binance.candledicks c 
;
SELECT min(trade_minute)
FROM binance.orderbook o 
;

-- How well DO the DATA SETS MATCH up? 

SELECT count(*)
FROM binance.candledicks c 
LEFT JOIN binance.orderbook o ON o.coin_pair = c.coin_pair  AND o.trade_minute = c.trade_minute 
WHERE c.coin_pair in ('btcusdt')
AND o.trade_minute IS NULL 
AND c.trade_minute < 26351999
AND c.trade_minute > 25655701
;


-- join orderbook front and back of minute
WITH end_of_minute_orderbook AS (
	SELECT trade_minute - 1 AS lag_trade_minute, * 
	FROM binance.orderbook
)
SELECT o_beg.trade_minute, o_beg.trade_minute*60, o_beg.coin_pair, o_end.trade_minute, o_end.lag_trade_minute, o_end.trade_minute*60, c.close_datetime, c.*, o_beg.*
FROM binance.candledicks c 
INNER JOIN binance.orderbook o_beg ON o_beg.coin_pair = c.coin_pair  AND o_beg.trade_minute = c.trade_minute 
INNER JOIN end_of_minute_orderbook o_end ON o_end.coin_pair = c.coin_pair  AND o_end.lag_trade_minute = c.trade_minute
WHERE c.coin_pair = 'btcusdt'
AND c.trade_minute < 26351999
ORDER BY c.trade_minute desc
LIMIT 2000
;


-- combine starting bids with taker_buy percentage and 
-- combine starting asks (availble supply at what prices) (price weighted volume?) with buyer quote asset volume
---- ?? we're not capturing orderbook volume. Only average price at different volume levels. What would this look like? 
------ does how many limit orders you'd have to cover matter? or just volume
------- also, don't forget orderbook is snapshot, changes rapidly. current available supply may not make sense to be compared directly to volume across a whole minute
-------- It's the change in the demand compared to the change in supply that matters!!!!!!!

-- all minute intervals from 0-10
--- both single step and growing step intervals 

-- for:
--- total volume current and changes over time (quote asset volume) for demand
--- sell/buy volume distribution percentages current and over time for demand
--- average trade count current and over time 

--- compare price spread of each side of the orderbook for a set volume target, and how this changes over time - could give us an idea of the direction of supply
--- price changes between demand? -- redundant with above?
--- volume adjusted reversion to the mean for rolling price -- current price (current market order or orderbook price? - orderbook will be demand sensitive) compared to rolling mean (may want to try longer intervals here)

-- combine starting asks with taker_sell percentage and quote asset volume 


WITH end_of_minute_orderbook AS (
	SELECT trade_minute - 1 AS lag_trade_minute, * 
	FROM binance.orderbook
	WHERE coin_pair  = 'btcusdt'
	ORDER BY trade_minute DESC 
	LIMIT 5
),
beg_of_minute_orderbook AS (
	SELECT * 
	FROM binance.orderbook
	WHERE coin_pair  = 'btcusdt'
	ORDER BY trade_minute DESC 
	LIMIT 5
),
candlesticks_cte AS (
	SELECT *
	FROM binance.candledicks c
	WHERE coin_pair  = 'btcusdt'
	ORDER BY trade_minute DESC 
	LIMIT 5
)
SELECT quote_asset_volume
	, ((quote_asset_volume - LEAD(quote_asset_volume, 1) OVER (ORDER BY c.trade_minute DESC)) 
       / LEAD(quote_asset_volume, 1) OVER (ORDER BY c.trade_minute DESC)) * 100 AS prev_5_quote_asset_volume_perc_chg
    , taker_sell_volume_percentage * 100 AS taker_sell_volume_perc_of_total
    , ((taker_sell_volume_percentage - LEAD(taker_sell_volume_percentage, 1) OVER (ORDER BY c.trade_minute DESC)) 
       / LEAD(taker_sell_volume_percentage, 1) OVER (ORDER BY c.trade_minute DESC)) * 100 AS prev_5_taker_sell_volume_perc_of_total_chg
    , trade_count
    , ((trade_count::float - LEAD(trade_count::float, 1) OVER (ORDER BY c.trade_minute DESC)) 
       / LEAD(trade_count::float, 1) OVER (ORDER BY c.trade_minute DESC)) * 100 AS prev_5_trade_count_perc_chg
    , o_end.bids_cum_50000_weighted_avg - o_beg.bids_cum_50000_weighted_avg AS crnt_interval_bids_50000_price_diff
    , ((o_end.bids_cum_50000_weighted_avg - LEAD(o_end.bids_cum_50000_weighted_avg, 1) OVER (ORDER BY c.trade_minute DESC)) 
       / LEAD(o_end.bids_cum_50000_weighted_avg, 1) OVER (ORDER BY c.trade_minute DESC)) * 100 AS prev_5_bids_50000_perc_chg
    , o_end.bids_cum_50000_weighted_avg - o_end.asks_cum_50000_weighted_avg AS crnt_interval_bids_v_asks_50000_price_diff -- add more of these at different price targets? higher targets more meaningful? 
    , o_end.bids_cum_50000_weighted_std - o_beg.bids_cum_50000_weighted_std AS crnt_interval_bids_50000_std_diff
    , ((o_end.bids_cum_50000_weighted_std - LEAD(o_end.bids_cum_50000_weighted_std, 1) OVER (ORDER BY c.trade_minute DESC)) 
       / LEAD(o_end.bids_cum_50000_weighted_std, 1) OVER (ORDER BY c.trade_minute DESC)) * 100 AS prev_5_bids_50000_std_chg
    , o_end.bids_cum_50000_weighted_std - o_end.asks_cum_50000_weighted_std AS crnt_interval_bids_v_asks_50000_std_diff
    , o_end.bids_cum_50000_weighted_std / (o_end.bids_cum_50000_weighted_std + o_end.asks_cum_50000_weighted_std) AS crnt_bids_50000_std_perc_of_total
    , o_end.bids_cum_200000_weighted_std / (o_end.bids_cum_200000_weighted_std + o_end.asks_cum_200000_weighted_std) AS crnt_bids_200000_std_perc_of_total
    , (o_end.bids_cum_200000_weighted_std / (o_end.bids_cum_200000_weighted_std + o_end.asks_cum_200000_weighted_std) 
       + LEAD(o_end.bids_cum_200000_weighted_std, 1) OVER (ORDER BY c.trade_minute DESC) / (LEAD(o_end.bids_cum_200000_weighted_std, 1) OVER (ORDER BY c.trade_minute DESC) + LEAD(o_end.asks_cum_200000_weighted_std, 1) OVER (ORDER BY c.trade_minute DESC)) 
       + LEAD(o_end.bids_cum_200000_weighted_std, 2) OVER (ORDER BY c.trade_minute DESC) / (LEAD(o_end.bids_cum_200000_weighted_std, 2) OVER (ORDER BY c.trade_minute DESC) + LEAD(o_end.asks_cum_200000_weighted_std, 2) OVER (ORDER BY c.trade_minute DESC))
       + LEAD(o_end.bids_cum_200000_weighted_std, 3) OVER (ORDER BY c.trade_minute DESC) / (LEAD(o_end.bids_cum_200000_weighted_std, 3) OVER (ORDER BY c.trade_minute DESC) + LEAD(o_end.asks_cum_200000_weighted_std, 3) OVER (ORDER BY c.trade_minute DESC))
       + LEAD(o_end.bids_cum_200000_weighted_std, 4) OVER (ORDER BY c.trade_minute DESC) / (LEAD(o_end.bids_cum_200000_weighted_std, 4) OVER (ORDER BY c.trade_minute DESC) + LEAD(o_end.asks_cum_200000_weighted_std, 4) OVER (ORDER BY c.trade_minute DESC))
       ) / 5 AS bids_200000_std_perc_of_total_avg
, o_beg.trade_minute, o_beg.trade_minute*60, o_beg.coin_pair, o_end.trade_minute, o_end.lag_trade_minute, o_end.trade_minute*60, c.close_datetime, c.*, o_beg.*
FROM candlesticks_cte c 
INNER JOIN beg_of_minute_orderbook o_beg ON o_beg.coin_pair = c.coin_pair  AND o_beg.trade_minute = c.trade_minute 
INNER JOIN end_of_minute_orderbook o_end ON o_end.coin_pair = c.coin_pair  AND o_end.lag_trade_minute = c.trade_minute
WHERE c.coin_pair = 'btcusdt'
ORDER BY c.trade_minute desc
LIMIT 5;













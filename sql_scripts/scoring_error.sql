---- Trade threshold exploration
-- How does the models error change overtime? Is this something we could predict in order to set a better trade threshold? How is this different than putting features of error model directly into base model? The fact that its generalized?
-- Previous error (average? std dev? lag?) as a feature in future model scoring
select sr.trade_datetime
	, sr.trade_minute
	, ob_start.trade_minute as start_trade_minute
	, ob_end.trade_minute as end_trade_minute
	, ob_start.asks_cum_5000_weighted_avg
	, ob_end.bids_cum_5000_weighted_avg
	, sr.scoring_latency_seconds
	, (ob_end.bids_cum_5000_weighted_avg - ob_start.asks_cum_5000_weighted_avg) / ob_start.asks_cum_5000_weighted_avg * 100 as full_return
	, (ob_end.bids_cum_5000_weighted_avg - ob_delayed_start.asks_cum_5000_weighted_avg) / ob_delayed_start.asks_cum_5000_weighted_avg * 100 as delayed_start_return
	, sr.predicted_return
	, sr.predicted_return - ((ob_end.bids_cum_5000_weighted_avg - ob_start.asks_cum_5000_weighted_avg) / ob_start.asks_cum_5000_weighted_avg * 100) as full_error
	, ((sell_price - buy_price) / buy_price) * 100 as actual_return
from the_logic.scoring_results as sr
inner join binance.orderbook as ob_start on sr.trade_minute = ob_start.trade_minute 
	and sr.target_coin = ob_start.coin_pair 
inner join binance.orderbook as ob_delayed_start on (sr.trade_minute + 1) = ob_delayed_start.trade_minute 
	and sr.target_coin = ob_delayed_start.coin_pair 
inner join binance.orderbook as ob_end on (sr.trade_minute + sr.trade_duration) = ob_end.trade_minute 
	and sr.target_coin = ob_end.coin_pair 
where trade_datetime > '2019-01-01 04:00:43'
	and sr.trade_duration = 4
	and sr.predicted_return > .2
order by sr.trade_datetime asc
limit 2000;

-- Challenge: The model is predicting the growth between minute 0 and the start of minute 4. However, we're likely executing trades between minute 1 and 2. 


SELECT *
FROM binance.orderbook
order by trade_minute DESC
limit 2000;

select *
from the_logic.scoring_results
where trade_datetime > '2019-01-01 04:00:43'
order by trade_datetime asc
limit 2000;
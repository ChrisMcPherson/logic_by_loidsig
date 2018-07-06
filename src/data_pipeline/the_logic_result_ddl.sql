-- Model scoring results
create table the_logic.scoring_results (
	  trade_datetime timestamp
	, trade_minute int
	, target_coin varchar(12)
	, trade_duration smallint
	, predicted_return double precision
	, predicted_growth_rate double precision
	, highest_return boolean
	, is_trade boolean
	, trade_quantity double precision
	, order_id int
	, client_order_id text
	, trade_threshold double precision
	, feature_window_space int[]
	, trade_duration_space int[]
	, coin_pair_definition json
	, scoring_latency_seconds double precision
);

---- order data
-- raw 
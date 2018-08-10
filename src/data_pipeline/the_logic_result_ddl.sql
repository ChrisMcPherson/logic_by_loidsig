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
	, trade_threshold double precision
	, feature_window_space int[]
	, trade_duration_space int[]
	, coin_pair_definition json
	, scoring_latency_seconds double precision
	, buy_quantity double precision
	, sell_quantity double precision
	, buy_price double precision
	, sell_price double precision
	, buy_commission double precision
	, sell_commission double precision
	, buy_commission_coin varchar(12)
	, sell_commission_coin varchar(12)
	, buy_order_id int
	, buy_client_order_id text
	, sell_order_id int
	, sell_client_order_id text
);

---- Model Versions
create table the_logic.model_versions(
	model_version double precision
  , description text
);

select * from the_logic.model_versions;

insert into the_logic.model_versions
values (1.3, 'Features: added feature scaler and current funds dependent trade quantity');
CREATE TABLE binance.candlesticks (
	trade_minute int 
    , coin_pair varchar(12)
    , open_timestamp int
    , close_timestamp int
    , open double precision
    , high double precision	
    , low double precision	
    , close double precision	
    , volume double precision	
    , quote_asset_volume double precision	
    , trade_count int
    , taker_buy_base_asset_volume double precision	
    , taker_buy_quote_asset_volume double precision	
    , open_datetime	timestamp
    , close_datetime timestamp
    , file_name text
    , PRIMARY KEY(coin_pair, trade_minute)
);
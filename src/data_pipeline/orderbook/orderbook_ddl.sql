
CREATE TABLE cobinhood.orderbook (
	trade_minute int 
    , coin_pair varchar(12)
    , bids_top_price double precision
    , bids_cum_5000_weighted_avg double precision
    , bids_cum_10000_weighted_avg double precision
    , bids_cum_20000_weighted_avg double precision
    , bids_cum_50000_weighted_avg double precision
    , bids_cum_100000_weighted_avg double precision
    , bids_cum_200000_weighted_avg double precision
    , bids_cum_5000_weighted_std double precision
    , bids_cum_10000_weighted_std double precision
    , bids_cum_20000_weighted_std double precision
    , bids_cum_50000_weighted_std double precision
    , bids_cum_100000_weighted_std double precision
    , bids_cum_200000_weighted_std double precision
    , asks_top_price double precision
    , asks_cum_5000_weighted_avg double precision
    , asks_cum_10000_weighted_avg double precision
    , asks_cum_20000_weighted_avg double precision
    , asks_cum_50000_weighted_avg double precision
    , asks_cum_100000_weighted_avg double precision
    , asks_cum_200000_weighted_avg double precision
    , asks_cum_5000_weighted_std double precision
    , asks_cum_10000_weighted_std double precision
    , asks_cum_20000_weighted_std double precision
    , asks_cum_50000_weighted_std double precision
    , asks_cum_100000_weighted_std double precision
    , asks_cum_200000_weighted_std double precision
    , PRIMARY KEY(coin_pair, trade_minute)
);

CREATE TABLE binance.orderbook (
	trade_minute int 
    , coin_pair varchar(12)
    , bids_top_price double precision
    , bids_cum_5000_weighted_avg double precision
    , bids_cum_10000_weighted_avg double precision
    , bids_cum_20000_weighted_avg double precision
    , bids_cum_50000_weighted_avg double precision
    , bids_cum_100000_weighted_avg double precision
    , bids_cum_200000_weighted_avg double precision
    , bids_cum_5000_weighted_std double precision
    , bids_cum_10000_weighted_std double precision
    , bids_cum_20000_weighted_std double precision
    , bids_cum_50000_weighted_std double precision
    , bids_cum_100000_weighted_std double precision
    , bids_cum_200000_weighted_std double precision
    , asks_top_price double precision
    , asks_cum_5000_weighted_avg double precision
    , asks_cum_10000_weighted_avg double precision
    , asks_cum_20000_weighted_avg double precision
    , asks_cum_50000_weighted_avg double precision
    , asks_cum_100000_weighted_avg double precision
    , asks_cum_200000_weighted_avg double precision
    , asks_cum_5000_weighted_std double precision
    , asks_cum_10000_weighted_std double precision
    , asks_cum_20000_weighted_std double precision
    , asks_cum_50000_weighted_std double precision
    , asks_cum_100000_weighted_std double precision
    , asks_cum_200000_weighted_std double precision
    , PRIMARY KEY(coin_pair, trade_minute)
);



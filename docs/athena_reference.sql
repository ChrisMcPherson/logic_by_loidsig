-- Add and check partitions
ALTER TABLE historic_trades 
ADD PARTITION (coin_partition='ltcusdt') location 's3://loidsig-crypto/binance/historic_trades/ltcusdt/',
PARTITION (coin_partition='enjeth') location 's3://loidsig-crypto/binance/historic_trades/enjeth/',
PARTITION (coin_partition='ethbtc') location 's3://loidsig-crypto/binance/historic_trades/ethbtc/',
PARTITION (coin_partition='neoeth') location 's3://loidsig-crypto/binance/historic_trades/neoeth/',
PARTITION (coin_partition='xrpeth') location 's3://loidsig-crypto/binance/historic_trades/xrpeth/',;

MSCK REPAIR TABLE binance.historic_trades;
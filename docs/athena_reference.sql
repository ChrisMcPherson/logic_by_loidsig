-- Add and check partitions
-- Only seems to work if you are using the date=20180101 file partition format (https://stackoverflow.com/questions/48421866/failure-to-repair-partitions-in-amazon-athena)
ALTER TABLE historic_trades 
ADD PARTITION (coin_partition='ltcusdt') location 's3://loidsig-crypto/binance/historic_trades/ltcusdt/',
PARTITION (coin_partition='enjeth') location 's3://loidsig-crypto/binance/historic_trades/enjeth/',
PARTITION (coin_partition='ethbtc') location 's3://loidsig-crypto/binance/historic_trades/ethbtc/',
PARTITION (coin_partition='neoeth') location 's3://loidsig-crypto/binance/historic_trades/neoeth/',
PARTITION (coin_partition='xrpeth') location 's3://loidsig-crypto/binance/historic_trades/xrpeth/',;

MSCK REPAIR TABLE binance.historic_trades;
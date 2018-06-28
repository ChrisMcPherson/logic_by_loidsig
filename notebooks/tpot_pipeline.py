import boto3
import pandas as pd
import numpy as np
import io
import sys
import os
# local libraries
sys.path.append(os.path.abspath(os.path.join(sys.path[0], '..', 'src', 'lib')))
import athena_connect

from sklearn import ensemble
from sklearn import linear_model
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.svm import SVR
from sklearn.utils import resample
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import PolynomialFeatures
from sklearn.metrics import r2_score, classification_report
from sklearn.model_selection import train_test_split

from tpot import TPOTRegressor

pd.options.mode.chained_assignment = None

# Extract queried data from Athena
features_sql = """WITH target_trxeth AS (
  SELECT coin_partition
    , DATE(from_unixtime(cast(close_timestamp AS BIGINT) / 1000)) AS trade_date
    , from_unixtime(cast(close_timestamp AS BIGINT) / 1000) AS trade_datetime
    , (CAST(close_timestamp AS BIGINT) / 1000 / 60) AS trade_minute
    , CAST(open AS DOUBLE) AS open, CAST(high AS DOUBLE) AS high, CAST(low AS DOUBLE) AS low, CAST(close AS DOUBLE) AS close
    , CAST(volume AS DOUBLE) AS volume, CAST(quote_asset_volume AS DOUBLE) AS quote_asset_volume, CAST(trade_count AS BIGINT) AS trade_count
    , CAST(taker_buy_base_asset_volume AS DOUBLE) as tbbav, CAST(taker_buy_quote_asset_volume AS DOUBLE) as tbqav
  FROM binance.historic_candlesticks 
  WHERE coin_partition = 'trxeth'
),
alt_ethbtc AS (
  SELECT coin_partition AS ethbtc_coin_partition
    , DATE(from_unixtime(cast(close_timestamp AS BIGINT) / 1000)) AS ethbtc_trade_date
    , (CAST(close_timestamp AS BIGINT) / 1000 / 60) AS ethbtc_trade_minute
    , CAST(open AS DOUBLE) AS ethbtc_open, CAST(high AS DOUBLE) AS ethbtc_high, CAST(low AS DOUBLE) AS ethbtc_low
    , CAST(close AS DOUBLE) AS ethbtc_close, CAST(volume AS DOUBLE) AS ethbtc_volume
    , CAST(quote_asset_volume AS DOUBLE) AS ethbtc_quote_asset_volume, CAST(trade_count AS BIGINT) AS ethbtc_trade_count
    , CAST(taker_buy_base_asset_volume AS DOUBLE) AS ethbtc_tbbav, CAST(taker_buy_quote_asset_volume AS DOUBLE) AS ethbtc_tbqav
  FROM binance.historic_candlesticks 
  WHERE coin_partition = 'ethbtc'
),
through_ethusdt AS (
  SELECT coin_partition AS ethusdt_coin_partition
    , DATE(from_unixtime(cast(close_timestamp AS BIGINT) / 1000)) AS ethusdt_trade_date
    , (CAST(close_timestamp AS BIGINT) / 1000 / 60) AS ethusdt_trade_minute
    , CAST(open AS DOUBLE) AS ethusdt_open, CAST(high AS DOUBLE) AS ethusdt_high, CAST(low AS DOUBLE) AS ethusdt_low
    , CAST(close AS DOUBLE) AS ethusdt_close, CAST(volume AS DOUBLE) AS ethusdt_volume
    , CAST(quote_asset_volume AS DOUBLE) AS ethusdt_quote_asset_volume, CAST(trade_count AS BIGINT) AS ethusdt_trade_count
    , CAST(taker_buy_base_asset_volume AS DOUBLE) AS ethusdt_tbbav, CAST(taker_buy_quote_asset_volume AS DOUBLE) AS ethusdt_tbqav
  FROM binance.historic_candlesticks 
  WHERE coin_partition = 'ethusdt'
)
SELECT trade_datetime, CAST(day_of_week(trade_datetime) AS SMALLINT) as trade_day_of_week, CAST(hour(trade_datetime) AS SMALLINT) as trade_hour
-- Target Features
 , open, high, low, close, volume, quote_asset_volume, trade_count, tbbav, tbqav
-- Alt Features
 , ethbtc_open, ethbtc_high, ethbtc_low, ethbtc_close, ethbtc_volume
 , ethbtc_quote_asset_volume, ethbtc_trade_count, ethbtc_tbbav, ethbtc_tbqav
-- Through Features
 , ethusdt_open, ethusdt_high, ethusdt_low, close, ethusdt_volume
 , ethusdt_quote_asset_volume, ethusdt_trade_count, ethusdt_tbbav, ethusdt_tbqav
-- LAG Features
 ,((open - LEAD(open) OVER (ORDER BY trade_minute DESC)) / LEAD(open) OVER (ORDER BY trade_minute DESC)) * 100 AS prev_1_open_perc_chg
 ,((LEAD(open) OVER (ORDER BY trade_minute DESC) - LEAD(open, 2) OVER (ORDER BY trade_minute DESC)) / LEAD(open, 2) OVER (ORDER BY trade_minute DESC)) * 100 AS prev_2_3_open_perc_chg
 ,((LEAD(open) OVER (ORDER BY trade_minute DESC) - LEAD(open, 3) OVER (ORDER BY trade_minute DESC)) / LEAD(open, 3) OVER (ORDER BY trade_minute DESC)) * 100 AS prev_3_4_open_perc_chg
 ,((LEAD(open) OVER (ORDER BY trade_minute DESC) - LEAD(open, 4) OVER (ORDER BY trade_minute DESC)) / LEAD(open, 4) OVER (ORDER BY trade_minute DESC)) * 100 AS prev_4_5_open_perc_chg
 ,((open - LEAD(open, 5) OVER (ORDER BY trade_minute DESC)) / LEAD(open, 5) OVER (ORDER BY trade_minute DESC)) * 100 AS prev_5_open_perc_chg
 ,((open - LEAD(open, 10) OVER (ORDER BY trade_minute DESC)) / LEAD(open, 10) OVER (ORDER BY trade_minute DESC)) * 100 AS prev_10_open_perc_chg
 ,(((open - LEAD(open) OVER (ORDER BY trade_minute DESC)) / LEAD(open) OVER (ORDER BY trade_minute DESC)) * 100) -
   (((open - LEAD(open, 5) OVER (ORDER BY trade_minute DESC)) / LEAD(open, 5) OVER (ORDER BY trade_minute DESC)) * 100) AS prev_5_open_rate_chg
 ,(((open - LEAD(open) OVER (ORDER BY trade_minute DESC)) / LEAD(open) OVER (ORDER BY trade_minute DESC)) * 100) -
   (((open - LEAD(open, 10) OVER (ORDER BY trade_minute DESC)) / LEAD(open, 10) OVER (ORDER BY trade_minute DESC)) * 100) AS prev_10_open_rate_chg
 ,((high - LEAD(high, 5) OVER (ORDER BY trade_minute DESC)) / LEAD(high, 5) OVER (ORDER BY trade_minute DESC)) * 100 AS prev_5_high_perc_chg
 ,((high - LEAD(high, 10) OVER (ORDER BY trade_minute DESC)) / LEAD(high, 10) OVER (ORDER BY trade_minute DESC)) * 100 AS prev_10_high_perc_chg
 ,((low - LEAD(low, 5) OVER (ORDER BY trade_minute DESC)) / LEAD(low, 5) OVER (ORDER BY trade_minute DESC)) * 100 AS prev_5_low_perc_chg
 ,((low - LEAD(low, 10) OVER (ORDER BY trade_minute DESC)) / LEAD(low, 10) OVER (ORDER BY trade_minute DESC)) * 100 AS prev_10_low_perc_chg
 ,CAST(COALESCE(TRY(((volume - LEAD(volume, 5) OVER (ORDER BY trade_minute DESC)) / LEAD(volume, 5) OVER (ORDER BY trade_minute DESC)) * 100),0) AS DOUBLE) AS prev_5_volume_perc_chg
 ,COALESCE(TRY(((volume - LEAD(volume, 10) OVER (ORDER BY trade_minute DESC)) / LEAD(volume, 10) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_10_volume_perc_chg 
 ,COALESCE(TRY(((quote_asset_volume - LEAD(quote_asset_volume, 5) OVER (ORDER BY trade_minute DESC)) / LEAD(quote_asset_volume, 5) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_5_qav_perc_chg
 ,COALESCE(TRY(((quote_asset_volume - LEAD(quote_asset_volume, 10) OVER (ORDER BY trade_minute DESC)) / LEAD(quote_asset_volume, 10) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_10_qav_perc_chg
 ,COALESCE(TRY(((trade_count - LEAD(trade_count, 5) OVER (ORDER BY trade_minute DESC)) / LEAD(trade_count, 5) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_5_trade_count_perc_chg
 ,COALESCE(TRY(((trade_count - LEAD(trade_count, 10) OVER (ORDER BY trade_minute DESC)) / LEAD(trade_count, 10) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_10_trade_count_perc_chg 
,COALESCE(TRY(((tbbav - LEAD(tbbav, 5) OVER (ORDER BY trade_minute DESC)) / LEAD(tbbav, 5) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_5_tbbav_perc_chg
 ,COALESCE(TRY(((tbbav - LEAD(tbbav, 10) OVER (ORDER BY trade_minute DESC)) / LEAD(tbbav, 10) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_10_tbbav_perc_chg 
 ,COALESCE(TRY(((tbqav - LEAD(tbqav, 5) OVER (ORDER BY trade_minute DESC)) / LEAD(tbqav, 5) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_5_tbqav_perc_chg
 ,COALESCE(TRY(((tbqav - LEAD(tbqav, 10) OVER (ORDER BY trade_minute DESC)) / LEAD(tbqav, 10) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_10_tbqav_perc_chg 

-- LAG Alt Features
 ,((ethbtc_open - LEAD(ethbtc_open) OVER (ORDER BY trade_minute DESC)) / LEAD(ethbtc_open) OVER (ORDER BY trade_minute DESC)) * 100 AS prev_1_open_perc_chg
 ,((ethbtc_open - LEAD(ethbtc_open, 5) OVER (ORDER BY trade_minute DESC)) / LEAD(ethbtc_open, 5) OVER (ORDER BY trade_minute DESC)) * 100 AS prev_5_ethbtc_open_perc_chg
 ,((ethbtc_open - LEAD(ethbtc_open, 10) OVER (ORDER BY trade_minute DESC)) / LEAD(ethbtc_open, 10) OVER (ORDER BY trade_minute DESC)) * 100 AS prev_10_ethbtc_open_perc_chg
 ,(((ethbtc_open - LEAD(ethbtc_open) OVER (ORDER BY trade_minute DESC)) / LEAD(ethbtc_open) OVER (ORDER BY trade_minute DESC)) * 100) -
   (((ethbtc_open - LEAD(ethbtc_open, 5) OVER (ORDER BY trade_minute DESC)) / LEAD(ethbtc_open, 5) OVER (ORDER BY trade_minute DESC)) * 100) AS prev_5_ethbtc_open_rate_chg
 ,(((ethbtc_open - LEAD(ethbtc_open) OVER (ORDER BY trade_minute DESC)) / LEAD(ethbtc_open) OVER (ORDER BY trade_minute DESC)) * 100) -
   (((ethbtc_open - LEAD(ethbtc_open, 10) OVER (ORDER BY trade_minute DESC)) / LEAD(ethbtc_open, 10) OVER (ORDER BY trade_minute DESC)) * 100) AS prev_10_open_rate_chg   
 ,((ethbtc_high - LEAD(ethbtc_high, 5) OVER (ORDER BY trade_minute DESC)) / LEAD(ethbtc_high, 5) OVER (ORDER BY trade_minute DESC)) * 100 AS prev_5_ethbtc_high_perc_chg
 ,((ethbtc_high - LEAD(ethbtc_high, 10) OVER (ORDER BY trade_minute DESC)) / LEAD(ethbtc_high, 10) OVER (ORDER BY trade_minute DESC)) * 100 AS prev_10_high_perc_chg
 ,((ethbtc_low - LEAD(ethbtc_low, 5) OVER (ORDER BY trade_minute DESC)) / LEAD(ethbtc_low, 5) OVER (ORDER BY trade_minute DESC)) * 100 AS prev_5_ethbtc_low_perc_chg
 ,((ethbtc_low - LEAD(ethbtc_low, 10) OVER (ORDER BY trade_minute DESC)) / LEAD(ethbtc_low, 10) OVER (ORDER BY trade_minute DESC)) * 100 AS prev_10_ethbtc_low_perc_chg   
 ,COALESCE(TRY(((ethbtc_volume - LEAD(ethbtc_volume, 5) OVER (ORDER BY trade_minute DESC)) / LEAD(ethbtc_volume, 5) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_5_ethbtc_volume_perc_chg
 ,COALESCE(TRY(((ethbtc_volume - LEAD(ethbtc_volume, 10) OVER (ORDER BY trade_minute DESC)) / LEAD(ethbtc_volume, 10) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_10_ethbtc_volume_perc_chg 
 ,COALESCE(TRY(((ethbtc_quote_asset_volume - LEAD(ethbtc_quote_asset_volume, 5) OVER (ORDER BY trade_minute DESC)) / LEAD(ethbtc_quote_asset_volume, 5) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_5_ethbtc_qav_perc_chg
 ,COALESCE(TRY(((ethbtc_quote_asset_volume - LEAD(ethbtc_quote_asset_volume, 10) OVER (ORDER BY trade_minute DESC)) / LEAD(ethbtc_quote_asset_volume, 10) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_10_ethbtc_qav_perc_chg 
 ,COALESCE(TRY(((ethbtc_trade_count - LEAD(ethbtc_trade_count, 5) OVER (ORDER BY trade_minute DESC)) / LEAD(ethbtc_trade_count, 5) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_5_ethbtc_trade_count_perc_chg
 ,COALESCE(TRY(((ethbtc_trade_count - LEAD(ethbtc_trade_count, 10) OVER (ORDER BY trade_minute DESC)) / LEAD(ethbtc_trade_count, 10) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_10_ethbtc_trade_count_perc_chg 
 ,COALESCE(TRY(((ethbtc_tbbav - LEAD(ethbtc_tbbav, 5) OVER (ORDER BY trade_minute DESC)) / LEAD(ethbtc_tbbav, 5) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_5_ethbtc_tbbav_perc_chg
 ,COALESCE(TRY(((ethbtc_tbbav - LEAD(ethbtc_tbbav, 10) OVER (ORDER BY trade_minute DESC)) / LEAD(ethbtc_tbbav, 10) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_10_ethbtc_tbbav_perc_chg 
 ,COALESCE(TRY(((ethbtc_tbqav - LEAD(ethbtc_tbqav, 5) OVER (ORDER BY trade_minute DESC)) / LEAD(ethbtc_tbqav, 5) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_5_ethbtc_tbqav_perc_chg
 ,COALESCE(TRY(((ethbtc_tbqav - LEAD(ethbtc_tbqav, 10) OVER (ORDER BY trade_minute DESC)) / LEAD(ethbtc_tbqav, 10) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_10_ethbtc_tbqav_perc_chg 
 -- LAG Through Features
 ,((ethusdt_open - LEAD(ethusdt_open) OVER (ORDER BY trade_minute DESC)) / LEAD(open) OVER (ORDER BY trade_minute DESC)) * 100 AS prev_1_open_perc_chg
 ,((ethusdt_open - LEAD(ethusdt_open, 5) OVER (ORDER BY trade_minute DESC)) / LEAD(ethusdt_open, 5) OVER (ORDER BY trade_minute DESC)) * 100 AS prev_5_ethusdt_open_perc_chg
 ,((ethusdt_open - LEAD(ethusdt_open, 10) OVER (ORDER BY trade_minute DESC)) / LEAD(ethusdt_open, 10) OVER (ORDER BY trade_minute DESC)) * 100 AS prev_10_ethusdt_open_perc_chg
 ,(((ethusdt_open - LEAD(ethusdt_open) OVER (ORDER BY trade_minute DESC)) / LEAD(ethusdt_open) OVER (ORDER BY trade_minute DESC)) * 100) -
   (((ethusdt_open - LEAD(ethusdt_open, 5) OVER (ORDER BY trade_minute DESC)) / LEAD(ethusdt_open, 5) OVER (ORDER BY trade_minute DESC)) * 100) AS prev_5_ethusdt_open_rate_chg
 ,(((ethusdt_open - LEAD(ethusdt_open) OVER (ORDER BY trade_minute DESC)) / LEAD(ethusdt_open) OVER (ORDER BY trade_minute DESC)) * 100) -
   (((ethusdt_open - LEAD(ethusdt_open, 10) OVER (ORDER BY trade_minute DESC)) / LEAD(ethusdt_open, 10) OVER (ORDER BY trade_minute DESC)) * 100) AS prev_10_ethusdt_open_rate_chg  
 ,((ethusdt_high - LEAD(ethusdt_high, 5) OVER (ORDER BY trade_minute DESC)) / LEAD(ethusdt_high, 5) OVER (ORDER BY trade_minute DESC)) * 100 AS prev_5_ethusdt_high_perc_chg
 ,((ethusdt_high - LEAD(ethusdt_high, 10) OVER (ORDER BY trade_minute DESC)) / LEAD(ethusdt_high, 10) OVER (ORDER BY trade_minute DESC)) * 100 AS prev_10_ethusdt_high_perc_chg 
 ,((ethusdt_low - LEAD(ethusdt_low, 5) OVER (ORDER BY trade_minute DESC)) / LEAD(ethusdt_low, 5) OVER (ORDER BY trade_minute DESC)) * 100 AS prev_5_ethusdt_low_perc_chg
 ,((ethusdt_low - LEAD(ethusdt_low, 10) OVER (ORDER BY trade_minute DESC)) / LEAD(ethusdt_low, 10) OVER (ORDER BY trade_minute DESC)) * 100 AS prev_10_ethusdt_low_perc_chg  
 ,COALESCE(TRY(((ethusdt_volume - LEAD(ethusdt_volume, 5) OVER (ORDER BY trade_minute DESC)) / LEAD(ethusdt_volume, 5) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_5_ethusdt_volume_perc_chg
 ,COALESCE(TRY(((ethusdt_volume - LEAD(ethusdt_volume, 10) OVER (ORDER BY trade_minute DESC)) / LEAD(ethusdt_volume, 10) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_10_ethusdt_volume_perc_chg
 ,COALESCE(TRY(((ethusdt_quote_asset_volume - LEAD(ethusdt_quote_asset_volume, 5) OVER (ORDER BY trade_minute DESC)) / LEAD(ethusdt_quote_asset_volume, 5) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_5_ethusdt_qav_perc_chg
 ,COALESCE(TRY(((ethusdt_quote_asset_volume - LEAD(ethusdt_quote_asset_volume, 10) OVER (ORDER BY trade_minute DESC)) / LEAD(ethusdt_quote_asset_volume, 10) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_10_ethusdt_qav_perc_chg
 ,COALESCE(TRY(((ethusdt_trade_count - LEAD(ethusdt_trade_count, 5) OVER (ORDER BY trade_minute DESC)) / LEAD(ethusdt_trade_count, 5) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_5_ethusdt_trade_count_perc_chg
 ,COALESCE(TRY(((ethusdt_trade_count - LEAD(ethusdt_trade_count, 10) OVER (ORDER BY trade_minute DESC)) / LEAD(ethusdt_trade_count, 10) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_10_ethusdt_trade_count_perc_chg
 ,COALESCE(TRY(((ethusdt_tbbav - LEAD(ethusdt_tbbav, 5) OVER (ORDER BY trade_minute DESC)) / LEAD(ethusdt_tbbav, 5) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_5_ethusdt_tbbav_perc_chg
 ,COALESCE(TRY(((ethusdt_tbbav - LEAD(ethusdt_tbbav, 10) OVER (ORDER BY trade_minute DESC)) / LEAD(ethusdt_tbbav, 10) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_10_ethusdt_tbbav_perc_chg
 ,COALESCE(TRY(((ethusdt_tbqav - LEAD(ethusdt_tbqav, 5) OVER (ORDER BY trade_minute DESC)) / LEAD(ethusdt_tbqav, 5) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_5_ethusdt_tbqav_perc_chg
 ,COALESCE(TRY(((ethusdt_tbqav - LEAD(ethusdt_tbqav, 10) OVER (ORDER BY trade_minute DESC)) / LEAD(ethusdt_tbqav, 10) OVER (ORDER BY trade_minute DESC)) * 100),0) AS prev_10_ethusdt_tbqav_perc_chg
  -- Target Variables
 ,((LAG(open) OVER (ORDER BY trade_minute DESC) - open) / open) * 100 AS futr_1_open_perc_chg
 ,((LAG(open, 5) OVER (ORDER BY trade_minute DESC) - open) / open) * 100 AS futr_5_open_perc_chg
 ,((LAG(open, 10) OVER (ORDER BY trade_minute DESC) - open) / open) * 100 AS futr_10_open_perc_chg 
FROM target_trxeth 
LEFT JOIN alt_ethbtc ON target_trxeth.trade_minute = alt_ethbtc.ethbtc_trade_minute
LEFT JOIN through_ethusdt ON target_trxeth.trade_minute = through_ethusdt.ethusdt_trade_minute
ORDER BY trade_minute ASC"""

athena = athena_connect.Athena()
features_df = athena.pandas_read_athena(features_sql)
features_df.fillna(0, inplace=True)
print(features_df.shape)
features_df.head(20)

feature_cols = ['trade_day_of_week', 'trade_hour', 'open', 'high',
       'low', 'close', 'volume', 'quote_asset_volume', 'trade_count', 'tbbav',
       'tbqav', 'ethbtc_open', 'ethbtc_high', 'ethbtc_low', 'ethbtc_close',
       'ethbtc_volume', 'ethbtc_quote_asset_volume', 'ethbtc_trade_count',
       'ethbtc_tbbav', 'ethbtc_tbqav', 'ethusdt_open', 'ethusdt_high',
       'ethusdt_low', 'close.1', 'ethusdt_volume',
       'ethusdt_quote_asset_volume', 'ethusdt_trade_count', 'ethusdt_tbbav',
       'ethusdt_tbqav', 'prev_1_open_perc_chg', 'prev_2_3_open_perc_chg',
       'prev_3_4_open_perc_chg', 'prev_4_5_open_perc_chg',
       'prev_5_open_perc_chg', 'prev_10_open_perc_chg', 'prev_5_open_rate_chg',
       'prev_10_open_rate_chg', 'prev_5_high_perc_chg',
       'prev_10_high_perc_chg', 'prev_5_low_perc_chg', 'prev_10_low_perc_chg',
       'prev_5_volume_perc_chg', 'prev_10_volume_perc_chg',
       'prev_5_qav_perc_chg', 'prev_10_qav_perc_chg',
       'prev_5_trade_count_perc_chg', 'prev_10_trade_count_perc_chg',
       'prev_5_tbbav_perc_chg', 'prev_10_tbbav_perc_chg',
       'prev_5_tbqav_perc_chg', 'prev_10_tbqav_perc_chg',
       'prev_1_open_perc_chg.1', 'prev_5_ethbtc_open_perc_chg',
       'prev_10_ethbtc_open_perc_chg', 'prev_5_ethbtc_open_rate_chg',
       'prev_10_open_rate_chg.1', 'prev_5_ethbtc_high_perc_chg',
       'prev_10_high_perc_chg.1', 'prev_5_ethbtc_low_perc_chg',
       'prev_10_ethbtc_low_perc_chg', 'prev_5_ethbtc_volume_perc_chg',
       'prev_10_ethbtc_volume_perc_chg', 'prev_5_ethbtc_qav_perc_chg',
       'prev_10_ethbtc_qav_perc_chg', 'prev_5_ethbtc_trade_count_perc_chg',
       'prev_10_ethbtc_trade_count_perc_chg', 'prev_5_ethbtc_tbbav_perc_chg',
       'prev_10_ethbtc_tbbav_perc_chg', 'prev_5_ethbtc_tbqav_perc_chg',
       'prev_10_ethbtc_tbqav_perc_chg', 'prev_1_open_perc_chg.2',
       'prev_5_ethusdt_open_perc_chg', 'prev_10_ethusdt_open_perc_chg',
       'prev_5_ethusdt_open_rate_chg', 'prev_10_ethusdt_open_rate_chg',
       'prev_5_ethusdt_high_perc_chg', 'prev_10_ethusdt_high_perc_chg',
       'prev_5_ethusdt_low_perc_chg', 'prev_10_ethusdt_low_perc_chg',
       'prev_5_ethusdt_volume_perc_chg', 'prev_10_ethusdt_volume_perc_chg',
       'prev_5_ethusdt_qav_perc_chg', 'prev_10_ethusdt_qav_perc_chg',
       'prev_5_ethusdt_trade_count_perc_chg',
       'prev_10_ethusdt_trade_count_perc_chg', 'prev_5_ethusdt_tbbav_perc_chg',
       'prev_10_ethusdt_tbbav_perc_chg', 'prev_5_ethusdt_tbqav_perc_chg',
       'prev_10_ethusdt_tbqav_perc_chg']
target_col = 'futr_10_open_perc_chg'

features_df = features_df[:-10]

# Remove infinity string
features_df.replace({'Infinity': 0}, inplace=True)

features_df['prev_5_volume_perc_chg'] = features_df['prev_5_volume_perc_chg'].apply(pd.to_numeric)
features_df['prev_10_volume_perc_chg'] = features_df['prev_10_volume_perc_chg'].apply(pd.to_numeric)
features_df['prev_5_qav_perc_chg'] = features_df['prev_5_qav_perc_chg'].apply(pd.to_numeric)                      
features_df['prev_10_qav_perc_chg'] = features_df['prev_10_qav_perc_chg'].apply(pd.to_numeric)                        
features_df['prev_5_tbbav_perc_chg'] = features_df['prev_5_tbbav_perc_chg'].apply(pd.to_numeric)                      
features_df['prev_10_tbbav_perc_chg'] = features_df['prev_10_tbbav_perc_chg'].apply(pd.to_numeric)                     
features_df['prev_5_tbqav_perc_chg'] = features_df['prev_5_tbqav_perc_chg'].apply(pd.to_numeric)                        
features_df['prev_10_tbqav_perc_chg'] = features_df['prev_10_tbqav_perc_chg'].apply(pd.to_numeric)                       
features_df['prev_5_ethbtc_volume_perc_chg'] = features_df['prev_5_ethbtc_volume_perc_chg'].apply(pd.to_numeric)                
features_df['prev_10_ethbtc_volume_perc_chg'] = features_df['prev_10_ethbtc_volume_perc_chg'].apply(pd.to_numeric)                 
features_df['prev_5_ethbtc_qav_perc_chg'] = features_df['prev_5_ethbtc_qav_perc_chg'].apply(pd.to_numeric)                    
features_df['prev_10_ethbtc_qav_perc_chg'] = features_df['prev_10_ethbtc_qav_perc_chg'].apply(pd.to_numeric)                    
features_df['prev_5_ethbtc_tbbav_perc_chg'] = features_df['prev_5_ethbtc_tbbav_perc_chg'].apply(pd.to_numeric)                 
features_df['prev_10_ethbtc_tbbav_perc_chg'] = features_df['prev_10_ethbtc_tbbav_perc_chg'].apply(pd.to_numeric)                 
features_df['prev_5_ethbtc_tbqav_perc_chg'] = features_df['prev_5_ethbtc_tbqav_perc_chg'].apply(pd.to_numeric)                  
features_df['prev_10_ethbtc_tbqav_perc_chg'] = features_df['prev_10_ethbtc_tbqav_perc_chg'].apply(pd.to_numeric)                  
features_df['prev_5_ethusdt_volume_perc_chg'] = features_df['prev_5_ethusdt_volume_perc_chg'].apply(pd.to_numeric)                
features_df['prev_10_ethusdt_volume_perc_chg'] = features_df['prev_10_ethusdt_volume_perc_chg'].apply(pd.to_numeric)                
features_df['prev_5_ethusdt_qav_perc_chg'] = features_df['prev_5_ethusdt_qav_perc_chg'].apply(pd.to_numeric)                
features_df['prev_10_ethusdt_qav_perc_chg'] = features_df['prev_10_ethusdt_qav_perc_chg'].apply(pd.to_numeric)                
features_df['prev_5_ethusdt_tbbav_perc_chg'] = features_df['prev_5_ethusdt_tbbav_perc_chg'].apply(pd.to_numeric)                 
features_df['prev_10_ethusdt_tbbav_perc_chg'] = features_df['prev_10_ethusdt_tbbav_perc_chg'].apply(pd.to_numeric)                 
features_df['prev_5_ethusdt_tbqav_perc_chg'] = features_df['prev_5_ethusdt_tbqav_perc_chg'].apply(pd.to_numeric)
features_df['prev_10_ethusdt_tbqav_perc_chg'] = features_df['prev_10_ethusdt_tbqav_perc_chg'].apply(pd.to_numeric)

# Split for last 4.5 hours training and adjust for look ahead 
#X_train, y_train = features_df[-300:-20][feature_cols], features_df[-300:-20][target_col] 
#X_test, y_test = features_df[-10:][feature_cols], features_df[-10:][target_col]

# Split for last x days training and adjust for look ahead 
days_training = 30 * -1440
hours_test = 8 * -60
X_train, y_train = features_df[days_training:(hours_test-10)][feature_cols], features_df[days_training:(hours_test-10)][target_col] 
X_test, y_test = features_df[hours_test:][feature_cols], features_df[hours_test:][target_col]

tpot = TPOTRegressor(generations=15, population_size=100, verbosity=2)
tpot.fit(X_train, y_train)
print(tpot.score(X_test, y_test))
tpot.export(f'tpot_{days_training/-1440}days_train_{hours_test/-60}hour_test_pipeline.py')

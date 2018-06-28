import boto3
import pandas as pd
import numpy as np
import io
import sys
import os
import ast
import time
import datetime
import json
from joblib import Parallel, delayed
import multiprocessing
# local libraries
sys.path.append(os.path.abspath(os.path.join(sys.path[0], '..', 'src', 'lib')))
import athena_connect
# modeling
from sklearn import linear_model
from sklearn import ensemble
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.svm import SVR
from sklearn.utils import resample
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import PolynomialFeatures
from sklearn.metrics import r2_score, classification_report
pd.options.mode.chained_assignment = None

def main():
    print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    
    # Default configuration
    start = 1
    target_coin = ['trxeth']
    feature_col = ['trade_day_of_week', 'trade_hour', 'open', 'high',
       'low', 'close', 'volume', 'quote_asset_volume', 'trade_count', 'tbbav',
       'tbqav', 'ethbtc_open', 'ethbtc_high', 'ethbtc_low', 'ethbtc_close',
       'ethbtc_volume', 'ethbtc_quote_asset_volume', 'ethbtc_trade_count',
       'ethbtc_tbbav', 'ethbtc_tbqav', 'ethusdt_open', 'ethusdt_high',
       'ethusdt_low', 'close.1', 'ethusdt_volume',
       'ethusdt_quote_asset_volume', 'ethusdt_trade_count', 'ethusdt_tbbav',
       'ethusdt_tbqav', 'avg_10_open_interaction',
       'prev_1_open_perc_chg', 'prev_2_3_open_perc_chg',
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
    target_col_list = ['futr_10_open_perc_chg']
    training_min_list = [360]
    test_min_list = [1]
    model_list = ['polynomial']
    poly_list = [3]
    
    # Get argument configurations
    target_coin_arg = [s for s in sys.argv[1:] if 'coin' in s.split('=',1)[0]]
    if target_coin_arg:
        target_coin_list = ast.literal_eval(target_coin_arg[0].split('=',1)[1]) # use eval instead of literal_eval to pick up models
        
    start_arg = [s for s in sys.argv[1:] if 'start' in s.split('=',1)[0]]
    if start_arg:
        start = int(start_arg[0].split('=',1)[1])
        
    feature_arg = [s for s in sys.argv[1:] if 'features' in s.split('=',1)[0]]
    if feature_arg:
        feature_col = ast.literal_eval(feature_arg[0].split('=',1)[1])
        
    target_arg = [s for s in sys.argv[1:] if 'target' in s.split('=',1)[0]]
    if target_arg:
        target_col_list = ast.literal_eval(target_arg[0].split('=',1)[1])
    
    training_min_arg = [s for s in sys.argv[1:] if 'train_min' in s.split('=',1)[0]]
    if training_min_arg:
        training_min_list = ast.literal_eval(training_min_arg[0].split('=',1)[1])
        
    test_min_arg = [s for s in sys.argv[1:] if 'test_min' in s.split('=',1)[0]]
    if test_min_arg:
        test_min_list = ast.literal_eval(test_min_arg[0].split('=',1)[1])
    
    model_arg = [s for s in sys.argv[1:] if 'model' in s.split('=',1)[0]]
    if model_arg:
        model_list = ast.literal_eval(model_arg[0].split('=',1)[1])
        
    poly_arg = [s for s in sys.argv[1:] if 'poly' in s.split('=',1)[0]]
    if poly_arg:
        poly_list = ast.literal_eval(poly_arg[0].split('=',1)[1])
    
    print(f"Coin(s): {target_coin_list}")    
    print(f"Model(s): {model_list}")
    print(f"Features: {feature_col}")
    print(f"Target Col(s): {target_col_list}")
    print(f"Sim Start Day: {start}")
    print(f"Training Min(s): {training_min_list}")
    print(f"Test Min(s): {test_min_list}")
    print(f"Poly(s): {poly_list}")
    
    # Write out configuration
    result_stats_json = json.dumps({'Coin(s)': ','.join(map(str, target_coin_list))
                                , 'Model(s)': ','.join(map(str, model_list))
                                , 'Target(s)': ','.join(map(str, target_col_list))
                                , 'Sim Start': start
                                , 'Train Min': ','.join(map(str, training_min_list))
                                , 'Test Min': ','.join(map(str, test_min_list))})
    with open('dominostats.json', 'w') as f:
        f.write(result_stats_json)
        
    # Simulate!
    for target_coin in target_coin_list:
        # Get features for target coin
        features_df = features(target_coin)
        for model in model_list:
            print(f"Model: {model}")
            for target_col in target_col_list:
                for test_min in test_min_list:
                    for polynomial in poly_list:
                        print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
                        sim_daily_trades_list = Parallel(n_jobs=multiprocessing.cpu_count())(delayed(simulate_return)(model, features_df, feature_col, target_col, target_coin, start_days=start, training_mins=training_min, test_mins=test_min, poly_degree=polynomial) for training_min in training_min_list)
                
    sim_daily_trades = pd.concat(sim_daily_trades_list)
    finish_time = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H-%M-%S')
    sim_daily_trades.to_csv(f"notebooks/sim_results/sim_daily_trades_{finish_time}.csv", index = False)
                    
def features(target_coin):
    # If an alt coin, include pass through features
    if 'usdt' in target_coin:
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
        WHERE coin_partition = 'ethusdt'
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
        WHERE coin_partition = 'trxeth'
        )
        SELECT trade_date, trade_datetime, CAST(day_of_week(trade_datetime) AS SMALLINT) as trade_day_of_week, CAST(hour(trade_datetime) AS SMALLINT) as trade_hour
        -- Target Features
        , open, high, low, close, volume, quote_asset_volume, trade_count, tbbav, tbqav
        -- Alt Features
        , ethbtc_open, ethbtc_high, ethbtc_low, ethbtc_close, ethbtc_volume
        , ethbtc_quote_asset_volume, ethbtc_trade_count, ethbtc_tbbav, ethbtc_tbqav
        -- Through Features
        , ethusdt_open, ethusdt_high, ethusdt_low, close, ethusdt_volume
        , ethusdt_quote_asset_volume, ethusdt_trade_count, ethusdt_tbbav, ethusdt_tbqav
        -- Interaction Features
        ,AVG((open-ethbtc_open)/open) OVER (PARTITION BY coin_partition ORDER BY trade_minute DESC ROWS 10 PRECEDING) - ((open-ethbtc_open)/open) AS avg_10_open_interaction
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
    else:
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
        SELECT trade_date, trade_minute, CAST(day_of_week(trade_datetime) AS SMALLINT) as trade_day_of_week, CAST(hour(trade_datetime) AS SMALLINT) as trade_hour
        -- Target Features
        , open, high, low, close, volume, quote_asset_volume, trade_count, tbbav, tbqav
        -- Alt Features
        , ethbtc_open, ethbtc_high, ethbtc_low, ethbtc_close, ethbtc_volume
        , ethbtc_quote_asset_volume, ethbtc_trade_count, ethbtc_tbbav, ethbtc_tbqav
        -- Through Features
        , ethusdt_open, ethusdt_high, ethusdt_low, close, ethusdt_volume
        , ethusdt_quote_asset_volume, ethusdt_trade_count, ethusdt_tbbav, ethusdt_tbqav
        -- Interaction Features
        ,AVG((open-ethbtc_open)/open) OVER (PARTITION BY coin_partition ORDER BY trade_minute DESC ROWS 10 PRECEDING) - ((open-ethbtc_open)/open) AS avg_10_open_interaction
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
    features_df['avg_10_open_interaction'] = features_df['avg_10_open_interaction'].apply(pd.to_numeric)

    print(features_df.shape)
    return features_df
    
def simulate_return(model, df, feature_cols, target_col, coin, start_days=1, training_mins=None, test_mins=1440, poly_degree=3):
    start_ix = 1440 * start_days
    results_df_list = []
    intervals = df[start_ix:].trade_date.nunique() * 1440 / test_mins
    future_days = int(''.join(filter(str.isdigit, target_col)))
    for day in range(int(intervals)):
        end_train_ix = (day * test_mins) + start_ix - future_days # subtract future day from outcome variable 
        end_test_ix = ((day * test_mins) + test_mins) + start_ix
        start_test_ix = end_train_ix + future_days
        if training_mins:
            start_train_ix = end_train_ix - training_mins
            start_train_ix = 0 if start_train_ix < 0 else start_train_ix
            train_df = df.iloc[start_train_ix:end_train_ix,:]
        else:
            train_df = df.iloc[:end_train_ix,:]
        test_df = df.iloc[start_test_ix:end_test_ix,:]
        if test_df.empty:
            break
        X = train_df.loc[:,feature_cols]
        y = train_df.loc[:,target_col]
        X_sim = test_df.loc[:,feature_cols]
        
        if model == 'polynomial':
            poly = PolynomialFeatures(degree=poly_degree)
            X = poly.fit_transform(X)
            X_ = poly.fit_transform(X_sim)
            clf = linear_model.RidgeCV(alphas=(.00001,.0001,.001,.01,.1,1,10), normalize=True)
            #clf = linear_model.Ridge(alpha=.01, normalize=True)
            #clf = linear_model.LinearRegression()
            clf.fit(X, y)
            y_sim = clf.predict(X_)
        elif model == 'sgd':
            model = linear_model.SGDRegressor(penalty='l2', alpha=0.15, max_iter=2000)
            scaler = StandardScaler()
            scaler.fit(X)
            X = scaler.transform(X)
            X_ = scaler.transform(X_sim)
            model.fit(X, y)
            y_sim = model.predict(X_)
        elif model == 'gb':
            model = ensemble.GradientBoostingRegressor(n_estimators=500, learning_rate=.01, max_depth=6, 
                                                         max_features=.1, min_samples_leaf=1)
            model.fit(X, y)
            X_sim = test_df.loc[:,feature_cols]
            y_sim = model.predict(X_sim)
        else:
            model.fit(X, y)
            X_sim = test_df.loc[:,feature_cols]
            y_sim = model.predict(X_sim)
        y_act = test_df.loc[:,target_col]
        # 
        test_df.loc[:,'return'] = test_df[target_col] - .1
        test_df.loc[:,'predicted'] = y_sim
        results_df_list.append(test_df)
        
    results_df = pd.concat(results_df_list)
    
    # Identify best cut off
    optimal_buy_threshold = None
    best_return = 0
    num_trades = 0
    for thresh in list(np.arange(-4.0, 4.0, 0.1)):
        return_df = results_df.loc[results_df['predicted'] > thresh]
        if (return_df['return'].sum() > best_return) or (optimal_buy_threshold == None):
            optimal_buy_threshold = thresh
            best_return, num_trades = return_df['return'].sum(), len(return_df.index)
            
    # print/return
    results_df.loc[results_df['predicted'] >= optimal_buy_threshold, 'buy'] = 1 # reset buy threshold with optimum
    print(f"""`{coin}` Best Return at {optimal_buy_threshold}: {best_return}%
    Target: {target_col}; training_mins: {training_mins}; test_mins: {test_mins}
    Number of intervals simulating {intervals}
    Trades: {num_trades}""")
    daily_trades = results_df.loc[results_df['buy'] == 1].groupby('trade_date').agg({'return':'sum','trade_minute':'count'}).reset_index()
    daily_trades.rename(columns={'trade_minute':'num_daily_trades','return':'daily_return'}, inplace=True)
    daily_trades['target_coin'] = coin
    daily_trades['model'] = str(model)
    daily_trades['target_col'] = target_col
    daily_trades['start_days'] = start_days
    daily_trades['training_mins'] = training_mins
    daily_trades['test_mins'] = test_mins
    daily_trades['optimal_buy_threshold'] = optimal_buy_threshold
    daily_trades['best_return'] = best_return
    print(f"Best return {best_return} at {optimal_buy_threshold} threshold")
                 
    return daily_trades


if __name__ == '__main__':
    main()
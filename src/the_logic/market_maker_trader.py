import boto3
import pandas as pd
import numpy as np
import io
import sys
import os
import json
import time
import datetime
# local libraries
sys.path.append(os.path.abspath(os.path.join(sys.path[0], '..', 'lib')))
import market_maker_scoring
# modeling
from sklearn import linear_model
from sklearn import ensemble
from sklearn.utils import resample
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import PolynomialFeatures
from sklearn.metrics import r2_score, classification_report
pd.options.mode.chained_assignment = None

# Config
predicted_return_threshold = .6
model_version = 1.3

def main():
    iter_ = 1
    while True:
        time.sleep(30)
        logic(iter_)
        iter_ += 1

def logic(iter_):
    """Control scoring against a market maker model"""
    start = time.time()
    
    mm_scoring = market_maker_scoring.MarketMakerScoring()
    # Get trained models
    model_object_dict = mm_scoring.get_model_objects()
    # Set scoring data and retrieve the most recent minutes features
    mm_scoring.set_scoring_data(in_parallel=True)
    try:
        recent_df = mm_scoring.scoring_features_df.sort_values('open_time')
    except:
        return
    X_scoring = recent_df[mm_scoring.feature_column_list]
    X_scoring = X_scoring.iloc[-2]
    X_scoring = X_scoring.values.reshape(1, -1)
    
    # get standardize object    
    scaler = mm_scoring.get_model_standardizer()
    X_scoring = scaler.transform(X_scoring)
    
    # Iterate over each trained model and save predicted results to dict
    scoring_result_dict = {}
    i = 0
    optimal_growth_rate = 0
    optimal_hold_minutes = 0
    for model_path, model in model_object_dict.items():
        trade_hold_minutes = int(''.join(filter(str.isdigit, model_path)))
        predicted_growth = model.predict(X_scoring)
        predicted_growth_rate = predicted_growth / trade_hold_minutes
        # Set optimal growth rate and associated trade holding minutes
        if i == 0:
            optimal_growth_rate = predicted_growth#predicted_growth_rate
            optimal_hold_minutes = trade_hold_minutes
        elif predicted_growth > optimal_growth_rate:
            optimal_growth_rate = predicted_growth#predicted_growth_rate
            optimal_hold_minutes = trade_hold_minutes
        scoring_result_dict[trade_hold_minutes] = [predicted_growth, predicted_growth_rate]
        i += 1

    end = time.time()
    latest_timestamp = recent_df.iloc[-2]['close_time'].item() / 1000
    scoring_timestamp = time.time()
    data_latency_seconds = scoring_timestamp - latest_timestamp

    latest_minute = recent_df.iloc[-2]['minute'].item()

    # Print important time latency information on first iteration
    if iter_ == 1:
        print(f"From data collection to prediction, {int(end - start)} seconds have elapsed")
        # Validate amount of time passage
        print(f"Last timestamp in scoring data: {latest_timestamp} compared to current timestamp: {time.time()} with {data_latency_seconds} diff")
    
    # Buy/Sell
    scoring_datetime = datetime.datetime.fromtimestamp(scoring_timestamp).strftime('%Y-%m-%d %H:%M:%S')
    if scoring_result_dict[optimal_hold_minutes][0][0] >= predicted_return_threshold:
        # TODO: Add the exchange and percent_funds_trading to config
        trade_qty = mm_scoring.get_trade_qty(target_coin=mm_scoring.target_coin.upper(), exchange='binance', percent_funds_trading=.9)
        print(scoring_datetime)
        print(f'Buying with predicted {optimal_hold_minutes} min return of: {scoring_result_dict[optimal_hold_minutes][0][0]}')
        # Trade for specified time
        # TODO: the quantity will need to be standardized for different coin evaluations
        buy_order = mm_scoring.bnb_client.order_market_buy(symbol=mm_scoring.target_coin.upper(), quantity=trade_qty, newOrderRespType='FULL')
        print(f"Buy info: {buy_order}")
        time.sleep((optimal_hold_minutes*60)-data_latency_seconds)
        sell_order = mm_scoring.bnb_client.order_market_sell(symbol=mm_scoring.target_coin.upper(), quantity=trade_qty, newOrderRespType='FULL')
        print(f"Sell info: {sell_order}")
        # Persist scoring results to DB
        mm_scoring.persist_scoring_results(scoring_result_dict, optimal_hold_minutes, predicted_return_threshold, data_latency_seconds, latest_minute, model_version, buy_order, sell_order)
    else:
        # Persist scoring results to DB
        mm_scoring.persist_scoring_results(scoring_result_dict, optimal_hold_minutes, predicted_return_threshold, data_latency_seconds, latest_minute, model_version)

if __name__ == '__main__':
    main()
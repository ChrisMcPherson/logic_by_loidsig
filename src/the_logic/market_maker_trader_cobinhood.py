import boto3
import pandas as pd
import numpy as np
import io
import sys
import os
import json
import time
import datetime
import logging.config
from cobinhood_api import Cobinhood
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
predicted_return_threshold = .04
model_version = 2.2

requests_logger = logging.getLogger('cobinhood_api')
requests_logger.setLevel(logging.WARNING)
logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': True,
})

def main():
    iter_ = 1
    while True:
        time.sleep(30)
        logic(iter_)
        iter_ += 1

def logic(iter_):
    """Control scoring against a market maker model"""
    start = time.time()
    
    mm_scoring = market_maker_scoring.CobinhoodScoring()
    # Get trained models
    model_object_dict = mm_scoring.get_model_objects()
    # Set scoring data and retrieve the most recent minutes features
    mm_scoring.set_scoring_data(in_parallel=False)
    try:
        recent_df = mm_scoring.scoring_features_df.sort_values('open_time')
    except:
        return
    scoring_df = recent_df[mm_scoring.feature_column_list]
    scoring_row = scoring_df.iloc[-2]
    X_scoring = scoring_row.values.reshape(1, -1)
    
    # get standardize object    
    #scaler = mm_scoring.get_model_standardizer()
    #X_scoring = scaler.transform(X_scoring)
    
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
    latest_timestamp = recent_df.iloc[-2]['close_timestamp'].item() / 1000
    scoring_timestamp = time.time()
    data_latency_seconds = scoring_timestamp - latest_timestamp
    # TODO: With cobinhood features, do we really need to go back a minute? Is the qty feature the only one that would be effected? how much? But when we join to Binance it matters...
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
        cob_client = market_maker_scoring.CobinhoodScoring.cobinhood_client()
        trade_qty = mm_scoring.get_trade_qty(percent_funds_trading=.9)
        print(scoring_datetime)
        print(f'Buying with predicted {optimal_hold_minutes} min return of: {scoring_result_dict[optimal_hold_minutes][0][0]}')
        # Buy
        buy_order_start_time = time.time()
        target_coin_price_pre_buy_order = mm_scoring.get_target_coin_price()
        bid_trade_data = {
            "trading_pair_id": mm_scoring.formatted_target_coin,
            "side": "bid",
            "type": "market",
            "size": f"{trade_qty}"
        }
        buy_order = cob_client.trading.post_orders(bid_trade_data)
        if not buy_order['success']:
            raise ValueError(f'The buy order returned unsuccessfully! Payload: {buy_order}')
        buy_order_id = buy_order['result']['order']['id']
        buy_order_state = 'sent'
        while buy_order_state != 'filled':
            time.sleep(5)
            buy_order = cob_client.trading.get_orders(buy_order_id)
            if not buy_order['success']:
                raise ValueError(f'The buy order returned unsuccessfully! Payload: {buy_order}')
            buy_order_state = buy_order['result']['order']['state']
            if buy_order_state == 'cancelled':
                raise ValueError("The buy order was cancelled. Investigate if this behavior should be handled going forward.") 
        print(f"Buy info: {buy_order}")
        buy_order_fill_time = time.time()
        buy_fill_latency_seconds = buy_order_fill_time - buy_order_start_time
        # Sleep for entire trade duration less operational lag
        time.sleep((optimal_hold_minutes * 60) - data_latency_seconds - buy_fill_latency_seconds)
        # Sell
        sell_order_start_time = time.time()
        target_coin_price_pre_sell_order = mm_scoring.get_target_coin_price()
        ask_trade_data = {
            "trading_pair_id": mm_scoring.formatted_target_coin,
            "side": "ask",
            "type": "market",
            "size": f"{trade_qty}"
        }
        sell_order = cob_client.trading.post_orders(ask_trade_data)
        if not sell_order['success']:
            raise ValueError(f'The buy order returned unsuccessfully! Payload: {sell_order}')
        sell_order_id = sell_order['result']['order']['id']
        sell_order_state = 'sent'
        while sell_order_state != 'filled':
            time.sleep(5)
            sell_order = cob_client.trading.get_orders(sell_order_id)
            if not sell_order['success']:
                raise ValueError(f'The buy order returned unsuccessfully! Payload: {sell_order}')
            sell_order_state = sell_order['result']['order']['state']
            if sell_order_state == 'cancelled':
                raise ValueError("The sell order was cancelled. Investigate if this behavior should be handled going forward.") 
        print(f"Sell info: {sell_order}")
        sell_order_fill_time = time.time()
        sell_fill_latency_seconds = sell_order_fill_time - sell_order_start_time
        # Persist scoring results to DB
        mm_scoring.persist_scoring_results(scoring_result_dict, 
                                            optimal_hold_minutes, 
                                            predicted_return_threshold, 
                                            data_latency_seconds, 
                                            latest_minute, 
                                            scoring_datetime, 
                                            model_version, 
                                            scoring_row[f'{mm_scoring.target_coin}_close'],
                                            target_coin_price_pre_buy_order, 
                                            buy_order, 
                                            target_coin_price_pre_sell_order, 
                                            sell_order, 
                                            buy_fill_latency_seconds, 
                                            sell_fill_latency_seconds
                                            )
    else:
        # Persist scoring results to DB
        mm_scoring.persist_scoring_results(scoring_result_dict, 
                                            optimal_hold_minutes, 
                                            predicted_return_threshold, 
                                            data_latency_seconds, 
                                            latest_minute, 
                                            scoring_datetime, 
                                            model_version,
                                            scoring_row[f'{mm_scoring.target_coin}_close']
                                            )

if __name__ == '__main__':
    main()
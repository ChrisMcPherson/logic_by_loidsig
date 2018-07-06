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
predicted_return_threshold = .2

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
    X_scoring = X_scoring.iloc[-1]
    
    # Iterate over each trained model and save predicted results to dict
    scoring_result_dict = {}
    for model_path, model in model_object_dict.items():
        trade_hold_minutes = int(''.join(filter(str.isdigit, model_path)))
        predicted_growth = model.predict(X_scoring.reshape(1, -1))
        predicted_growth_rate = predicted_growth / trade_hold_minutes
        scoring_result_dict[trade_hold_minutes] = [predicted_growth, predicted_growth_rate]

    # Identify max predicted return rate out of all models (different trade holding lengths)
    optimal_hold_minutes = max(scoring_result_dict)
    end = time.time()
    latest_timestamp = recent_df.iloc[-1:]['close_time_x'].item() / 1000
    scoring_timestamp = time.time()

    # Print important time latency information on first iteration
    if iter_ == 1:
        print(f"From data collection to prediction, {int(end - start)} seconds have elapsed")
        # Validate amount of time passage
        print(f"Last timestamp in scoring data: {latest_timestamp} compared to current timestamp: {time.time()} with {scoring_timestamp - latest_timestamp} diff")
    
    # Buy/Sell
    scoring_datetime = datetime.datetime.fromtimestamp(scoring_timestamp).strftime('%Y-%m-%d %H:%M:%S')
    if scoring_result_dict[optimal_hold_minutes][0][0] > predicted_return_threshold:
        print(scoring_datetime)
        print(f'Buying with predicted {optimal_hold_minutes} min return of: {scoring_result_dict[optimal_hold_minutes][0][0]}')
        # Trade for specified time
        # TODO: the quantity will need to be standardized for different coin evaluations
        order = mm_scoring.bnb_client.order_market_buy(symbol=mm_scoring.target_coin.upper(), quantity=1)
        print(f"Buy info: {order}")
        time.sleep(optimal_hold_minutes*60)
        order = mm_scoring.bnb_client.order_market_sell(symbol=mm_scoring.target_coin.upper(), quantity=1)
        print(f"Sell info: {order}")

    # Persis scoring results to DB
    for trade_hold_minutes, payload in scoring_result_dict.items():
        if trade_hold_minutes == optimal_hold_minutes:
            highest_return = True
            if scoring_result_dict[optimal_hold_minutes][0][0] > predicted_return_threshold:
                is_trade = True
                order_json = json.loads(order)
                order_id = order_json['orderId']
                client_order_id = order_json['clientOrderId']
                trade_quantity = order_json['executedQty']
            else:
                is_trade = False
        else:
            highest_return = False
            is_trade = False
            order_id = 'NULL'
            client_order_id = ''
            trade_quantity = 0

        column_list_string = """trade_datetime
        , trade_minute
        , target_coin
        , trade_duration
        , predicted_return
        , predicted_growth_rate
        , highest_return
        , is_trade
        , trade_quantity
        , order_id
        , client_order_id
        , trade_threshold
        , feature_window_space
        , trade_duration_space
        , coin_pair_definition
        , scoring_latency_seconds"""

        values = f"""'{scoring_datetime}'
        , {recent_df.iloc[-1:]['minute'].item()}
        , '{mm_scoring.target_coin}'
        , {trade_hold_minutes}
        , {payload[0][0]}
        , {payload[1][0]}
        , {highest_return}
        , {is_trade}
        , {trade_quantity}
        , {order_id}
        , '{client_order_id}'
        , {predicted_return_threshold}
        , ARRAY{mm_scoring.feature_minutes_list}
        , ARRAY{mm_scoring.trade_window_list}
        , '{json.dumps(mm_scoring.coin_pair_dict)}'
        , {scoring_timestamp - latest_timestamp}
        """
        mm_scoring.insert_into_postgres('the_logic', 'scoring_results', column_list_string, values)

if __name__ == '__main__':
    main()
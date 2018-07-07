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
# TODO: add this config somewhere external
trade_qty = 1

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
    
    # Iterate over each trained model and save predicted results to dict
    scoring_result_dict = {}
    i = 0
    optimal_growth_rate = 0
    optimal_hold_minutes = 0
    for model_path, model in model_object_dict.items():
        trade_hold_minutes = int(''.join(filter(str.isdigit, model_path)))
        predicted_growth = model.predict(X_scoring.reshape(1, -1))
        predicted_growth_rate = predicted_growth / trade_hold_minutes
        # Set optimal growth rate and associated trade holding minutes
        if i == 0:
            optimal_growth_rate = predicted_growth_rate
            optimal_hold_minutes = trade_hold_minutes
        elif predicted_growth_rate > optimal_growth_rate:
            optimal_growth_rate = predicted_growth_rate
            optimal_hold_minutes = trade_hold_minutes
        scoring_result_dict[trade_hold_minutes] = [predicted_growth, predicted_growth_rate]
        i += 1

    end = time.time()
    latest_timestamp = recent_df.iloc[-2]['close_time'].item() / 1000
    scoring_timestamp = time.time()
    data_latency_seconds = scoring_timestamp - latest_timestamp

    # Print important time latency information on first iteration
    if iter_ == 1:
        print(f"From data collection to prediction, {int(end - start)} seconds have elapsed")
        # Validate amount of time passage
        print(f"Last timestamp in scoring data: {latest_timestamp} compared to current timestamp: {time.time()} with {data_latency_seconds} diff")
    
    # Buy/Sell
    scoring_datetime = datetime.datetime.fromtimestamp(scoring_timestamp).strftime('%Y-%m-%d %H:%M:%S')
    if scoring_result_dict[optimal_hold_minutes][0][0] > predicted_return_threshold:
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
    for trade_hold_minutes, payload in scoring_result_dict.items():
        # default values
        highest_return = False
        is_trade = False
        buy_order_id = 'Null'
        buy_client_order_id = 'Null'
        buy_quantity = 'Null'
        buy_commission = 'Null'
        buy_price = 'Null'
        buy_commission_coin = 'Null'
        sell_order_id = 'Null'
        sell_client_order_id = 'Null'
        sell_quantity = 'Null'
        sell_commission = 'Null'
        sell_price = 'Null'
        buy_commission_coin = 'Null'
        if trade_hold_minutes == optimal_hold_minutes:
            highest_return = True
            if scoring_result_dict[optimal_hold_minutes][0][0] > predicted_return_threshold:
                is_trade = True
                buy_order_id = buy_order['orderId']
                buy_client_order_id = buy_order['clientOrderId']
                buy_quantity = buy_order['executedQty']
                buy_fills_df = pd.DataFrame(buy_order['fills'])
                buy_fills_df['commission'] = pd.to_numeric(buy_fills_df['commission'], errors='coerce')
                buy_fills_df['price'] = pd.to_numeric(buy_fills_df['price'], errors='coerce')
                buy_fills_df['qty'] = pd.to_numeric(buy_fills_df['qty'], errors='coerce')
                buy_commission = buy_fills_df['commission'].sum()
                total_buy_qty = buy_fills_df['qty'].sum()
                buy_fills_df['qty_perc'] = buy_fills_df['qty'] / total_buy_qty
                buy_fills_df['price_x_qty_perc'] = buy_fills_df['price'] * buy_fills_df['qty_perc']
                buy_price = buy_fills_df['price_x_qty_perc'].sum()
                buy_commission_coin = buy_fills_df.iloc[0]['commissionAsset'].item()

                sell_order_id = sell_order['orderId']
                sell_client_order_id = sell_order['clientOrderId']
                sell_quantity = sell_order['executedQty']
                sell_fills_df = pd.DataFrame(sell_order['fills'])
                sell_fills_df['commission'] = pd.to_numeric(sell_fills_df['commission'], errors='coerce')
                sell_fills_df['price'] = pd.to_numeric(sell_fills_df['price'], errors='coerce')
                sell_fills_df['qty'] = pd.to_numeric(sell_fills_df['qty'], errors='coerce')
                sell_commission = sell_fills_df['commission'].sum()
                total_sell_qty = sell_fills_df['qty'].sum()
                sell_fills_df['qty_perc'] = sell_fills_df['qty'] / total_sell_qty
                sell_fills_df['price_x_qty_perc'] = sell_fills_df['price'] * sell_fills_df['qty_perc']
                sell_price = sell_fills_df['price_x_qty_perc'].sum()
                sell_commission_coin = sell_fills_df.iloc[0]['commissionAsset'].item()
            else:
                is_trade = False

        column_list_string = """trade_datetime
        , trade_minute
        , target_coin
        , trade_duration
        , predicted_return
        , predicted_growth_rate
        , highest_return
        , is_trade
        , trade_threshold
        , feature_window_space
        , trade_duration_space
        , coin_pair_definition
        , scoring_latency_seconds
        , buy_quantity
        , sell_quantity
        , buy_price
        , sell_price
        , buy_commission
        , sell_commission
        , buy_commission_coin
        , sell_commission_coin
        , buy_order_id
        , buy_client_order_id
        , sell_order_id int
        , sell_client_order_id
        """

        values = f"""'{scoring_datetime}'
        , {recent_df.iloc[-2]['minute'].item()}
        , '{mm_scoring.target_coin}'
        , {trade_hold_minutes}
        , {payload[0][0]}
        , {payload[1][0]}
        , {highest_return}
        , {is_trade}
        , {predicted_return_threshold}
        , ARRAY{mm_scoring.feature_minutes_list}
        , ARRAY{mm_scoring.trade_window_list}
        , '{json.dumps(mm_scoring.coin_pair_dict)}'
        , {data_latency_seconds}
        , {buy_quantity}
        , {sell_quantity}
        , {buy_price}
        , {sell_price}
        , {buy_commission}
        , {sell_commission}
        , '{buy_commission_coin}'
        , '{sell_commission_coin}'
        , {buy_order_id}
        , '{buy_client_order_id}'
        , {sell_order_id}
        , '{sell_client_order_id}'
        """
        mm_scoring.insert_into_postgres('the_logic', 'scoring_results', column_list_string, values)

if __name__ == '__main__':
    main()
import boto3
import pandas as pd
import numpy as np
import io
import sys
import os
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
    X_scoring = mm_scoring.scoring_features_df.sort_values('open_time')
    X_scoring = X_scoring[mm_scoring.feature_column_list]
    X_scoring = X_scoring.iloc[-1]

    
    scoring_result_dict = {}
    for model_path, model in model_object_dict.items():
        trade_hold_minutes = int(''.join(filter(str.isdigit, model_path)))
        predicted_growth = model.predict(X_scoring.reshape(1, -1))
        predicted_growth_rate = predicted_growth / trade_hold_minutes
        scoring_result_dict[trade_hold_minutes] = [predicted_growth, predicted_growth_rate]
    # Identify max predicted return rate for all trade holding lengths modeled
    optimal_hold_minutes = max(scoring_result_dict)
    end = time.time()
    if iter_ == 1:
        print(f"From data collection to prediction, {int(end - start)} seconds have elapsed")
    # Buy/Sell
    # Does the growth rate need to be higher/lower for longer/shorter buy times?? I don't think so.
    if scoring_result_dict[optimal_hold_minutes][0] > predicted_return_threshold:
        print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        print(f'Buying with predicted {optimal_hold_minutes} min return of: {predicted_growth}')
        # Trade for specified time
        # TODO: the quantity will need to be standardized for different coin evaluations
        order = mm_scoring.bnb_client.order_market_buy(symbol=mm_scoring.target_coin.upper(), quantity=1)
        print(f"Buy info: {order}")
        time.sleep(optimal_hold_minutes*60)
        order = mm_scoring.bnb_client.order_market_sell(symbol=mm_scoring.target_coin.upper(), quantity=1)
        print(f"Sell info: {order}")

if __name__ == '__main__':
    main()
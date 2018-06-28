import boto3
import pandas as pd
import numpy as np
import io
import sys
import os
import time
# local libraries
sys.path.append(os.path.abspath(os.path.join(sys.path[0], '..', 'lib')))
import market_maker_functions
# modeling
from sklearn import linear_model
from sklearn import ensemble
from sklearn.utils import resample
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import PolynomialFeatures
from sklearn.metrics import r2_score, classification_report
pd.options.mode.chained_assignment = None

# Config
coin_pair_dict = {'ethusdt':'target',
                  'btcusdt':'alt',
                  'trxeth':'through'}
                  
#feature_minutes_list = [1,3,5,10,20,30,40,50,60,120,240,480,960]
#trade_window_list = [1,5,10,20]
feature_minutes_list = [1,5,10]
trade_window_list = [10]

mm = market_maker_functions.MarketMaker(coin_pair_dict, feature_minutes_list, trade_window_list)

def main():
    iter_ = 1
    while True:
        time.sleep(30)
        logic(iter_)
        iter_ += 1

def logic(iter_):
    start = time.time()
    # Get historic features and train model
    try:
        mm.set_training_data()
    except Exception as e:
        print(f"Failed setting training data: {e}")
        return
    X = mm.training_df[mm.mm.feature_column_list]
    y = mm.training_df[mm.mm.target_column_list]
    print(f"Training data dtypes: {mm.training_df.dtypes}")
    mm.training_df.to_csv('test_train_features.csv')
    #clf = ensemble.GradientBoostingRegressor(n_estimators=500, learning_rate=.01, max_depth=6, 
    #                                                     max_features=.1, min_samples_leaf=1)
    #clf.fit(X, y)
    
    # Set scoring data and retrieve the most recent minutes features
    mm.set_scoring_data()
    X_scoring = mm.scoring_features_df.sort_values('open_time').iloc[-1, mm.feature_column_list]
    #X_scoring = mm.scoring_features[mm.feature_column_list]
    print(f"Scoring data dtypes: {X_scoring.dtypes}")
    X_scoring.to_csv('test_recent_features.csv') ###
    
    # standardize and model
    #scaler = StandardScaler()
    #scaler.fit(X)
    #X = scaler.transform(X)
    #X_ = scaler.transform(X_scoring.reshape(1, -1))
    #model = linear_model.SGDRegressor(penalty='l2', alpha=0.15, max_iter=2000)
    #model.fit(X, y)
    #predicted_growth = model.predict(X_)

    #poly = PolynomialFeatures(degree=4)
    #X = poly.fit_transform(X)
    #X_ = poly.fit_transform(X_scoring.reshape(1, -1))

    #predicted_growth = clf.predict(X_scoring.reshape(1, -1))
    
    end = time.time()
    if iter_ == 1:
        print(f"From data collection to prediction, {int(end - start)} seconds have elapsed")
    
    # Buy/Sell
    # if predicted_growth > .4:
    #     print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    #     print(f'Buying with predicted 10 return of: {predicted_growth}')
    #     # trade for 10 min
    #     order = mm.bnb_client.order_market_buy(symbol=target_coin.upper(), quantity=1)
    #     print(f"Buy info: {order}")
    #     time.sleep(600)
    #     order = mm.bnb_client.order_market_sell(symbol=target_coin.upper(), quantity=1)
    #     print(f"Sell info: {order}")

if __name__ == '__main__':
    main()
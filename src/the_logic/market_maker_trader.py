import boto3
import pandas as pd
import numpy as np
import io
import sys
import os
import time
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

def main():
    iter_ = 1
    while True:
        time.sleep(30)
        logic(iter_)
        iter_ += 1

def logic(iter_):
    """Control scoring against a market maker model"""
    start = time.time()
    # Get trained model
    mm_scoring = market_maker_scoring.MarketMakerScoring()
    model = mm_scoring.get_model()
    
    # Set scoring data and retrieve the most recent minutes features
    mm_scoring.set_scoring_data()
    print(f"Scoring data dtypes: {mm_scoring.scoring_features_df.dtypes}")
    X_scoring = mm_scoring.scoring_features_df.sort_values('open_time')
    print(mm_scoring.feature_column_list)
    X_scoring = X_scoring.iloc[-1, mm_scoring.feature_column_list]
    #X_scoring = mm_scoring.scoring_features[mm_scoring.feature_column_list]
    print(f"Scoring data dtypes: {X_scoring.dtypes}")
    X_scoring.head().to_csv('test_recent_features.csv') ###
    
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
    #     order = mm_scoring.bnb_client.order_market_buy(symbol=target_coin.upper(), quantity=1)
    #     print(f"Buy info: {order}")
    #     time.sleep(600)
    #     order = mm_scoring.bnb_client.order_market_sell(symbol=target_coin.upper(), quantity=1)
    #     print(f"Sell info: {order}")

if __name__ == '__main__':
    main()
import boto3
import pandas as pd
import numpy as np
import io
import sys
import os
import re
import time
# local libraries
sys.path.append(os.path.abspath(os.path.join(sys.path[0], '..', 'lib')))
import market_maker_training
# modeling
from sklearn.preprocessing import StandardScaler
from sklearn import linear_model
from sklearn import ensemble
import xgboost as xgb
from sklearn.utils import resample
from sklearn.metrics import r2_score, classification_report
pd.options.mode.chained_assignment = None

# Config
coin_pair_dict = {'target':'ethusdt',
                  'alt':'btcusdt',
                  'through':'trxeth'}

                  
feature_minutes_list = [1, 3, 5, 8, 11, 14, 18, 22, 30, 40, 50, 60, 120, 240, 480, 960]
trade_window_list = [18,22]

def main():
    """Control the training and persistance of a market maker model"""
    print("------- Starting training of market maker model --------")
    print(f"Coin pair config: {coin_pair_dict}")
    print(f"Feature minutes list: {feature_minutes_list}")
    print(f"Trade window list: {trade_window_list}")
    # Get historic features and train model
    mm_training = market_maker_training.BinanceTraining(coin_pair_dict, feature_minutes_list, trade_window_list)
    try:
        mm_training.set_training_data()
    except Exception as e:
        print(f"Failed setting training data: {e}")
        return
    # Train a model for each trade window configured
    for target_column in mm_training.target_column_list:
        X = mm_training.training_df.loc[:,mm_training.feature_column_list]
        y = mm_training.training_df.loc[:,target_column]
        model = linear_model.LinearRegression()
        # Standardize features (specific for sgd and other sensitive models)
        scaler = StandardScaler()
        scaler.fit(X)
        X = scaler.transform(X)
        # Fit model
        model.fit(X, y)
        # Persist model and standardizer
        print(f"{target_column} r2: {r2_score(y, model.predict(X))}")
        #mm_training.persist_model(model, int(''.join(filter(str.isdigit, target_column))))
        trade_duration = [int(s) for s in re.findall(r'-?\d+\.?\d*', target_column)][0]
        mm_training.persist_model(model, trade_duration)
        mm_training.persist_standardizer(scaler)
    # Persist configuration
    mm_training.persist_model_config()

if __name__ == '__main__':
    main()
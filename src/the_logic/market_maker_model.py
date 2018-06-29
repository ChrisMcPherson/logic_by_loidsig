import boto3
import pandas as pd
import numpy as np
import io
import sys
import os
import time
# local libraries
sys.path.append(os.path.abspath(os.path.join(sys.path[0], '..', 'lib')))
import market_maker_training
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

def main():
    """Control the training and persistance of a market maker model"""
    # Get historic features and train model
    mm_training = market_maker_training.MarketMakerTraining(coin_pair_dict, feature_minutes_list, trade_window_list)
    try:
        mm_training.set_training_data()
    except Exception as e:
        print(f"Failed setting training data: {e}")
        return
    X = mm_training.training_df[mm_training.feature_column_list]
    y = mm_training.training_df[mm_training.target_column_list]
    #mm_training.training_df.head().to_csv('test_train_features.csv')
    model = ensemble.GradientBoostingRegressor(n_estimators=500, learning_rate=.01, max_depth=6, 
                                              max_features=.1, min_samples_leaf=1)
    model.fit(X, y)
    # Persist model and configuration
    mm_training.persist_model(model)
    mm_training.persist_model_config()
    

if __name__ == '__main__':
    main()
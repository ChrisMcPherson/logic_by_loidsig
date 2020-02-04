import boto3
import pandas as pd
import numpy as np
import io
import sys
import os
# local libraries
sys.path.append(os.path.abspath(os.path.join(sys.path[0], '..', 'lib')))
import market_maker_training

from sklearn import ensemble
from sklearn import linear_model
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.svm import SVR
from sklearn.utils import resample
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import PolynomialFeatures
from sklearn.metrics import r2_score, classification_report
from sklearn.model_selection import train_test_split

from tpot import TPOTRegressor

pd.options.mode.chained_assignment = None

# Extract queried data from Athena
def features(feature_minutes_list, trade_window_list=[8]):
    #TODO: move this config to simulation argument 
    coin_pair_dict = {'target':'btcusdt',
                  'alt':'ethusdt',
                  'through':'btceth'}

    print(f"Coin feature configuration: {coin_pair_dict}")

    mm_training = market_maker_training.BinanceTraining(coin_pair_dict, feature_minutes_list, trade_window_list)
    try:
        mm_training.set_training_data()
    except Exception as e:
        print(f"Failed setting training data: {e}")
        return
    return mm_training.training_df, mm_training.feature_column_list, mm_training.target_column_list

feature_minutes_list = [1, 3, 5, 8, 11, 14, 18, 22, 30,60,120,1440]
features_df, feature_cols, target_col_list = features(feature_minutes_list)

features_df = features_df[:-14]

# Split for last 4.5 hours training and adjust for look ahead 
#X_train, y_train = features_df[-300:-20][feature_cols], features_df[-300:-20][target_col] 
#X_test, y_test = features_df[-10:][feature_cols], features_df[-10:][target_col]

# Split for last x days training and adjust for look ahead 
days_training = 400 * -1440
hours_test = 120 * -60
X_train, y_train = features_df[days_training:(hours_test-14)][feature_cols], features_df[days_training:(hours_test-14)][target_col_list[0]] 
X_test, y_test = features_df[hours_test:][feature_cols], features_df[hours_test:][target_col_list[0]]

tpot = TPOTRegressor(generations=5, population_size=10, verbosity=2, n_jobs=-1)
tpot.fit(X_train, y_train)
print(tpot.score(X_test, y_test))
tpot.export(f'tpot_{days_training/-1440}days_train_{hours_test/-60}hour_test_pipeline.py')

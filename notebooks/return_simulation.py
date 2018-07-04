import boto3
import pandas as pd
pd.options.mode.chained_assignment = None
import numpy as np
import io
import sys
import os
import ast
import time
import datetime
import json
from joblib import Parallel, delayed
import multiprocessing
# local libraries
sys.path.append(os.path.abspath(os.path.join(sys.path[0], '..', 'src', 'lib')))
import athena_connect
import market_maker_training
# modeling
from sklearn import linear_model
from sklearn import ensemble
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.svm import SVR
from sklearn.utils import resample
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import PolynomialFeatures
from sklearn.metrics import r2_score, classification_report
import xgboost as xgb

def main():
    print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    
    # Default configuration
    start = 175
    target_coin_list = ['ethusdt']
    feature_minutes_list = [1,5,10]
    target_col_list = [2,5,10,20]
    training_min_list = ['none']
    test_min_list = [1440]
    model_list = ['xgb']
    poly_list = [3]
    
    # Get argument configurations
    target_coin_arg = [s for s in sys.argv[1:] if 'coin' in s.split('=',1)[0]]
    if target_coin_arg:
        target_coin_list = ast.literal_eval(target_coin_arg[0].split('=',1)[1]) # use eval instead of literal_eval to pick up models
        
    start_arg = [s for s in sys.argv[1:] if 'start' in s.split('=',1)[0]]
    if start_arg:
        start = int(start_arg[0].split('=',1)[1])
        
    feature_arg = [s for s in sys.argv[1:] if 'feature_minutes_list' in s.split('=',1)[0]]
    if feature_arg:
        feature_minutes_list = ast.literal_eval(feature_arg[0].split('=',1)[1])
        
    target_arg = [s for s in sys.argv[1:] if 'target' in s.split('=',1)[0]]
    if target_arg:
        target_col_list = ast.literal_eval(target_arg[0].split('=',1)[1])
    
    training_min_arg = [s for s in sys.argv[1:] if 'train_min' in s.split('=',1)[0]]
    if training_min_arg:
        training_min_list = ast.literal_eval(training_min_arg[0].split('=',1)[1])
        
    test_min_arg = [s for s in sys.argv[1:] if 'test_min' in s.split('=',1)[0]]
    if test_min_arg:
        test_min_list = ast.literal_eval(test_min_arg[0].split('=',1)[1])
    
    model_arg = [s for s in sys.argv[1:] if 'model' in s.split('=',1)[0]]
    if model_arg:
        model_list = ast.literal_eval(model_arg[0].split('=',1)[1])
        
    poly_arg = [s for s in sys.argv[1:] if 'poly' in s.split('=',1)[0]]
    if poly_arg:
        poly_list = ast.literal_eval(poly_arg[0].split('=',1)[1])
    
    print(f"Model(s): {model_list}")
    print(f"Sim Start Day: {start}")
    print(f"Training feature minutes: {feature_minutes_list}")
    print(f"Training Min(s): {training_min_list}")
    print(f"Test Min(s): {test_min_list}")
    print(f"Poly(s): {poly_list}")
    
    # Write out configuration
    result_stats_json = json.dumps({'Coin(s)': ','.join(map(str, target_coin_list))
                                , 'Model(s)': ','.join(map(str, model_list))
                                , 'Target(s)': ','.join(map(str, target_col_list))
                                , 'Sim Start': start
                                , 'Train Min': ','.join(map(str, training_min_list))
                                , 'Test Min': ','.join(map(str, test_min_list))})
    with open('dominostats.json', 'w') as f:
        f.write(result_stats_json)
        
    # Simulate!
    for target_coin in target_coin_list:
        # Get features for target coin
        features_df, feature_col, target_col_list = features(feature_minutes_list, target_col_list)
        # Iterate over all simulation configurations
        sim_daily_trades_list = []
        for model in model_list:
            print(f"Model: {model}")
            for target_col in target_col_list:
                for test_min in test_min_list:
                    for polynomial in poly_list:
                        for training_min in training_min_list:
                            print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
                            start_ix = 1440 * start
                            results_df_list = []
                            days_to_test = features_df.loc[start_ix:,f'{target_coin}_trade_date'].nunique()
                            test_intervals = days_to_test * 1440 / test_min
                            trade_duration = int(''.join(filter(str.isdigit, target_col)))
                            print(f"Number of days to iterate: {days_to_test}")
                            # results_df_list = Parallel(n_jobs=multiprocessing.cpu_count())(delayed(simulate_return)(model, features_df, feature_col, 
                            #                                                                 target_col, target_coin, interval, start_ix, trade_duration, start_days=start, 
                            #                                                                 training_mins=training_min, test_mins=test_min, 
                            #                                                                 poly_degree=polynomial) for interval in range(int(test_intervals)))
                            results_df_list = [simulate_return(model, features_df, feature_col, 
                                            target_col, target_coin, interval, start_ix, trade_duration, start_days=start, 
                                            training_mins=training_min, test_mins=test_min, 
                                            poly_degree=polynomial) for interval in range(int(test_intervals))]
                            results_df = pd.concat(results_df_list)
                            daily_trades_df = identify_best_return(model, results_df, feature_col, target_col, target_coin, test_intervals, start_days=start, 
                                                                    training_mins=training_min, test_mins=test_min, poly_degree=polynomial)
                            sim_daily_trades_list.append(daily_trades_df)
    sim_daily_trades = pd.concat(sim_daily_trades_list)
    finish_time = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H-%M-%S')
    sim_daily_trades.to_csv(f"notebooks/sim_results/sim_daily_trades_{finish_time}.csv", index = False)
                    
def features(feature_minutes_list, trade_window_list):
    #TODO: move this config to simulation argument 
    coin_pair_dict = {'ethusdt':'target',
                  'btcusdt':'alt',
                  'trxeth':'through'}
    print(f"Coin feature configuration: {coin_pair_dict}")

    mm_training = market_maker_training.MarketMakerTraining(coin_pair_dict, feature_minutes_list, trade_window_list)
    try:
        mm_training.set_training_data()
    except Exception as e:
        print(f"Failed setting training data: {e}")
        return
    return mm_training.training_df, mm_training.feature_column_list, mm_training.target_column_list
    
def simulate_return(model, df, feature_cols, target_col, coin, interval, start_ix, trade_duration, start_days=1, training_mins=None, test_mins=1440, poly_degree=3):
    end_train_ix = (interval * test_mins) + start_ix - trade_duration # subtract future day from outcome variable 
    end_test_ix = ((interval * test_mins) + test_mins) + start_ix
    start_test_ix = end_train_ix + trade_duration
    if training_mins != 'none':
        start_train_ix = end_train_ix - training_mins
        start_train_ix = 0 if start_train_ix < 0 else start_train_ix
        train_df = df.iloc[start_train_ix:end_train_ix,:]
    else:
        train_df = df.iloc[:end_train_ix,:]
    if end_train_ix > start_test_ix - trade_duration:
        print("Error! You are training on data to be tested!")
        return
    test_df = df.iloc[start_test_ix:end_test_ix,:]
    if test_df.empty:
        print("Skipping day that does not have adequate test data (likely end of iteration)")
        return
    X = train_df.loc[:,feature_cols]
    y = train_df.loc[:,target_col]
    X_sim = test_df.loc[:,feature_cols]
    
    if model == 'polynomial':
        poly = PolynomialFeatures(degree=poly_degree)
        X = poly.fit_transform(X)
        X_ = poly.fit_transform(X_sim)
        clf = linear_model.RidgeCV(alphas=(.00001,.0001,.001,.01,.1,1,10), normalize=True)
        #clf = linear_model.Ridge(alpha=.01, normalize=True)
        #clf = linear_model.LinearRegression()
        clf.fit(X, y)
        y_sim = clf.predict(X_)
    elif model == 'sgd':
        model = linear_model.SGDRegressor(penalty='l2', alpha=0.15, max_iter=2000)
        scaler = StandardScaler()
        scaler.fit(X)
        X = scaler.transform(X)
        X_ = scaler.transform(X_sim)
        model.fit(X, y)
        y_sim = model.predict(X_)
    elif model == 'gb':
        model = ensemble.GradientBoostingRegressor(n_estimators=500, learning_rate=.01, max_depth=6, 
                                                        max_features=.1, min_samples_leaf=1)
        model.fit(X, y)
        X_sim = test_df.loc[:,feature_cols]
        y_sim = model.predict(X_sim)
    elif model == 'linear':
        model = linear_model.LinearRegression()
        model.fit(X, y)
        X_sim = test_df.loc[:,feature_cols]
        y_sim = model.predict(X_sim)
    elif model == 'xgb':
        model = xgb.XGBRegressor()
        model.fit(X, y)
        X_sim = test_df.loc[:,feature_cols]
        y_sim = model.predict(X_sim)
    else:
        model.fit(X, y)
        X_sim = test_df.loc[:,feature_cols]
        y_sim = model.predict(X_sim)
    y_act = test_df.loc[:,target_col]
    # 
    test_df.loc[:,'return'] = test_df[target_col] - .1
    test_df.loc[:,'predicted'] = y_sim
    return test_df
        
def identify_best_return(model, results_df, feature_cols, target_col, coin, intervals, start_days=1, training_mins=None, test_mins=1440, poly_degree=3):
    pd.options.mode.chained_assignment = None
    # Identify best cut off
    optimal_buy_threshold = None
    best_return = 0
    num_trades = 0
    for thresh in list(np.arange(0, 1.0, 0.1)):
        return_df = results_df.loc[results_df['predicted'] > thresh]
        print(f"Return at {thresh}: {return_df['return'].sum()}% with {len(return_df.index)}")
        # Save all trades for specified target threshold
        if thresh == .2:
            finish_time = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H-%M-%S')
            return_df[[f'{coin}_trade_datetime','predicted', 'return']].to_csv(f"notebooks/sim_results/sim_2_thresh_trades_{finish_time}.csv", index = False)
        # Retain optimal threshold
        if (return_df['return'].sum() > best_return) or (optimal_buy_threshold == None):
            optimal_buy_threshold = thresh
            best_return, num_trades = return_df['return'].sum(), len(return_df.index)
            
    # print/return
    results_df.loc[results_df['predicted'] >= optimal_buy_threshold, 'buy'] = 1 # reset buy threshold with optimum
    print(f"""`{coin}` Best Return at {optimal_buy_threshold}: {best_return}%
    Target: {target_col}; training_mins: {training_mins}; test_mins: {test_mins}
    Number of intervals simulating {intervals}
    Trades: {num_trades}""")
    daily_trades = results_df.loc[results_df['buy'] == 1]
    daily_trades = daily_trades.groupby(f'{coin}_trade_date').agg({'return':'sum',f'{coin}_trade_minute':'count'}).reset_index()
    daily_trades.rename(columns={f'{coin}_trade_minute':'num_daily_trades','return':'daily_return'}, inplace=True)
    daily_trades['target_coin'] = coin
    daily_trades['model'] = str(model)
    daily_trades['target_col'] = target_col
    daily_trades['start_days'] = start_days
    daily_trades['training_mins'] = training_mins
    daily_trades['test_mins'] = test_mins
    daily_trades['optimal_buy_threshold'] = optimal_buy_threshold
    daily_trades['best_return'] = best_return
    print(f"Best return {best_return} at {optimal_buy_threshold} threshold")
    return daily_trades


if __name__ == '__main__':
    main()
import boto3
import pandas as pd
pd.options.mode.chained_assignment = None
import numpy as np
import sys
import os
import io
import ast
import json
import pickle
from functools import reduce
import time
import datetime
from datetime import timedelta
import multiprocessing
import dill
from joblib import Parallel, delayed
from binance.client import Client
# local libraries
sys.path.append(os.path.abspath(os.path.join(sys.path[0], '..', 'lib')))
import athena_connect

class MarketMakerScoring():
    def __init__(self):
        """Market maker class to fascilitate data engineering for scoring models
        """
        self.s3_bucket = 'loidsig-crypto'
        try:
            self.boto_session = boto3.Session(profile_name='loidsig')
            self.s3_client = self.boto_session.client('s3')
            self.s3_resource = self.boto_session.resource('s3')
        except:
            self.s3_client = boto3.client('s3')
            self.s3_resource = boto3.resource('s3')
        model_config_json = self.get_model_config()
        self.coin_pair_dict = model_config_json['coin_pair_dict']
        self.target_coin = self.get_target_coin()
        self.feature_minutes_list = model_config_json['feature_minutes_list']
        self.trade_window_list = model_config_json['trade_window_list']
        self.feature_column_list = model_config_json['feature_column_list']
        self.bnb_client = MarketMakerScoring.binance_client()


    def pandas_get_trades(self, coin_pair, start_time_str='360 min ago UTC'):
        """Return individual recent trades from Binance api for a given coin pair and historical interval

        Args:
            coin_pair (str): i.e. 'ethusdt' is buying eth with usdt or selling eth into usdt
        """
        trades = self.bnb_client.aggregate_trade_iter(symbol=coin_pair.upper(), start_str=start_time_str)
        trade_dict_list = list(trades)
        df = pd.DataFrame(trade_dict_list)
        df.rename(columns={'a':'trade_count','p':'avg_price','q':'quantity','f':'first_trade_id','l':'last_trade_id',
                        'T':'micro_timestamp','m':'buyer_maker','M':'best_price_match'}, inplace=True)
        df['avg_price'] = pd.to_numeric(df['avg_price'])
        df['quantity'] = pd.to_numeric(df['quantity'])
        df['minute'] = pd.to_numeric(df['micro_timestamp']/1000/60).astype(int)
        return df

    @staticmethod
    def pandas_get_candlesticks(coin_pair, pair_type, start_time_str='1 min ago UTC'):
        """Return minute granularity recent candlestick metrics from Binance api for a given coin pair and historical interval

        Returns:
            (list): ['coin_pair',['pair_type', pandas.DataFrame]]
        """
        bnb_client = MarketMakerScoring.binance_client()
        prefix = f"{coin_pair.lower()}"
        coin_pair_upper = coin_pair.upper()
        klines = bnb_client.get_historical_klines(coin_pair_upper, Client.KLINE_INTERVAL_1MINUTE, start_time_str)
        df = pd.DataFrame(klines, columns=['open_time',f'{prefix}_open',f'{prefix}_high',f'{prefix}_low',f'{prefix}_close',f'{prefix}_volume',
                        'close_time',f'{prefix}_quote_asset_volume',f'{prefix}_trade_count', f'{prefix}_tbbav',f'{prefix}_tbqav',f'{prefix}_ignore']
                    )
        df[f'{prefix}_open'] = pd.to_numeric(df[f'{prefix}_open'])
        df[f'{prefix}_high'] = pd.to_numeric(df[f'{prefix}_high'])
        df[f'{prefix}_low'] = pd.to_numeric(df[f'{prefix}_low'])
        df[f'{prefix}_close'] = pd.to_numeric(df[f'{prefix}_close'])
        df[f'{prefix}_volume'] = pd.to_numeric(df[f'{prefix}_volume'])
        df[f'{prefix}_quote_asset_volume'] = pd.to_numeric(df[f'{prefix}_quote_asset_volume'])
        df[f'{prefix}_tbbav'] = pd.to_numeric(df[f'{prefix}_tbbav'])
        df[f'{prefix}_tbqav'] = pd.to_numeric(df[f'{prefix}_tbqav'])
        df['minute'] = pd.to_numeric(df['close_time']/1000/60).astype(int)
        recent_df_list = []
        recent_df_list.append(coin_pair)
        recent_df_list.append([pair_type, df])
        return recent_df_list

    def set_scoring_data(self, in_parallel=True):
        """Set data for model scoring from latest candlestick metrics features"""
        if self.feature_minutes_list == None or self.trade_window_list == None:
            raise Exception("To construct scoring dataframe, the optional feature_minutes_list and trade_window_list attributes must be set!")
        # Get recent trade data for both target coin pair and through coin pair
        recent_trades_interval = max(self.feature_minutes_list) + 10
        cores = multiprocessing.cpu_count()
        try:
            if in_parallel:
                scoring_df_list = Parallel(n_jobs=cores)(delayed(MarketMakerScoring.pandas_get_candlesticks)(coin_pair, pair_type, f'{recent_trades_interval} min ago UTC') for coin_pair, pair_type in self.coin_pair_dict.items())
                scoring_df_dict = dict(scoring_df_list)
            else:
                scoring_df_list = [MarketMakerScoring.pandas_get_candlesticks(coin_pair, pair_type, f'{recent_trades_interval} min ago UTC') for coin_pair, pair_type in self.coin_pair_dict.items()]
                scoring_df_dict = dict(scoring_df_list)
        except Exception as e:
            print(f"Unable to get recent data for scoring: {e}")
            return

        scoring_features_df = self.engineer_scoring_features(scoring_df_dict)
        scoring_features_df.fillna(0, inplace=True)
        scoring_features_df.replace([np.inf, -np.inf], 0, inplace=True)
        self.scoring_features_df = scoring_features_df

    def engineer_scoring_features(self, scoring_df_dict):
        """Engineer scoring features for multiple coin pairs"""
        coin_df_list = []

        for coin_pair, coin_data_list in scoring_df_dict.items():
            pair_type, coin_df = coin_data_list

            if pair_type == 'target':
                coin_df['trade_hour'] = pd.to_datetime(coin_df['open_time']/1000, unit='s').dt.hour
                coin_df['trade_day_of_week'] = pd.to_datetime(coin_df['open_time']/1000, unit='s').dt.dayofweek

            # Lag features
            for interval in self.feature_minutes_list:
                coin_df[f'prev_{interval}_{coin_pair}_open'] = coin_df[f'{coin_pair}_open'].shift(interval)
                coin_df[f'prev_{interval}_{coin_pair}_open_perc_chg'] = (coin_df[f'{coin_pair}_open'] - coin_df[f'prev_{interval}_{coin_pair}_open']) / coin_df[f'prev_{interval}_{coin_pair}_open'] * 100
                coin_df[f'prev_{interval}_{coin_pair}_open_rate_chg'] = (((coin_df[f'{coin_pair}_open'] - coin_df[f'prev_{interval}_{coin_pair}_open']) / coin_df[f'prev_{interval}_{coin_pair}_open'] * 100) -
                                                    ((coin_df[f'{coin_pair}_open'] - coin_df[f'prev_{interval}_{coin_pair}_open']) / coin_df[f'prev_{interval}_{coin_pair}_open'] * 100))

                coin_df[f'prev_{interval}_{coin_pair}_high'] = coin_df[f'{coin_pair}_high'].shift(interval)
                coin_df[f'prev_{interval}_{coin_pair}_high_perc_chg'] = (coin_df[f'{coin_pair}_high'] - coin_df[f'prev_{interval}_{coin_pair}_high']) / coin_df[f'prev_{interval}_{coin_pair}_high'] * 100
                coin_df[f'prev_{interval}_{coin_pair}_high_rate_chg'] = (((coin_df[f'{coin_pair}_high'] - coin_df[f'prev_{interval}_{coin_pair}_high']) / coin_df[f'prev_{interval}_{coin_pair}_high'] * 100) -
                                                    ((coin_df[f'{coin_pair}_high'] - coin_df[f'prev_{interval}_{coin_pair}_high']) / coin_df[f'prev_{interval}_{coin_pair}_high'] * 100))

                coin_df[f'prev_{interval}_{coin_pair}_low'] = coin_df[f'{coin_pair}_low'].shift(interval)
                coin_df[f'prev_{interval}_{coin_pair}_low_perc_chg'] = (coin_df[f'{coin_pair}_low'] - coin_df[f'prev_{interval}_{coin_pair}_low']) / coin_df[f'prev_{interval}_{coin_pair}_low'] * 100
                coin_df[f'prev_{interval}_{coin_pair}_low_rate_chg'] = (((coin_df[f'{coin_pair}_low'] - coin_df[f'prev_{interval}_{coin_pair}_low']) / coin_df[f'prev_{interval}_{coin_pair}_low'] * 100) -
                                                    ((coin_df[f'{coin_pair}_low'] - coin_df[f'prev_{interval}_{coin_pair}_low']) / coin_df[f'prev_{interval}_{coin_pair}_low'] * 100))

                coin_df[f'prev_{interval}_{coin_pair}_volume'] = coin_df[f'{coin_pair}_volume'].shift(interval)
                coin_df[f'prev_{interval}_{coin_pair}_volume_perc_chg'] = (coin_df[f'{coin_pair}_volume'] - coin_df[f'prev_{interval}_{coin_pair}_volume']) / coin_df[f'prev_{interval}_{coin_pair}_volume'] * 100
                coin_df[f'prev_{interval}_{coin_pair}_volume_rate_chg'] = (((coin_df[f'{coin_pair}_volume'] - coin_df[f'prev_{interval}_{coin_pair}_volume']) / coin_df[f'prev_{interval}_{coin_pair}_volume'] * 100) -
                                                    ((coin_df[f'{coin_pair}_volume'] - coin_df[f'prev_{interval}_{coin_pair}_volume']) / coin_df[f'prev_{interval}_{coin_pair}_volume'] * 100))

                coin_df[f'prev_{interval}_{coin_pair}_qav'] = coin_df[f'{coin_pair}_quote_asset_volume'].shift(interval)
                coin_df[f'prev_{interval}_{coin_pair}_qav_perc_chg'] = (coin_df[f'{coin_pair}_quote_asset_volume'] - coin_df[f'prev_{interval}_{coin_pair}_qav']) / coin_df[f'prev_{interval}_{coin_pair}_qav'] * 100
                coin_df[f'prev_{interval}_{coin_pair}_qav_rate_chg'] = (((coin_df[f'{coin_pair}_quote_asset_volume'] - coin_df[f'prev_{interval}_{coin_pair}_qav']) / coin_df[f'prev_{interval}_{coin_pair}_qav'] * 100) -
                                                    ((coin_df[f'{coin_pair}_quote_asset_volume'] - coin_df[f'prev_{interval}_{coin_pair}_qav']) / coin_df[f'prev_{interval}_{coin_pair}_qav'] * 100))

                coin_df[f'prev_{interval}_{coin_pair}_trade_count'] = coin_df[f'{coin_pair}_trade_count'].shift(interval)
                coin_df[f'prev_{interval}_{coin_pair}_trade_count_perc_chg'] = (coin_df[f'{coin_pair}_trade_count'] - coin_df[f'prev_{interval}_{coin_pair}_trade_count']) / coin_df[f'prev_{interval}_{coin_pair}_trade_count'] * 100
                coin_df[f'prev_{interval}_{coin_pair}_trade_count_rate_chg'] = (((coin_df[f'{coin_pair}_trade_count'] - coin_df[f'prev_{interval}_{coin_pair}_trade_count']) / coin_df[f'prev_{interval}_{coin_pair}_trade_count'] * 100) -
                                                    ((coin_df[f'{coin_pair}_trade_count'] - coin_df[f'prev_{interval}_{coin_pair}_trade_count']) / coin_df[f'prev_{interval}_{coin_pair}_trade_count'] * 100))

                coin_df[f'prev_{interval}_{coin_pair}_tbbav'] = coin_df[f'{coin_pair}_tbbav'].shift(interval)
                coin_df[f'prev_{interval}_{coin_pair}_tbbav_perc_chg'] = (coin_df[f'{coin_pair}_tbbav'] - coin_df[f'prev_{interval}_{coin_pair}_tbbav']) / coin_df[f'prev_{interval}_{coin_pair}_tbbav'] * 100
                coin_df[f'prev_{interval}_{coin_pair}_tbbav_rate_chg'] = (((coin_df[f'{coin_pair}_tbbav'] - coin_df[f'prev_{interval}_{coin_pair}_tbbav']) / coin_df[f'prev_{interval}_{coin_pair}_tbbav'] * 100) -
                                                    ((coin_df[f'{coin_pair}_tbbav'] - coin_df[f'prev_{interval}_{coin_pair}_tbbav']) / coin_df[f'prev_{interval}_{coin_pair}_tbbav'] * 100))

                coin_df[f'prev_{interval}_{coin_pair}_tbqav'] = coin_df[f'{coin_pair}_tbqav'].shift(interval)
                coin_df[f'prev_{interval}_{coin_pair}_tbqav_perc_chg'] = (coin_df[f'{coin_pair}_tbqav'] - coin_df[f'prev_{interval}_{coin_pair}_tbqav']) / coin_df[f'prev_{interval}_{coin_pair}_tbqav'] * 100
                coin_df[f'prev_{interval}_{coin_pair}_tbqav_rate_chg'] = (((coin_df[f'{coin_pair}_tbqav'] - coin_df[f'prev_{interval}_{coin_pair}_tbqav']) / coin_df[f'prev_{interval}_{coin_pair}_tbqav'] * 100) -
                                                    ((coin_df[f'{coin_pair}_tbqav'] - coin_df[f'prev_{interval}_{coin_pair}_tbqav']) / coin_df[f'prev_{interval}_{coin_pair}_tbqav'] * 100))
            coin_df_list.append(coin_df)

        # Combine features
        features_df = reduce(lambda x, y: pd.merge(x, y, on='open_time', how='inner'), coin_df_list)
        # Create Interaction Features
        for coin_pair, coin_data_list in scoring_df_dict.items():
            pair_type, coin_df = coin_data_list
            if pair_type == 'alt':
                features_df['current_interaction'] = (features_df[f'{self.target_coin}_close']-features_df[f'{coin_pair}_close'])/features_df[f'{self.target_coin}_close']
                features_df['current_1_interaction'] = (features_df[f'{self.target_coin}_close'].shift(1)-features_df[f'{coin_pair}_close'].shift(1))/features_df[f'{self.target_coin}_close'].shift(1)
                features_df['current_2_interaction'] = (features_df[f'{self.target_coin}_close'].shift(2)-features_df[f'{coin_pair}_close'].shift(2))/features_df[f'{self.target_coin}_close'].shift(2)
                features_df['current_3_interaction'] = (features_df[f'{self.target_coin}_close'].shift(3)-features_df[f'{coin_pair}_close'].shift(3))/features_df[f'{self.target_coin}_close'].shift(3)
                features_df['current_4_interaction'] = (features_df[f'{self.target_coin}_close'].shift(4)-features_df[f'{coin_pair}_close'].shift(4))/features_df[f'{self.target_coin}_close'].shift(4)
                features_df['current_5_interaction'] = (features_df[f'{self.target_coin}_close'].shift(5)-features_df[f'{coin_pair}_close'].shift(5))/features_df[f'{self.target_coin}_close'].shift(5)
                features_df['interaction_average'] = (features_df['current_interaction'] + features_df['current_1_interaction'] + features_df['current_2_interaction'] + 
                                    features_df['current_3_interaction'] + features_df['current_4_interaction'] + features_df['current_5_interaction']) / 6
                features_df[f'avg_5_{coin_pair}_open_interaction'] = features_df['interaction_average'] - features_df['current_interaction']

                features_df['current_6_interaction'] = (features_df[f'{self.target_coin}_close'].shift(6)-features_df[f'{coin_pair}_close'].shift(6))/features_df[f'{self.target_coin}_close'].shift(6)
                features_df['current_7_interaction'] = (features_df[f'{self.target_coin}_close'].shift(7)-features_df[f'{coin_pair}_close'].shift(7))/features_df[f'{self.target_coin}_close'].shift(7)
                features_df['current_8_interaction'] = (features_df[f'{self.target_coin}_close'].shift(8)-features_df[f'{coin_pair}_close'].shift(8))/features_df[f'{self.target_coin}_close'].shift(8)
                features_df['current_9_interaction'] = (features_df[f'{self.target_coin}_close'].shift(9)-features_df[f'{coin_pair}_close'].shift(9))/features_df[f'{self.target_coin}_close'].shift(9)
                features_df['current_10_interaction'] = (features_df[f'{self.target_coin}_close'].shift(10)-features_df[f'{coin_pair}_close'].shift(10))/features_df[f'{self.target_coin}_close'].shift(10)
                features_df['interaction_average'] = (features_df['current_interaction'] + features_df['current_1_interaction'] + features_df['current_2_interaction'] + 
                                    features_df['current_3_interaction'] + features_df['current_4_interaction'] + features_df['current_5_interaction'] + 
                                    features_df['current_6_interaction'] + features_df['current_7_interaction'] + features_df['current_8_interaction'] + 
                                    features_df['current_9_interaction'] + features_df['current_10_interaction']) / 11
                features_df[f'avg_10_{coin_pair}_open_interaction'] = features_df['interaction_average'] - features_df['current_interaction']

                features_df['current_11_interaction'] = (features_df[f'{self.target_coin}_close'].shift(11)-features_df[f'{coin_pair}_close'].shift(11))/features_df[f'{self.target_coin}_close'].shift(11)
                features_df['current_12_interaction'] = (features_df[f'{self.target_coin}_close'].shift(12)-features_df[f'{coin_pair}_close'].shift(12))/features_df[f'{self.target_coin}_close'].shift(12)
                features_df['current_13_interaction'] = (features_df[f'{self.target_coin}_close'].shift(13)-features_df[f'{coin_pair}_close'].shift(13))/features_df[f'{self.target_coin}_close'].shift(13)
                features_df['current_14_interaction'] = (features_df[f'{self.target_coin}_close'].shift(14)-features_df[f'{coin_pair}_close'].shift(14))/features_df[f'{self.target_coin}_close'].shift(14)
                features_df['current_15_interaction'] = (features_df[f'{self.target_coin}_close'].shift(15)-features_df[f'{coin_pair}_close'].shift(15))/features_df[f'{self.target_coin}_close'].shift(15)
                features_df['current_16_interaction'] = (features_df[f'{self.target_coin}_close'].shift(16)-features_df[f'{coin_pair}_close'].shift(16))/features_df[f'{self.target_coin}_close'].shift(16)
                features_df['current_17_interaction'] = (features_df[f'{self.target_coin}_close'].shift(17)-features_df[f'{coin_pair}_close'].shift(17))/features_df[f'{self.target_coin}_close'].shift(17)
                features_df['current_18_interaction'] = (features_df[f'{self.target_coin}_close'].shift(18)-features_df[f'{coin_pair}_close'].shift(18))/features_df[f'{self.target_coin}_close'].shift(18)
                features_df['current_19_interaction'] = (features_df[f'{self.target_coin}_close'].shift(19)-features_df[f'{coin_pair}_close'].shift(19))/features_df[f'{self.target_coin}_close'].shift(19)
                features_df['current_20_interaction'] = (features_df[f'{self.target_coin}_close'].shift(20)-features_df[f'{coin_pair}_close'].shift(20))/features_df[f'{self.target_coin}_close'].shift(20)
                features_df['interaction_average'] = (features_df['current_interaction'] + features_df['current_1_interaction'] + features_df['current_2_interaction'] + 
                                    features_df['current_3_interaction'] + features_df['current_4_interaction'] + features_df['current_5_interaction'] + 
                                    features_df['current_6_interaction'] + features_df['current_7_interaction'] + features_df['current_8_interaction'] + 
                                    features_df['current_9_interaction'] + features_df['current_10_interaction'] + features_df['current_11_interaction'] + 
                                    features_df['current_12_interaction'] + features_df['current_13_interaction'] + features_df['current_14_interaction'] +
                                    features_df['current_15_interaction'] + features_df['current_16_interaction'] + features_df['current_17_interaction'] + 
                                    features_df['current_18_interaction'] + features_df['current_19_interaction'] + features_df['current_20_interaction']) / 21
                features_df[f'avg_20_{coin_pair}_open_interaction'] = features_df['interaction_average'] - features_df['current_interaction']
        return features_df

    def get_model_config(self):
        """Retrieve latest trained model configuration"""
        object_path = 'model_objects/'
        content_object = self.s3_resource.Object(self.s3_bucket, f"{object_path}model_config.json")
        file_content = content_object.get()['Body'].read().decode('utf-8')
        model_config_json = json.loads(file_content)
        return model_config_json

    def get_target_coin(self):
        """Return target coin pair that will be traded"""
        target_coin_list = [cp for cp, ct in self.coin_pair_dict.items() if ct == 'target']
        if len(target_coin_list) > 1:
            raise Exception(f"There must only be a single target coin initialized in the coin pair dictionary. Values: {target_coin_list}")
        return target_coin_list[0]

    def get_model_objects(self):
        """Retrieve full S3 Key for model object and retrieve model object

        Args:
            model_name (str): a supported model name string

        Returns:
            obj: a model object
        """
        model_object_dict = {}
        object_path = 'model_objects'
        model_path_list = self.get_s3_object_keys(self.s3_bucket, f"{object_path}/market_maker_model_")
        if not model_path_list:
            raise AttributeError(f"No model was found at the S3 path [{object_path}]")
        for model_path in model_path_list:
            s3_key = model_path[0]
            model_object_dict[s3_key] = self.get_pickle_from_s3(s3_key)
        return model_object_dict

    def get_pickle_from_s3(self, s3_key):
        """Retrieve pickle object from S3 and unload it into Python object

        Args:
            s3_bucket (str): an S3 bucket name
            s3_key (str): a full S3 object key (file) name

        Returns:
            obj: an unloaded pickle object
        """
        obj = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
        pickle_obj = pickle.load(io.BytesIO(obj['Body'].read()))
        return pickle_obj

    def get_s3_object_keys(self, s3_bucket, s3_prefix):
        """Fetches S3 object keys

        Given a bucket and key prefix, this method will return full keys
        for every object that shares the prefix

        Args:
            s3_bucket (str): An s3 bucket name string
            s3_prefix (str): An s3 key prefix string

        Returns:
            list: A list containing sublists containing object key and file name
        """
        bucket = self.s3_resource.Bucket(s3_bucket)
        object_name_list = []
        for obj in bucket.objects.filter(Prefix=s3_prefix):
            object_name_list.append([obj.key, os.path.basename(obj.key)])
        return object_name_list

    @staticmethod
    def binance_client():
        # Instantiate Binance resources
        try:
            boto_session = boto3.Session(profile_name='loidsig')
        except:
            boto_session = boto3.Session()
        sm_client = boto_session.client(
            service_name='secretsmanager',
            region_name='us-east-1',
            endpoint_url='https://secretsmanager.us-east-1.amazonaws.com'
        )
        get_secret_value_response = sm_client.get_secret_value(SecretId='CPM_Binance')
        key, value = ast.literal_eval(get_secret_value_response['SecretString']).popitem()
        return Client(key, value)
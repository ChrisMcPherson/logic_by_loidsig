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
import psycopg2
from functools import reduce
import logging.config
import time
import datetime
from datetime import timedelta
import multiprocessing
import dill
from joblib import Parallel, delayed
# exchange clients
from binance.client import Client
from cobinhood_api import Cobinhood
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

        requests_logger = logging.getLogger('cobinhood_api')
        requests_logger.setLevel(logging.WARNING)
        logging.config.dictConfig({
            'version': 1,
            'disable_existing_loggers': True,
        })


    def get_model_config(self):
        """Retrieve latest trained model configuration"""
        object_path = 'model_objects/'
        content_object = self.s3_resource.Object(self.s3_bucket, f"{object_path}model_config.json")
        file_content = content_object.get()['Body'].read().decode('utf-8')
        model_config_json = json.loads(file_content)
        return model_config_json

    def get_target_coin(self):
        """Return target coin pair that will be traded"""
        target_coin = self.coin_pair_dict['target']
        return target_coin

    def get_model_standardizer(self):
        """Retrieve full S3 Key for standardize object and retrieve standardize object

        Args:
            model_name (str): a supported standardize name string

        Returns:
            obj: a model object
        """
        object_path = 'model_objects'
        model_path_list = self.get_s3_object_keys(self.s3_bucket, f"{object_path}/market_maker_standardizer_")
        if not model_path_list:
            raise AttributeError(f"No standardizer was found at the S3 path [{object_path}]")
        s3_key = model_path_list[0][0]
        standardize_object = self.get_pickle_from_s3(s3_key)
        return standardize_object

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

    def insert_into_postgres(self, schema, table, column_list_string, values):
        """Inserts scoring results into Postgres db table

        Args:
            schema (str): A schema name
            table (str): A table name
            column_list_string (str): A comma delimited string of column names
            values (str): comma seperated values to insert
        """
        conn = self.logic_db_connection()
        try:
            cur = conn.cursor()
            insert_dml = """INSERT INTO {0}.{1}
                    ({2})
                    VALUES ({3}) 
                    ;""".format(schema, table, column_list_string, values)
            cur.execute(insert_dml)
            conn.commit()
        except Exception as e:
            print(f'Unable to insert into Postgres table {table}. DDL: {insert_dml} Error: {e}')
            raise
        finally:
            conn.close()
        return
    
    def pandas_read_postgres(self, sql_query):
        """Given a Redshift SQL query, this method returns a pandas dataframe

        Args:
            sql_query: a Presto sql query (use the Hue Presto notebook interface to validate sql)

        Returns:
            A pandas dataframe
        """
        logic_db_conn = self.logic_db_connection()
        df = pd.read_sql_query(sql_query, logic_db_conn)
        logic_db_conn.close()
        return df

    def logic_db_connection(self):
        """Fetches Logic DB postgres connection object

        Returns:
            A database connection object for Postgres
        """
        try:
            boto_session = boto3.Session(profile_name='loidsig')
        except:
            boto_session = boto3.Session()
        sm_client = boto_session.client(
            service_name='secretsmanager',
            region_name='us-east-1',
            endpoint_url='https://secretsmanager.us-east-1.amazonaws.com'
        )
        get_secret_value_response = sm_client.get_secret_value(SecretId='Loidsig_DB')
        cred_dict = ast.literal_eval(get_secret_value_response['SecretString'])
        db_user, db_pass = cred_dict['username'], cred_dict['password']
        db_host, db_port, db_name = cred_dict['host'], cred_dict['port'], cred_dict['dbname']

        try:
            conn = psycopg2.connect(
                host=db_host,
                port=db_port,
                user=db_user,
                password=db_pass,
                database=db_name,
            )
        except Exception as e:
            print("Unable to connect to postgres! Error: {}".format(e))
            raise
        return conn


class BinanceScoring(MarketMakerScoring):
    """
    Score against a model with data from the Binance exchange
    """

    def __init__(self):
        super().__init__()
        self.bnb_client = BinanceScoring.binance_client()


    def set_scoring_data(self, in_parallel=True):
        """Set data for model scoring from latest candlestick metrics features"""
        if self.feature_minutes_list == None or self.trade_window_list == None:
            raise Exception("To construct scoring dataframe, the optional feature_minutes_list and trade_window_list attributes must be set!")
        # Get recent trade data for both target coin pair and through coin pair
        recent_trades_interval = max(self.feature_minutes_list) + 10
        cores = multiprocessing.cpu_count()
        try:
            if in_parallel:
                scoring_df_list = Parallel(n_jobs=cores)(delayed(BinanceScoring.pandas_get_candlesticks)(coin_pair, pair_type, f'{recent_trades_interval} min ago UTC') for coin_pair, pair_type in self.coin_pair_dict.items()) #f'2880 min ago UTC'
                scoring_df_dict = dict(scoring_df_list)
            else:
                scoring_df_list = [BinanceScoring.pandas_get_candlesticks(coin_pair, pair_type, f'{recent_trades_interval} min ago UTC') for coin_pair, pair_type in self.coin_pair_dict.items()]
                scoring_df_dict = dict(scoring_df_list)
        except Exception as e:
            print(f"Unable to get recent data for scoring: {e}")
            return

        scoring_features_df = self.engineer_scoring_features(scoring_df_dict)
        scoring_features_df.fillna(0, inplace=True)
        scoring_features_df.replace([np.inf, -np.inf], 0, inplace=True)
        self.scoring_features_df = scoring_features_df
    
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
        bnb_client = BinanceScoring.binance_client()
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
        recent_df_list.append(pair_type)
        recent_df_list.append([coin_pair, df])
        return recent_df_list

    def engineer_scoring_features(self, scoring_df_dict):
        """Engineer scoring features for multiple coin pairs"""
        coin_df_list = []

        for pair_type, coin_data_list in scoring_df_dict.items():
            coin_pair, coin_df = coin_data_list

            if pair_type == 'target':
                coin_df['trade_hour'] = pd.to_datetime(coin_df['close_time']/1000, unit='s').dt.hour
                coin_df['trade_day_of_week'] = pd.to_datetime(coin_df['close_time']/1000, unit='s').dt.dayofweek + 1 # adjust for 0 indexed day of week

            # Lag features
            for interval in self.feature_minutes_list:
                coin_df[f'prev_{interval}_{coin_pair}_close'] = coin_df[f'{coin_pair}_close'].shift(interval)
                coin_df[f'prev_{interval}_{coin_pair}_close_perc_chg'] = (coin_df[f'{coin_pair}_close'] - coin_df[f'prev_{interval}_{coin_pair}_close']) / coin_df[f'prev_{interval}_{coin_pair}_close'] * 100
                coin_df[f'prev_{interval}_{coin_pair}_close_rate_chg'] = (((coin_df[f'{coin_pair}_close'] - coin_df[f'prev_1_{coin_pair}_close']) / coin_df[f'prev_1_{coin_pair}_close']) -
                                                    ((coin_df[f'{coin_pair}_close'] - coin_df[f'prev_{interval}_{coin_pair}_close']) / coin_df[f'prev_{interval}_{coin_pair}_close'])) * 100

                coin_df[f'prev_{interval}_{coin_pair}_high'] = coin_df[f'{coin_pair}_high'].shift(interval)
                coin_df[f'prev_{interval}_{coin_pair}_high_perc_chg'] = (coin_df[f'{coin_pair}_high'] - coin_df[f'prev_{interval}_{coin_pair}_high']) / coin_df[f'prev_{interval}_{coin_pair}_high'] * 100
                coin_df[f'prev_{interval}_{coin_pair}_high_rate_chg'] = (((coin_df[f'{coin_pair}_high'] - coin_df[f'prev_1_{coin_pair}_high']) / coin_df[f'prev_1_{coin_pair}_high']) -
                                                    ((coin_df[f'{coin_pair}_high'] - coin_df[f'prev_{interval}_{coin_pair}_high']) / coin_df[f'prev_{interval}_{coin_pair}_high'])) * 100

                coin_df[f'prev_{interval}_{coin_pair}_low'] = coin_df[f'{coin_pair}_low'].shift(interval)
                coin_df[f'prev_{interval}_{coin_pair}_low_perc_chg'] = (coin_df[f'{coin_pair}_low'] - coin_df[f'prev_{interval}_{coin_pair}_low']) / coin_df[f'prev_{interval}_{coin_pair}_low'] * 100
                coin_df[f'prev_{interval}_{coin_pair}_low_rate_chg'] = (((coin_df[f'{coin_pair}_low'] - coin_df[f'prev_1_{coin_pair}_low']) / coin_df[f'prev_1_{coin_pair}_low'] * 100) -
                                                    ((coin_df[f'{coin_pair}_low'] - coin_df[f'prev_{interval}_{coin_pair}_low']) / coin_df[f'prev_{interval}_{coin_pair}_low'])) * 100

                coin_df[f'prev_{interval}_{coin_pair}_volume'] = coin_df[f'{coin_pair}_volume'].shift(interval)
                coin_df[f'prev_{interval}_{coin_pair}_volume_perc_chg'] = (coin_df[f'{coin_pair}_volume'] - coin_df[f'prev_{interval}_{coin_pair}_volume']) / coin_df[f'prev_{interval}_{coin_pair}_volume'] * 100
                coin_df[f'prev_{interval}_{coin_pair}_volume_rate_chg'] = (((coin_df[f'{coin_pair}_volume'] - coin_df[f'prev_1_{coin_pair}_volume']) / coin_df[f'prev_1_{coin_pair}_volume']) -
                                                    ((coin_df[f'{coin_pair}_volume'] - coin_df[f'prev_{interval}_{coin_pair}_volume']) / coin_df[f'prev_{interval}_{coin_pair}_volume'])) * 100

                coin_df[f'prev_{interval}_{coin_pair}_qav'] = coin_df[f'{coin_pair}_quote_asset_volume'].shift(interval)
                coin_df[f'prev_{interval}_{coin_pair}_qav_perc_chg'] = (coin_df[f'{coin_pair}_quote_asset_volume'] - coin_df[f'prev_{interval}_{coin_pair}_qav']) / coin_df[f'prev_{interval}_{coin_pair}_qav'] * 100
                coin_df[f'prev_{interval}_{coin_pair}_qav_rate_chg'] = (((coin_df[f'{coin_pair}_quote_asset_volume'] - coin_df[f'prev_1_{coin_pair}_qav']) / coin_df[f'prev_1_{coin_pair}_qav']) -
                                                    ((coin_df[f'{coin_pair}_quote_asset_volume'] - coin_df[f'prev_{interval}_{coin_pair}_qav']) / coin_df[f'prev_{interval}_{coin_pair}_qav'])) * 100

                coin_df[f'prev_{interval}_{coin_pair}_trade_count'] = coin_df[f'{coin_pair}_trade_count'].shift(interval)
                coin_df[f'prev_{interval}_{coin_pair}_trade_count_perc_chg'] = (coin_df[f'{coin_pair}_trade_count'] - coin_df[f'prev_{interval}_{coin_pair}_trade_count']) / coin_df[f'prev_{interval}_{coin_pair}_trade_count'] * 100
                coin_df[f'prev_{interval}_{coin_pair}_trade_count_rate_chg'] = (((coin_df[f'{coin_pair}_trade_count'] - coin_df[f'prev_1_{coin_pair}_trade_count']) / coin_df[f'prev_1_{coin_pair}_trade_count']) -
                                                    ((coin_df[f'{coin_pair}_trade_count'] - coin_df[f'prev_{interval}_{coin_pair}_trade_count']) / coin_df[f'prev_{interval}_{coin_pair}_trade_count'])) * 100

                coin_df[f'prev_{interval}_{coin_pair}_tbbav'] = coin_df[f'{coin_pair}_tbbav'].shift(interval)
                coin_df[f'prev_{interval}_{coin_pair}_tbbav_perc_chg'] = (coin_df[f'{coin_pair}_tbbav'] - coin_df[f'prev_{interval}_{coin_pair}_tbbav']) / coin_df[f'prev_{interval}_{coin_pair}_tbbav'] * 100
                coin_df[f'prev_{interval}_{coin_pair}_tbbav_rate_chg'] = (((coin_df[f'{coin_pair}_tbbav'] - coin_df[f'prev_1_{coin_pair}_tbbav']) / coin_df[f'prev_1_{coin_pair}_tbbav']) -
                                                    ((coin_df[f'{coin_pair}_tbbav'] - coin_df[f'prev_{interval}_{coin_pair}_tbbav']) / coin_df[f'prev_{interval}_{coin_pair}_tbbav'])) * 100

                coin_df[f'prev_{interval}_{coin_pair}_tbqav'] = coin_df[f'{coin_pair}_tbqav'].shift(interval)
                coin_df[f'prev_{interval}_{coin_pair}_tbqav_perc_chg'] = (coin_df[f'{coin_pair}_tbqav'] - coin_df[f'prev_{interval}_{coin_pair}_tbqav']) / coin_df[f'prev_{interval}_{coin_pair}_tbqav'] * 100
                coin_df[f'prev_{interval}_{coin_pair}_tbqav_rate_chg'] = (((coin_df[f'{coin_pair}_tbqav'] - coin_df[f'prev_1_{coin_pair}_tbqav']) / coin_df[f'prev_1_{coin_pair}_tbqav']) -
                                                    ((coin_df[f'{coin_pair}_tbqav'] - coin_df[f'prev_{interval}_{coin_pair}_tbqav']) / coin_df[f'prev_{interval}_{coin_pair}_tbqav'])) * 100
            coin_df_list.append(coin_df)

        # Combine features
        features_df = reduce(lambda x, y: pd.merge(x, y, on='close_time', how='inner'), coin_df_list)
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
                features_df[f'avg_5_{coin_pair}_close_interaction'] = features_df['interaction_average'] - features_df['current_interaction']

                features_df['current_6_interaction'] = (features_df[f'{self.target_coin}_close'].shift(6)-features_df[f'{coin_pair}_close'].shift(6))/features_df[f'{self.target_coin}_close'].shift(6)
                features_df['current_7_interaction'] = (features_df[f'{self.target_coin}_close'].shift(7)-features_df[f'{coin_pair}_close'].shift(7))/features_df[f'{self.target_coin}_close'].shift(7)
                features_df['current_8_interaction'] = (features_df[f'{self.target_coin}_close'].shift(8)-features_df[f'{coin_pair}_close'].shift(8))/features_df[f'{self.target_coin}_close'].shift(8)
                features_df['current_9_interaction'] = (features_df[f'{self.target_coin}_close'].shift(9)-features_df[f'{coin_pair}_close'].shift(9))/features_df[f'{self.target_coin}_close'].shift(9)
                features_df['current_10_interaction'] = (features_df[f'{self.target_coin}_close'].shift(10)-features_df[f'{coin_pair}_close'].shift(10))/features_df[f'{self.target_coin}_close'].shift(10)
                features_df['interaction_average'] = (features_df['current_interaction'] + features_df['current_1_interaction'] + features_df['current_2_interaction'] + 
                                    features_df['current_3_interaction'] + features_df['current_4_interaction'] + features_df['current_5_interaction'] + 
                                    features_df['current_6_interaction'] + features_df['current_7_interaction'] + features_df['current_8_interaction'] + 
                                    features_df['current_9_interaction'] + features_df['current_10_interaction']) / 11
                features_df[f'avg_10_{coin_pair}_close_interaction'] = features_df['interaction_average'] - features_df['current_interaction']

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
                features_df[f'avg_20_{coin_pair}_close_interaction'] = features_df['interaction_average'] - features_df['current_interaction']
        return features_df

    def get_trade_qty(self, target_coin, percent_funds_trading=.9):
        usdt_balance = float(self.bnb_client.get_asset_balance(asset='usdt')['free'])
        all_tickers_df = pd.DataFrame(self.bnb_client.get_all_tickers())
        target_price = float(all_tickers_df.loc[all_tickers_df['symbol'] == target_coin, 'price'].item())
        quantity_to_trade = (usdt_balance / target_price) * percent_funds_trading
        quantity_to_trade = round(quantity_to_trade, 6)
        return quantity_to_trade

    def persist_scoring_results(self, scoring_result_dict, optimal_hold_minutes, predicted_return_threshold, data_latency_seconds, latest_minute, scoring_datetime, model_version, buy_order=None, sell_order=None):
        """Inserts results from completed scoring cycle

        Args:
            scoring_result_dict (dict): A dictionary containing the scoring results for each trade hold minute scored
        """
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
            sell_commission_coin = 'Null'
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
                    buy_commission_coin = buy_fills_df.iloc[0]['commissionAsset']

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
                    sell_commission_coin = sell_fills_df.iloc[0]['commissionAsset']
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
            , sell_order_id
            , sell_client_order_id
            , model_version
            """

            values = f"""'{scoring_datetime}'
            , {latest_minute}
            , '{self.target_coin}'
            , {trade_hold_minutes}
            , {payload[0][0]}
            , {payload[1][0]}
            , {highest_return}
            , {is_trade}
            , {predicted_return_threshold}
            , ARRAY{self.feature_minutes_list}
            , ARRAY{self.trade_window_list}
            , '{json.dumps(self.coin_pair_dict)}'
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
            , {model_version}
            """
            self.insert_into_postgres('the_logic', 'scoring_results', column_list_string, values)

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
        get_secret_value_response = sm_client.get_secret_value(SecretId='Loidsig_CPM_Binance')
        key, value = ast.literal_eval(get_secret_value_response['SecretString']).popitem()
        return Client(key, value)


class CobinhoodScoring(MarketMakerScoring):
    """
    Score against a model with data from the Cobinhood exchange
    """

    def __init__(self):
        super().__init__()
        self.cob_client = CobinhoodScoring.cobinhood_client()
        self.set_formatted_target_coin()


    def set_scoring_data(self, in_parallel=True):
        """Set data for model scoring from latest candlestick metrics features"""
        if self.feature_minutes_list == None or self.trade_window_list == None:
            raise Exception("To construct scoring dataframe, the optional feature_minutes_list and trade_window_list attributes must be set!")
        # Get recent trade data for both target coin pair and through coin pair
        recent_trades_interval = max(self.feature_minutes_list) + 10
        #recent_trades_interval = 2890
        cores = multiprocessing.cpu_count()
        try:
            if in_parallel:
                scoring_df_list = Parallel(n_jobs=cores)(delayed(CobinhoodScoring.pandas_get_candlesticks)(coin_pair, pair_type, recent_trades_interval) for pair_type, coin_pair in self.coin_pair_dict.items())
                scoring_df_dict = dict(scoring_df_list)
            else:
                scoring_df_list = [CobinhoodScoring.pandas_get_candlesticks(coin_pair, pair_type, recent_trades_interval) for pair_type, coin_pair in self.coin_pair_dict.items()]
                scoring_df_dict = dict(scoring_df_list)
        except Exception as e:
           print(f"Unable to get recent data for scoring: {e}")
           return

        scoring_features_df = self.engineer_scoring_features(scoring_df_dict)
        scoring_features_df.fillna(0, inplace=True)
        scoring_features_df.replace([np.inf, -np.inf], 0, inplace=True)
        self.scoring_features_df = scoring_features_df

    @staticmethod
    def pandas_get_candlesticks(coin_pair, pair_type, min_ago=1):
        """Return minute granularity recent candlestick metrics from Cobinhood or Binance api for a given coin pair and historical interval

        Returns:
            (list): ['coin_pair',['pair_type', pandas.DataFrame]]
        """
        if 'excharb' not in pair_type:
            cob_client = CobinhoodScoring.cobinhood_client()
            prefix = f"{coin_pair}"
            formatted_coin_pair = CobinhoodScoring.get_formatted_coin_pair(coin_pair)
            starting_unix_time = int((time.time() - (min_ago * 60)) * 1000)
            ending_unix_time = int(time.time() * 1000)

            cob_candles_list = cob_client.chart.get_candles(trading_pair_id=formatted_coin_pair, start_time=starting_unix_time, end_time=ending_unix_time, timeframe='1m')['result']['candles']
            df = pd.DataFrame(cob_candles_list)
            df.rename(columns={'close':f'{prefix}_close',
                                'high':f'{prefix}_high',
                                'low':f'{prefix}_low',
                                'open':f'{prefix}_open',
                                'timestamp':'close_timestamp',
                                'trading_pair_id':'coin',
                                'volume':f'{prefix}_volume'}, inplace=True)
            df[f'{prefix}_open'] = pd.to_numeric(df[f'{prefix}_open'])
            df[f'{prefix}_high'] = pd.to_numeric(df[f'{prefix}_high'])
            df[f'{prefix}_low'] = pd.to_numeric(df[f'{prefix}_low'])
            df[f'{prefix}_close'] = pd.to_numeric(df[f'{prefix}_close'])
            df[f'{prefix}_volume'] = pd.to_numeric(df[f'{prefix}_volume'])
            df['coin'] = df['coin'].replace(regex=True, to_replace=r'-', value=r'')
            df['close_timestamp_trim'] = df['close_timestamp']/1000
            df['close_datetime'] = pd.to_datetime(df['close_timestamp_trim'], unit='s')
            df['minute'] = pd.to_numeric(df['close_timestamp']/1000/60).astype(int)
        else:
            bnb_client = BinanceScoring.binance_client()
            prefix = f"{coin_pair}_excharb"
            coin_pair_upper = coin_pair.upper()
            klines = bnb_client.get_historical_klines(coin_pair_upper, Client.KLINE_INTERVAL_1MINUTE, f'{min_ago} min ago UTC')
            df = pd.DataFrame(klines, columns=['open_time',f'{prefix}_open',f'{prefix}_high',f'{prefix}_low',f'{prefix}_close',f'{prefix}_volume',
                            'close_timestamp',f'{prefix}_quote_asset_volume',f'{prefix}_trade_count', f'{prefix}_tbbav',f'{prefix}_tbqav',f'{prefix}_ignore']
                        )
            df[f'{prefix}_open'] = pd.to_numeric(df[f'{prefix}_open'])
            df[f'{prefix}_high'] = pd.to_numeric(df[f'{prefix}_high'])
            df[f'{prefix}_low'] = pd.to_numeric(df[f'{prefix}_low'])
            df[f'{prefix}_close'] = pd.to_numeric(df[f'{prefix}_close'])
            df[f'{prefix}_volume'] = pd.to_numeric(df[f'{prefix}_volume'])
            df[f'{prefix}_quote_asset_volume'] = pd.to_numeric(df[f'{prefix}_quote_asset_volume'])
            df[f'{prefix}_tbbav'] = pd.to_numeric(df[f'{prefix}_tbbav'])
            df[f'{prefix}_tbqav'] = pd.to_numeric(df[f'{prefix}_tbqav'])
            df['minute'] = pd.to_numeric(df['close_timestamp']/1000/60).astype(int)

        recent_df_list = []
        recent_df_list.append(pair_type)
        recent_df_list.append([coin_pair, df])
        return recent_df_list

    def engineer_scoring_features(self, scoring_df_dict):
        """Engineer scoring features for multiple coin pairs"""
        coin_df_list = []

        for pair_type, coin_data_list in scoring_df_dict.items():
            coin_pair, coin_df = coin_data_list

            if pair_type == 'target':
                coin_df['trade_hour'] = pd.to_datetime(coin_df['close_timestamp']/1000, unit='s').dt.hour
                coin_df['trade_day_of_week'] = pd.to_datetime(coin_df['close_timestamp']/1000, unit='s').dt.dayofweek + 1 # adjust for 0 indexed day of week

            # Lag features
            if 'excharb' not in pair_type:
                for interval in self.feature_minutes_list:
                    coin_df[f'prev_{interval}_{coin_pair}_close'] = coin_df[f'{coin_pair}_close'].shift(interval)
                    coin_df[f'prev_{interval}_{coin_pair}_close_perc_chg'] = (coin_df[f'{coin_pair}_close'] - coin_df[f'prev_{interval}_{coin_pair}_close']) / coin_df[f'prev_{interval}_{coin_pair}_close'] * 100
                    coin_df[f'prev_{interval}_{coin_pair}_close_rate_chg'] = (((coin_df[f'{coin_pair}_close'] - coin_df[f'prev_1_{coin_pair}_close']) / coin_df[f'prev_1_{coin_pair}_close']) -
                                                        ((coin_df[f'{coin_pair}_close'] - coin_df[f'prev_{interval}_{coin_pair}_close']) / coin_df[f'prev_{interval}_{coin_pair}_close'])) * 100

                    coin_df[f'prev_{interval}_{coin_pair}_high'] = coin_df[f'{coin_pair}_high'].shift(interval)
                    coin_df[f'prev_{interval}_{coin_pair}_high_perc_chg'] = (coin_df[f'{coin_pair}_high'] - coin_df[f'prev_{interval}_{coin_pair}_high']) / coin_df[f'prev_{interval}_{coin_pair}_high'] * 100
                    coin_df[f'prev_{interval}_{coin_pair}_high_rate_chg'] = (((coin_df[f'{coin_pair}_high'] - coin_df[f'prev_1_{coin_pair}_high']) / coin_df[f'prev_1_{coin_pair}_high']) -
                                                        ((coin_df[f'{coin_pair}_high'] - coin_df[f'prev_{interval}_{coin_pair}_high']) / coin_df[f'prev_{interval}_{coin_pair}_high'])) * 100

                    coin_df[f'prev_{interval}_{coin_pair}_low'] = coin_df[f'{coin_pair}_low'].shift(interval)
                    coin_df[f'prev_{interval}_{coin_pair}_low_perc_chg'] = (coin_df[f'{coin_pair}_low'] - coin_df[f'prev_{interval}_{coin_pair}_low']) / coin_df[f'prev_{interval}_{coin_pair}_low'] * 100
                    coin_df[f'prev_{interval}_{coin_pair}_low_rate_chg'] = (((coin_df[f'{coin_pair}_low'] - coin_df[f'prev_1_{coin_pair}_low']) / coin_df[f'prev_1_{coin_pair}_low'] * 100) -
                                                        ((coin_df[f'{coin_pair}_low'] - coin_df[f'prev_{interval}_{coin_pair}_low']) / coin_df[f'prev_{interval}_{coin_pair}_low'])) * 100

                    coin_df[f'prev_{interval}_{coin_pair}_volume'] = coin_df[f'{coin_pair}_volume'].shift(interval)
                    coin_df[f'prev_{interval}_{coin_pair}_volume_perc_chg'] = (coin_df[f'{coin_pair}_volume'] - coin_df[f'prev_{interval}_{coin_pair}_volume']) / coin_df[f'prev_{interval}_{coin_pair}_volume'] * 100
                    coin_df[f'prev_{interval}_{coin_pair}_volume_rate_chg'] = (((coin_df[f'{coin_pair}_volume'] - coin_df[f'prev_1_{coin_pair}_volume']) / coin_df[f'prev_1_{coin_pair}_volume']) -
                                                        ((coin_df[f'{coin_pair}_volume'] - coin_df[f'prev_{interval}_{coin_pair}_volume']) / coin_df[f'prev_{interval}_{coin_pair}_volume'])) * 100
                    coin_df[f'prev_{interval}_{coin_pair}_volume_direction'] = (coin_df[f'{coin_pair}_close'] - coin_df[f'prev_{interval}_{coin_pair}_close']) / coin_df[f'prev_{interval}_{coin_pair}_close'] * 100 * coin_df[f'{coin_pair}_volume']
            
            # Drop shared columns that aren't relevant
            if pair_type != 'target':
                coin_df.drop(['close_timestamp'], axis=1, inplace=True)

            coin_df_list.append(coin_df)

        # Combine features
        features_df = reduce(lambda x, y: pd.merge(x, y, on='minute', how='inner'), coin_df_list)

        # Create Interaction Features
        for pair_type, coin_data_list in scoring_df_dict.items():
            coin_pair, coin_df = coin_data_list
            if pair_type == 'alt':
                features_df['current_interaction'] = (features_df[f'{self.target_coin}_close']-features_df[f'{coin_pair}_close'])/features_df[f'{self.target_coin}_close']
                features_df['current_1_interaction'] = (features_df[f'{self.target_coin}_close'].shift(1)-features_df[f'{coin_pair}_close'].shift(1))/features_df[f'{self.target_coin}_close'].shift(1)
                features_df['interaction_average'] = (features_df['current_interaction'] + features_df['current_1_interaction']) / 2
                features_df[f'avg_1_{coin_pair}_close_interaction'] = features_df['interaction_average'] - features_df['current_interaction']

                features_df['current_2_interaction'] = (features_df[f'{self.target_coin}_close'].shift(2)-features_df[f'{coin_pair}_close'].shift(2))/features_df[f'{self.target_coin}_close'].shift(2)
                features_df['current_3_interaction'] = (features_df[f'{self.target_coin}_close'].shift(3)-features_df[f'{coin_pair}_close'].shift(3))/features_df[f'{self.target_coin}_close'].shift(3)
                features_df['current_4_interaction'] = (features_df[f'{self.target_coin}_close'].shift(4)-features_df[f'{coin_pair}_close'].shift(4))/features_df[f'{self.target_coin}_close'].shift(4)
                features_df['current_5_interaction'] = (features_df[f'{self.target_coin}_close'].shift(5)-features_df[f'{coin_pair}_close'].shift(5))/features_df[f'{self.target_coin}_close'].shift(5)
                features_df['interaction_average'] = (features_df['current_interaction'] + features_df['current_1_interaction'] + features_df['current_2_interaction'] + 
                                    features_df['current_3_interaction'] + features_df['current_4_interaction'] + features_df['current_5_interaction']) / 6
                features_df[f'avg_5_{coin_pair}_close_interaction'] = features_df['interaction_average'] - features_df['current_interaction']

                features_df['current_6_interaction'] = (features_df[f'{self.target_coin}_close'].shift(6)-features_df[f'{coin_pair}_close'].shift(6))/features_df[f'{self.target_coin}_close'].shift(6)
                features_df['current_7_interaction'] = (features_df[f'{self.target_coin}_close'].shift(7)-features_df[f'{coin_pair}_close'].shift(7))/features_df[f'{self.target_coin}_close'].shift(7)
                features_df['current_8_interaction'] = (features_df[f'{self.target_coin}_close'].shift(8)-features_df[f'{coin_pair}_close'].shift(8))/features_df[f'{self.target_coin}_close'].shift(8)
                features_df['current_9_interaction'] = (features_df[f'{self.target_coin}_close'].shift(9)-features_df[f'{coin_pair}_close'].shift(9))/features_df[f'{self.target_coin}_close'].shift(9)
                features_df['current_10_interaction'] = (features_df[f'{self.target_coin}_close'].shift(10)-features_df[f'{coin_pair}_close'].shift(10))/features_df[f'{self.target_coin}_close'].shift(10)
                features_df['interaction_average'] = (features_df['current_interaction'] + features_df['current_1_interaction'] + features_df['current_2_interaction'] + 
                                    features_df['current_3_interaction'] + features_df['current_4_interaction'] + features_df['current_5_interaction'] + 
                                    features_df['current_6_interaction'] + features_df['current_7_interaction'] + features_df['current_8_interaction'] + 
                                    features_df['current_9_interaction'] + features_df['current_10_interaction']) / 11
                features_df[f'avg_10_{coin_pair}_close_interaction'] = features_df['interaction_average'] - features_df['current_interaction']

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
                features_df[f'avg_20_{coin_pair}_close_interaction'] = features_df['interaction_average'] - features_df['current_interaction']

            elif 'excharb' in pair_type:
                features_df['current_interaction'] = (features_df[f'{self.target_coin}_close'] - features_df[f'{coin_pair}_excharb_close']) / features_df[f'{self.target_coin}_close']
                features_df['current_1_interaction'] = (features_df[f'{self.target_coin}_close'].shift(1) - features_df[f'{coin_pair}_excharb_close'].shift(1) ) / features_df[f'{self.target_coin}_close'].shift(1)
                features_df['interaction_average'] = (features_df['current_interaction'] + features_df['current_1_interaction']) / 2
                features_df[f'avg_1_{coin_pair}_excharb_close_interaction'] = features_df['interaction_average'] - features_df['current_interaction']

                features_df['current_2_interaction'] = (features_df[f'{self.target_coin}_close'].shift(2) - features_df[f'{coin_pair}_excharb_close'].shift(2) ) / features_df[f'{self.target_coin}_close'].shift(2)
                features_df['interaction_average'] = (features_df['current_interaction'] + features_df['current_1_interaction'] + features_df['current_2_interaction']) / 3
                features_df[f'avg_2_{coin_pair}_excharb_close_interaction'] = features_df['interaction_average'] - features_df['current_interaction']

                features_df['current_3_interaction'] = (features_df[f'{self.target_coin}_close'].shift(3) - features_df[f'{coin_pair}_excharb_close'].shift(3) ) / features_df[f'{self.target_coin}_close'].shift(3)
                features_df['interaction_average'] = (features_df['current_interaction'] + features_df['current_1_interaction'] + features_df['current_2_interaction'] + features_df['current_3_interaction']) / 4
                features_df[f'avg_3_{coin_pair}_excharb_close_interaction'] = features_df['interaction_average'] - features_df['current_interaction']

                features_df['current_4_interaction'] = (features_df[f'{self.target_coin}_close'].shift(4)-features_df[f'{coin_pair}_excharb_close'].shift(4))/features_df[f'{self.target_coin}_close'].shift(4)
                features_df['current_5_interaction'] = (features_df[f'{self.target_coin}_close'].shift(5)-features_df[f'{coin_pair}_excharb_close'].shift(5))/features_df[f'{self.target_coin}_close'].shift(5)
                features_df['current_6_interaction'] = (features_df[f'{self.target_coin}_close'].shift(6)-features_df[f'{coin_pair}_excharb_close'].shift(6))/features_df[f'{self.target_coin}_close'].shift(6)
                features_df['current_7_interaction'] = (features_df[f'{self.target_coin}_close'].shift(7)-features_df[f'{coin_pair}_excharb_close'].shift(7))/features_df[f'{self.target_coin}_close'].shift(7)
                features_df['current_8_interaction'] = (features_df[f'{self.target_coin}_close'].shift(8)-features_df[f'{coin_pair}_excharb_close'].shift(8))/features_df[f'{self.target_coin}_close'].shift(8)
                features_df['current_9_interaction'] = (features_df[f'{self.target_coin}_close'].shift(9)-features_df[f'{coin_pair}_excharb_close'].shift(9))/features_df[f'{self.target_coin}_close'].shift(9)
                features_df['current_10_interaction'] = (features_df[f'{self.target_coin}_close'].shift(10)-features_df[f'{coin_pair}_excharb_close'].shift(10))/features_df[f'{self.target_coin}_close'].shift(10)
                features_df['interaction_average'] = (features_df['current_interaction'] + features_df['current_1_interaction'] + features_df['current_2_interaction'] + 
                                    features_df['current_3_interaction'] + features_df['current_4_interaction'] + features_df['current_5_interaction'] + 
                                    features_df['current_6_interaction'] + features_df['current_7_interaction'] + features_df['current_8_interaction'] + 
                                    features_df['current_9_interaction'] + features_df['current_10_interaction']) / 11
                features_df[f'avg_10_{coin_pair}_excharb_close_interaction'] = features_df['interaction_average'] - features_df['current_interaction']

                features_df['current_11_interaction'] = (features_df[f'{self.target_coin}_close'].shift(11)-features_df[f'{coin_pair}_excharb_close'].shift(11))/features_df[f'{self.target_coin}_close'].shift(11)
                features_df['current_12_interaction'] = (features_df[f'{self.target_coin}_close'].shift(12)-features_df[f'{coin_pair}_excharb_close'].shift(12))/features_df[f'{self.target_coin}_close'].shift(12)
                features_df['current_13_interaction'] = (features_df[f'{self.target_coin}_close'].shift(13)-features_df[f'{coin_pair}_excharb_close'].shift(13))/features_df[f'{self.target_coin}_close'].shift(13)
                features_df['current_14_interaction'] = (features_df[f'{self.target_coin}_close'].shift(14)-features_df[f'{coin_pair}_excharb_close'].shift(14))/features_df[f'{self.target_coin}_close'].shift(14)
                features_df['current_15_interaction'] = (features_df[f'{self.target_coin}_close'].shift(15)-features_df[f'{coin_pair}_excharb_close'].shift(15))/features_df[f'{self.target_coin}_close'].shift(15)
                features_df['current_16_interaction'] = (features_df[f'{self.target_coin}_close'].shift(16)-features_df[f'{coin_pair}_excharb_close'].shift(16))/features_df[f'{self.target_coin}_close'].shift(16)
                features_df['current_17_interaction'] = (features_df[f'{self.target_coin}_close'].shift(17)-features_df[f'{coin_pair}_excharb_close'].shift(17))/features_df[f'{self.target_coin}_close'].shift(17)
                features_df['current_18_interaction'] = (features_df[f'{self.target_coin}_close'].shift(18)-features_df[f'{coin_pair}_excharb_close'].shift(18))/features_df[f'{self.target_coin}_close'].shift(18)
                features_df['current_19_interaction'] = (features_df[f'{self.target_coin}_close'].shift(19)-features_df[f'{coin_pair}_excharb_close'].shift(19))/features_df[f'{self.target_coin}_close'].shift(19)
                features_df['current_20_interaction'] = (features_df[f'{self.target_coin}_close'].shift(20)-features_df[f'{coin_pair}_excharb_close'].shift(20))/features_df[f'{self.target_coin}_close'].shift(20)
                features_df['interaction_average'] = (features_df['current_interaction'] + features_df['current_1_interaction'] + features_df['current_2_interaction'] + 
                                    features_df['current_3_interaction'] + features_df['current_4_interaction'] + features_df['current_5_interaction'] + 
                                    features_df['current_6_interaction'] + features_df['current_7_interaction'] + features_df['current_8_interaction'] + 
                                    features_df['current_9_interaction'] + features_df['current_10_interaction'] + features_df['current_11_interaction'] + 
                                    features_df['current_12_interaction'] + features_df['current_13_interaction'] + features_df['current_14_interaction'] +
                                    features_df['current_15_interaction'] + features_df['current_16_interaction'] + features_df['current_17_interaction'] + 
                                    features_df['current_18_interaction'] + features_df['current_19_interaction'] + features_df['current_20_interaction']) / 21
                features_df[f'avg_20_{coin_pair}_excharb_close_interaction'] = features_df['interaction_average'] - features_df['current_interaction']

        return features_df

    def get_trade_qty(self, percent_funds_trading=.9):
        usdt_balance = self.get_usdt_wallet_balance()
        target_price = self.get_target_coin_price()
        quantity_to_trade = (usdt_balance / target_price) * percent_funds_trading
        quantity_to_trade = round(quantity_to_trade, 6)
        return quantity_to_trade

    def get_usdt_wallet_balance(self):
        return float([balance for balance in self.cob_client.wallet.get_balances()['result']['balances'] if balance['currency'] == 'USDT'][0]['total'])

    def get_target_coin_price(self):
        return float(self.cob_client.market.get_tickers(self.formatted_target_coin)['result']['ticker']['last_trade_price'])

    def set_formatted_target_coin(self):
        formatted_target_coin = CobinhoodScoring.get_formatted_coin_pair(self.target_coin)
        self.formatted_target_coin = formatted_target_coin

    @staticmethod
    def get_formatted_coin_pair(coin_pair):
        """Given an unformatted coin pair, return the cobinhood formatted coin pair
        """
        cob_client = CobinhoodScoring.cobinhood_client()
        pairs_payload = cob_client.market.get_trading_pairs()
        pairs_list = [[pair['id'].lower().replace('-', ''), pair['id']] for pair in pairs_payload['result']['trading_pairs']]
        pairs_dict = dict(pairs_list)
        formatted_coin_pair = pairs_dict[coin_pair]
        return formatted_coin_pair

    def persist_scoring_results(self, scoring_result_dict, optimal_hold_minutes, predicted_return_threshold, data_latency_seconds, latest_minute, scoring_datetime, model_version, scoring_close_price, target_coin_price_pre_buy_order='Null', buy_order=None, target_coin_price_pre_sell_order='Null', sell_order=None, buy_fill_latency_seconds='Null', sell_fill_latency_seconds='Null'):
        """Inserts results from completed scoring cycle

        Args:
            scoring_result_dict (dict): A dictionary containing the scoring results for each trade hold minute scored
        """
        for trade_hold_minutes, payload in scoring_result_dict.items():
            # default values
            highest_return = False
            is_trade = False
            buy_timestamp = 'Null'
            buy_order_id = 'Null'
            buy_client_order_id = 'Null'
            buy_quantity = 'Null'
            buy_commission = 'Null'
            buy_price = 'Null'
            buy_commission_coin = 'Null'
            sell_timestamp = 'Null'
            sell_order_id = 'Null'
            sell_client_order_id = 'Null'
            sell_quantity = 'Null'
            sell_commission = 'Null'
            sell_price = 'Null'
            sell_commission_coin = 'Null'
            if trade_hold_minutes == optimal_hold_minutes:
                highest_return = True
                if scoring_result_dict[optimal_hold_minutes][0][0] > predicted_return_threshold:
                    is_trade = True
                    buy_order_id = 0
                    buy_client_order_id = buy_order['result']['order']['id']
                    buy_quantity = buy_order['result']['order']['size']
                    buy_commission = 0
                    buy_price = buy_order['result']['order']['eq_price']
                    buy_commission_coin = ''
                    buy_timestamp = buy_order['result']['order']['timestamp']

                    sell_order_id = 0
                    sell_client_order_id = sell_order['result']['order']['id']
                    sell_quantity = sell_order['result']['order']['size']
                    sell_commission = 0
                    sell_price = sell_order['result']['order']['eq_price']
                    sell_commission_coin = ''
                    sell_timestamp = sell_order['result']['order']['timestamp']
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
            , scoring_close_price
            , buy_timestamp
            , sell_timestamp
            , buy_quantity
            , sell_quantity
            , buy_price_pre_order 
            , sell_price_pre_order
            , buy_price
            , sell_price
            , buy_commission
            , sell_commission
            , buy_commission_coin
            , sell_commission_coin
            , buy_order_id
            , buy_client_order_id
            , sell_order_id
            , sell_client_order_id
            , buy_fill_latency_seconds
            , sell_fill_latency_seconds
            , model_version
            """

            values = f"""'{scoring_datetime}'
            , {latest_minute}
            , '{self.target_coin}'
            , {trade_hold_minutes}
            , {payload[0][0]}
            , {payload[1][0]}
            , {highest_return}
            , {is_trade}
            , {predicted_return_threshold}
            , ARRAY{self.feature_minutes_list}
            , ARRAY{self.trade_window_list}
            , '{json.dumps(self.coin_pair_dict)}'
            , {data_latency_seconds}
            , {scoring_close_price}
            , {buy_timestamp}
            , {sell_timestamp}
            , {buy_quantity}
            , {sell_quantity}
            , {target_coin_price_pre_buy_order}
            , {target_coin_price_pre_sell_order}
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
            , {buy_fill_latency_seconds}
            , {sell_fill_latency_seconds}
            , {model_version}
            """
            self.insert_into_postgres('the_logic', 'scoring_results', column_list_string, values)

    @staticmethod
    def cobinhood_client():
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
        get_secret_value_response = sm_client.get_secret_value(SecretId='Loidsig_CPM_Cobinhood')
        key, value = ast.literal_eval(get_secret_value_response['SecretString']).popitem()
        return Cobinhood(API_TOKEN=value)
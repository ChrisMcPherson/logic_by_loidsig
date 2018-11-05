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
from sqlalchemy import create_engine
# local libraries
#sys.path.append(os.path.abspath(os.path.join(sys.path[0], '..', 'lib')))
#import athena_connect

class MarketMakerTraining():
    def __init__(self, coin_pair_dict, feature_minutes_list, trade_window_list, training_period=None):
        """Market maker class to fascilitate data engineering for training models
        """
        self.s3_bucket = 'loidsig-crypto'
        try:
            self.boto_session = boto3.Session(profile_name='loidsig')
            self.s3_client = self.boto_session.client('s3')
        except:
            self.s3_client = boto3.client('s3')
        self.coin_pair_dict = coin_pair_dict
        self.target_coin = self.get_target_coin()
        self.feature_minutes_list = feature_minutes_list
        self.trade_window_list = trade_window_list
        self.training_period = training_period
        self.training_data_sql = None
        self.feature_column_list = None
        self.target_column_list = None

    def set_training_data(self):
        """Pull binance only training data from Athena and engineer features"""
        # Optional training data period
        # TODO: add training data period feature to training data query
        if not self.training_period == None:
            training_period_date = (datetime.datetime.utcnow() - timedelta(days=self.training_period)).strftime("%Y-%m-%d")
        # Extract queried data from Athena
        #athena = athena_connect.Athena()
        #features_df = athena.pandas_read_athena(self.training_data_sql)
        features_df = pd.read_sql(self.training_data_sql, self.logic_db_engine())
        features_df.fillna(0, inplace=True)
        print(features_df.shape)
        features_df = features_df[max(self.feature_minutes_list):]
        print(features_df.shape)
        # Remove infinity string
        features_df.replace({'Infinity': 0}, inplace=True)
        # Convert all object fields to numeric except date fields
        object_col_list = features_df.columns[features_df.dtypes.eq('object')]
        object_col_list = [col for col in object_col_list if 'trade_date' not in col]
        features_df[object_col_list] = features_df[object_col_list].apply(pd.to_numeric, errors='coerce')
        self.training_df = features_df

    def get_target_coin(self):
        """Return target coin pair that will be traded"""
        target_coin = self.coin_pair_dict['target']
        return target_coin

    def persist_standardizer(self, std_object):
        """Persist standardize object as pkl to S3"""
        object_path = 'model_objects/'
        file_name = f'market_maker_standardizer_{self.target_coin}.pkl'
        self.s3_client.put_object(Bucket=self.s3_bucket,
                        Key=object_path + file_name,
                        Body=pickle.dumps(std_object, pickle.HIGHEST_PROTOCOL)
                    )
        return

    def persist_model(self, model, trade_window):
        """Persist model object as pkl to S3"""
        object_path = 'model_objects/'
        file_name = f'market_maker_model_{self.target_coin}_{trade_window}.pkl'
        self.s3_client.put_object(Bucket=self.s3_bucket,
                        Key=object_path + file_name,
                        Body=pickle.dumps(model, pickle.HIGHEST_PROTOCOL)
                    )
        return

    def persist_model_config(self):
        """Save market maker model configurations to S3 to be consumed when scoring"""
        config_dict = {}
        config_dict['coin_pair_dict'] = self.coin_pair_dict
        config_dict['feature_column_list'] = self.feature_column_list
        config_dict['feature_minutes_list'] = self.feature_minutes_list
        config_dict['trade_window_list'] = self.trade_window_list
        part_json = json.dumps(config_dict, indent=4)
        object_path = 'model_objects/'
        file_name = "model_config.json"
        self.s3_client.put_object(Bucket=self.s3_bucket,
                    Key= object_path + file_name,
                    Body= part_json)
        return

    def logic_db_engine(self):
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
            postgres_engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')
        except Exception as e:
            print("Unable to connect to postgres! Error: {}".format(e))
            raise
        return postgres_engine


class BinanceTraining(MarketMakerTraining):
    """
    Train a model with data from the Binance exchange
    """

    def __init__(self, coin_pair_dict, feature_minutes_list, trade_window_list, training_period=None):
        super().__init__(coin_pair_dict, feature_minutes_list, trade_window_list, training_period=None)
        self.training_data_sql, self.feature_column_list, self.target_column_list = self.construct_training_data_query()
        

    def construct_training_data_query(self):
        """Return training data query from dynamic template"""
        # FUTURE: make dollar return target/features dynamic
        if self.feature_minutes_list == None or self.trade_window_list == None:
            raise Exception("To construct training data query, the optional feature_minutes_list and trade_window_list attributes must be set!")
        
        feature_col_list = []
        target_col_list = []
        raw_features_list = []
        base_features_list = []
        interaction_features_list = []
        lag_features_list = []
        join_conditions_list = []
        target_variables_list = []

        for pair_type, coin_pair in self.coin_pair_dict.items():
            # Raw base features
            raw_features_list.append(f"""{pair_type}_{coin_pair} AS (
                                    SELECT coin_pair AS {coin_pair}_coin_partition
                                        , to_timestamp(trade_minute * 60) AS {coin_pair}_trade_datetime
                                        , DATE(to_timestamp(trade_minute * 60)) AS {coin_pair}_trade_date
                                        , trade_minute AS {coin_pair}_trade_minute
                                        , bids_top_price AS {coin_pair}_bids_top_price
                                        , (asks_top_price - bids_top_price) AS {coin_pair}_bid_ask_spread
                                        , (asks_top_price + bids_top_price) / 2 AS {coin_pair}_bid_ask_average_price
                                        , bids_cum_5000_weighted_avg AS {coin_pair}_bids_cum_5000_weighted_avg
                                        , bids_cum_10000_weighted_avg AS {coin_pair}_bids_cum_10000_weighted_avg
                                        , bids_cum_20000_weighted_avg AS {coin_pair}_bids_cum_20000_weighted_avg
                                        , bids_cum_50000_weighted_avg AS {coin_pair}_bids_cum_50000_weighted_avg
                                        , bids_cum_100000_weighted_avg AS {coin_pair}_bids_cum_100000_weighted_avg
                                        , bids_cum_200000_weighted_avg AS {coin_pair}_bids_cum_200000_weighted_avg
                                        , bids_cum_5000_weighted_std AS {coin_pair}_bids_cum_5000_weighted_std
                                        , bids_cum_10000_weighted_std AS {coin_pair}_bids_cum_10000_weighted_std
                                        , bids_cum_20000_weighted_std AS {coin_pair}_bids_cum_20000_weighted_std
                                        , bids_cum_50000_weighted_std AS {coin_pair}_bids_cum_50000_weighted_std
                                        , bids_cum_100000_weighted_std AS {coin_pair}_bids_cum_100000_weighted_std
                                        , bids_cum_200000_weighted_std AS {coin_pair}_bids_cum_200000_weighted_std
                                        , asks_top_price AS {coin_pair}_asks_top_price
                                        , asks_cum_5000_weighted_avg AS {coin_pair}_asks_cum_5000_weighted_avg
                                        , asks_cum_10000_weighted_avg AS {coin_pair}_asks_cum_10000_weighted_avg
                                        , asks_cum_20000_weighted_avg AS {coin_pair}_asks_cum_20000_weighted_avg
                                        , asks_cum_50000_weighted_avg AS {coin_pair}_asks_cum_50000_weighted_avg
                                        , asks_cum_100000_weighted_avg AS {coin_pair}_asks_cum_100000_weighted_avg
                                        , asks_cum_200000_weighted_avg AS {coin_pair}_asks_cum_200000_weighted_avg
                                        , asks_cum_5000_weighted_std AS {coin_pair}_asks_cum_5000_weighted_std
                                        , asks_cum_10000_weighted_std AS {coin_pair}_asks_cum_10000_weighted_std
                                        , asks_cum_20000_weighted_std AS {coin_pair}_asks_cum_20000_weighted_std
                                        , asks_cum_50000_weighted_std AS {coin_pair}_asks_cum_50000_weighted_std
                                        , asks_cum_100000_weighted_std AS {coin_pair}_asks_cum_100000_weighted_std
                                        , asks_cum_200000_weighted_std AS {coin_pair}_asks_cum_200000_weighted_std
                                    FROM binance.orderbook 
                                    WHERE coin_pair = '{coin_pair}'
                                    )""")
            # Base features
            if pair_type == 'target':
                base_features_list.append(f"""{coin_pair}_trade_datetime, {coin_pair}_trade_date, {coin_pair}_trade_minute
                                        , extract(isodow from {coin_pair}_trade_datetime) as trade_day_of_week
                                        , date_part('hour', {coin_pair}_trade_datetime) as trade_hour""")
                feature_col_list.extend(['trade_day_of_week', 'trade_hour'])
            base_features_list.append(f"""{coin_pair}_bid_ask_spread
                                        , {coin_pair}_bid_ask_average_price
                                        , {coin_pair}_bids_cum_5000_weighted_avg
                                        , {coin_pair}_bids_cum_10000_weighted_avg
                                        , {coin_pair}_bids_cum_20000_weighted_avg
                                        , {coin_pair}_bids_cum_50000_weighted_avg
                                        , {coin_pair}_bids_cum_100000_weighted_avg
                                        , {coin_pair}_bids_cum_200000_weighted_avg
                                        , {coin_pair}_bids_cum_5000_weighted_std
                                        , {coin_pair}_bids_cum_10000_weighted_std
                                        , {coin_pair}_bids_cum_20000_weighted_std
                                        , {coin_pair}_bids_cum_50000_weighted_std
                                        , {coin_pair}_bids_cum_100000_weighted_std
                                        , {coin_pair}_bids_cum_200000_weighted_std
                                        , {coin_pair}_asks_cum_5000_weighted_avg
                                        , {coin_pair}_asks_cum_10000_weighted_avg
                                        , {coin_pair}_asks_cum_20000_weighted_avg
                                        , {coin_pair}_asks_cum_50000_weighted_avg
                                        , {coin_pair}_asks_cum_100000_weighted_avg
                                        , {coin_pair}_asks_cum_200000_weighted_avg
                                        , {coin_pair}_asks_cum_5000_weighted_std
                                        , {coin_pair}_asks_cum_10000_weighted_std
                                        , {coin_pair}_asks_cum_20000_weighted_std
                                        , {coin_pair}_asks_cum_50000_weighted_std
                                        , {coin_pair}_asks_cum_100000_weighted_std
                                        , {coin_pair}_asks_cum_200000_weighted_std""")
            feature_col_list.extend([f'{coin_pair}_bid_ask_spread'
                                    , f'{coin_pair}_bid_ask_average_price'
                                    , f'{coin_pair}_bids_cum_5000_weighted_avg'
                                    , f'{coin_pair}_bids_cum_10000_weighted_avg'
                                    , f'{coin_pair}_bids_cum_20000_weighted_avg'
                                    , f'{coin_pair}_bids_cum_50000_weighted_avg'
                                    , f'{coin_pair}_bids_cum_100000_weighted_avg'
                                    , f'{coin_pair}_bids_cum_200000_weighted_avg'
                                    , f'{coin_pair}_bids_cum_5000_weighted_std'
                                    , f'{coin_pair}_bids_cum_10000_weighted_std'
                                    , f'{coin_pair}_bids_cum_20000_weighted_std'
                                    , f'{coin_pair}_bids_cum_50000_weighted_std'
                                    , f'{coin_pair}_bids_cum_100000_weighted_std'
                                    , f'{coin_pair}_bids_cum_200000_weighted_std'
                                    , f'{coin_pair}_asks_cum_5000_weighted_avg'
                                    , f'{coin_pair}_asks_cum_10000_weighted_avg'
                                    , f'{coin_pair}_asks_cum_20000_weighted_avg'
                                    , f'{coin_pair}_asks_cum_50000_weighted_avg'
                                    , f'{coin_pair}_asks_cum_100000_weighted_avg'
                                    , f'{coin_pair}_asks_cum_200000_weighted_avg'
                                    , f'{coin_pair}_asks_cum_5000_weighted_std'
                                    , f'{coin_pair}_asks_cum_10000_weighted_std'
                                    , f'{coin_pair}_asks_cum_20000_weighted_std'
                                    , f'{coin_pair}_asks_cum_50000_weighted_std'
                                    , f'{coin_pair}_asks_cum_100000_weighted_std'
                                    , f'{coin_pair}_asks_cum_200000_weighted_std'])
            # Interaction features for alt coins (base usdt)
            if pair_type == 'alt':
                interaction_features_list.append(f"""AVG(({self.target_coin}_bid_ask_average_price-{coin_pair}_bid_ask_average_price)/{self.target_coin}_bid_ask_average_price) OVER (PARTITION BY {self.target_coin}_coin_partition ORDER BY {self.target_coin}_trade_minute ASC ROWS 5 PRECEDING) 
                                                    - (({self.target_coin}_bid_ask_average_price-{coin_pair}_bid_ask_average_price)/{self.target_coin}_bid_ask_average_price) AS avg_5_{coin_pair}_bid_ask_average_price_interaction""")
                interaction_features_list.append(f"""AVG(({self.target_coin}_bid_ask_average_price-{coin_pair}_bid_ask_average_price)/{self.target_coin}_bid_ask_average_price) OVER (PARTITION BY {self.target_coin}_coin_partition ORDER BY {self.target_coin}_trade_minute ASC ROWS 10 PRECEDING) 
                                                    - (({self.target_coin}_bid_ask_average_price-{coin_pair}_bid_ask_average_price)/{self.target_coin}_bid_ask_average_price) AS avg_10_{coin_pair}_bid_ask_average_price_interaction""")
                interaction_features_list.append(f"""AVG(({self.target_coin}_bid_ask_average_price-{coin_pair}_bid_ask_average_price)/{self.target_coin}_bid_ask_average_price) OVER (PARTITION BY {self.target_coin}_coin_partition ORDER BY {self.target_coin}_trade_minute ASC ROWS 20 PRECEDING) 
                                                    - (({self.target_coin}_bid_ask_average_price-{coin_pair}_bid_ask_average_price)/{self.target_coin}_bid_ask_average_price) AS avg_20_{coin_pair}_bid_ask_average_price_interaction""")
                feature_col_list.extend([f'avg_5_{coin_pair}_bid_ask_average_price_interaction',f'avg_10_{coin_pair}_bid_ask_average_price_interaction',f'avg_20_{coin_pair}_bid_ask_average_price_interaction'])
            # Lag features for every interval configured at runtime
            for interval in self.feature_minutes_list:
                interval_list = []
                interval_list.append(f"""(({coin_pair}_bid_ask_average_price - LEAD({coin_pair}_bid_ask_average_price, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                            / LEAD({coin_pair}_bid_ask_average_price, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) * 100 AS prev_{interval}_{coin_pair}_bid_ask_average_price_perc_chg
                                        ,((({coin_pair}_bid_ask_average_price - LEAD({coin_pair}_bid_ask_average_price, 1) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                            / LEAD({coin_pair}_bid_ask_average_price, 1) OVER (ORDER BY {self.target_coin}_trade_minute DESC))
                                           - (({coin_pair}_bid_ask_average_price - LEAD({coin_pair}_bid_ask_average_price, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                            / LEAD({coin_pair}_bid_ask_average_price, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC))) * 100 AS prev_{interval}_{coin_pair}_bid_ask_average_price_rate_chg
                                        ,(({coin_pair}_bids_cum_5000_weighted_avg - LEAD({coin_pair}_bids_cum_5000_weighted_avg, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                            / LEAD({coin_pair}_bids_cum_5000_weighted_avg, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) * 100 AS prev_{interval}_{coin_pair}_bids_cum_5000_weighted_avg_perc_chg
                                        ,(({coin_pair}_bids_cum_50000_weighted_avg - LEAD({coin_pair}_bids_cum_50000_weighted_avg, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                            / LEAD({coin_pair}_bids_cum_50000_weighted_avg, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) * 100 AS prev_{interval}_{coin_pair}_bids_cum_50000_weighted_avg_perc_chg
                                        ,(({coin_pair}_bids_cum_100000_weighted_avg - LEAD({coin_pair}_bids_cum_100000_weighted_avg, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                            / LEAD({coin_pair}_bids_cum_100000_weighted_avg, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) * 100 AS prev_{interval}_{coin_pair}_bids_cum_100000_weighted_avg_perc_chg
                                        ,(({coin_pair}_bids_cum_200000_weighted_avg - LEAD({coin_pair}_bids_cum_200000_weighted_avg, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                            / LEAD({coin_pair}_bids_cum_200000_weighted_avg, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) * 100 AS prev_{interval}_{coin_pair}_bids_cum_200000_weighted_avg_perc_chg
                                        ,(({coin_pair}_asks_cum_5000_weighted_avg - LEAD({coin_pair}_asks_cum_5000_weighted_avg, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                            / LEAD({coin_pair}_asks_cum_5000_weighted_avg, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) * 100 AS prev_{interval}_{coin_pair}_asks_cum_5000_weighted_avg_perc_chg
                                        ,(({coin_pair}_asks_cum_50000_weighted_avg - LEAD({coin_pair}_asks_cum_50000_weighted_avg, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                            / LEAD({coin_pair}_asks_cum_50000_weighted_avg, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) * 100 AS prev_{interval}_{coin_pair}_asks_cum_50000_weighted_avg_perc_chg
                                        ,(({coin_pair}_asks_cum_100000_weighted_avg - LEAD({coin_pair}_asks_cum_100000_weighted_avg, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                            / LEAD({coin_pair}_asks_cum_100000_weighted_avg, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) * 100 AS prev_{interval}_{coin_pair}_asks_cum_100000_weighted_avg_perc_chg
                                        ,(({coin_pair}_asks_cum_200000_weighted_avg - LEAD({coin_pair}_asks_cum_200000_weighted_avg, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                            / LEAD({coin_pair}_asks_cum_200000_weighted_avg, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) * 100 AS prev_{interval}_{coin_pair}_asks_cum_200000_weighted_avg_perc_chg
                                        , ((LEAD({coin_pair}_asks_cum_5000_weighted_avg, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC) - {coin_pair}_bids_cum_5000_weighted_avg) 
                                            / {coin_pair}_bids_cum_5000_weighted_avg) * 100 AS futr_{coin_pair}_{interval}_askbid_cum_5000_weighted_avg_perc_chg
                                        """)  
                lag_features_list.append(','.join(interval_list))  
                feature_col_list.extend([f'prev_{interval}_{coin_pair}_bid_ask_average_price_perc_chg'
                                        ,f'prev_{interval}_{coin_pair}_bid_ask_average_price_rate_chg'
                                        ,f'prev_{interval}_{coin_pair}_bids_cum_5000_weighted_avg_perc_chg'
                                        ,f'prev_{interval}_{coin_pair}_bids_cum_50000_weighted_avg_perc_chg'
                                        ,f'prev_{interval}_{coin_pair}_bids_cum_100000_weighted_avg_perc_chg'
                                        ,f'prev_{interval}_{coin_pair}_bids_cum_200000_weighted_avg_perc_chg'
                                        ,f'prev_{interval}_{coin_pair}_asks_cum_5000_weighted_avg_perc_chg'
                                        ,f'prev_{interval}_{coin_pair}_asks_cum_50000_weighted_avg_perc_chg'
                                        ,f'prev_{interval}_{coin_pair}_asks_cum_100000_weighted_avg_perc_chg'
                                        ,f'prev_{interval}_{coin_pair}_asks_cum_200000_weighted_avg_perc_chg'])
            # Target variables for every interval configured at runtime
            if pair_type == 'target':
                for target in self.trade_window_list:
                    target_variables_list.append(f"""((LAG({self.target_coin}_asks_cum_5000_weighted_avg, {target}) OVER (ORDER BY {self.target_coin}_trade_minute DESC) - {self.target_coin}_bids_cum_5000_weighted_avg) / {self.target_coin}_bids_cum_5000_weighted_avg) * 100 AS futr_{target}_askbid_cum_5000_weighted_avg_perc_chg""")
                    target_col_list.append(f'futr_{target}_askbid_cum_5000_weighted_avg_perc_chg')
                # Join conditions
                join_conditions_list.append(f"""{pair_type}_{coin_pair}""")      
            else:
                join_conditions_list.append(f"""{pair_type}_{coin_pair} ON target_{self.target_coin}.{self.target_coin}_trade_minute = {pair_type}_{coin_pair}.{coin_pair}_trade_minute""")

        raw_features = ','.join(raw_features_list)
        base_features = ','.join(base_features_list)
        interaction_features = ','.join(interaction_features_list)
        lag_features = ','.join(lag_features_list)
        target_variables = ','.join(target_variables_list)
        join_conditions = ' LEFT JOIN '.join(join_conditions_list)

        query_template = f"""WITH {raw_features}
                            SELECT {base_features}
                                ,{interaction_features}
                                ,{lag_features}
                                ,{target_variables}
                            FROM {join_conditions}
                            ORDER BY {self.target_coin}_trade_minute ASC"""

        return query_template, feature_col_list, target_col_list


class CobinhoodTraining(MarketMakerTraining):
    """
    Train a model with data from the Cobinhood exchange
    """

    def __init__(self, coin_pair_dict, feature_minutes_list, trade_window_list, training_period=None):
        super().__init__(coin_pair_dict, feature_minutes_list, trade_window_list, training_period=None)
        self.training_data_sql, self.feature_column_list, self.target_column_list = self.construct_training_data_query()


    def construct_training_data_query(self):
        """Return training data Athena query from dynamic template"""
        if self.feature_minutes_list == None or self.trade_window_list == None:
            raise Exception("To construct training data query, the optional feature_minutes_list and trade_window_list attributes must be set!")
        
        feature_col_list = []
        target_col_list = []
        raw_features_list = []
        base_features_list = []
        interaction_features_list = []
        excharb_features_list = []
        lag_features_list = []
        join_conditions_list = []
        target_variables_list = []

        for pair_type, coin_pair in self.coin_pair_dict.items():
            # Raw base features
            raw_features_list.append(f"""{pair_type}_{coin_pair} AS (
                                    SELECT coin_partition AS {coin_pair}{'_excharb' if 'excharb' in pair_type else ''}_coin_partition
                                        , from_unixtime(cast(open_timestamp AS BIGINT) / 1000) AS {coin_pair}{'_excharb' if 'excharb' in pair_type else ''}_trade_datetime
                                        , DATE(from_unixtime(cast(open_timestamp AS BIGINT) / 1000)) AS {coin_pair}{'_excharb' if 'excharb' in pair_type else ''}_trade_date
                                        , (CAST(open_timestamp AS BIGINT) / 1000 / 60) AS {coin_pair}{'_excharb' if 'excharb' in pair_type else ''}_trade_minute
                                        , CAST(open AS DOUBLE) AS {coin_pair}{'_excharb' if 'excharb' in pair_type else ''}_open
                                        , CAST(high AS DOUBLE) AS {coin_pair}{'_excharb' if 'excharb' in pair_type else ''}_high
                                        , CAST(low AS DOUBLE) AS {coin_pair}{'_excharb' if 'excharb' in pair_type else ''}_low
                                        , CAST(close AS DOUBLE) AS {coin_pair}{'_excharb' if 'excharb' in pair_type else ''}_close
                                        , CAST(volume AS DOUBLE) AS {coin_pair}{'_excharb' if 'excharb' in pair_type else ''}_volume
                                    FROM {'binance' if 'excharb' in pair_type else 'cobinhood'}.historic_candlesticks 
                                    WHERE coin_partition = '{coin_pair}'
                                        AND DATE(from_unixtime(cast(open_timestamp AS BIGINT) / 1000)) > DATE('2018-06-01')
                                    )""")
            # Base features
            if pair_type == 'target':
                base_features_list.append(f"""{coin_pair}_trade_datetime, {coin_pair}_trade_date, {coin_pair}_trade_minute
                                        , CAST(day_of_week({coin_pair}_trade_datetime) AS SMALLINT) as trade_day_of_week
                                        , CAST(hour({coin_pair}_trade_datetime) AS SMALLINT) as trade_hour""")
                feature_col_list.extend(['trade_day_of_week', 'trade_hour'])
            if 'excharb' not in pair_type:
                base_features_list.append(f"""{coin_pair}_close, {coin_pair}_volume""") #{coin_pair}_open, {coin_pair}_high, {coin_pair}_low, 
                feature_col_list.extend([f'{coin_pair}_close', f'{coin_pair}_volume']) #f'{coin_pair}_open', f'{coin_pair}_high', f'{coin_pair}_low', 
            # Interaction features for alt coins (base usdt)
            if pair_type == 'alt':
                interaction_features_list.append(f"""AVG(({self.target_coin}_close-{coin_pair}_close)/{self.target_coin}_close) OVER (PARTITION BY {self.target_coin}_coin_partition ORDER BY {self.target_coin}_trade_minute ASC ROWS 1 PRECEDING) 
                                                    - (({self.target_coin}_close-{coin_pair}_close)/{self.target_coin}_close) AS avg_1_{coin_pair}_close_interaction""")
                interaction_features_list.append(f"""AVG(({self.target_coin}_close-{coin_pair}_close)/{self.target_coin}_close) OVER (PARTITION BY {self.target_coin}_coin_partition ORDER BY {self.target_coin}_trade_minute ASC ROWS 5 PRECEDING) 
                                                    - (({self.target_coin}_close-{coin_pair}_close)/{self.target_coin}_close) AS avg_5_{coin_pair}_close_interaction""")
                interaction_features_list.append(f"""AVG(({self.target_coin}_close-{coin_pair}_close)/{self.target_coin}_close) OVER (PARTITION BY {self.target_coin}_coin_partition ORDER BY {self.target_coin}_trade_minute ASC ROWS 10 PRECEDING) 
                                                    - (({self.target_coin}_close-{coin_pair}_close)/{self.target_coin}_close) AS avg_10_{coin_pair}_close_interaction""")
                interaction_features_list.append(f"""AVG(({self.target_coin}_close-{coin_pair}_close)/{self.target_coin}_close) OVER (PARTITION BY {self.target_coin}_coin_partition ORDER BY {self.target_coin}_trade_minute ASC ROWS 20 PRECEDING) 
                                                    - (({self.target_coin}_close-{coin_pair}_close)/{self.target_coin}_close) AS avg_20_{coin_pair}_close_interaction""")
                feature_col_list.extend([f'avg_1_{coin_pair}_close_interaction',f'avg_5_{coin_pair}_close_interaction',f'avg_10_{coin_pair}_close_interaction',f'avg_20_{coin_pair}_close_interaction'])
            elif 'excharb' in pair_type:
                excharb_features_list.append(f"""AVG(({self.target_coin}_close-{coin_pair}_excharb_close)/{self.target_coin}_close) OVER (PARTITION BY {self.target_coin}_coin_partition ORDER BY {self.target_coin}_trade_minute ASC ROWS 1 PRECEDING) 
                                                    - (({self.target_coin}_close-{coin_pair}_excharb_close)/{self.target_coin}_close) AS avg_1_{coin_pair}_excharb_close_interaction""")
                excharb_features_list.append(f"""AVG(({self.target_coin}_close-{coin_pair}_excharb_close)/{self.target_coin}_close) OVER (PARTITION BY {self.target_coin}_coin_partition ORDER BY {self.target_coin}_trade_minute ASC ROWS 2 PRECEDING) 
                                                    - (({self.target_coin}_close-{coin_pair}_excharb_close)/{self.target_coin}_close) AS avg_2_{coin_pair}_excharb_close_interaction""")
                excharb_features_list.append(f"""AVG(({self.target_coin}_close-{coin_pair}_excharb_close)/{self.target_coin}_close) OVER (PARTITION BY {self.target_coin}_coin_partition ORDER BY {self.target_coin}_trade_minute ASC ROWS 3 PRECEDING) 
                                                    - (({self.target_coin}_close-{coin_pair}_excharb_close)/{self.target_coin}_close) AS avg_3_{coin_pair}_excharb_close_interaction""")

                excharb_features_list.append(f"""AVG(({self.target_coin}_close-{coin_pair}_excharb_close)/{self.target_coin}_close) OVER (PARTITION BY {self.target_coin}_coin_partition ORDER BY {self.target_coin}_trade_minute ASC ROWS 10 PRECEDING) 
                                                    - (({self.target_coin}_close-{coin_pair}_excharb_close)/{self.target_coin}_close) AS avg_10_{coin_pair}_excharb_close_interaction""")
                excharb_features_list.append(f"""AVG(({self.target_coin}_close-{coin_pair}_excharb_close)/{self.target_coin}_close) OVER (PARTITION BY {self.target_coin}_coin_partition ORDER BY {self.target_coin}_trade_minute ASC ROWS 20 PRECEDING) 
                                                    - (({self.target_coin}_close-{coin_pair}_excharb_close)/{self.target_coin}_close) AS avg_20_{coin_pair}_excharb_close_interaction""")

                feature_col_list.extend([f'avg_1_{coin_pair}_excharb_close_interaction',f'avg_2_{coin_pair}_excharb_close_interaction',f'avg_3_{coin_pair}_excharb_close_interaction'
                                        ,f'avg_10_{coin_pair}_excharb_close_interaction',f'avg_20_{coin_pair}_excharb_close_interaction'])
            # Lag features for every interval configured at runtime
            if 'excharb' not in pair_type:
                for interval in self.feature_minutes_list:
                    interval_list = []
                    interval_list.append(f"""(({coin_pair}_close - LEAD({coin_pair}_close, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                                / LEAD({coin_pair}_close, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) * 100 AS prev_{interval}_{coin_pair}_close_perc_chg
                                            ,((({coin_pair}_close - LEAD({coin_pair}_close, 1) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                                / LEAD({coin_pair}_close, 1) OVER (ORDER BY {self.target_coin}_trade_minute DESC))
                                            - (({coin_pair}_close - LEAD({coin_pair}_close, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                                / LEAD({coin_pair}_close, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC))) * 100 AS prev_{interval}_{coin_pair}_close_rate_chg
                                            ,(({coin_pair}_high - LEAD({coin_pair}_high, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                                / LEAD({coin_pair}_high, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) * 100 AS prev_{interval}_{coin_pair}_high_perc_chg
                                            ,(({coin_pair}_low - LEAD({coin_pair}_low, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                                / LEAD({coin_pair}_low, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) * 100 AS prev_{interval}_{coin_pair}_low_perc_chg
                                            ,COALESCE(TRY((({coin_pair}_volume - LEAD({coin_pair}_volume, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                                / LEAD({coin_pair}_volume, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) * 100),0) AS prev_{interval}_{coin_pair}_volume_perc_chg
                                            ,(({coin_pair}_close - LEAD({coin_pair}_close, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                                / LEAD({coin_pair}_close, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) * 100 * {coin_pair}_volume AS prev_{interval}_{coin_pair}_volume_direction""")  
                    lag_features_list.append(','.join(interval_list))  
                    feature_col_list.extend([f'prev_{interval}_{coin_pair}_close_perc_chg',f'prev_{interval}_{coin_pair}_close_rate_chg'#,f'prev_{interval}_{coin_pair}_high_perc_chg',
                                            #f'prev_{interval}_{coin_pair}_low_perc_chg'
                                            ,f'prev_{interval}_{coin_pair}_volume_perc_chg',f'prev_{interval}_{coin_pair}_volume_direction'])
            # Target variables for every interval configured at runtime
            if pair_type == 'target':
                for target in self.trade_window_list:
                    target_variables_list.append(f"""((LAG({self.target_coin}_close, {target}) OVER (ORDER BY {self.target_coin}_trade_minute DESC) - {self.target_coin}_close) / {self.target_coin}_close) * 100 AS futr_{target}_close_perc_chg""")
                    target_col_list.append(f'futr_{target}_close_perc_chg')
                # Join conditions
                join_conditions_list.append(f"""{pair_type}_{coin_pair}""")      
            else:
                join_conditions_list.append(f"""{pair_type}_{coin_pair} ON target_{self.target_coin}.{self.target_coin}_trade_minute = {pair_type}_{coin_pair}.{coin_pair}{'_excharb' if 'excharb' in pair_type else ''}_trade_minute""")

        raw_features = ','.join(raw_features_list)
        base_features = ','.join(base_features_list)
        interaction_features = ','.join(interaction_features_list)
        excharb_features = ','.join(excharb_features_list)
        lag_features = ','.join(lag_features_list)
        target_variables = ','.join(target_variables_list)
        join_conditions = ' LEFT JOIN '.join(join_conditions_list)

        query_template = f"""WITH {raw_features}
                            SELECT {base_features}
                                ,{interaction_features}
                                {','+excharb_features if excharb_features else ''}
                                ,{target_variables}
                                ,{lag_features}
                                ,{target_variables}
                            FROM {join_conditions}
                            ORDER BY {self.target_coin}_trade_minute ASC"""

        return query_template, feature_col_list, target_col_list
import boto3
import pandas as pd
pd.options.mode.chained_assignment = None
import numpy as np
import sys
import os
import io
import ast
from functools import reduce
import time
import datetime
from datetime import timedelta
import multiprocessing
from joblib import Parallel, delayed
from binance.client import Client
# local libraries
sys.path.append(os.path.abspath(os.path.join(sys.path[0], '..', 'lib')))
import athena_connect

class MarketMaker():
    def __init__(self, coin_pair_dict, feature_minutes_list=None, trade_window_list=None, training_period=None):
        """Market maker class to fascilitate data engineering for both training and scoring models
        """
        self.bnb_client = self.binance_client()
        self.coin_pair_dict = coin_pair_dict
        self.target_coin = self.get_target_coin()
        self.feature_minutes_list = feature_minutes_list
        self.trade_window_list = trade_window_list
        self.training_period = training_period


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

    def pandas_get_candlesticks(self, coin_pair, pair_type, start_time_str='1 min ago UTC'):
        """Return minute granularity recent candlestick metrics from Binance api for a given coin pair and historical interval

        Returns:
            (list): ['coin_pair',['pair_type', pandas.DataFrame]]
        """
        prefix = f"{coin_pair.lower()}"
        coin_pair_upper = coin_pair.upper()
        klines = self.bnb_client.get_historical_klines(coin_pair_upper, Client.KLINE_INTERVAL_1MINUTE, start_time_str)
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

    def set_scoring_data(self):
        """Set data for model scoring from latest candlestick metrics features"""
        if self.feature_minutes_list == None or self.trade_window_list == None:
            raise Exception("To construct scoring dataframe, the optional feature_minutes_list and trade_window_list attributes must be set!")
        # Get recent trade data for both target coin pair and through coin pair
        recent_trades_interval = max(self.feature_minutes_list) + 10
        cores = multiprocessing.cpu_count()
        try:
            scoring_df_list = Parallel(n_jobs=cores)(delayed(self.pandas_get_candlesticks)(coin_pair, pair_type, f'{recent_trades_interval} min ago UTC') for coin_pair, pair_type in self.coin_pair_dict.items())
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
                features_df['current_6_interaction'] = (features_df[f'{self.target_coin}_close'].shift(6)-features_df[f'{coin_pair}_close'].shift(6))/features_df[f'{self.target_coin}_close'].shift(6)
                features_df['current_7_interaction'] = (features_df[f'{self.target_coin}_close'].shift(7)-features_df[f'{coin_pair}_close'].shift(7))/features_df[f'{self.target_coin}_close'].shift(7)
                features_df['current_8_interaction'] = (features_df[f'{self.target_coin}_close'].shift(8)-features_df[f'{coin_pair}_close'].shift(8))/features_df[f'{self.target_coin}_close'].shift(8)
                features_df['current_9_interaction'] = (features_df[f'{self.target_coin}_close'].shift(9)-features_df[f'{coin_pair}_close'].shift(9))/features_df[f'{self.target_coin}_close'].shift(9)
                features_df['interaction_average'] = (features_df['current_interaction'] + features_df['current_1_interaction'] + features_df['current_1_interaction'] + 
                                    features_df['current_1_interaction'] + features_df['current_1_interaction'] + features_df['current_1_interaction'] + 
                                    features_df['current_1_interaction'] + features_df['current_1_interaction'] + features_df['current_1_interaction'] + 
                                    features_df['current_1_interaction']) / 10
                features_df['avg_10_{coin_pair}_open_interaction'] = features_df['interaction_average'] - features_df['current_interaction']
        
        return features_df

    def set_training_data(self):
        """Pull training data from Athena and engineer features"""
        # Optional training data period
        if not self.training_period == None:
            training_period_date = (datetime.datetime.utcnow() - timedelta(days=self.training_period)).strftime("%Y-%m-%d")
        # Extract queried data from Athena
        training_data_sql, self.feature_column_list, self.target_column_list = self.construct_training_data_query()
        athena = athena_connect.Athena()
        features_df = athena.pandas_read_athena(training_data_sql)
        features_df.fillna(0, inplace=True)
        print(features_df.shape)
        features_df = features_df[:-10] # Remove ??
        # Remove infinity string
        features_df.replace({'Infinity': 0}, inplace=True)
        # Convert all object fields to numeric
        object_cols = features_df.columns[features_df.dtypes.eq('object')]
        features_df[object_cols] = features_df[object_cols].apply(pd.to_numeric, errors='coerce')
        self.training_df = features_df

    def construct_training_data_query(self):
        """Return training data Athena query from dynamic template"""
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

        for coin_pair, pair_type in self.coin_pair_dict.items():
            # Raw base features
            raw_features_list.append(f"""{pair_type}_{coin_pair} AS (
                                    SELECT coin_partition AS {coin_pair}_coin_partition
                                        , from_unixtime(cast(close_timestamp AS BIGINT) / 1000) AS {coin_pair}_trade_datetime
                                        , DATE(from_unixtime(cast(close_timestamp AS BIGINT) / 1000)) AS {coin_pair}_trade_date
                                        , (CAST(close_timestamp AS BIGINT) / 1000 / 60) AS {coin_pair}_trade_minute
                                        , CAST(open AS DOUBLE) AS {coin_pair}_open, CAST(high AS DOUBLE) AS {coin_pair}_high, CAST(low AS DOUBLE) AS {coin_pair}_low
                                        , CAST(close AS DOUBLE) AS {coin_pair}_close, CAST(volume AS DOUBLE) AS {coin_pair}_volume
                                        , CAST(quote_asset_volume AS DOUBLE) AS {coin_pair}_quote_asset_volume, CAST(trade_count AS BIGINT) AS {coin_pair}_trade_count
                                        , CAST(taker_buy_base_asset_volume AS DOUBLE) AS {coin_pair}_tbbav, CAST(taker_buy_quote_asset_volume AS DOUBLE) AS {coin_pair}_tbqav
                                    FROM binance.historic_candlesticks 
                                    WHERE coin_partition = '{coin_pair}'
                                    )""")
            # Base features
            if pair_type == 'target':
                base_features_list.append(f"""{coin_pair}_trade_datetime, CAST(day_of_week({coin_pair}_trade_datetime) AS SMALLINT) as trade_day_of_week
                                        , CAST(hour({coin_pair}_trade_datetime) AS SMALLINT) as trade_hour""")
                feature_col_list.extend(['trade_day_of_week', 'trade_hour'])
            base_features_list.append(f"""{coin_pair}_open, {coin_pair}_high, {coin_pair}_low, {coin_pair}_close, {coin_pair}_volume
                                        , {coin_pair}_quote_asset_volume, {coin_pair}_trade_count, {coin_pair}_tbbav, {coin_pair}_tbqav""")
            feature_col_list.extend([f'{coin_pair}_open', f'{coin_pair}_high', f'{coin_pair}_low', f'{coin_pair}_close', f'{coin_pair}_volume'
                                        , f'{coin_pair}_quote_asset_volume', f'{coin_pair}_trade_count', f'{coin_pair}_tbbav', f'{coin_pair}_tbqav'])
            # Interaction features for alt coins (base usdt)
            if pair_type == 'alt':
                interaction_features_list.append(f"""AVG(({self.target_coin}_open-{coin_pair}_open)/{self.target_coin}_open) OVER (PARTITION BY {self.target_coin}_coin_partition ORDER BY {self.target_coin}_trade_minute DESC ROWS 10 PRECEDING) 
                                                    - (({self.target_coin}_open-{coin_pair}_open)/{self.target_coin}_open) AS avg_10_{coin_pair}_open_interaction""")
                feature_col_list.append(f'avg_10_{coin_pair}_open_interaction')
            # Lag features for every interval configured at runtime
            for interval in self.feature_minutes_list:
                interval_list = []
                interval_list.append(f"""(({coin_pair}_open - LEAD({coin_pair}_open, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                            / LEAD({coin_pair}_open, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) * 100 AS prev_{interval}_{coin_pair}_open_perc_chg
                                        ,((({coin_pair}_open - LEAD({coin_pair}_open) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                            / LEAD({coin_pair}_open) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) * 100) -
                                            ((({coin_pair}_open - LEAD({coin_pair}_open, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                            / LEAD({coin_pair}_open, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) * 100) AS prev_{interval}_{coin_pair}_open_rate_chg
                                        ,(({coin_pair}_high - LEAD({coin_pair}_high, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                            / LEAD({coin_pair}_high, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) * 100 AS prev_{interval}_{coin_pair}_high_perc_chg
                                        ,(({coin_pair}_low - LEAD({coin_pair}_low, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                            / LEAD({coin_pair}_low, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) * 100 AS prev_{interval}_{coin_pair}_low_perc_chg
                                        ,COALESCE(TRY((({coin_pair}_volume - LEAD({coin_pair}_volume, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                            / LEAD({coin_pair}_volume, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) * 100),0) AS prev_{interval}_{coin_pair}_volume_perc_chg
                                        ,COALESCE(TRY((({coin_pair}_quote_asset_volume - LEAD({coin_pair}_quote_asset_volume, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                            / LEAD({coin_pair}_quote_asset_volume, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) * 100),0) AS prev_{interval}_{coin_pair}_qav_perc_chg
                                        ,COALESCE(TRY((({coin_pair}_trade_count - LEAD({coin_pair}_trade_count, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                            / LEAD({coin_pair}_trade_count, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) * 100),0) AS prev_{interval}_{coin_pair}_trade_count_perc_chg
                                        ,COALESCE(TRY((({coin_pair}_tbbav - LEAD({coin_pair}_tbbav, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                            / LEAD({coin_pair}_tbbav, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) * 100),0) AS prev_{interval}_{coin_pair}_tbbav_perc_chg
                                        ,COALESCE(TRY((({coin_pair}_tbqav - LEAD({coin_pair}_tbqav, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) 
                                            / LEAD({coin_pair}_tbqav, {interval}) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) * 100),0) AS prev_{interval}_{coin_pair}_tbqav_perc_chg""")  
                lag_features_list.append(','.join(interval_list))  
                feature_col_list.extend([f'prev_{interval}_{coin_pair}_open_perc_chg',f'prev_{interval}_{coin_pair}_open_rate_chg',f'prev_{interval}_{coin_pair}_high_perc_chg',
                                        f'prev_{interval}_{coin_pair}_low_perc_chg',f'prev_{interval}_{coin_pair}_volume_perc_chg',f'prev_{interval}_{coin_pair}_qav_perc_chg',
                                        f'prev_{interval}_{coin_pair}_trade_count_perc_chg',f'prev_{interval}_{coin_pair}_tbbav_perc_chg',f'prev_{interval}_{coin_pair}_tbqav_perc_chg'])
            # Target variables for every interval configured at runtime
            if pair_type == 'target':
                for target in self.trade_window_list:
                    target_variables_list.append(f"""((LAG({self.target_coin}_open, {target}) OVER (ORDER BY {self.target_coin}_trade_minute DESC) - {self.target_coin}_open) / {self.target_coin}_open) * 100 AS futr_{target}_open_perc_chg""")
                    target_col_list.append(f'futr_{target}_open_perc_chg')
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
                                ,{target_variables}
                                ,{lag_features}
                                ,{target_variables}
                            FROM {join_conditions}
                            ORDER BY {self.target_coin}_trade_minute ASC"""

        return query_template, feature_col_list, target_col_list

    def get_target_coin(self):
        """Return target coin pair that will be traded"""
        target_coin_list = [cp for cp, ct in self.coin_pair_dict.items() if ct == 'target']
        if len(target_coin_list) > 1:
            raise Exception(f"There must only be a single target coin initialized in the coin pair dictionary. Values: {target_coin_list}")
        return target_coin_list[0]

    def binance_client(self):
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
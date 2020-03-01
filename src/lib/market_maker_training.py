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
            training_period_date = (datetime.datetime.utcnow() - timedelta(minutes=self.training_period)).strftime("%Y-%m-%d")
            print(f"Training data start date: {training_period_date}")
        # Extract queried data from Athena
        #athena = athena_connect.Athena()
        #features_df = athena.pandas_read_athena(self.training_data_sql)
        with open('feature_sql.txt', 'w') as f:
            print(self.training_data_sql, file=f) 
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
        """Persist model object as pkl to S3
        pickle.dump(model, open('xgb_model.pkl','wb'))
        """
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

    def __init__(self, coin_pair_dict, feature_minutes_list, trade_window_list, training_period=None, operation='training'):
        super().__init__(coin_pair_dict, feature_minutes_list, trade_window_list, training_period=None)
        self.training_data_sql, self.feature_column_list, self.target_column_list = self.construct_training_data_query(operation)
        

    def construct_training_data_query(self, operation='training'):
        """Return training data query from dynamic template"""
        # FUTURE: make dollar return target/features dynamic
        if self.feature_minutes_list == None or self.trade_window_list == None:
            raise Exception("To construct training data query, the optional feature_minutes_list and trade_window_list attributes must be set!")
        
        feature_col_list = []
        target_col_list = []
        base_ctes_list = []
        feature_cte_list = []
        final_col_list = []
        interaction_features_list = []
        join_conditions_list = []

        # Limit rows returned when pulling scoring features
        limit_where_clause = ''
        limit_clause = ''
        if operation == 'scoring':
            limit_minutes = max(self.feature_minutes_list) + 10
            limit_clause = f'LIMIT {limit_minutes}'
            # trying to move away from the where clause - limits are faster
            limit_trade_minute = (time.time() / 60) - limit_minutes - (5*60) 
            limit_where_clause = f'AND trade_minute > {limit_trade_minute}'
        elif self.training_period is not None:
            limit_minutes = self.training_period + max(self.feature_minutes_list)
            limit_clause = f'LIMIT {limit_minutes}'
            print(f"Training data query being limited to the first {limit_minutes} minutes. Training period plus {max(self.feature_minutes_list)} (max feature interval)")
            # trying to move away from the where clause - limits are faster
            limit_trade_minute = (time.time() / 60) - self.training_period - (5*60)
            limit_where_clause = f'AND trade_minute > {limit_trade_minute}'


        for pair_type, coin_pair in self.coin_pair_dict.items():
            """
            pair_type: 'alt', 'target'
            """
            base_features_list = []
            base_ctes_list.append(f"""
                {pair_type}_{coin_pair}_end_orderbook AS (
                SELECT trade_minute - 1 AS lag_trade_minute, * 
                FROM binance.orderbook
                WHERE coin_pair  = '{coin_pair}'
                ORDER BY trade_minute DESC 
                {limit_clause}
            ),
            {pair_type}_{coin_pair}_beg_orderbook AS (
                SELECT * 
                FROM binance.orderbook
                WHERE coin_pair  = '{coin_pair}'
                ORDER BY trade_minute DESC 
                {limit_clause}
            ),
            {pair_type}_{coin_pair}_candlesticks AS (
                SELECT *
                FROM binance.candledicks c
                WHERE coin_pair  = '{coin_pair}'
                ORDER BY trade_minute DESC 
                {limit_clause}
            )""")
            # Base target variable features
            if pair_type == 'target':
                base_features_list.append(f"""
                    c.close_datetime AS {coin_pair}_trade_close_datetime
                    , extract(isodow from c.close_datetime) as trade_day_of_week
                    , date_part('hour', c.close_datetime) as trade_hour
                    , c.close_datetime::date - current_date as days_old
                """)
                final_col_list.append(f"""
                    {coin_pair}_trade_close_datetime
                    , trade_day_of_week
                    , trade_hour
                    , days_old
                """)
                feature_col_list.extend(['trade_day_of_week', 'trade_hour', 'days_old'])
            # Base features
            base_features_list.append(f"""
                c.trade_minute AS {coin_pair}_trade_minute
                , quote_asset_volume as {coin_pair}_quote_asset_volume
                , taker_sell_volume_percentage * 100 AS {coin_pair}_taker_sell_volume_perc_of_total
                , trade_count as {coin_pair}_trade_count
                , o_end.bids_cum_50000_weighted_avg - o_beg.bids_cum_50000_weighted_avg AS {coin_pair}_crnt_interval_bids_50000_price_diff
                , o_end.bids_cum_50000_weighted_avg - o_end.asks_cum_50000_weighted_avg AS {coin_pair}_crnt_interval_bids_v_asks_50000_price_diff 
                , o_end.bids_cum_50000_weighted_std - o_beg.bids_cum_50000_weighted_std AS {coin_pair}_crnt_interval_bids_50000_std_diff
                , o_end.bids_cum_50000_weighted_std - o_end.asks_cum_50000_weighted_std AS {coin_pair}_crnt_interval_bids_v_asks_50000_std_diff
                , o_end.bids_cum_50000_weighted_std / (o_end.bids_cum_50000_weighted_std + o_end.asks_cum_50000_weighted_std) AS {coin_pair}_crnt_bids_50000_std_perc_of_total
                , o_end.bids_cum_200000_weighted_std / (o_end.bids_cum_200000_weighted_std + o_end.asks_cum_200000_weighted_std) AS {coin_pair}_crnt_bids_200000_std_perc_of_total
                , (o_end.bids_cum_200000_weighted_std / (o_end.bids_cum_200000_weighted_std + o_end.asks_cum_200000_weighted_std) 
                    + LEAD(o_end.bids_cum_200000_weighted_std, 1) OVER (ORDER BY c.trade_minute DESC) / (LEAD(o_end.bids_cum_200000_weighted_std, 1) OVER (ORDER BY c.trade_minute DESC) + LEAD(o_end.asks_cum_200000_weighted_std, 1) OVER (ORDER BY c.trade_minute DESC)) 
                    + LEAD(o_end.bids_cum_200000_weighted_std, 2) OVER (ORDER BY c.trade_minute DESC) / (LEAD(o_end.bids_cum_200000_weighted_std, 2) OVER (ORDER BY c.trade_minute DESC) + LEAD(o_end.asks_cum_200000_weighted_std, 2) OVER (ORDER BY c.trade_minute DESC))
                    + LEAD(o_end.bids_cum_200000_weighted_std, 3) OVER (ORDER BY c.trade_minute DESC) / (LEAD(o_end.bids_cum_200000_weighted_std, 3) OVER (ORDER BY c.trade_minute DESC) + LEAD(o_end.asks_cum_200000_weighted_std, 3) OVER (ORDER BY c.trade_minute DESC))
                    + LEAD(o_end.bids_cum_200000_weighted_std, 4) OVER (ORDER BY c.trade_minute DESC) / (LEAD(o_end.bids_cum_200000_weighted_std, 4) OVER (ORDER BY c.trade_minute DESC) + LEAD(o_end.asks_cum_200000_weighted_std, 4) OVER (ORDER BY c.trade_minute DESC))
                    ) / 5 AS {coin_pair}_bids_200000_std_perc_of_total_avg
            """)
            final_col_list.append(f"""
                {coin_pair}_trade_minute
                , {coin_pair}_quote_asset_volume
                , {coin_pair}_taker_sell_volume_perc_of_total
                , {coin_pair}_trade_count
                , {coin_pair}_crnt_interval_bids_50000_price_diff
                , {coin_pair}_crnt_interval_bids_v_asks_50000_price_diff
                , {coin_pair}_crnt_interval_bids_50000_std_diff
                , {coin_pair}_crnt_interval_bids_v_asks_50000_std_diff
                , {coin_pair}_crnt_bids_50000_std_perc_of_total
                , {coin_pair}_crnt_bids_200000_std_perc_of_total
                , {coin_pair}_bids_200000_std_perc_of_total_avg
            """)
            feature_col_list.extend([
                f'{coin_pair}_quote_asset_volume'
                , f'{coin_pair}_taker_sell_volume_perc_of_total'
                , f'{coin_pair}_trade_count'
                , f'{coin_pair}_crnt_interval_bids_50000_price_diff'
                , f'{coin_pair}_crnt_interval_bids_v_asks_50000_price_diff'
                , f'{coin_pair}_crnt_interval_bids_50000_std_diff'
                , f'{coin_pair}_crnt_interval_bids_v_asks_50000_std_diff'
                , f'{coin_pair}_crnt_bids_50000_std_perc_of_total'
                , f'{coin_pair}_crnt_bids_200000_std_perc_of_total'
                , f'{coin_pair}_bids_200000_std_perc_of_total_avg'
            ])
            
            # Lag features for every interval configured at runtime
            for interval in self.feature_minutes_list:
                interval_list = []
                base_features_list.append(f"""
                    ((quote_asset_volume - LEAD(quote_asset_volume, {interval}) OVER (ORDER BY c.trade_minute DESC)) 
                        / LEAD(quote_asset_volume, {interval}) OVER (ORDER BY c.trade_minute DESC)) * 100 AS prev_{interval}_{coin_pair}_quote_asset_volume_perc_chg
                    , ((taker_sell_volume_percentage - LEAD(taker_sell_volume_percentage, {interval}) OVER (ORDER BY c.trade_minute DESC)) 
                        / LEAD(taker_sell_volume_percentage, {interval}) OVER (ORDER BY c.trade_minute DESC)) * 100 AS prev_{interval}_{coin_pair}_taker_sell_volume_perc_of_total_chg
                    , ((trade_count::float - LEAD(trade_count::float, {interval}) OVER (ORDER BY c.trade_minute DESC)) 
                        / LEAD(trade_count::float, {interval}) OVER (ORDER BY c.trade_minute DESC)) * 100 AS prev_{interval}_{coin_pair}_trade_count_perc_chg
                    , ((o_end.bids_cum_50000_weighted_avg - LEAD(o_end.bids_cum_50000_weighted_avg, {interval}) OVER (ORDER BY c.trade_minute DESC)) 
                        / LEAD(o_end.bids_cum_50000_weighted_avg, {interval}) OVER (ORDER BY c.trade_minute DESC)) * 100 AS prev_{interval}_{coin_pair}_bids_50000_perc_chg
                    , ((o_end.bids_cum_50000_weighted_std - LEAD(o_end.bids_cum_50000_weighted_std, {interval}) OVER (ORDER BY c.trade_minute DESC)) 
                        / LEAD(o_end.bids_cum_50000_weighted_std, {interval}) OVER (ORDER BY c.trade_minute DESC)) * 100 AS prev_{interval}_{coin_pair}_bids_50000_std_chg
                """)
                final_col_list.append(f"""
                    prev_{interval}_{coin_pair}_quote_asset_volume_perc_chg
                    , prev_{interval}_{coin_pair}_taker_sell_volume_perc_of_total_chg
                    , prev_{interval}_{coin_pair}_trade_count_perc_chg
                    , prev_{interval}_{coin_pair}_bids_50000_perc_chg
                    , prev_{interval}_{coin_pair}_bids_50000_std_chg
                """)  
                feature_col_list.extend([
                    f'prev_{interval}_{coin_pair}_quote_asset_volume_perc_chg'
                    ,f'prev_{interval}_{coin_pair}_taker_sell_volume_perc_of_total_chg'
                    ,f'prev_{interval}_{coin_pair}_trade_count_perc_chg'
                    ,f'prev_{interval}_{coin_pair}_bids_50000_perc_chg'
                    ,f'prev_{interval}_{coin_pair}_bids_50000_std_chg'
                ])
            
            if pair_type == 'target':
                for target in self.trade_window_list:
                    base_features_list.append(f"""((LAG({self.target_coin}_bids_cum_5000_weighted_avg, {target}) OVER (ORDER BY {self.target_coin}_trade_minute DESC) - {self.target_coin}_asks_cum_5000_weighted_avg) / {self.target_coin}_asks_cum_5000_weighted_avg * 100) AS futr_{target}_askbid_cum_5000_weighted_avg_perc_chg""")
                    # experiment with predicting return starting at minute 1 instead of minute 0 to account for our scoring->trade delay.
                    #base_features_list.append(f"""((LAG({self.target_coin}_bids_cum_5000_weighted_avg, {target}) OVER (ORDER BY {self.target_coin}_trade_minute DESC) - LAG({self.target_coin}_asks_cum_5000_weighted_avg, 1) OVER (ORDER BY {self.target_coin}_trade_minute DESC)) / LAG({self.target_coin}_asks_cum_5000_weighted_avg, 1) OVER (ORDER BY {self.target_coin}_trade_minute DESC) * 100) AS futr_{target}_askbid_cum_5000_weighted_avg_perc_chg""")
                    final_col_list.append(f'futr_{target}_askbid_cum_5000_weighted_avg_perc_chg')  
                    target_col_list.append(f'futr_{target}_askbid_cum_5000_weighted_avg_perc_chg')

            # Coin level CTE 
            feature_cte_list.append(f"""
                {pair_type}_{coin_pair}_features AS (
                    SELECT {','.join(base_features_list)}
                    FROM {pair_type}_{coin_pair}_candlesticks c 
                    INNER JOIN {pair_type}_{coin_pair}_beg_orderbook o_beg ON o_beg.coin_pair = c.coin_pair  AND o_beg.trade_minute = c.trade_minute 
                    INNER JOIN {pair_type}_{coin_pair}_end_orderbook o_end ON o_end.coin_pair = c.coin_pair  AND o_end.lag_trade_minute = c.trade_minute
            )""")

            # Interaction features for alt coins (base usdt)
            interaction_features = ''
            if pair_type == 'alt':
                interaction_features_list.append(f"""AVG(({self.target_coin}_bid_ask_average_price-{coin_pair}_bid_ask_average_price)/{self.target_coin}_bid_ask_average_price) OVER (PARTITION BY {self.target_coin}_coin_partition ORDER BY {self.target_coin}_trade_minute ASC ROWS 5 PRECEDING) 
                                                    - (({self.target_coin}_bid_ask_average_price-{coin_pair}_bid_ask_average_price)/{self.target_coin}_bid_ask_average_price) AS avg_5_{coin_pair}_bid_ask_average_price_interaction""")
                interaction_features_list.append(f"""AVG(({self.target_coin}_bid_ask_average_price-{coin_pair}_bid_ask_average_price)/{self.target_coin}_bid_ask_average_price) OVER (PARTITION BY {self.target_coin}_coin_partition ORDER BY {self.target_coin}_trade_minute ASC ROWS 10 PRECEDING) 
                                                    - (({self.target_coin}_bid_ask_average_price-{coin_pair}_bid_ask_average_price)/{self.target_coin}_bid_ask_average_price) AS avg_10_{coin_pair}_bid_ask_average_price_interaction""")
                interaction_features_list.append(f"""AVG(({self.target_coin}_bid_ask_average_price-{coin_pair}_bid_ask_average_price)/{self.target_coin}_bid_ask_average_price) OVER (PARTITION BY {self.target_coin}_coin_partition ORDER BY {self.target_coin}_trade_minute ASC ROWS 20 PRECEDING) 
                                                    - (({self.target_coin}_bid_ask_average_price-{coin_pair}_bid_ask_average_price)/{self.target_coin}_bid_ask_average_price) AS avg_20_{coin_pair}_bid_ask_average_price_interaction""")
                feature_col_list.extend([f'avg_5_{coin_pair}_bid_ask_average_price_interaction',f'avg_10_{coin_pair}_bid_ask_average_price_interaction',f'avg_20_{coin_pair}_bid_ask_average_price_interaction'])
                interaction_features = ','.join(interaction_features_list)
                interaction_features = ',' + interaction_features

            # Join conditions
            if pair_type == 'target':
                join_conditions_list.append(f"""{pair_type}_{coin_pair}_features""")    
            else:
                join_conditions_list.append(f"""{pair_type}_{coin_pair}_features ON target_{self.target_coin}_features.{self.target_coin}_trade_minute = {pair_type}_{coin_pair}_features.{coin_pair}_trade_minute""")

        base_ctes = ','.join(base_ctes_list)
        feature_ctes = ','.join(feature_cte_list)
        feature_ctes = ',' + feature_ctes
        final_cols = ','.join(final_col_list)
        join_conditions = ' LEFT JOIN '.join(join_conditions_list)

        query_template = f"""WITH {base_ctes}
                                  {feature_ctes}
                            SELECT {final_cols}
                                {interaction_features}
                            FROM {join_conditions}
                            ORDER BY {self.target_coin}_trade_minute {'DESC' if operation == 'scoring' else 'ASC'}
                            {'LIMIT 1' if operation == 'scoring' else ''}""" # LIMIT SCORING DATA - NOT ALL DATA IS RELEVANT TO CURRENT

        return query_template, feature_col_list, target_col_list
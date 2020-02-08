import numpy as np
import pandas as pd
import math
import ast
import re
import boto3
import json
import time
import psycopg2 
import s3fs

# Configypt
s3_bucket = 'loidsig-crypto'
s3_prefixes =  ['binance/historic_candlesticks']

# AWS resources
boto_session = boto3.Session()
s3_resource = boto_session.resource('s3')
s3_client = boto_session.client('s3')
loidsig_fs = s3fs.S3FileSystem()

def main():
    processed_messages = 0
    start = time.time()
    for s3_prefix in s3_prefixes:
        s3_key_list = get_s3_keys_from_prefix(s3_prefix)
        
        print(f"{len(s3_key_list)} keys to replay")
        # Continue while list is not empty
        while s3_key_list:
            s3_key = s3_key_list[0]
            # get from s3 into df
            #s3://loidsig-crypto/binance/historic_candledicks/bnbusdt/2018-01-01.csv
            f = loidsig_fs.open(f's3://{s3_bucket}/{s3_key}', "r")
            candledick_df = pd.read_csv(f)
            # transforms
            candledick_df['coin_pair'] = candledick_df['coin'].str.lower()
            candledick_df['open_timestamp'] = candledick_df['open_timestamp'] / 1000
            candledick_df['close_timestamp'] = candledick_df['close_timestamp'] / 1000
            candledick_df['trade_minute'] = candledick_df['open_timestamp'] / 60
            candledick_df['open_timestamp'] = candledick_df['open_timestamp'].astype(int)
            candledick_df['close_timestamp'] = candledick_df['close_timestamp'].astype(int)
            candledick_df['trade_minute'] = candledick_df['trade_minute'].astype(int)
            candledick_df['taker_sell_base_asset_volume'] = candledick_df['volume'] - candledick_df['taker_buy_base_asset_volume']
            candledick_df['taker_sell_quote_asset_volume'] = candledick_df['quote_asset_volume'] - candledick_df['taker_buy_quote_asset_volume']
            candledick_df['taker_sell_volume_percentage'] = candledick_df['taker_sell_base_asset_volume'] / candledick_df['volume']
            candledick_df['taker_buy_volume_percentage'] = candledick_df['taker_buy_base_asset_volume'] / candledick_df['volume']

            # insert into db
            df_to_rds(candledick_df, 'binance')
            # finish
            s3_key_list.pop(0)
            processed_messages += 1
            if processed_messages % 10000 == 0:
                print(f"Messages processed: {processed_messages} in {(time.time() - start) / 60} Minutes")
                start = time.time()
    print(f"Processed {processed_messages} messages.") 

def get_s3_keys_from_prefix(s3_prefix):
    paginator = s3_client.get_paginator('list_objects')
    operation_parameters = {'Bucket': s3_bucket,
                            'Prefix': s3_prefix}
    page_iterator = paginator.paginate(**operation_parameters)
    s3_keys = []
    for page in page_iterator:
        page_s3_keys = [key['Key'] for key in page['Contents']]
        s3_keys.extend(page_s3_keys)
    return s3_keys

def df_to_rds(df, exchange):
    pk_column = ['trade_minute','coin_pair']
    column_list_string = """trade_minute
                        , coin_pair
                        , open_timestamp
                        , close_timestamp
                        , open
                        , high
                        , low
                        , close	
                        , volume
                        , quote_asset_volume
                        , trade_count
                        , taker_buy_base_asset_volume
                        , taker_buy_quote_asset_volume
                        , taker_sell_base_asset_volume
                        , taker_sell_quote_asset_volume
                        , taker_sell_volume_percentage
                        , taker_buy_volume_percentage
                        , open_datetime
                        , close_datetime
                        , file_name
                        """
    for i in range(len(df)):
        value_list_string = f"""
                '{df.trade_minute.iloc[i]}'
                , '{df.coin_pair.iloc[i]}'
                , '{df.open_timestamp.iloc[i]}'
                , '{df.close_timestamp.iloc[i]}'
                , '{df.open.iloc[i]}' 
                , '{df.high.iloc[i]}' 
                , '{df.low.iloc[i]}' 
                , '{df.close.iloc[i]}' 
                , '{df.volume.iloc[i]}' 
                , '{df.quote_asset_volume.iloc[i]}' 
                , '{df.trade_count.iloc[i]}' 
                , '{df.taker_buy_base_asset_volume.iloc[i]}' 
                , '{df.taker_buy_quote_asset_volume.iloc[i]}' 
                , '{df.taker_sell_base_asset_volume.iloc[i]}' 
                , '{df.taker_sell_quote_asset_volume.iloc[i]}' 
                , '{df.taker_sell_volume_percentage.iloc[i]}' 
                , '{df.taker_buy_volume_percentage.iloc[i]}' 
                , '{df.open_datetime.iloc[i]}' 
                , '{df.close_datetime.iloc[i]}' 
                , '{df.file_name.iloc[i]}' 
                """
        try:
            insert_into_postgres(exchange, 'candledicks', column_list_string, value_list_string)
        except psycopg2.IntegrityError:
            # Update row where PK already exists
            # Combine column value assignment
            column_list = column_list_string.replace('\n','').split(',')
            value_list = value_list_string.replace('\n','').split(',')
            # Get 1st pk value
            pk_column_ix = [i for i, item in enumerate(column_list) if pk_column[0] in item][0]
            pk_value = value_list.pop(pk_column_ix)
            column_list.pop(pk_column_ix)
            # Get 2nd pk value
            pk_column_ix_2 = [i for i, item in enumerate(column_list) if pk_column[1] in item][0]
            pk_value_2 = value_list.pop(pk_column_ix_2)
            column_list.pop(pk_column_ix_2)
            # Where clause
            where_clause = f"{pk_column[0]} = {pk_value} AND {pk_column[1]} = {pk_value_2}"
            # Values to update
            column_value_list = []
            for col in list(zip(column_list, value_list)):
                column_value_list.append(f"{col[0]} = {col[1]}")
            column_value_list_string = ','.join(column_value_list)
            print(f"PK already exists. Updating {where_clause}")
            update_postgres(exchange, 'candledicks', column_value_list_string, where_clause)
    

def insert_into_postgres(schema, table, column_list_string, values):
        """Inserts scoring results into Postgres db table

        Args:
            schema (str): A schema name
            table (str): A table name
            column_list_string (str): A comma delimited string of column names
            values (str): comma seperated values to insert
        """
        conn = logic_db_connection()
        try:
            cur = conn.cursor()
            insert_dml = """INSERT INTO {0}.{1}
                    ({2})
                    VALUES ({3}) 
                    ;""".format(schema, table, column_list_string, values)
            cur.execute(insert_dml)
            conn.commit()
        except Exception as e:
            if type(e) is not type(psycopg2.IntegrityError()):
                print(f'Unable to insert into Postgres table {table}. DML: {insert_dml} Error: {e}')
            raise
        finally:
            conn.close()
        return

def update_postgres(schema, table, values, where_clause):
        """Inserts scoring results into Postgres db table

        Args:
            schema (str): A schema name
            table (str): A table name
            column_list_string (str): A comma delimited string of column names
            values (str): comma seperated 'column = value' to insert
        """
        conn = logic_db_connection()
        try:
            cur = conn.cursor()
            insert_dml = """UPDATE {0}.{1}
                    SET {2}
                    WHERE {3}
                    ;""".format(schema, table, values, where_clause)
            cur.execute(insert_dml)
            conn.commit()
        except Exception as e:
            print(f'Unable to update Postgres table {table}. DML: {insert_dml} Error: {e}')
            raise
        finally:
            conn.close()
        return

def logic_db_connection():
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

if __name__ == '__main__':
    main()
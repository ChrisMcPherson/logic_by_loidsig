import numpy as np
import pandas as pd
import math
import ast
import re
import boto3
import json
import psycopg2 

# Instantiate resources
try:
    boto_session = boto3.Session(profile_name='loidsig')
except:
    boto_session = boto3.Session()
s3_resource = boto_session.resource('s3')
s3_bucket = 'loidsig-crypto'


def main(event, context):
    candlestick_str = event['Records'][0]['body']
    candlestick_json = json.loads(candlestick_str)
    exchange = candlestick_json['exchange']
    coin_pair = candlestick_json['coin_pair']
    data_call_unix_timestamp = candlestick_json['data_call_unix_timestamp']
    print(f"File (data call) timestamp: {data_call_unix_timestamp}")
    print(f"File coin pair: {coin_pair}")

    df = pd.DataFrame(candlestick_list, columns=['open_timestamp','open','high','low','close','volume','close_timestamp',
                                        'quote_asset_volume', 'trade_count', 'taker_buy_base_asset_volume',
                                        'taker_buy_quote_asset_volume','ignore'])
    # Timestamp cleaning
    df['open_timestamp_trim'] = df['open_timestamp']/1000
    df['close_timestamp_trim'] = df['close_timestamp']/1000
    df['open_datetime'] = pd.to_datetime(df['open_timestamp_trim'], unit='s')
    df['close_datetime'] = pd.to_datetime(df['close_timestamp_trim'], unit='s')
    df['coin'] = coin
    df.drop(['close_timestamp_trim','close_timestamp_trim'], axis=1, inplace=True)
    
    
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
    Parallel(n_jobs=multiprocessing.cpu_count())(delayed(row_to_rds)(candledick_df, i, 'binance') for i in range(len(candledick_df)))



    
    # # Build dataframes
    # bids_df = build_orderbook_df(orderbook_json, 'bids', coin_pair, unix_timestamp)
    # asks_df = build_orderbook_df(orderbook_json, 'asks', coin_pair, unix_timestamp)
    # # Engineer features
    # try:
    #     asks_fea_df = engineer_features(asks_df, 'asks')
    #     bids_fea_df = engineer_features(bids_df, 'bids')
    # except Exception as e:
    #     print(f"Not able to find price data! Error: {e}")
    #     return
    # # Combine asks and bids
    # orderbook_df = pd.merge(bids_fea_df, asks_fea_df, on=['unix_timestamp','coin_pair'], how='inner')
    # orderbook_df['trade_minute'] = int(orderbook_df['unix_timestamp'] / 60)
    # orderbook_df.drop('unix_timestamp', axis=1, inplace=True)
    # # Write to RDS
    # orderbook_df_to_rds(orderbook_df, exchange)

# def build_orderbook_df(orderbook_json, order_type, coin_pair, unix_timestamp):
#     if order_type == 'bids':
#         order_asc = False
#     else:
#         order_asc = True
#     try:
#         orderbook_df = pd.DataFrame(orderbook_json[order_type], columns=['price','volume'])
#     except Exception:
#         orderbook_df = pd.DataFrame(orderbook_json[order_type], columns=['price','volume','empty'])
#         orderbook_df.drop(['empty'], inplace=True, axis=1)
#     orderbook_df['price'] = pd.to_numeric(orderbook_df['price'])
#     orderbook_df['volume'] = pd.to_numeric(orderbook_df['volume'])
#     orderbook_df['order_type'] = order_type
#     orderbook_df['coin_pair'] = coin_pair
#     orderbook_df.sort_values('price', ascending=order_asc, inplace=True)
#     orderbook_df.insert(0, 'order_position', range(1, 1 + len(orderbook_df.index)))
#     orderbook_df['unix_timestamp'] = unix_timestamp
#     return orderbook_df



def row_to_rds(df, i, exchange):
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
        #print(f"PK already exists. Updating {where_clause}")
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
            #if type(e) is not type(psycopg2.IntegrityError()): # doesn't work need to use pg error code catching
            #    print(f'Unable to insert into Postgres table {table}. DML: {insert_dml} Error: {e}')
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
    message = {"exchange": "binance", "coin_pair": "ethusdt", "data_call_unix_timestamp": 1582493913, "candlesticks": [[1581575400000, "272.92000000", "273.73000000", "271.35000000", "272.49000000", "15438.77061000", 1581577199999, "4205656.18441520", 7162, "9313.19716000", "2536938.24397030", "0"], [1581577200000, "272.48000000", "272.59000000", "268.86000000", "269.70000000", "24189.36763000", 1581578999999, "6546619.98249080", 9426, "12204.49698000", "3303664.25850650", "0"]]}
    main(message, None)
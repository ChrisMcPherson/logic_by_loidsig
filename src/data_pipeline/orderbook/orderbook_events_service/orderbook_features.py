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

# Standardized price targets
std_price_target_dict = {
    'btcusdt':[5000,10000,20000,50000,100000,200000],
    'bnbusdt':[5000,10000,20000,50000,100000,200000],
    'ethusdt':[5000,10000,20000,50000,100000,200000],
    'ltcusdt':[5000,10000,20000,50000,100000,200000],
    'cobusdt':[5000,10000,20000,50000,100000,200000],
    'tusdbtc':[.77, 1.54, 3.08, 7.7, 15.4, 30.8],
    'ethbtc':[.77, 1.54, 3.08, 7.7, 15.4, 30.8],
    'tusdeth':[24,49,98,245,490,980],
    'trxeth':[24,49,98,245,490,980],
    'xrpeth':[24,49,98,245,490,980],
    'neoeth':[24,49,98,245,490,980],
    'eoseth':[24,49,98,245,490,980],
    'tusdbnb':[516,1032,2064,5160,10321,20643]
}

def main(event, context):
    orderbook_str = event['Records'][0]['body']
    orderbook_json = json.loads(orderbook_str)
    exchange = orderbook_json['exchange']
    coin_pair = orderbook_json['coin_pair']
    unix_timestamp = orderbook_json['unix_timestamp']
    # Build dataframes
    bids_df = build_orderbook_df(orderbook_json, 'bids', coin_pair, unix_timestamp)
    asks_df = build_orderbook_df(orderbook_json, 'asks', coin_pair, unix_timestamp)
    # Engineer features
    asks_fea_df = engineer_features(asks_df, 'asks')
    bids_fea_df = engineer_features(bids_df, 'bids')
    # Combine asks and bids
    orderbook_df = pd.merge(bids_fea_df, asks_fea_df, on=['unix_timestamp','coin_pair'], how='inner')
    orderbook_df['trade_minute'] = int(orderbook_df['unix_timestamp'] / 60)
    orderbook_df.drop('unix_timestamp', axis=1, inplace=True)
    # Write to RDS
    orderbook_df_to_rds(orderbook_df, exchange)

def build_orderbook_df(orderbook_json, order_type, coin_pair, unix_timestamp):
    if order_type == 'bids':
        order_asc = False
    else:
        order_asc = True
    orderbook_df = pd.DataFrame(orderbook_json[order_type], columns=['price','volume','empty'])
    orderbook_df['price'] = pd.to_numeric(orderbook_df['price'])
    orderbook_df['volume'] = pd.to_numeric(orderbook_df['volume'])
    orderbook_df['order_type'] = order_type
    orderbook_df['coin_pair'] = coin_pair
    orderbook_df.sort_values('price', ascending=order_asc, inplace=True)
    orderbook_df.insert(0, 'order_position', range(1, 1 + len(orderbook_df.index)))
    orderbook_df['unix_timestamp'] = unix_timestamp
    return orderbook_df

def weighted_avg(values, weights):
    """
    Return the weighted average.

    values, weights -- Numpy ndarrays with the same shape.
    """
    try:
        weighted_avg = np.average(values, weights=weights)
    except ZeroDivisionError:
        weighted_avg = 0
        raise
    return weighted_avg

def weighted_std(values, weights):
    """
    Return the weighted standard deviation.

    values, weights -- Numpy ndarrays with the same shape.
    """
    try:
        average = weighted_avg(values, weights)
        variance = np.average((values-average)**2, weights=weights)
        weighted_std = math.sqrt(variance)
    except ZeroDivisionError:
        weighted_std = 0
    return weighted_std

def orders_taken_to_fill(df, target_volume):
    remaining_volume = target_volume
    price_list = []
    volume_list = []
    for price, dollar_volume in zip(df.price, df.dollar_volume):   
        if (remaining_volume - dollar_volume) < 0.0:
            price_list.append(price)
            volume_list.append(remaining_volume)
            break
        else:
            price_list.append(price)
            volume_list.append(dollar_volume)
        remaining_volume = remaining_volume - dollar_volume
    return price_list, volume_list

def weighted_avg_price(df, target_volume):
    price_list, volume_list = orders_taken_to_fill(df, target_volume)
    return weighted_avg(price_list, volume_list)

def weighted_std_price(df, target_volume):
    price_list, volume_list = orders_taken_to_fill(df, target_volume)
    return weighted_std(price_list, volume_list)
     
def engineer_features(df, order_type):
    df['dollar_volume'] = df['price'] * df['volume']
    df['cumsum_dollar_dollar_bills'] = df['dollar_volume'].cumsum()

    # retrieve standardized price target list
    price_target_list = std_price_target_dict[df['coin_pair'][0]]

    fea_df = pd.DataFrame(
        [[
            df['coin_pair'][0],
            df['unix_timestamp'][0],
            df['price'][0],
            weighted_avg_price(df, price_target_list[0]),
            weighted_avg_price(df, price_target_list[1]),
            weighted_avg_price(df, price_target_list[2]),
            weighted_avg_price(df, price_target_list[3]),
            weighted_avg_price(df, price_target_list[4]),
            weighted_avg_price(df, price_target_list[5]),
            weighted_std_price(df, price_target_list[0]),
            weighted_std_price(df, price_target_list[1]),
            weighted_std_price(df, price_target_list[2]),
            weighted_std_price(df, price_target_list[3]),
            weighted_std_price(df, price_target_list[4]),
            weighted_std_price(df, price_target_list[5]),
        ]],
        columns=[
                    "coin_pair", 
                    "unix_timestamp", 
                    f"{df['order_type'][0]}_top_price", 
                    f"{df['order_type'][0]}_cum_5000_weighted_avg", 
                    f"{df['order_type'][0]}_cum_10000_weighted_avg", 
                    f"{df['order_type'][0]}_cum_20000_weighted_avg",
                    f"{df['order_type'][0]}_cum_50000_weighted_avg",
                    f"{df['order_type'][0]}_cum_100000_weighted_avg",
                    f"{df['order_type'][0]}_cum_200000_weighted_avg",
                    f"{df['order_type'][0]}_cum_5000_weighted_std", 
                    f"{df['order_type'][0]}_cum_10000_weighted_std", 
                    f"{df['order_type'][0]}_cum_20000_weighted_std",
                    f"{df['order_type'][0]}_cum_50000_weighted_std",
                    f"{df['order_type'][0]}_cum_100000_weighted_std",
                    f"{df['order_type'][0]}_cum_200000_weighted_std"
                ]
    )
    return fea_df

def orderbook_df_to_rds(df, exchange):
    pk_column = ['trade_minute','coin_pair']
    column_list_string = """
            trade_minute
            , coin_pair
            , bids_top_price
            , bids_cum_5000_weighted_avg 
            , bids_cum_10000_weighted_avg 
            , bids_cum_20000_weighted_avg 
            , bids_cum_50000_weighted_avg 
            , bids_cum_100000_weighted_avg 
            , bids_cum_200000_weighted_avg 
            , bids_cum_5000_weighted_std 
            , bids_cum_10000_weighted_std 
            , bids_cum_20000_weighted_std 
            , bids_cum_50000_weighted_std 
            , bids_cum_100000_weighted_std 
            , bids_cum_200000_weighted_std 
            , asks_top_price 
            , asks_cum_5000_weighted_avg 
            , asks_cum_10000_weighted_avg 
            , asks_cum_20000_weighted_avg 
            , asks_cum_50000_weighted_avg 
            , asks_cum_100000_weighted_avg 
            , asks_cum_200000_weighted_avg 
            , asks_cum_5000_weighted_std 
            , asks_cum_10000_weighted_std 
            , asks_cum_20000_weighted_std 
            , asks_cum_50000_weighted_std 
            , asks_cum_100000_weighted_std 
            , asks_cum_200000_weighted_std 
            """

    value_list_string = f"""
            '{df.trade_minute.iloc[0]}'
            , '{df.coin_pair.iloc[0]}'
            , '{df.bids_top_price.iloc[0]}'
            , '{df.bids_cum_5000_weighted_avg.iloc[0]}' 
            , '{df.bids_cum_10000_weighted_avg.iloc[0]}' 
            , '{df.bids_cum_20000_weighted_avg.iloc[0]}' 
            , '{df.bids_cum_50000_weighted_avg.iloc[0]}' 
            , '{df.bids_cum_100000_weighted_avg.iloc[0]}' 
            , '{df.bids_cum_200000_weighted_avg.iloc[0]}' 
            , '{df.bids_cum_5000_weighted_std.iloc[0]}' 
            , '{df.bids_cum_10000_weighted_std.iloc[0]}' 
            , '{df.bids_cum_20000_weighted_std.iloc[0]}' 
            , '{df.bids_cum_50000_weighted_std.iloc[0]}' 
            , '{df.bids_cum_100000_weighted_std.iloc[0]}' 
            , '{df.bids_cum_200000_weighted_std.iloc[0]}' 
            , '{df.asks_top_price.iloc[0]}' 
            , '{df.asks_cum_5000_weighted_avg.iloc[0]}' 
            , '{df.asks_cum_10000_weighted_avg.iloc[0]}' 
            , '{df.asks_cum_20000_weighted_avg.iloc[0]}' 
            , '{df.asks_cum_50000_weighted_avg.iloc[0]}' 
            , '{df.asks_cum_100000_weighted_avg.iloc[0]}' 
            , '{df.asks_cum_200000_weighted_avg.iloc[0]}' 
            , '{df.asks_cum_5000_weighted_std.iloc[0]}' 
            , '{df.asks_cum_10000_weighted_std.iloc[0]}' 
            , '{df.asks_cum_20000_weighted_std.iloc[0]}' 
            , '{df.asks_cum_50000_weighted_std.iloc[0]}' 
            , '{df.asks_cum_100000_weighted_std.iloc[0]}' 
            , '{df.asks_cum_200000_weighted_std.iloc[0]}' 
            """
    try:
        insert_into_postgres(exchange, 'orderbook', column_list_string, value_list_string)
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
        update_postgres(exchange, 'orderbook', column_value_list_string, where_clause)
    

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
    message = {"Records":[{"body":'{"exchange": "binance", "coin_pair": "ethbtc", "unix_timestamp": 1538742108, "bids": [["0.03398100", "4.08200000", []], ["0.03398000", "20.00000000", []], ["0.03396500", "0.09000000", []], ["0.03396000", "10.00000000", []], ["0.03395300", "18.00000000", []], ["0.03395200", "11.17500000", []], ["0.03394800", "0.32200000", []], ["0.03394100", "0.11100000", []], ["0.03394000", "1.58600000", []], ["0.03393700", "0.11100000", []], ["0.03393500", "0.11100000", []], ["0.03392700", "1.12900000", []], ["0.03392500", "11.71300000", []], ["0.03391300", "10.00100000", []], ["0.03391100", "0.18900000", []], ["0.03391000", "0.50000000", []], ["0.03390900", "0.62100000", []], ["0.03390400", "43.26400000", []], ["0.03390300", "19.09700000", []], ["0.03390100", "29.29200000", []], ["0.03390000", "12.26300000", []], ["0.03389700", "7.49100000", []], ["0.03389300", "3.50700000", []], ["0.03389000", "0.92100000", []], ["0.03388800", "39.55200000", []], ["0.03388700", "25.00000000", []], ["0.03388600", "3.02100000", []], ["0.03388400", "60.18100000", []], ["0.03388300", "1.19300000", []], ["0.03388200", "0.23000000", []], ["0.03388100", "1.97700000", []], ["0.03387900", "1.17800000", []], ["0.03387800", "2.33500000", []], ["0.03387700", "0.48100000", []], ["0.03387600", "30.07500000", []], ["0.03387500", "3.04100000", []], ["0.03387400", "31.95700000", []], ["0.03387300", "33.46500000", []], ["0.03387100", "0.11100000", []], ["0.03387000", "1.09100000", []], ["0.03386900", "0.46000000", []], ["0.03386800", "32.90900000", []], ["0.03386700", "1.29200000", []], ["0.03386400", "0.11200000", []], ["0.03386300", "49.30000000", []], ["0.03386200", "30.47400000", []], ["0.03386100", "57.08600000", []], ["0.03386000", "20.02900000", []], ["0.03385900", "1.24500000", []], ["0.03385700", "23.11100000", []], ["0.03385500", "0.50000000", []], ["0.03385400", "8.86100000", []], ["0.03385100", "0.11100000", []], ["0.03385000", "120.99800000", []], ["0.03384900", "13.69100000", []], ["0.03384800", "20.71000000", []], ["0.03384600", "6.62000000", []], ["0.03384500", "0.14700000", []], ["0.03384200", "0.57000000", []], ["0.03384000", "0.21600000", []], ["0.03383900", "0.15600000", []], ["0.03383800", "1.00300000", []], ["0.03383700", "1.07600000", []], ["0.03383600", "0.72900000", []], ["0.03383400", "9.11300000", []], ["0.03383300", "3.05000000", []], ["0.03383200", "0.57400000", []], ["0.03383100", "0.64600000", []], ["0.03383000", "0.03400000", []], ["0.03382900", "2.14100000", []], ["0.03382800", "1.97000000", []], ["0.03382700", "11.00000000", []], ["0.03382600", "5.90200000", []], ["0.03382400", "0.04800000", []], ["0.03382300", "1.67700000", []], ["0.03382100", "0.27300000", []], ["0.03382000", "3.48200000", []], ["0.03381900", "10.53700000", []], ["0.03380600", "42.79700000", []], ["0.03380000", "20.59700000", []], ["0.03379500", "0.30700000", []], ["0.03379300", "0.72600000", []], ["0.03378800", "0.25000000", []], ["0.03378200", "0.16200000", []], ["0.03377400", "0.59100000", []], ["0.03377300", "24.07200000", []], ["0.03377200", "23.90100000", []], ["0.03377100", "1.50400000", []], ["0.03377000", "11.82400000", []], ["0.03376900", "0.14000000", []], ["0.03376300", "23.39800000", []], ["0.03375700", "0.05000000", []], ["0.03375600", "2.16400000", []], ["0.03375500", "176.79200000", []], ["0.03375400", "0.41400000", []], ["0.03375100", "38.96100000", []], ["0.03375000", "20.00000000", []], ["0.03374700", "0.11800000", []], ["0.03374400", "0.13800000", []], ["0.03372700", "7.35600000", []], ["0.03372500", "2.70000000", []], ["0.03372300", "0.08800000", []], ["0.03372200", "0.03300000", []], ["0.03372100", "0.03000000", []], ["0.03371700", "23.78400000", []], ["0.03371600", "23.79800000", []], ["0.03371400", "0.05900000", []], ["0.03371000", "334.57300000", []], ["0.03370800", "52.73500000", []], ["0.03370700", "0.20800000", []], ["0.03370600", "0.29900000", []], ["0.03370500", "0.04300000", []], ["0.03370400", "0.30300000", []], ["0.03370200", "0.38000000", []], ["0.03370000", "79.70100000", []], ["0.03369900", "2.22800000", []], ["0.03369800", "0.05000000", []], ["0.03369500", "0.60500000", []], ["0.03369300", "0.30800000", []], ["0.03369200", "0.74200000", []], ["0.03369100", "0.67900000", []], ["0.03369000", "0.10600000", []], ["0.03368900", "138.94600000", []], ["0.03368800", "1.30700000", []], ["0.03368700", "0.66800000", []], ["0.03368600", "1.06600000", []], ["0.03368500", "3.25700000", []], ["0.03368400", "7.56700000", []], ["0.03368200", "0.85400000", []], ["0.03368100", "2.74000000", []], ["0.03367900", "0.15000000", []], ["0.03367600", "0.12700000", []], ["0.03367100", "2.86800000", []], ["0.03367000", "0.92600000", []], ["0.03366900", "0.27300000", []], ["0.03366600", "0.06500000", []], ["0.03366400", "0.47700000", []], ["0.03366200", "0.17700000", []], ["0.03365700", "0.11100000", []], ["0.03365600", "0.06800000", []], ["0.03365500", "8.00000000", []]], "asks": [["0.03398800", "0.04300000", []], ["0.03399100", "1.68200000", []], ["0.03399200", "0.12000000", []], ["0.03399400", "0.11400000", []], ["0.03399500", "44.94000000", []], ["0.03399900", "4.50700000", []], ["0.03400100", "0.55900000", []], ["0.03400200", "2.33000000", []], ["0.03400300", "0.65500000", []], ["0.03400400", "21.10400000", []], ["0.03400500", "21.63500000", []], ["0.03400600", "20.63900000", []], ["0.03400700", "0.10300000", []], ["0.03400900", "5.20000000", []], ["0.03401000", "0.03000000", []], ["0.03401100", "1.29300000", []], ["0.03401300", "0.10300000", []], ["0.03401400", "0.26800000", []], ["0.03401600", "0.04400000", []], ["0.03401700", "0.32700000", []], ["0.03401800", "0.09300000", []], ["0.03401900", "0.05600000", []], ["0.03402000", "0.14200000", []], ["0.03402100", "0.03000000", []], ["0.03402200", "0.16900000", []], ["0.03402300", "0.30000000", []], ["0.03402500", "12.00000000", []], ["0.03402600", "0.03300000", []], ["0.03402800", "28.62800000", []], ["0.03402900", "15.00000000", []], ["0.03403300", "67.24700000", []], ["0.03403400", "66.25200000", []], ["0.03403500", "45.03700000", []], ["0.03403600", "74.03500000", []], ["0.03403700", "40.67200000", []], ["0.03403800", "0.23400000", []], ["0.03403900", "0.03000000", []], ["0.03404000", "2.77600000", []], ["0.03404200", "25.09700000", []], ["0.03404300", "0.28500000", []], ["0.03404400", "0.79900000", []], ["0.03404700", "0.07300000", []], ["0.03404800", "1.32500000", []], ["0.03405000", "2.14600000", []], ["0.03405100", "0.12000000", []], ["0.03405200", "0.81400000", []], ["0.03405300", "35.44600000", []], ["0.03405400", "17.00000000", []], ["0.03405500", "0.04500000", []], ["0.03405600", "0.73000000", []], ["0.03405900", "0.53900000", []], ["0.03406300", "8.79900000", []], ["0.03406400", "18.82300000", []], ["0.03406500", "1.46000000", []], ["0.03407100", "3.50000000", []], ["0.03407300", "0.05000000", []], ["0.03407500", "4.48500000", []], ["0.03407700", "0.05000000", []], ["0.03407900", "2.00000000", []], ["0.03408000", "6.10000000", []], ["0.03408100", "11.49700000", []], ["0.03408400", "0.06000000", []], ["0.03408500", "0.21800000", []], ["0.03409000", "10.49900000", []], ["0.03409500", "58.56700000", []], ["0.03409900", "51.20000000", []], ["0.03410000", "41.95900000", []], ["0.03410300", "0.30000000", []], ["0.03410500", "2.70000000", []], ["0.03410700", "0.19100000", []], ["0.03410800", "1.91100000", []], ["0.03410900", "16.61800000", []], ["0.03411000", "67.39800000", []], ["0.03411100", "0.05500000", []], ["0.03411300", "1.72500000", []], ["0.03411500", "0.25000000", []], ["0.03411600", "0.16600000", []], ["0.03411800", "0.07300000", []], ["0.03411900", "0.05000000", []], ["0.03412000", "0.45600000", []], ["0.03412100", "0.47300000", []], ["0.03412200", "0.34200000", []], ["0.03412300", "0.07500000", []], ["0.03412500", "15.71700000", []], ["0.03412600", "3.06300000", []], ["0.03412800", "0.23200000", []], ["0.03413000", "1.00000000", []], ["0.03413600", "1.69400000", []], ["0.03413700", "0.34900000", []], ["0.03413800", "15.40900000", []], ["0.03413900", "0.41000000", []], ["0.03414000", "15.10300000", []], ["0.03414300", "0.30000000", []], ["0.03414400", "2.29600000", []], ["0.03414700", "0.06200000", []], ["0.03414800", "173.40000000", []], ["0.03414900", "0.42100000", []], ["0.03415000", "0.57600000", []], ["0.03415200", "0.06300000", []], ["0.03415300", "0.59800000", []], ["0.03415400", "16.34800000", []], ["0.03415600", "15.77400000", []], ["0.03415700", "0.25200000", []], ["0.03415800", "0.11800000", []], ["0.03416000", "0.35500000", []], ["0.03416300", "0.30000000", []], ["0.03416600", "0.15200000", []], ["0.03417000", "2.62900000", []], ["0.03417200", "0.05800000", []], ["0.03417400", "0.06400000", []], ["0.03417500", "2.20500000", []], ["0.03417700", "3.06300000", []], ["0.03417800", "0.08700000", []], ["0.03418100", "1.30700000", []], ["0.03418200", "0.25200000", []], ["0.03418300", "0.08800000", []], ["0.03418400", "0.03000000", []], ["0.03418700", "0.07400000", []], ["0.03418900", "15.55000000", []], ["0.03419000", "41.50300000", []], ["0.03419400", "1.13200000", []], ["0.03419600", "1.15300000", []], ["0.03419800", "30.25800000", []], ["0.03419900", "0.24700000", []]]}'}]}
    main(message, None)
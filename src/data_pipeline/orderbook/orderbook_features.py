import numpy as np
import pandas as pd
import math
from io import StringIO
import ast
import boto3
import json
import sqlalchemy 

# Instantiate resources
try:
    boto_session = boto3.Session(profile_name='loidsig')
except:
    boto_session = boto3.Session()
s3_resource = boto_session.resource('s3')
s3_bucket = 'loidsig-crypto'


def main(event, context):
    #orderbook_str = event.body
    orderbook_str = event
    orderbook_json = json.loads(orderbook_str)
    exchange = orderbook_json['exchange']
    coin_pair = orderbook_json['coin_pair']
    unix_timestamp = orderbook_json['unix_timestamp']
    # Build dataframes
    bids_df = build_orderbook_df(orderbook_json, 'bids', coin_pair, unix_timestamp)
    asks_df = build_orderbook_df(orderbook_json, 'asks', coin_pair, unix_timestamp)
    # Write to S3 archive
    #df_to_s3(bids_df, exchange, coin_pair, 'bid', unix_timestamp)
    #df_to_s3(asks_df, exchange, coin_pair, 'ask', unix_timestamp)
    # Engineer features
    asks_fea_df = engineer_features(asks_df, 'asks')
    bids_fea_df = engineer_features(bids_df, 'bids')
    # Write to RDS
    df_to_rds(bids_fea_df, asks_fea_df, exchange)

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
    return np.average(values, weights=weights)

def weighted_std(values, weights):
    """
    Return the weighted standard deviation.

    values, weights -- Numpy ndarrays with the same shape.
    """
    average = weighted_avg(values, weights)
    # Fast and numerically precise:
    variance = np.average((values-average)**2, weights=weights)
    return math.sqrt(variance)

def weighted_avg_price(df, amount):
    return weighted_avg(df[df['cumsum_dollar_dollar_bills'] < amount]['price'], df[df['cumsum_dollar_dollar_bills'] < amount]['volume'])

def weighted_std_price(df, amount):
    return weighted_std(df[df['cumsum_dollar_dollar_bills'] < amount]['price'], df[df['cumsum_dollar_dollar_bills'] < amount]['volume'])
     
def engineer_features(df, order_type):
    df['dollar_volume'] = df['price'] * df['volume']
    df['cumsum_dollar_dollar_bills'] = df['dollar_volume'].cumsum()

    fea_df = pd.DataFrame(
        [[
            df['coin_pair'][0],
            df['unix_timestamp'][0],
            df['price'][0],
            weighted_avg_price(df, 5000),
            weighted_avg_price(df, 10000),
            weighted_avg_price(df, 20000),
            weighted_avg_price(df, 50000),
            weighted_avg_price(df, 100000),
            weighted_avg_price(df, 200000),
            weighted_std_price(df, 5000),
            weighted_std_price(df, 10000),
            weighted_std_price(df, 20000),
            weighted_std_price(df, 50000),
            weighted_std_price(df, 100000),
            weighted_std_price(df, 200000),
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

def df_to_s3(df, exchange, coin_pair, order_type, timestamp):
    file_name = f"{order_type}/{coin_pair}/{timestamp}.csv"
    df['file_name'] = file_name
    file_path = f"{exchange}/historic_orderbook/{file_name}"
    # Write out csv
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3_resource.Object(s3_bucket, file_path).put(Body=csv_buffer.getvalue())

def df_to_rds(bids_df, asks_df, exchange):
    df = pd.merge(bids_df, asks_df, on=['unix_timestamp','coin_pair'], how='inner')
    df['trade_minute'] = int(df['unix_timestamp'] / 60)
    df.drop('unix_timestamp', axis=1, inplace=True)
    print('f')
    #df.to_sql(exchange, logic_db_engine(), schema='orderbook', if_exists='append', index=False)
    

def logic_db_engine():
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
    print(db_user)

    try:
        postgres_engine = sqlalchemy.create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')
    except Exception as e:
        print("Unable to connect to postgres! Error: {}".format(e))
        raise
    return postgres_engine


if __name__ == '__main__':
    message = '{"exchange": "cobinhood", "coin_pair": "NOPE", "unix_timestamp": 1538741747, "bids": [["0.0256001", 23.3022, []], ["0.0256", 5.0, []], ["0.025584", 5.22, []], ["0.025", 4.0, []], ["0.02411", 20.0, []], ["0.0241", 9.0, []], ["0.024", 21.8199, []], ["0.0238", 5.0, []], ["0.0235", 5.0, []], ["0.0229", 33.0, []], ["0.0228", 15.0, []], ["0.0226", 0.6789, []], ["0.0225", 5.0, []], ["0.0223", 5.0, []], ["0.0220004", 50.0, []], ["0.0215", 50.0, []], ["0.021", 5.0, []], ["0.018991", 9.0, []], ["0.0189001", 70.1436, []], ["0.0189", 11.54, []], ["0.0188001", 21.8433, []], ["0.0187306", 176.097, []], ["0.01873", 25.1444, []], ["0.018728", 31.1344, []], ["0.018701", 40.0, []], ["0.018675", 4.0, []], ["0.0186", 7.1505, []], ["0.0182", 10.0, []], ["0.01702", 30.266, []], ["0.0170073", 66.4742, []], ["0.017007", 10.0, []], ["0.0170001", 10.0, []], ["0.017", 590.0, []], ["0.0169347", 8.0, []], ["0.0164104", 6.2389, []], ["0.0161101", 7.5256, []], ["0.015", 7.0, []], ["0.014", 5.0, []], ["0.013", 5.0, []], ["0.0123", 36.7195, []], ["0.012", 5.0, []], ["0.0114", 40.0, []], ["0.0112", 5.0, []], ["0.0100035", 22.384, []], ["0.0100034", 14.0, []], ["0.0100033", 20.0, []], ["0.0100032", 20.0, []], ["0.0100031", 20.0, []], ["0.010003", 20.0, []], ["0.0100029", 20.0, []], ["0.0100028", 20.0, []], ["0.0100001", 20.0, []], ["0.01", 24.0, []], ["0.0099", 500.0, []], ["0.0095", 5.0, []], ["0.00909", 8.0, []], ["0.009", 208.0, []], ["0.0088", 5.0, []], ["0.0071", 3.0, []], ["0.0065", 40.0, []], ["0.006", 60.0, []], ["0.0043245", 47.4103, []], ["0.004", 10.0, []], ["0.003401", 20.0, []], ["0.0033", 10.0, []], ["0.003", 10.0, []], ["0.0022655", 18.0, []], ["0.00211", 888.4465, []], ["0.0009887", 1000.0, []], ["0.00036", 200.0, []], ["0.00035", 259.0, []], ["0.0003", 4800.0, []], ["0.0002571", 25.0, []], ["0.00025", 200.0, []], ["0.00024", 100.0, []], ["0.0001", 7826.0, []], ["0.00008", 250.0, []], ["0.00007", 300.0, []], ["0.000065", 3000.0, []], ["0.00006", 2670.0, []], ["0.00005", 400.0, []], ["0.00004", 500.0, []], ["0.00003", 667.0, []], ["0.000025", 20000.0, []], ["0.0000201", 12362.6864, []], ["0.00002", 1000.0, []], ["0.00001", 8000.0, []], ["0.000005", 4000.0, []], ["0.000004", 5000.0, []], ["0.000003", 6666.0, []], ["0.000002", 10000.0, []], ["0.000001", 20000.0, []]], "asks": [["0.0257931", 11.4432, []], ["0.025795", 6.0, []], ["0.0257951", 5.18, []], ["0.02618", 17.9697, []], ["0.0262", 2.92, []], ["0.0266885", 4.5, []], ["0.0266888", 596.7818, []], ["0.0268888", 60.8478, []], ["0.0269876", 4.5, []], ["0.028", 57.5, []], ["0.0285", 5.0, []], ["0.0288887", 5.2291, []], ["0.0288888", 119.67, []], ["0.029", 4.0, []], ["0.037", 86.18, []], ["0.0498", 6.0, []], ["0.0499999", 47.2938, []], ["0.05", 140.0, []], ["0.065", 5.41, []], ["0.0664997", 16.0, []], ["0.0664998", 4.3596, []], ["0.06789", 6.0939, []], ["0.09", 5.0, []], ["0.099", 5.0, []], ["0.1", 50.0, []], ["0.19", 50.0, []], ["0.19999", 27.0, []], ["0.2", 20.0001, []], ["0.28", 5.0, []], ["999999", 7.55000002, []]]}'
    main(message, None)
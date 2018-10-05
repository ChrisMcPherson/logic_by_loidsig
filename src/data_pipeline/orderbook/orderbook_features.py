import numpy as np
import pandas as pd
import math
from io import StringIO
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
    #df_to_rds(bids_fea_df, asks_fea_df, exchange)

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
    df = pd.merge(bids_df, asks_df, on='unix_timestamp', how='inner')


if __name__ == '__main__':
    message = '{"exchange": "cobinhood", "coin_pair": "cobusdt", "unix_timestamp": 1538243713, "bids": [["0.026003", 2395.35937313, []], ["0.026001", 767.9, []], ["0.025426", 50000.0, []], ["0.0254", 1000.0, []], ["0.0253", 3000.0, []], ["0.02521", 2500.0, []], ["0.025208", 2460.0, []], ["0.025202", 1536.00018554, []], ["0.0252", 1000.0, []], ["0.025013", 3000.0, []], ["0.025", 1000.0, []], ["0.02473", 768.0, []], ["0.024501", 2042.05341975, []], ["0.0245", 12000.0, []], ["0.024062", 1700.0, []], ["0.024", 1000.0, []], ["0.0235", 114788.40600000002, []], ["0.023123", 1300.0, []], ["0.023121", 2798.52303101, []], ["0.023011", 1150.0, []], ["0.023009", 1341.0, []], ["0.023003", 2032.30913359, []], ["0.023002", 1295.75430499, []], ["0.023", 159682.25567615998, []], ["0.022683", 2600.03738482, []], ["0.022555", 2500.0, []], ["0.022501", 9888.46397936, []], ["0.0225", 43000.0, []], ["0.022", 24900.0, []], ["0.0215", 5000.0, []], ["0.02101", 2936.12485986, []], ["0.021", 9766.94395026, []], ["0.0206", 734.0, []], ["0.02051", 10000.0, []], ["0.0201", 365.0, []], ["0.02001", 10000.0, []], ["0.02", 340404.40375, []], ["0.01951", 10000.0, []], ["0.0195", 10000.0, []], ["0.01901", 10000.0, []], ["0.01897", 9567.0, []], ["0.01851", 10000.0, []], ["0.0185", 972.001, []], ["0.01801", 10000.0, []], ["0.018", 365.0, []], ["0.0165", 365.0, []], ["0.0162", 1413.79629629, []], ["0.01611", 760.0, []], ["0.015984", 96262.0, []], ["0.015501", 1207.0, []]], "asks": [["0.027799", 10505.11312, []], ["0.0278", 2978.72860635, []], ["0.027994", 9000.0, []], ["0.027995", 3792.0, []], ["0.027998", 1293.99645051, []], ["0.028", 50437.86976458, []], ["0.0285", 15388.0, []], ["0.028988", 5000.0, []], ["0.02899", 1850.0, []], ["0.029", 6649.662, []], ["0.0295", 7000.0, []], ["0.02997", 2000.0, []], ["0.02998", 2000.0, []], ["0.029999", 2264.836, []], ["0.03", 461476.94295096, []], ["0.031153", 1034.0, []], ["0.031154", 10000.0, []], ["0.03199", 935.13477877, []], ["0.033", 2280.81614963, []], ["0.034", 1000.0, []], ["0.035", 55377.89765439, []], ["0.0365", 1000.0, []], ["0.036536", 15564.05522935, []], ["0.0366", 1000.0, []], ["0.03799", 4000.0, []], ["0.039969", 1000.0, []], ["0.04", 1500.0, []], ["0.04022", 459.9999387, []], ["0.0403", 723.07325385, []], ["0.040998", 367.0, []], ["0.0415", 2000.0, []], ["0.0423", 685.0, []], ["0.0425", 4863.47715061, []], ["0.04255", 4.68122277, []], ["0.04399", 2000.0, []], ["0.0441", 826.49437642, []], ["0.0446", 3000.0, []], ["0.045", 2250.0, []], ["0.0455", 800.0, []], ["0.046", 2000.0, []], ["0.0479", 1131.0, []], ["0.047979", 977.0, []], ["0.048", 8430.0, []], ["0.048869", 800.0, []], ["0.048999", 500.0, []], ["0.04929", 413.60899535, []], ["0.0499", 1100.0, []], ["0.05", 13437.22759378, []], ["0.0515", 1500.0, []], ["0.051555", 408.31128882, []]]}'
    main(message, None)
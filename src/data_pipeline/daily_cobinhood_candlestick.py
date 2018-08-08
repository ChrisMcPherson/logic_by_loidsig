import os
import sys
import pandas as pd
import ast
from io import StringIO
import requests
import datetime
import time
import calendar
import dateparser
import boto3
from cobinhood_api import Cobinhood

# import module in relative path
sys.path.append(os.path.abspath(os.path.join(sys.path[0], '..', 'lib')))
import athena_connect

# Configs
start_date = datetime.date(2018, 3, 1)

start_date = calendar.timegm(start_date.timetuple())

# Instantiate resources
try:
    boto_session = boto3.Session(profile_name='loidsig')
except:
    boto_session = boto3.Session()
s3_resource = boto_session.resource('s3')
s3_bucket = 'loidsig-crypto'

sm_client = boto_session.client(
    service_name='secretsmanager',
    region_name='us-east-1',
    endpoint_url='https://secretsmanager.us-east-1.amazonaws.com'
)
get_secret_value_response = sm_client.get_secret_value(SecretId='Loidsig_CPM_Cobinhood')
key, value = ast.literal_eval(get_secret_value_response['SecretString']).popitem()
cob = Cobinhood(API_TOKEN=value)

athena_functions = athena_connect.Athena()

def get_candlestick():
    print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

    # Convert dates to day_ids (days since Jan 1 1970)
    start_day_id = int(start_date/60/60/24)
    current_day_id = int(time.time()/60/60/24)
    days_between_list = list(range(start_day_id, current_day_id))

    # Process coin pairs
    coins = ('BTC-USDT','ETH-USDT','COB-USDT','TRX-ETH','LTC-USDT','ETH-BTC','XRP-ETH','NEO-ETH','ENJ-ETH')
    
    for coin in coins:
        coin_strip_lower = coin.lower().replace('-', '')
        ## Get last processed date from Athena (if empty, process as new coin)
        processed_days_df = athena_functions.pandas_read_athena(f"""SELECT DISTINCT CAST(open_timestamp as bigint)/1000/60/60/24 as day_id
                                                                  FROM cobinhood.historic_candlesticks
                                                                  WHERE coin_partition = '{coin_strip_lower}'
                                                                  UNION all SELECT 1 AS day_id""")
        processed_days_list = list(processed_days_df['day_id'])
        #processed_days_list = [1] # when no table exists

        days_to_process_list = [day for day in days_between_list if day not in processed_days_list]
        print(f"{coin}: {len(days_to_process_list)} unprocessed days between {start_date} and the day before today")
        
        for day in days_to_process_list:
            process_date = day*24*60*60*1000
        
            # Get api endpoint generator for date
            try:
                candlesticks = cob.chart.get_candles(trading_pair_id=coin, start_time=process_date, end_time=process_date+90000000, timeframe='1m')['result']['candles']
            except Exception as e:
                print(f"Unable to get {coin} trades for day {process_date}: {e}")
                continue
            if not candlesticks:
                print(f"**Aggregate trade results from API returned empty for {coin} for day {process_date}")
                continue

            candlestick_list = []
            trade_day_id = int(candlesticks[0]['timestamp']/1000/60/60/24)
            if trade_day_id < day-2:
                #raise ValueError(f"The first trade from the generator has the day_id {trade_day_id} but should have {day}")
                print(f"The first trade from the generator has the day_id {trade_day_id} but should have {day}")
            try:
                for trade in candlesticks:
                    if int(trade['timestamp']/1000/60/60/24) < trade_day_id:
                        continue
                    if int(trade['timestamp']/1000/60/60/24) > trade_day_id:
                        #print(f"--Finished looping over day_id {day} with {len(candlestick_list)} trades.")
                        break
                    else:
                        candlestick_list.append(trade)
            except Exception as e:
                print(f"Failed getting next trade, skipping day. {e}")
                continue

            # Convert raw trade data (list of dicts) to Dataframe
            df = pd.DataFrame(candlestick_list)
            df.rename(columns={'trading_pair_id':'coin','timestamp':'open_timestamp'}, inplace=True)
            df['coin'] = df['coin'].replace(regex=True, to_replace=r'-', value=r'')
            df['open_unix'] = df['open_timestamp']/1000
            df['open_datetime'] = pd.to_datetime(df['open_unix'], unit='s')

            # Validate days in Dataframe
            daily_group_df = df.groupby([df['open_datetime'].dt.date])
            daily_df_list = [daily_group_df.get_group(x) for x in daily_group_df.groups]
            if len(daily_df_list) != 1:
                raise ValueError(f"There are {len(daily_df_list)} dates in the dataframe for {process_date}")

            # Load day as csv file to S3
            print(f"{datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}: Beginning S3 load for {process_date} with {len(df.index)} rows")
            if not df['open_datetime'].dt.time.max() > datetime.time(23,58):
                print(f"**Time {df['open_datetime'].dt.time.max()} is not end of day for {df['open_datetime'].max()}...")
            recents_file_name = f"{coin_strip_lower}/{str(df['open_datetime'].dt.date.iloc[0])}.csv"
            df['file_name'] = recents_file_name
            recents_file_path = f"cobinhood/historic_candlesticks/{recents_file_name}"
            # Write out csv
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)
            s3_resource.Object(s3_bucket, recents_file_path).put(Body=csv_buffer.getvalue())

    print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    return

if __name__ == '__main__':
    get_candlestick()
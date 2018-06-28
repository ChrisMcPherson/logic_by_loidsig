import os
import sys
import pandas as pd
import ast
from io import StringIO
import requests
import datetime
import time
import dateparser
import boto3
from binance.client import Client
# import module in relative path
sys.path.append(os.path.abspath(os.path.join(sys.path[0], '..', 'lib')))
import athena_connect

# Configs
start_date = '1 Jan. 2018 UTC'

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
get_secret_value_response = sm_client.get_secret_value(SecretId='CPM_Binance')
key, value = ast.literal_eval(get_secret_value_response['SecretString']).popitem()
bnb_client = Client(key, value)

athena_functions = athena_connect.Athena()

def get_agg_trades():
    print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

    # Convert dates to day_ids (days since Jan 1 1970)
    start_day_id = int(dateparser.parse(start_date).timestamp()/60/60/24)
    current_day_id = int(time.time()/60/60/24)
    days_between_list = list(range(start_day_id, current_day_id))

    # Process coin pairs
    coins = ('ETHUSDT','BNBUSDT','TRXETH','BTCUSDT','LTCUSDT','ETHBTC','XRPETH','NEOETH','ENJETH')
    
    for coin in coins:
        # Get last processed date from Athena (if empty, process as new coin)
        processed_days_df = athena_functions.pandas_read_athena(f"""SELECT DISTINCT CAST(micro_timestamp as bigint)/1000/60/60/24 as day_id
                                                                  FROM binance.historic_trades
                                                                  WHERE coin_partition = '{coin.lower()}'
                                                                  UNION all SELECT 1 AS day_id""")
        processed_days_list = list(processed_days_df['day_id'])
        days_to_process_list = [day for day in days_between_list if day not in processed_days_list]
        print(f"{coin}: {len(days_to_process_list)} unprocessed days between {start_date} and the day before today")

        for day in days_to_process_list:
            process_date_str = datetime.datetime.utcfromtimestamp(day*24*60*60).strftime('%Y-%m-%d')
        
            # Get api endpoint generator for date
            try:
                agg_trades = bnb_client.aggregate_trade_iter(symbol=coin, start_str=process_date_str)
            except Exception as e:
                print(f"Unable to get {coin} trades for day {process_date_str}: {e}")
                continue
            if not agg_trades:
                print(f"**Aggregate trade results from API returned empty for {coin} for day {process_date_str}")
                continue

            # Iterate over generator until current date being processed has elapsed
            try:
                agg_trade_list = [next(agg_trades)]
            except Exception as e:
                print(f"Failed to get agg trade list for {process_date_str}. {e}")
                continue
            trade_day_id = int(agg_trade_list[0]['T']/1000/60/60/24)
            if trade_day_id != day:
                raise ValueError(f"The first trade from the generator has the day_id {trade_day_id} but should have {day}")
            try:
                for trade in agg_trades:
                    if int(trade['T']/1000/60/60/24) > trade_day_id:
                        #print(f"--Finished looping over day_id {day} with {len(agg_trade_list)} trades.")
                        break
                    else:
                        agg_trade_list.append(trade)
            except Exception as e:
                print(f"Failed getting next trade, skipping day. {e}")
                continue

            # Convert raw trade data (list of dicts) to Dataframe
            df = pd.DataFrame(agg_trade_list)
            df.rename(columns={'a':'trade_id','p':'price','q':'quantity','f':'first_trade_id','l':'last_trade_id',
                            'T':'micro_timestamp','m':'buyer_maker','M':'best_price_match'}, inplace=True)
            # Timestamp cleaning
            df['unix_timestamp'] = df['micro_timestamp']/1000
            df['datetime'] = pd.to_datetime(df['unix_timestamp'], unit='s')
            df = df[['trade_id','datetime','price','quantity','buyer_maker','best_price_match',
                    'first_trade_id','last_trade_id','micro_timestamp']]
            df['coin'] = coin
            # Validate days in Dataframe
            daily_group_df = df.groupby([df['datetime'].dt.date])
            daily_df_list = [daily_group_df.get_group(x) for x in daily_group_df.groups]
            if len(daily_df_list) != 1:
                raise ValueError(f"There are {len(daily_df_list)} dates in the dataframe for {process_date_str}")

            # Load day as csv file to S3
            print(f"{datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}: Beginning S3 load for {process_date_str} with {len(df.index)} rows")
            if not df['datetime'].dt.time.max() > datetime.time(23,59):
                print(f"**Time {df['datetime'].dt.time.max()} is not end of day for {df['datetime'].max()}...")
            recents_file_name = f"{coin.lower()}/{str(df['datetime'].dt.date.iloc[0])}.csv"
            df['file_name'] = recents_file_name
            recents_file_path = f"binance/historic_trades/{recents_file_name}"
            # Write out csv
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)
            s3_resource.Object(s3_bucket, recents_file_path).put(Body=csv_buffer.getvalue())

    print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    return

if __name__ == '__main__':
    get_agg_trades()
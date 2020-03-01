import time
import boto3
import json
import ast
from binance.client import Client

# Instantiate resources
boto_session = boto3.Session()
s3_resource = boto_session.resource('s3')
s3_bucket = 'loidsig-crypto'

sm_client = boto_session.client(
    service_name='secretsmanager',
    region_name='us-east-1',
    endpoint_url='https://secretsmanager.us-east-1.amazonaws.com'
)
get_secret_value_response = sm_client.get_secret_value(SecretId='Loidsig_CPM_Binance')
key, value = ast.literal_eval(get_secret_value_response['SecretString']).popitem()
bnb = Client(key, value)

def main(event, context):
    time.sleep(2) # eth seems to be collected too early. 
    coins = (
        'ETHUSDT',
        'BTCUSDT',
        'ETHBTC',
        'BNBUSDT',
        'LTCUSDT',
        'BCHABCUSDT',
        'NEOUSDT',
        'ETCUSDT',
        'EOSUSDT',
        'TRXUSDT',
        'QTUMUSDT',
        'XRPUSDT',
        'TRXETH',
        'XRPETH',
        'NEOETH',
        'TUSDBNB',
        'TUSDBTC',
        'TUSDETH'
    )
    for coin_pair in coins:
        json_message, unix_timestamp = get_candlestick_message(coin_pair)
        message_to_s3(json_message, coin_pair, unix_timestamp)
        message_to_queue(json_message)

def get_candlestick_message(coin_pair):
    unix_timestamp = int(time.time())
    api_tries = 0
    while api_tries < 2:
        try:
            candles = bnb.get_klines(symbol=coin_pair, interval=Client.KLINE_INTERVAL_1MINUTE)
            break
        except Exception as e:
            print(f"Error! {e}")
            api_tries += 1
            time.sleep(5)

    # Build message
    message = {}
    message['exchange'] = 'binance'
    message['coin_pair'] = coin_pair.lower().replace('-', '')
    message['data_call_unix_timestamp'] = unix_timestamp
    message['candlesticks'] = candles
    message_json = json.dumps(message)
    return message_json, unix_timestamp

def message_to_s3(json, coin_pair, timestamp):
    file_name = f"{timestamp}.json"
    file_path = f"binance/candlesticks/{coin_pair}/{file_name}"
    s3_resource.Object(s3_bucket, file_path).put(Body=json)

def message_to_queue(message):
    # Send message
    sqs_resource = boto_session.resource('sqs', region_name='us-east-1')
    sqs_queue = sqs_resource.get_queue_by_name(QueueName='candlestick_events')
    response = sqs_queue.send_message(MessageBody=message)
    print(response.get('MessageId'))
    print(response.get('MD5OfMessageBody'))

if __name__ == '__main__':
    main(None, None)
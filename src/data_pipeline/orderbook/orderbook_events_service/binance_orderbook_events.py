import time
import boto3
import json
import ast
from binance.client import Client

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
get_secret_value_response = sm_client.get_secret_value(SecretId='Loidsig_CPM_Binance')
key, value = ast.literal_eval(get_secret_value_response['SecretString']).popitem()
bnb = Client(key, value)

def main(event, context):
    coins = ('ETHUSDT','BNBUSDT','BTCUSDT','LTCUSDT','ETHBTC','TRXETH','XRPETH','NEOETH', 'TUSDBNB', 'TUSDBTC', 'TUSDETH')
    for coin_pair in coins:
        json_message, unix_timestamp = get_orderbook_message(coin_pair)
        message_to_s3(json_message, coin_pair, unix_timestamp)
        #message_to_queue(json_message)

def get_orderbook_message(coin_pair):
    unix_timestamp = int(time.time())
    api_tries = 0
    while api_tries < 2:
        try:
            order_book = bnb.get_order_book(symbol=coin_pair, limit=1000)
            break
        except:
            api_tries += 1
            time.sleep(5)
    # Bids
    orderbook_bids = order_book['bids']
    # Asks
    orderbook_asks = order_book['asks']
    # Build message
    message = {}
    message['exchange'] = 'binance'
    message['coin_pair'] = coin_pair.lower().replace('-', '')
    message['unix_timestamp'] = unix_timestamp
    message['bids'] = orderbook_bids
    message['asks'] = orderbook_asks
    message_json = json.dumps(message)
    return message_json, unix_timestamp

def message_to_s3(json, coin_pair, timestamp):
    file_name = f"{timestamp}.json"
    file_path = f"binance/historic_orderbook_raw/{coin_pair}/{file_name}"
    s3_resource.Object(s3_bucket, file_path).put(Body=json)

def message_to_queue(message):
    # Send message
    sqs_resource = boto_session.resource('sqs', region_name='us-east-1')
    sqs_queue = sqs_resource.get_queue_by_name(QueueName='orderbook')
    response = sqs_queue.send_message(MessageBody=message)
    print(response.get('MessageId'))
    print(response.get('MD5OfMessageBody'))

if __name__ == '__main__':
    main(None, None)
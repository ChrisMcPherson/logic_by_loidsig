import time
import boto3
import json
from cobinhood_api import Cobinhood

# Instantiate resources
cob = Cobinhood()

try:
    boto_session = boto3.Session(profile_name='loidsig')
except:
    boto_session = boto3.Session()
s3_resource = boto_session.resource('s3')
s3_bucket = 'loidsig-crypto'

def main(event, context):
    coins = ('BTC-USDT','ETH-USDT','COB-USDT','ETH-BTC','TRX-ETH','EOS-ETH','LTC-USDT','NEO-ETH')
    for coin_pair in coins:
        json_message, unix_timestamp = get_orderbook_message(coin_pair)
        message_to_s3(json_message, coin_pair, unix_timestamp)
        message_to_queue(json_message)

def get_orderbook_message(coin_pair):
    unix_timestamp = int(time.time())
    order_book = cob.market.get_orderbooks(coin_pair, limit=100)
    # Bids - flatten volume/qty
    orderbook_bids = order_book['result']['orderbook']['bids']
    for bid in orderbook_bids:
        bid[1] = float(bid[1]) * float(bid[2])
        bid[2] = []
    # Asks - flatten volume/qty
    orderbook_asks = order_book['result']['orderbook']['asks']
    for ask in orderbook_asks:
        ask[1] = float(ask[1]) * float(ask[2])
        ask[2] = []
    # Build message
    message = {}
    message['exchange'] = 'cobinhood'
    message['coin_pair'] = coin_pair.lower().replace('-', '')
    message['unix_timestamp'] = unix_timestamp
    message['bids'] = orderbook_bids
    message['asks'] = orderbook_asks
    message_json = json.dumps(message)
    return message_json, unix_timestamp

def message_to_s3(json, coin_pair, timestamp):
    file_name = f"{coin_pair}/{timestamp}.json"
    file_path = f"cobinhood/historic_orderbook_raw/{file_name}"
    s3_resource.Object(s3_bucket, file_path).put(Body=json)

def message_to_queue(message):
    # Send message
    sqs_resource = boto_session.resource('sqs', region_name='us-east-1')
    sqs_queue = sqs_resource.get_queue_by_name(QueueName='cobinhood_orderbook')
    response = sqs_queue.send_message(MessageBody=message)
    print(response.get('MessageId'))
    print(response.get('MD5OfMessageBody'))

if __name__ == '__main__':
    main(None, None)
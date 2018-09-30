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
    coins = ('BTC-USDT','ETH-USDT','COB-USDT','ETH-BTC','TRX-ETH','EOS-ETH','LTC-USDT','NEO-ETH','ENJ-ETH')
    for coin_pair in coins:
        json_message = get_orderbook_message(coin_pair)
        message_to_queue(json_message)

def get_orderbook_message(coin_pair):
    unix_timestamp = int(time.time())
    order_book = cob.market.get_orderbooks(coin_pair, limit=100)
    # bids - flatten volume/qty
    orderbook_bids = order_book['result']['orderbook']['bids']
    for bid in orderbook_bids:
        bid[1] = float(bid[1]) * float(bid[2])
        bid[2] = []
    # asks - flatten volume/qty
    orderbook_asks = order_book['result']['orderbook']['asks']
    for ask in orderbook_asks:
        ask[1] = float(ask[1]) * float(ask[2])
        ask[2] = []
    # build message
    message = {}
    message['coin_pair'] = coin_pair.lower().replace('-', '')
    message['unix_timestamp'] = unix_timestamp
    message['bids'] = orderbook_bids
    message['asks'] = orderbook_asks
    message_json = json.dumps(message)
    return message_json

def message_to_queue(message):
    # Send message
    sqs_resource = boto_session.resource('sqs', region_name='us-east-1')
    sqs_queue = sqs_resource.get_queue_by_name(QueueName='cobinhood_orderbook')
    response = sqs_queue.send_message(MessageBody=message)
    print(response.get('MessageId'))
    print(response.get('MD5OfMessageBody'))

if __name__ == '__main__':
    main(None, None)
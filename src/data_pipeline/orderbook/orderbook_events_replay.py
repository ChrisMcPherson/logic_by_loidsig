import boto3
import time
import ast
import itertools
import psycopg2

# Config
s3_bucket = 'loidsig-crypto'
s3_prefixes = ['binance/historic_orderbook_raw', 'cobinhood/historic_orderbook_raw'] #BTCUSDT/153874
sqs_queue_name = 'raw_orderbook_events'
max_queue_inflight = 100000
replay_specific_keys = False

# AWS resources
try:
    boto_session = boto3.Session(profile_name='loidsig')
except:
    boto_session = boto3.Session()
s3_resource = boto_session.resource('s3')
s3_client = boto_session.client('s3')
sqs_resource = boto_session.resource('sqs', region_name='us-east-1')
sqs_queue = sqs_resource.get_queue_by_name(QueueName='raw_orderbook_events')

def main():
    processed_messages = 0
    start = time.time()
    for s3_prefix in s3_prefixes:
        s3_key_list = get_s3_keys_from_prefix(s3_prefix)
        if replay_specific_keys:
            exchange = s3_prefix.partition('/')[0]
            prefix_to_replace_list = get_keys_to_replay(exchange)
            s3_key_list = [s3_key for s3_key in s3_key_list if any(prefix in s3_key for prefix in prefix_to_replace_list)]
        print(f"{len(s3_key_list)} keys to replay")
        # Continue while list is not empty
        while s3_key_list:
            # Continue until queue is full then slowly trickle new messages
            empty_queue_spaces = max_queue_inflight - get_queue_num_inflight()
            while empty_queue_spaces > 0:
                try:
                    s3_key = s3_key_list[0]
                except IndexError:
                    break
                send_s3_object_to_queue(s3_key)
                s3_key_list.pop(0)
                processed_messages += 1
                if processed_messages % 10000 == 0:
                    print(f"Messages processed: {processed_messages} in {(time.time() - start) / 60} Minutes")
                    start = time.time()
            time.sleep(5)
    print(f"Processed {processed_messages} messages.")

def get_s3_keys_from_prefix(s3_prefix):
    paginator = s3_client.get_paginator('list_objects')
    operation_parameters = {'Bucket': s3_bucket,
                            'Prefix': s3_prefix}
    page_iterator = paginator.paginate(**operation_parameters)
    s3_keys = []
    for page in page_iterator:
        page_s3_keys = [key['Key'] for key in page['Contents']]
        s3_keys.extend(page_s3_keys)
    return s3_keys

def get_queue_num_inflight():
    sqs_attributes = sqs_queue.attributes
    return int(sqs_attributes['ApproximateNumberOfMessages'])

def get_keys_already_played():
    pass

def get_keys_to_replay(exchange):
    logic_db_conn = logic_db_connection()
    cur = logic_db_conn.cursor()

    sql = f"""
            SELECT  array_agg(CONCAT(UPPER(coin_pair), '/', LEFT((trade_minute*60)::text, 8)))
            FROM {exchange}.orderbook
            WHERE bids_cum_5000_weighted_avg = 0 OR asks_cum_5000_weighted_avg = 0
            ;"""
    cur.execute(sql)
    return cur.fetchall()[0][0]

def send_s3_object_to_queue(s3_key):
    obj = get_s3_object(s3_key)
    sqs_queue.send_message(MessageBody=obj)

def get_s3_object(s3_key):
    obj = s3_resource.Object(s3_bucket, s3_key)
    return obj.get()['Body'].read().decode('utf-8') 

def logic_db_connection():
    """Fetches Logic DB postgres connection object

    Returns:
        A database connection object for Postgres
    """
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
    main()
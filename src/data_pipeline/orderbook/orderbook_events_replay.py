import boto3
import time
import itertools

# Config
s3_bucket = 'loidsig-crypto'
s3_prefixes = ['binance/historic_orderbook_raw']#, 'cobinhood/historic_orderbook_raw']
sqs_queue_name = 'raw_orderbook_events'
max_queue_inflight = 100000

# AWS resources
try:
    boto_session = boto3.Session(profile_name='loidsig')
except:
    boto_session = boto3.Session()
s3_resource = boto_session.resource('s3')


def main():
    processed_messages = 0
    for s3_prefix in s3_prefixes:
        s3_key_gen = get_s3_keys_from_prefix(s3_bucket, s3_prefix)
        res = peek(s3_key_gen)
        # Continue while generator is not empty
        while res is not None:
            # Continue until queue is full then slowly trickle new messages
            while get_queue_num_inflight(sqs_queue_name) < max_queue_inflight:
                first, s3_key_gen = res
                s3_key = next(s3_key_gen)
                send_s3_object_to_queue(s3_key)
                processed_messages += 1
            time.sleep(5)
            res = peek(s3_key_gen)
    print(f"Processed {processed_messages} messages.")

def get_s3_objs_from_prefix(bucket, prefix='', suffix=''):
    """
    Generate objects in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    s3 = boto_session.client('s3')
    kwargs = {'Bucket': bucket}
    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix
    while True:
        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = s3.list_objects_v2(**kwargs)
        try:
            contents = resp['Contents']
        except KeyError:
            return
        for obj in contents:
            key = obj['Key']
            if key.startswith(prefix) and key.endswith(suffix):
                yield obj
        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

def get_s3_keys_from_prefix(bucket, prefix='', suffix=''):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    for obj in get_s3_objs_from_prefix(bucket, prefix, suffix):
        yield obj['Key']

def peek(iterable):
    try:
        first = next(iterable)
    except StopIteration:
        return None
    return first, itertools.chain([first], iterable)

def get_queue_num_inflight(queue):
    sqs_resource = boto_session.resource('sqs', region_name='us-east-1')
    sqs_queue = sqs_resource.get_queue_by_name(QueueName=queue)
    sqs_attributes = sqs_queue.attributes
    return int(sqs_attributes['ApproximateNumberOfMessages'])

def get_keys_already_processed():
    pass

def send_s3_object_to_queue(s3_key):
    pass

def get_s3_object(s3_key):
    pass

if __name__ == '__main__':
    main()
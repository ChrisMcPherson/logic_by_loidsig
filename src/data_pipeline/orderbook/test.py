import os
import pandas as pd
from io import StringIO
import time
import boto3
from cobinhood_api import Cobinhood


# Instantiate resources
cob = Cobinhood()

try:
    boto_session = boto3.Session(profile_name='loidsig')
except:
    boto_session = boto3.Session()

sqs = boto_session.resource('sqs', region_name='us-east-1')

# Get the queue
queue = sqs.get_queue_by_name(QueueName='cobinhood_orderbook')

# Process messages by printing out body and optional author name
for message in queue.receive_messages():
    # Get the custom author message attribute if it was set
    author_text = ''
    if message.message_attributes is not None:
        author_name = message.message_attributes.get('Author').get('StringValue')
        if author_name:
            author_text = ' ({0})'.format(author_name)

    # Print out the body and author (if set)
    print('Hello, {0}!{1}'.format(message.body, author_text))

    # Let the queue know that the message is processed
    message.delete()
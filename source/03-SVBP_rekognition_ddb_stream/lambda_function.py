from __future__ import print_function

import json
import boto3
import decimal
import os
import logging

# Lambda Variables
LOG_LEVEL = str(os.environ.get('LOG_LEVEL', 'INFO')).upper()

if LOG_LEVEL not in ['DEBUG', 'INFO','WARNING', 'ERROR','CRITICAL']:
    LOG_LEVEL = 'INFO'
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)

# Constants

# Global Variables

# Lambda Variables
SVBP_rekognition_iot =  os.getenv('SVBP_rekognition_iot')

# Services
dynamodb = boto3.resource('dynamodb')
dynamodb_processing = dynamodb.Table('svbp_processing')
dynamodb_results = dynamodb.Table('svbp_results')
lambda_client = boto3.client('lambda', region_name=os.environ['AWS_DEFAULT_REGION'])


# Helper section
def publish_message(payload):
    lambda_client.invoke(
        FunctionName=SVBP_rekognition_iot,
        InvocationType='Event',
        LogType='None',
        Payload=payload
    )

def get_item_from_ddb(ddb_object, hashkey, value):
    item = ddb_object.get_item(Key={hashkey: value})['Item']
    print("Item is: '{}'".format(item))
    return item

# Functions
def lambda_handler(event, context):
    logger.debug(event)
    key = event['Records'][0]['dynamodb']['Keys']['object_id']['S']

    # Obtain updated data from DDB
    row_processing_table = get_item_from_ddb(dynamodb_processing,'object_id',key)
    row_results_table = get_item_from_ddb(dynamodb_results,'object_id',key)

    # Check if the processing is finished
    logger.debug(row_processing_table)
    processing_list = row_processing_table['processingList']

    local_sum = 0
    for key, value in processing_list.iteritems():
        if value != "done":
            local_sum += 1

    if local_sum == 0:
        payload = json.dumps({
            'topic': row_results_table['iot_topic'],
            'type': 'redirect',
            'payload': {'identifier': row_processing_table['object_id']}
        })
        publish_message(payload)

    return 'Successfully processed {} records.'.format(len(event['Records']))

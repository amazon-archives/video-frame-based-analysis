from __future__ import print_function

import json
import boto3
from botocore.client import Config
from boto3.dynamodb.conditions import Key, Attr
import threading
import time
import decimal, sys, traceback, os
import urllib
import datetime
import logging

'''
This function waits the video processing completion, collects all analyzed frames from TABLE,
and create a timeline for each frame.
'''

# LOGGER SETUP
# Lambda Variables
LOG_LEVEL = str(os.environ.get('LOG_LEVEL', 'INFO')).upper()

if LOG_LEVEL not in ['DEBUG', 'INFO','WARNING', 'ERROR','CRITICAL']:
    LOG_LEVEL = 'INFO'
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)

logger.debug('Loading function')

# VARIABLES
RVA_VIDEOS_LABELS_TABLE='RVA_VIDEOS_LABELS_TABLE'

# CONSTANTS
DDB_BATCH_SIZE=50

# Lambda Variables
S3_VIDEO_BUCKET = os.environ['S3_VIDEO_BUCKET']
VFBA_SNS_RESULTS_TOPIC_ARN = os.environ['VFBA_SNS_RESULTS_TOPIC_ARN']

# SERVICES SETUP
s3_client = boto3.client('s3')
dynamodb_client = boto3.resource('dynamodb', config=Config(max_pool_connections=30))
videos_labels_table  = dynamodb_client.Table(RVA_VIDEOS_LABELS_TABLE)
sns_client = boto3.client('sns')


def publish_result(bucket, key):
    try:
        result = {
            "Type" : "TAGS",
            "Bucket" : bucket,
            "Key" : key
        }

        response = sns_client.publish(
            TopicArn=VFBA_SNS_MILESTONES_TOPIC_ARN,
            Message=json.dumps(result)
        )
        logger.debug("Message published.")
    except Exception as e:
        logger.error("Error publishing result '{}/{}'".format(bucket, key))
        logger.error(e)


def parse_items(labels, items):
    logger.debug(">parse_items items:\n{}".format(items))
    for item in items:
        try:
            time = int(item['Time']['S'])/1000

            for entry in item['Labels']['L']:
                label = entry['S']

                if label in labels:
                    labels[label].append(time)
                else:
                    labels[label] = [time]
        except Exception as e:
            logger.error("Failure parsing DynamoDB response. Skipping and moving on...")
            logger.error(e)
            traceback.print_exc(file=sys.stdout)

    return labels


def convert_to_key_value(labels):
    logger.debug(">convert_to_key_value labels: '{}'".format(labels))

    converted = {
        "tags" : []
    }

    for k in labels:
        entry = { "tag" : "", "times": [] }
        entry['tag'] = k
        entry['times'] = labels[k]
        converted['tags'].append(entry)

    return converted

def get_labels_from_ddb(key):
    logger.debug(">get_labels_from_ddb key: '{}'".format(key))

    labels = {}

    ddb_client = boto3.client("dynamodb")
    paginator = ddb_client.get_paginator("query")

    pages = paginator.paginate(
        TableName=RVA_VIDEOS_LABELS_TABLE,
        ConsistentRead=False,
        KeyConditionExpression="Identifier = :k",
        ExpressionAttributeValues={
            ":k" : { "S" : key }
        },
        PaginationConfig={'PageSize': DDB_BATCH_SIZE },
        ProjectionExpression="Labels, #t",
        ExpressionAttributeNames={"#t":"Time"},
	    ReturnConsumedCapacity='TOTAL'
    )

    for page in pages:
        try:
            logger.debug("Page:\n{}".format(page))

            count = int(page['Count'])

            if count > 0:
                parse_items(labels, page['Items'])
        except Exception as e:
            logger.error(e)

    logger.debug(json.dumps(labels, sort_keys=True,
        indent=4, separators=(',', ': ')))

    return labels

def lambda_handler(event, context):
    logger.debug(">lambda_handler event: '{}'".format(json.dumps(event)))

    labels = []

    for record in event['Records']:
        if 'Sns' in record:
            message = json.loads(record['Sns']['Message'])
            key = message['Identifier']
            labels = get_labels_from_ddb(key)
            formated  = convert_to_key_value(labels)

            logger.debug("Result: \n{}".format(json.dumps(formated)))

            tmp_file = "/tmp/" + key + "-tags.json"

            with open(tmp_file, 'w') as outfile:
                txt = json.dumps(formated)
                outfile.write(txt)

            result_key = "labels/{}.json".format(key)

            s3_client.upload_file(tmp_file, S3_VIDEO_BUCKET, result_key, ExtraArgs={"ServerSideEncryption": "AES256"})

            publish_result(S3_VIDEO_BUCKET, result_key)

    return "Labels found: '{}'".format(len(labels))

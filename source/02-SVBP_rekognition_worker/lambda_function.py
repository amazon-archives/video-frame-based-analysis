from __future__ import print_function

import json
import urllib
import boto3
from botocore.client import Config
import threading
import sys, traceback
import logging
import os
import re

# Lambda Variables
LOG_LEVEL = str(os.environ.get('LOG_LEVEL', 'INFO')).upper()

if LOG_LEVEL not in ['DEBUG', 'INFO','WARNING', 'ERROR','CRITICAL']:
    LOG_LEVEL = 'INFO'
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)

logger.debug('Loading function')

# Constants

# Global Variables

# Lambda Variables
SVBP_REKOGNITION_IOT = os.environ['SVBP_rekognition_iot']
METRICS_FUNCTION = os.environ['metrics_function']

# Services
s3 = boto3.client('s3')
rekognition = boto3.client('rekognition')
dynamodb = boto3.resource('dynamodb', config=Config(max_pool_connections=30))
dynamodb_processing = dynamodb.Table('svbp_processing')
dynamodb_results = dynamodb.Table('svbp_results')
lambda_client = boto3.client('lambda', region_name=os.environ['AWS_DEFAULT_REGION'])


# ------- Publish message to IoT ------- #
def publish_message(payload):
    lambda_client.invoke(
        FunctionName=SVBP_REKOGNITION_IOT,
        InvocationType='Event',
        LogType='None',
        Payload=payload
    )

# ------- Create DynamoDB item ------- #
def create_dynamodb(eTag, IotTopic):
    create_dynamodb = dynamodb_results.put_item(
       Item={
            'object_id': eTag,
            'iot_topic': IotTopic,
            'results': []
        }
    )

# ------- Update DynamoDB processing table ------- #
def update_dynamodb_processing(eTag, objectKey, imageKey):
    logger.debug('objectKey = '+objectKey)
    update_dynamodb = dynamodb_processing.update_item(
        Key = {'object_id': eTag},
        UpdateExpression = "SET processingList.#objectKey = :keyValue",
        ExpressionAttributeNames = {
            '#objectKey' : objectKey
        },
        ExpressionAttributeValues = {
            ":keyValue": 'done'
        }
    )


# ------- Update DynamoDB results table ------- #
def update_dynamodb_results(eTag, faceId, imageId, similarity, collection, videoId):
    update_dynamodb = dynamodb_results.update_item(
        Key = {'object_id': eTag},
        UpdateExpression = "SET results = list_append(results, :faceData)",
        ExpressionAttributeValues = {
            ":faceData": [{"FaceId": faceId,"VideoId": videoId, "ImageId": imageId, "Similarity": similarity, "Collection": collection}]
        }
    )

# ------- Obtain DynamoDB item from any table ------- #
def get_item_from_ddb(ddb_object, hashkey, value):
    return ddb_object.get_item(Key={hashkey: value})['Item']


def rekognition_search_faces_by_image(collection, imageBucket, imageKey):
    return rekognition.search_faces_by_image(
        Image={"S3Object": {"Bucket": imageBucket, "Name": imageKey}},
        CollectionId=collection,
        FaceMatchThreshold=85
    )


def search_faces(eTag, imageBucket, imageKey, collection):
    try:
        response = rekognition_search_faces_by_image(collection, imageBucket, imageKey)
        logger.debug("Searching face in collection "+collection)
        faceMatches = response['FaceMatches']

        if faceMatches:
            for face in faceMatches:
                faceId = face['Face']['FaceId']
                imageId = face['Face']['ExternalImageId']
                similarity = str(face['Similarity'])
                videoId = re.match('(.*)-\d{5}.jpg', imageId).group(1)

                logger.debug("VideoId is: "+videoId)
                logger.debug("Similarity is: "+similarity)
                logger.debug("eTag is: "+eTag)

                update_dynamodb_results(eTag, faceId, imageId, similarity, collection, videoId)

    except Exception as e:
        logger.error(e)
        logger.error('-' * 60)
        traceback.print_exc(file=sys.stdout)
        logger.error('-' * 60)

def lambda_handler(event, context):
    initial_percentage = 20.0
    final_percentage = 95.0

    bucket = event['Records'][0]['s3']['bucket']['name']
    objectKey = event['Records'][0]['s3']['object']['key'].split('/')[2]
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))

    s3Object = s3.get_object(Bucket=bucket, Key=key)
    objectContent = s3Object['Body'].read().decode('utf-8')
    objectContent = json.loads(objectContent)

    imageBucket = objectContent['objectBucket']
    imageKey = objectContent['objectKey']

    eTag = objectContent['eTag']

    get_iot_topic = s3.head_object(
        Bucket=imageBucket,
        Key=imageKey,
    )

    if 'topic' in get_iot_topic['Metadata']:
        IotTopic = get_iot_topic['Metadata']['topic']
    else:
        IotTopic = "none"

    create_dynamodb(eTag, IotTopic)

    collections = objectContent['collections']

    thread_list = []
    for collection in collections:
        logger.debug('Searching collection: ' + collection)
        t1 = threading.Thread(name='search_faces' + collection, target=search_faces, args=(eTag, imageBucket, imageKey, collection))
        thread_list.append(t1)

    for x in thread_list:
        x.start()

    for x in thread_list:
        x.join()

    logger.debug(thread_list)

    update_dynamodb_processing(eTag, objectKey, imageKey)

    # Obtein updated item from DDB.
    updated_item = get_item_from_ddb(dynamodb_processing, 'object_id', eTag)

    # Calculate the percentage of process is already completed.
    part_dict = updated_item['processingList']
    number_of_items = len(part_dict)
    local_sum = 0
    for key, value in part_dict.iteritems():
        if value == "done":
            local_sum += 1

    calculated_value = int(round(initial_percentage + (
                    ((final_percentage - initial_percentage) / number_of_items) * local_sum)))

    # publish message to IoT
    if IotTopic != "none":
        payload = json.dumps({
            'topic': IotTopic,
            'type': 'status',
            'payload': {'message': 'Searching image against database', 'percentage': calculated_value}
        })

        publish_message(payload)

    # send anonymous metrics
    metrics_payload = json.dumps({"Data": {"PhotosProcessed": 1}})
    lambda_client.invoke(
        FunctionName=METRICS_FUNCTION,
        InvocationType='Event',
        LogType='None',
        Payload=metrics_payload)

    return 'Done'

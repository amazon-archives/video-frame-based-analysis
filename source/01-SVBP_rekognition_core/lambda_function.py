from __future__ import print_function

import boto3
from botocore.client import Config
import json
import decimal
from decimal import Decimal
import logging
import os
from rekog_collection_controller import RekognitionCollectionController

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
RVA_COLLECTION_CONTROL = 'RVA_COLLECTION_CONTROL_TABLE'

# Global Variables

# Services
rekognition = boto3.client('rekognition')
s3 = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb', config=Config(max_pool_connections=30))
dynamodb_table = dynamodb.Table('svbp_processing')


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


# ------- Create DynamoDB item ------- #
def create_dynamodb(eTag, bucket, objectKey):
    create_dynamodb = dynamodb_table.put_item(
       Item={
            'object_id': eTag,
            's3_path': str(bucket+'/processing/'+eTag),
            'object_key': str(objectKey),
            'processingList': {}
        }
    )

def update_dynamodb(eTag, key_name, keyId):
    update_dynamodb = dynamodb_table.update_item(
        Key = {'object_id': eTag},
        UpdateExpression = "SET processingList.#keyProp = :keyValue",
        ExpressionAttributeNames = {
            '#keyProp' : key_name
        },
        ExpressionAttributeValues = {
            ":keyValue": 'pending'
        }
    )
    logger.debug(json.dumps(update_dynamodb, indent=4, cls=DecimalEncoder))


# had to create this to be able to patch during unit testing
def rekognition_list_collections():
    return rekognition.list_collections()


# Allow mocking during For testing
def init_RekognitionCollectionController():
    return RekognitionCollectionController(RVA_COLLECTION_CONTROL, 'COLLECTIONS')


def lambda_handler(event, context):
    logger.debug(event)

    # Get object informations
    eTag = event['Records'][0]['s3']['object']['eTag']
    objectKey = event['Records'][0]['s3']['object']['key']
    bucket = event['Records'][0]['s3']['bucket']['name']
    rcc = init_RekognitionCollectionController()


    # Create the DynamoDB entry, so we can update it later
    create_dynamodb(eTag, bucket, objectKey)

    keyId = 0

    # Get collectionIDs from Rekognition
    collectionIds = rekognition_list_collections()
    #collectionIds = collectionIds['CollectionIds']
    collectionIds = rcc.list_collections()
    collectionIds = [collectionIds[i:i+10] for i in range(0, len(collectionIds), 10)]

    # Create json with data to be processed by the workers
    processFile = {}
    processFile.update({'eTag': eTag, 'objectBucket': bucket, 'objectKey': objectKey})

    # Create files with maximum 10 collectionIDs
    for i in collectionIds:
        keyId = keyId + 1
        processFile.update({"collections": i})
        saveObject = s3.Object(bucket, 'processing/'+eTag+'/key'+str(keyId)+'.json').put(Body=json.dumps(processFile),ServerSideEncryption='AES256')
        key_name = 'key'+str(keyId)+'.json'
        update_dynamodb(eTag, key_name, keyId)
        logger.debug('Sucessfully updated')


    return collectionIds

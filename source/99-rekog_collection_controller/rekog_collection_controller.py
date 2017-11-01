from __future__ import print_function

from botocore.client import Config
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
import json
import boto3
import sys, traceback, os
import logging
import time
import string
import random

# Lambda Variables
LOG_LEVEL = str(os.environ.get('LOG_LEVEL', 'INFO')).upper()

if LOG_LEVEL not in ['DEBUG', 'INFO','WARNING', 'ERROR','CRITICAL']:
    LOG_LEVEL = 'INFO'
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)

# Constants
MAX_DYNAMODB_TPS_ALLOWED=40
MAX_BACKOFF = 15 # seconds
MAX_RETRIES = 5
RETRY_EXCEPTIONS = ('ProvisionedThroughputExceededException',
                    'ThrottlingException')

# Services

class RekognitionCollectionController:

    dva_collection_prefix = 'DVA-'
    dva_collection_max_size = 100000

    control_table_id = ""
    control_record_id = ""
    dynamodb_client = None
    rekognition = None


    def __init__(self, control_table_id, control_record_id):
        self.control_table_id = control_table_id
        self.control_record_id = control_record_id
        self.dynamodb_client = boto3.client('dynamodb')
        self.rekognition = boto3.client('rekognition')

    def set_collection_prefix(self, collection_prefix):
        self.dva_collection_prefix = collection_prefix


    def set_collection_size(self, collection_size):
        self.dva_collection_max_size = collection_size


    def id_generator(self, size=10, chars=string.ascii_uppercase + string.digits):
        return self.dva_collection_prefix + ''.join(random.choice(chars) for _ in range(size))


    def create_collection(self):
        collection_id = self.id_generator()
        r = self.create_collection_call(collection_id)
        max_tries = 0

        while r['StatusCode'] != 200:
            collection_id = self.id_generator()
            r = self.create_collection_call(collection_id)
            max_tries += 1
            if max_tries > 10:
                raise RuntimeError('Error creating a new collection. Maximum number of retries exceeded.')

        return collection_id


    def fetch_collection(self):
        r = self.get_current_collection()
        collection_id = ""

        if r is None: # No record to control collections found
            collection_id = self.create_collection()
            self.create_new_control_record(collection_id)
        else:
            curr_coll_size = int(r['Count']['N'])

            #TODO: Move to increment
            if curr_coll_size > self.dva_collection_max_size:
                logger.debug("Collection bigger than MAX collection size, starting a new one...")
                #create a new collection
                collection_id = self.create_collection()
                #update current to point to the new collection
                self.append_collection(collection_id)
            else:
                collection_id = r['Current']['S']

        return collection_id


    def create_collection_call(self, collection_id):
        logger.debug(">create_collection_call '{}'".format(collection_id))

        return self.rekognition.create_collection(CollectionId=collection_id)


    def create_new_control_record(self, collection_id):
        logger.debug(">create_current_alias CollId: '{}'".format(collection_id))

        r = self.dynamodb_client.update_item(
            TableName=self.control_table_id,
            Key={"Identifier": {"S": self.control_record_id}},
            UpdateExpression="SET #curr = :newvalue, #cnt = :count, #cols = :coll_id", #list_append(if_not_exists(#cols, :empty_list), :coll_id)",
            ExpressionAttributeNames={
                '#curr': 'Current',
                '#cnt': 'Count',
                '#cols': 'CollectionIds'
            },
            ExpressionAttributeValues={
                ':newvalue': {'S': collection_id},
                ':count': {'N': "0"},
                ':coll_id': {'L': [{'S': collection_id}]}
            },
            ReturnConsumedCapacity='TOTAL'
        )

        logger.debug("New control record created. Response: '{}'".format(json.dumps(r)))

    def append_collection(self, collection_id):
        logger.debug(">append_collection CollId: '{}'".format(collection_id))

        r = self.dynamodb_client.update_item(
            TableName=self.control_table_id,
            Key={"Identifier": {"S": self.control_record_id}},
            UpdateExpression="SET #curr = :newvalue, #cnt = :count, #cols = list_append(#cols, :coll_id)",
            ExpressionAttributeNames={
                '#curr': 'Current',
                '#cnt': 'Count',
                '#cols': 'CollectionIds'
            },
            ExpressionAttributeValues={
                ':newvalue': {'S': collection_id},
                ':count': {'N': "0"},
                ':coll_id': {'L': [{'S': collection_id}]}
            },
            ReturnConsumedCapacity='TOTAL'
        )

        logger.debug("New collection appended. Response: '{}'".format(json.dumps(r)))

    def increment_collection_count(self, collection_id, faces_indexed):
        logger.debug(">increment_collection_count CollId: '{}'".format(collection_id))

        new_cnt = 0

        r = self.dynamodb_client.update_item(
            TableName=self.control_table_id,
            Key={"Identifier": {"S": self.control_record_id}},
            UpdateExpression="SET #cnt = #cnt + :count",
            ReturnValues='UPDATED_NEW',
            ExpressionAttributeNames={
            #  '#curr': 'Current',
                '#cnt': 'Count',
            },
            #Quit trying to make this work
            # #ConditionExpression={
            #    'Current = :col_id'
            #},
            ExpressionAttributeValues={
                ':count': {'N': str(faces_indexed) },
                #':col_id': {'S': str(collection_id) }
            },
            ReturnConsumedCapacity='TOTAL'
        )

        if 'Attributes' in r:
            new_cnt = int(r[ 'Attributes'] [ 'Count' ] [ 'N' ])

        return new_cnt

    def list_collections(self):
        collections = []

        r = self.dynamodb_client.get_item(
            TableName=self.control_table_id,
            Key={"Identifier": {"S": self.control_record_id}},
            ConsistentRead=True,
            ReturnConsumedCapacity='TOTAL'
        )

        if 'Item' in r:
            for coll_id in r['Item']['CollectionIds']['L']:
                collections.append(coll_id['S'])

        return collections

    def get_current_collection(self):
        response = None

        r = self.dynamodb_client.get_item(
            TableName=self.control_table_id,
            Key={"Identifier": {"S": self.control_record_id}},
            ConsistentRead=True,
            ReturnConsumedCapacity='TOTAL'
        )

        logger.debug("Response: '{}'".format(json.dumps(r)))

        if 'Item' in r:
            response = r['Item']

        logger.debug("get_current_collection response: {}".format(json.dumps(response)))

        return response

import unittest
import boto3
import json
import mock

from lambda_function import lambda_handler
from moto import mock_s3, mock_dynamodb2
#from awscli.customizations.s3.subcommands import METADATA


class TestS3Actor(unittest.TestCase):

    @mock_s3
    @mock_dynamodb2
    @mock.patch('lambda_function.rekognition_list_collections')
    def test_lambda_handler(self, rlc):
        with open('rekognition_list_collections_response.json') as rekognition_file:
            rlc.return_value = json.load(rekognition_file)
            test_collection_ids = rlc.return_value
            test_collection_ids = test_collection_ids['CollectionIds']
            test_collection_ids = [test_collection_ids[i:i + 10] for i in range(0, len(test_collection_ids), 10)]

        s3 = boto3.resource('s3', region_name='us-west-2')
        # We need to create the bucket since this is all in Moto's 'virtual' AWS account
        s3.create_bucket(Bucket='deep-video-rekognition-photo')
        with open('event.json') as data_file:
            put_event = json.load(data_file)
        dynamodb = boto3.client('dynamodb')
        dynamodb_resource_client = boto3.resource('dynamodb')
        process_table = 'svbp_processing'
        dynamodb.create_table(TableName=process_table,
                          KeySchema=[{'AttributeName': 'object_id', 'KeyType': 'HASH'}],
                          AttributeDefinitions=[{'AttributeName': 'object_id', 'AttributeType': 'S'}],
                          ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5})

        print(put_event['Records'][0]['s3']['bucket']['name'])
        test_context = ''
        
        # Verify lambda function returns the correct status
        assert lambda_handler(put_event,test_context) == test_collection_ids

import unittest
import boto3
import json
import urllib
import mock
import threading

import lambda_function
from moto import mock_s3, mock_dynamodb2
#from awscli.customizations.s3.subcommands import METADATA

class MockLambdaClient(object):
    def __init__(self):
        pass

    def invoke(self,FunctionName,InvocationType,LogType,Payload):
        print(Payload)
        return Payload

class MockThreading(object):
    def __init__(self):
        pass

    def Thread(self,name, target, args):
        lambda_function.search_faces(args[0],args[1], args[2], args[3])
        print(args)
        return threading.Thread()

class MockThreadList(object):
    def __init__(self):
        pass

    def append(self,thread):
        return []

class TestS3Actor(unittest.TestCase):

    mock_iot_lambda = MockLambdaClient()
    mock_thread = MockThreading()
    mock_thread_list = MockThreadList()
    @mock_s3
    @mock_dynamodb2
    @mock.patch('lambda_function.rekognition_search_faces_by_image')
    @mock.patch.object(lambda_function,'lambda_client',mock_iot_lambda)
    @mock.patch.object(lambda_function,'threading',mock_thread)
    def test_lambda_handler(self, rsfbi):
        with open('search_faces_by_image_response.json') as rekognition_file:
            rsfbi.return_value = json.load(rekognition_file)
        s3 = boto3.resource('s3', region_name='us-west-2')
        # We need to create the bucket since this is all in Moto's 'virtual' AWS account
        with open('event.json') as data_file:
            put_event = json.load(data_file)
        with open('key1.json') as key_file:
            object_body = json.load(key_file)
        bucket = put_event['Records'][0]['s3']['bucket']['name']
        objectKey = put_event['Records'][0]['s3']['object']['key'].split('/')[2]
        key = urllib.unquote_plus(put_event['Records'][0]['s3']['object']['key'].encode('utf8'))
        s3.create_bucket(Bucket=bucket)
        object = s3.Object(bucket,key)
        object.put(Body=json.dumps(object_body))
        image_key = object_body['objectKey']
        image_object = s3.Object(bucket,image_key)
        image_object.put(Body='', Metadata={'topic': 'iot_topic'})
        eTag = object_body['eTag']

        dynamodb = boto3.client('dynamodb')
        dynamodb_resource_client = boto3.resource('dynamodb')
        process_table_name = 'svbp_processing'
        dynamodb.create_table(TableName=process_table_name,
                          KeySchema=[{'AttributeName': 'object_id', 'KeyType': 'HASH'}],
                          AttributeDefinitions=[{'AttributeName': 'object_id', 'AttributeType': 'S'}],
                          ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5})
        results_table_name = 'svbp_results'
        dynamodb.create_table(TableName=results_table_name,
                          KeySchema=[{'AttributeName': 'object_id', 'KeyType': 'HASH'}],
                          AttributeDefinitions=[{'AttributeName': 'object_id', 'AttributeType': 'S'}],
                          ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5},
                          StreamSpecification={'StreamEnabled': True,'StreamViewType': 'NEW_AND_OLD_IMAGES'})
        process_table = dynamodb_resource_client.Table(process_table_name)

        create_dynamodb = process_table.put_item(
            Item={
                'object_id': eTag,
                's3_path': str(bucket + '/processing/' + eTag),
                'object_key': str(objectKey),
                'processingList': { "key1.json" : "started" }
            }
        )

        test_context = ''
        
        # Verify lambda function returns the correct status
        # TODO - not sure why the exception is raised on the first uupdate_dynamodb_results in search_faces
        #       but the test seems to execute so we'll leave this to fix another time. The exception is caught and processing continues
        assert lambda_function.lambda_handler(put_event,test_context) == 'Done'
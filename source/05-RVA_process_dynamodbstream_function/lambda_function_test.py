import unittest
import mock
import json
import boto3
import logging
import os
import lambda_function
from moto import mock_s3, mock_dynamodb2, mock_dynamodb

videos_results_table_name = 'RVA_VIDEOS_RESULTS_TABLE'
videos_process_table = 'RVA_PROCESS_TABLE'

logger = logging.getLogger()
logger.level = logging.DEBUG

class MockRekognitionClient(object):

    def __init__(self):
        pass

    def create_collection(self,CollectionId):
        return None
    def delete_collection(self,CollectionId):
        return None

class MockLambdaClient(object):
    def __init__(self):
        pass

    def invoke(self,FunctionName,InvocationType,LogType,Payload):
        print(Payload)
        return Payload

class MockRcc(object):
    def __init__(self):
        pass

    def fetch_collection(str):
        return "DVA-00000"


class TestActor(unittest.TestCase):

    mock_rekognition = MockRekognitionClient()
    mock_lambda = MockLambdaClient()
    mock_rcc = MockRcc()
    test_video_identifier = 'fun-at-fair-343902224.mp4'
    
    def setUp(self):
        #Check required env variables
        assert "5" == os.environ['MAX_TPS']
        self.assertNotEqual(None, os.environ['RVA_IoT_publish_message_function'])
        self.assertNotEqual(None, os.environ['RVA_consolidate_results_function'])
        self.assertNotEqual(None, os.environ['RVA_crop_orchestration_function'])
        self.assertNotEqual(None, os.environ['RVA_process_photos_function'])
        self.assertNotEqual(None, os.environ['LOG_LEVEL'])
        self.assertNotEqual(None, os.environ['RVA_SNS_MILESTONES_TOPIC_ARN'])

    @mock_s3
    @mock_dynamodb2
    @mock.patch('lambda_function.execute_lambda_process_photos')
    @mock.patch('lambda_function.init_RekognitionCollectionController', return_value=mock_rcc)
    @mock.patch('lambda_function.publish_message')
    def test_all_pending(self, mock_pub, mock_rcc, mock_exec_lambda):
        context = ''
        
        dynamodb = boto3.client('dynamodb')
        dynamodb_resource_client = boto3.resource('dynamodb')

        create_VIDEOS_PROCESS_TABLE(dynamodb)
        create_VIDEOS_RESULTS_TABLE_NAME(dynamodb)

        with open('test_events/event_all_pending.json') as data_file:
            event = json.load(data_file)

        handler = lambda_function.lambda_handler(event, context)
        
        assert 5 == mock_exec_lambda.call_count

    @mock_s3
    @mock_dynamodb2
    @mock.patch('lambda_function.execute_lambda_process_photos')
    @mock.patch('lambda_function.init_RekognitionCollectionController', return_value=mock_rcc)
    @mock.patch('lambda_function.publish_message')
    def test_three_pending(self, mock_pub, mock_rcc, mock_exec_lambda):
        context = ''
        
        dynamodb = boto3.client('dynamodb')
        dynamodb_resource_client = boto3.resource('dynamodb')

        create_VIDEOS_PROCESS_TABLE(dynamodb)
        create_VIDEOS_RESULTS_TABLE_NAME(dynamodb)

        with open('test_events/event_three_pending.json') as data_file:
            event = json.load(data_file)

        handler = lambda_function.lambda_handler(event, context)
        
        assert 3 == mock_exec_lambda.call_count

    @mock_s3
    @mock_dynamodb2
    @mock.patch('lambda_function.execute_lambda_process_photos')
    @mock.patch('lambda_function.publish_message')
    def test_all_completed(self, mock_pub, mock_exec_lambda):
        context = ''

        dynamodb = boto3.client('dynamodb')
        dynamodb_resource_client = boto3.resource('dynamodb')

        create_VIDEOS_PROCESS_TABLE(dynamodb)
        create_VIDEOS_RESULTS_TABLE_NAME(dynamodb)

        with open('test_events/event_all_completed.json') as data_file:
            event = json.load(data_file)

        #print(json.dumps(event))

        handler = lambda_function.lambda_handler(event,context)

        check_item = dynamodb.get_item(
            TableName=videos_process_table,
            Key={"Identifier": {"S": 'VideoFile.mp4'}},
            ConsistentRead=True,
            ReturnConsumedCapacity='NONE',
        )

        assert 'CONSOLIDATING' == check_item['Item']['Status']['S']
        mock_exec_lambda.assert_not_called()

    @mock_s3
    @mock_dynamodb2
    @mock.patch('lambda_function.execute_lambda_process_photos')
    @mock.patch('lambda_function.publish_message')
    def test_all_processing(self, mock_pub, mock_exec_lambda):
        context = ''

        dynamodb = boto3.client('dynamodb')
        dynamodb_resource_client = boto3.resource('dynamodb')

        create_VIDEOS_PROCESS_TABLE(dynamodb)
        create_VIDEOS_RESULTS_TABLE_NAME(dynamodb)

        with open('test_events/event_all_completed.json') as data_file:
            event = json.load(data_file)

        handler = lambda_function.lambda_handler(event, context)
        
        mock_exec_lambda.assert_not_called()


def create_VIDEOS_RESULTS_TABLE_NAME(dynamodb):
    dynamodb.create_table(TableName=videos_results_table_name,
        KeySchema=[{'AttributeName': 'Identifier', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'Identifier', 'AttributeType': 'S'}],
        ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5})


def create_VIDEOS_PROCESS_TABLE(dynamodb):
    dynamodb.create_table(TableName=videos_process_table,
        KeySchema=[{'AttributeName': 'Identifier', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'Identifier', 'AttributeType': 'S'}],
        ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5})


if __name__ == '__main__':
    unittest.main()

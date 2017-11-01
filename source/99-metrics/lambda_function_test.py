import unittest
import boto3
import json
import mock

from lambda_function import lambda_handler
from moto import mock_s3, mock_dynamodb2
#from awscli.customizations.s3.subcommands import METADATA

class MockNotify(object):
    def __init__(self):
        pass

    def metrics(self,solution_id, uuid, data, metrics_url):
        return

class TestMetricsActor(unittest.TestCase):

    mock_notify = MockNotify()
    test_metrics_url = 'https://oszclq8tyh.execute-api.us-east-1.amazonaws.com/prod/generic'
    def test_lambda_handler(self):
        with open('event.json') as data_file:
            put_event = json.load(data_file)
        # Verify lambda function returns the correct status
        test_context = ''
        self.assertIsNone(lambda_handler(put_event,test_context))
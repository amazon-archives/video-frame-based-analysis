import unittest
import mock
import json
import boto3

import lambda_function
#from lambda_function import lambda_handler
from moto import mock_s3, mock_dynamodb2, mock_dynamodb
from moto import mock_sns_deprecated


class MockFunctions(object):

    def __init__(self):
        pass


    def get_labels_from_ddb(self, key):
        with open('./resp_get_labels_from_ddb_1.json') as event_file:
            event_input = json.load(event_file)
        return event_input


    def publish_result(self, bucket, key):
        return True


class TestActor(unittest.TestCase):
    mock_functions = MockFunctions()


    @mock_s3
    @mock_sns_deprecated
    @mock.patch('lambda_function.get_labels_from_ddb', side_effect=mock_functions.get_labels_from_ddb)
    @mock.patch('lambda_function.publish_result', side_effect=mock_functions.publish_result)
    def test_publish_result(self, f1, f2):
        context = ''
        bucket = "TEST_BUCKET"
        key = "tags/videoname.mp4-tags.json"

        with open('./event.json') as event_file:
            event_input = json.load(event_file)

        s3 = boto3.resource('s3', region_name='us-east-1')
        s3.create_bucket(Bucket=bucket)

        lambda_function.S3_VIDEO_BUCKET = bucket
        handler = lambda_function.lambda_handler(event_input, context)

        f1.assert_called_once_with(bucket, key)


    def test_parse_items(self):
        labels = {}
        items = [{"Identifier": {"S": "videoname.mp4"}, "Labels": {"L": [{"S": "Human"}, {"S": "People"}, {"S": "Person"}]}, "Key": {"S": "videoname.mp4-00135.jpg"}, "Time": {"S": "135000"}}, {"Identifier": {"S": "videoname.mp4"}, "Labels": {"L": [{"S": "People"}, {"S": "Person"}, {"S": "Human"}, {"S": "Confetti"}, {"S": "Paper"}]}, "Key": {"S": "videoname.mp4-00136.jpg"}, "Time": {"S": "136000"}}, {"Identifier": {"S": "videoname.mp4"}, "Labels": {"L": [{"S": "People"}, {"S": "Person"}, {"S": "Human"}]}, "Key": {"S": "videoname.mp4-00137.jpg"}, "Time": {"S": "137000"}}, {"Identifier": {"S": "videoname.mp4"}, "Labels": {"L": [{"S": "People"}, {"S": "Person"}, {"S": "Human"}, {"S": "Sink"}]}, "Key": {"S": "videoname.mp4-00138.jpg"}, "Time": {"S": "138000"}}, {"Identifier": {"S": "videoname.mp4"}, "Labels": {"L": [{"S": "Confetti"}, {"S": "Paper"}]}, "Key": {"S": "videoname.mp4-00139.jpg"}, "Time": {"S": "139000"}}, {"Identifier": {"S": "videoname.mp4"}, "Labels": {"L": [{"S": "Word"}]}, "Key": {"S": "videoname.mp4-00141.jpg"}, "Time": {"S": "141000"}}, {"Identifier": {"S": "videoname.mp4"}, "Labels": {"L": [{"S": "Word"}]}, "Key": {"S": "videoname.mp4-00142.jpg"}, "Time": {"S": "142000"}}, {"Identifier": {"S": "videoname.mp4"}, "Labels": {"L": [{"S": "Word"}]}, "Key": {"S": "videoname.mp4-00143.jpg"}, "Time": {"S": "143000"}}, {"Identifier": {"S": "videoname.mp4"}, "Labels": {"L": [{"S": "Word"}]}, "Key": {"S": "videoname.mp4-00144.jpg"}, "Time": {"S": "144000"}}]

        labels = lambda_function.parse_items(labels,items)

        print(labels)

        assert None != labels
        assert 'Word' in labels
        assert 'Human' in labels
        assert 'People' in labels
        assert 'Person' in labels
        assert 'Paper' in labels
        assert 'Sink' in labels


if __name__ == '__main__':
    unittest.main()

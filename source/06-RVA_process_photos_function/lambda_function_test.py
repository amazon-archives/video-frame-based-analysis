import unittest
import mock
import json
import boto3
import botocore
import logging
import sure
from boto.dynamodb2.exceptions import ValidationException

import lambda_function
#from lambda_function import lambda_handler
from moto import mock_s3, mock_dynamodb2, mock_dynamodb

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)

# CONSTANTS
PROCESS_TABLE = 'RVA_PROCESS_TABLE'
VIDEOS_RESULTS_TABLE = 'RVA_VIDEOS_RESULTS_TABLE'
FRAMES_RESULTS_TABLE = 'RVA_FRAMES_RESULTS_TABLE'
VIDEO_LABELS_TABLE = 'RVA_VIDEOS_LABELS_TABLE'

class TestActor(unittest.TestCase):

    test_video_identifier = 'fun-at-fair-343902224.mp4'
    faces_indexed = 0

    @mock_s3
    @mock_dynamodb2
    @mock.patch('lambda_function.index_faces_call')
    @mock.patch('lambda_function.detect_labels_call')
    @mock.patch.object(lambda_function,'video_identifier',test_video_identifier)
    @mock.patch.object(lambda_function,'faces_indexed', faces_indexed)
    def test_rekognition(self, rdl, rif):
        with open('./faces.json') as faces_file:
            rif.return_value = json.load(faces_file)
            #rif.return_value = faces_file.read()
        with open('./labels.json', 'rb') as labels_file:
            rdl.return_value = json.load(labels_file)
            #rdl.return_value = labels_file.read()
        dynamodb = boto3.client('dynamodb')
        dynamodb_resource_client = boto3.resource('dynamodb')

        create_RVA_PROCESS_TABLE(dynamodb)
        create_RVA_VIDEOS_RESULTS_TABLE(dynamodb)
        create_RVA_FRAMES_RESULTS_TABLE(dynamodb)
        create_RVA_VIDEOS_LABELS_TABLE(dynamodb)

        videos_results_table = dynamodb_resource_client.Table(VIDEOS_RESULTS_TABLE)
        videos_results_table.put_item(Item={
            'Identifier': 'fun-at-fair-343902224.mp4',
            'FaceDetails': {
                'Smile': {'Positive': 0, 'Negative': 0},
                'Eyeglasses': {'Positive': 0, 'Negative': 0},
                'Sunglasses': {'Positive': 0, 'Negative': 0},
                'Gender': {'Male': 0, 'Female': 0},
                'Beard': {'Positive': 0, 'Negative': 0},
                'Mustache': {'Positive': 0, 'Negative': 0},
                'EyesOpen': {'Positive': 0, 'Negative': 0},
                'MouthOpen': {'Positive': 0, 'Negative': 0},
                'Emotions': {'HAPPY': 0, 'SAD': 0, 'ANGRY': 0, 'DISGUSTED': 0, 'CONFUSED': 0,
                             'SURPRISED': 0}
            },
            'NumberFaceDetails': 0,
            'DetectedLabels': {},
            'Individuals': []
        })

        s3 = boto3.resource('s3', region_name='us-east-1')
        # We need to create the bucket since this is all in Moto's 'virtual' AWS account
        s3.create_bucket(Bucket='deep-west-video-rekognition-video')
        with open('d630ccbcf37810eb16187bd859a7e280.txt') as text_file:
            text_body = text_file.read()
        s3.Object('deep-west-video-rekognition-video', 'images/fun-at-fair-343902224.mp4/d630ccbcf37810eb16187bd859a7e280.txt').put(Body=text_body,Metadata={"topic": "topic"})
        s3.Object('deep-west-video-rekognition-video', 'images/fun-at-fair-343902224.mp4/fun-at-fair-343902224.mp4-3.jpg').put(Body='is awesome',Metadata={"topic": "topic"})
        s3.Object('deep-west-video-rekognition-video', 'images/fun-at-fair-343902224.mp4/fun-at-fair-343902224.mp4-4.jpg').put(Body='is awesome',Metadata={"topic": "topic"})
        s3.Object('deep-west-video-rekognition-video', 'images/fun-at-fair-343902224.mp4/fun-at-fair-343902224.mp4-6.jpg').put(Body='is awesome',Metadata={"topic": "topic"})
        s3.Object('deep-west-video-rekognition-video', 'images/fun-at-fair-343902224.mp4/fun-at-fair-343902224.mp4-1.jpg').put(Body='is awesome',Metadata={"topic": "topic"})
        s3.Object('deep-west-video-rekognition-video', 'images/fun-at-fair-343902224.mp4/fun-at-fair-343902224.mp4-11.jpg').put(Body='is awesome',Metadata={"topic": "topic"})
        s3.Object('deep-west-video-rekognition-video', 'images/fun-at-fair-343902224.mp4/fun-at-fair-343902224.mp4-2.jpg').put(Body='is awesome',Metadata={"topic": "topic"})
        s3.Object('deep-west-video-rekognition-video', 'images/fun-at-fair-343902224.mp4/fun-at-fair-343902224.mp4-5.jpg').put(Body='is awesome',Metadata={"topic": "topic"})
        with open('frame_event.json') as data_file:
            event = json.load(data_file)
        context = ''
        #handler = lambda_function.lambda_handler(event,context)

        detectlabels = lambda_function.detect_labels('deep-west-video-rekognition-video','images/fun-at-fair-343902224.mp4/fun-at-fair-343902224.mp4-3.jpg', 0)
        detectfaces = lambda_function.detect_faces('deep-west-video-rekognition-video','images/fun-at-fair-343902224.mp4/fun-at-fair-343902224.mp4-3.jpg','1000-01-01 00:00:00', 0)

        #print(dynamodb.describe_table(TableName=videos_results_table_name))

        check_item = dynamodb.get_item(
            TableName=VIDEOS_RESULTS_TABLE,
            Key={"Identifier": {"S": 'fun-at-fair-343902224.mp4'}},
            ConsistentRead=True,
            ReturnConsumedCapacity='NONE',
        )
        print(json.dumps(check_item))

        #print(check_item['Item']['FaceDetails.Gender.Male'])
        self.assertIsNone(None,check_item['Item']['FaceDetails.Gender.Male'])
        self.assertIsNone(None,check_item['Item']['FaceDetails.Smile.Positive'])
        self.assertIsNone(None,check_item['Item']['FaceDetails.Emotions.HAPPY'])

        #print(dynamodb.scan(
        #    TableName=videos_results_table.table_name,
        #    ConsistentRead=True,
        #    ReturnConsumedCapacity='NONE',
        #))


    @mock_s3
    @mock_dynamodb2
    @mock.patch('lambda_function.detect_labels_call', return_value=json.loads('{"Labels": [], "OrientationCorrection": "ROTATE_0"}'))
    @mock.patch.object(lambda_function,'video_identifier',test_video_identifier)
    def test_rekog_labels_empty_list(self, rdl):
        labels = lambda_function.detect_labels('deep-west-video-rekognition-video','images/fun-at-fair-343902224.mp4/fun-at-fair-343902224.mp4-3.jpg', 0)
        self.assertEqual(0, len(labels))


    @mock_s3
    @mock_dynamodb2
    @mock.patch('lambda_function.detect_labels_call', return_value=json.loads('{"Labels": [{ "Confidence": 74.410766601,  "Name": "Text" }], "OrientationCorrection": "ROTATE_0"}'))
    @mock.patch('lambda_function.store_labels_detected', return_value=None)
    @mock.patch.object(lambda_function,'video_identifier',test_video_identifier)
    def test_rekog_labels_ddb_reserved_words(self, f1, f2):

        dynamodb = boto3.client('dynamodb')

        create_RVA_VIDEOS_RESULTS_TABLE(dynamodb)

        labels = lambda_function.detect_labels('deep-west-video-rekognition-video','images/fun-at-fair-343902224.mp4/fun-at-fair-343902224.mp4-3.jpg', 0)

        self.assertEqual(1, len(labels))


    @mock_dynamodb2
    @mock.patch.object(lambda_function,'video_identifier',test_video_identifier)
    def test_store_labels_ddb_update(self):
        key = "images/video/video.jpg"
        timestamp = 1234
        labels = [ 'label1', 'label2' ]

        dynamodb = boto3.client('dynamodb')

        create_RVA_VIDEOS_LABELS_TABLE(dynamodb)

        lambda_function.store_labels_detected(key, timestamp, labels)

        check_item = dynamodb.get_item(
            TableName=VIDEO_LABELS_TABLE,
            Key={
                'Identifier': { 'S': 'fun-at-fair-343902224.mp4'},
                'Key': { 'S': lambda_function.extract_object_key(key) }
            }
        )

        labels = unmarshal_dynamodb_json(check_item['Item']['Labels'])

        self.assertEqual(2, len(labels), msg="Data written in DDB is wrong. Exp: {} Found: {}".format(2, labels))


## HELPERS

def unmarshal_dynamodb_json(node):
    data = dict({})
    data['M'] = node
    return _unmarshal_value(data)


def _unmarshal_value(node):
    if type(node) is not dict:
        return node

    for key, value in node.items():
        key = key.lower()
        if key == 'bool':
            return value
        if key == 'null':
            return None
        if key == 's':
            return value
        if key == 'n':
            if '.' in str(value):
                return float(value)
            return int(value)
        if key in ['m', 'l']:
            if key == 'm':
                data = {}
                for key1, value1 in value.items():
                    if key1.lower() == 'l':
                        data = [_unmarshal_value(n) for n in value1]
                    else:
                        if type(value1) is not dict:
                            return _unmarshal_value(value)
                        data[key1] = _unmarshal_value(value1)
                return data
            data = []
            for item in value:
                data.append(_unmarshal_value(item))
            return data


def create_RVA_PROCESS_TABLE(dynamodb):
    dynamodb.create_table(TableName=PROCESS_TABLE,
                        KeySchema=[{'AttributeName': 'Identifier', 'KeyType': 'HASH'}],
                        AttributeDefinitions=[{'AttributeName': 'Identifier', 'AttributeType': 'S'}],
                        ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5})

def create_RVA_VIDEOS_RESULTS_TABLE(dynamodb):
    dynamodb.create_table(TableName=VIDEOS_RESULTS_TABLE,
                        KeySchema=[{'AttributeName': 'Identifier', 'KeyType': 'HASH'}],
                        AttributeDefinitions=[{'AttributeName': 'Identifier', 'AttributeType': 'S'}],
                        ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5})


def create_RVA_FRAMES_RESULTS_TABLE(dynamodb):
    dynamodb.create_table(TableName=FRAMES_RESULTS_TABLE,
                        KeySchema=[{'AttributeName': 'Identifier', 'KeyType': 'HASH'},{'AttributeName': 'Key', 'KeyType': 'HASH'}],
                        AttributeDefinitions=[{'AttributeName': 'Identifier', 'AttributeType': 'S'},{'AttributeName': 'Key', 'AttributeType': 'S'}],
                        ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5})

def create_RVA_VIDEOS_LABELS_TABLE(dynamodb):
    dynamodb.create_table(TableName=VIDEO_LABELS_TABLE,
                        KeySchema=[{'AttributeName': 'Identifier', 'KeyType': 'HASH'},{'AttributeName': 'Key', 'KeyType': 'HASH'}],
                        AttributeDefinitions=[{'AttributeName': 'Identifier', 'AttributeType': 'S'},{'AttributeName': 'Key', 'AttributeType': 'S'}, {'AttributeName': 'Labels', 'AttributeType': 'S'},{'AttributeName': 'Time', 'AttributeType': 'S'}],
                        ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5})

if __name__ == '__main__':
    unittest.main()

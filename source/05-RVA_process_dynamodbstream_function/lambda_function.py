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
from rekog_collection_controller import RekognitionCollectionController
from functools import wraps

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
MAX_DYNAMODB_TPS_ALLOWED=40
MAX_BACKOFF = 15 # seconds
MAX_RETRIES = 5
RETRY_EXCEPTIONS = ('ProvisionedThroughputExceededException',
                    'ThrottlingException')

# Services
lambda_client = boto3.client('lambda', region_name=os.environ['AWS_DEFAULT_REGION'])
dynamodb_client = boto3.client('dynamodb')
dynamodb_resource_client = boto3.resource('dynamodb', config=Config(max_pool_connections=MAX_DYNAMODB_TPS_ALLOWED))
videos_results_table = dynamodb_resource_client.Table('RVA_VIDEOS_RESULTS_TABLE')
process_table = dynamodb_resource_client.Table('RVA_PROCESS_TABLE')
rekognition = boto3.client('rekognition', region_name=os.environ['AWS_DEFAULT_REGION'])
sns_client = boto3.client('sns')

# Lambda Variables
RVA_IoT_publish_message_function = os.environ['RVA_IoT_publish_message_function']
RVA_process_photos_function = os.environ['RVA_process_photos_function']
MAX_TPS = os.environ['MAX_TPS']
RVA_SNS_MILESTONES_TOPIC_ARN = os.environ['RVA_SNS_MILESTONES_TOPIC_ARN']
RVA_COLLECTION_MAX_SIZE = int(os.getenv('RVA_COLLECTION_MAX_SIZE'), 10)

# --------------- Retry decorator
def retry(ExceptionToCheck=RETRY_EXCEPTIONS, tries=5, max_backoff=MAX_BACKOFF, logger=None):
    def decorator_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries = 0
            while mtries <= tries:
                try:
                    if logger:
                        logger.debug("Calling decorated function '{}' Args: '{}'" .format(f.__name__, args))
                    else:
                        print("DEBUG: Calling decorated function '{}' Args: '{}'" .format(f.__name__, args))

                    return f(*args, **kwargs)

                except ClientError as err:
                    if err.response['Error']['Code'] not in RETRY_EXCEPTIONS:
                        if logger:
                            logger.error(err)
                        else:
                            print(err)
                        raise err

                    temp = min(max_backoff, 2 ** mtries)
                    sleep = temp / float(2) + random.uniform(0, temp / float(2))

                    if logger:
                        logger.warn("BACKOFF - {} - Waiting '{}' s Retries: '{}' Temp: '{}' Sleep: '{}' Args: '{}'" .format(f.__name__, sleep, mtries, temp, sleep, args))
                    else:
                        print("WARN: BACKOFF - {} - Waiting '{}' s Retries: '{}' Temp: '{}' Sleep: '{}' Args: '{}'" .format(f.__name__, sleep, mtries, temp, sleep, args))

                    time.sleep(sleep)
                    mtries += 1
            if logger:
                logger.error("BACKOFF - {} - Limit breached Args: '{}'".format(f.__name__, args))
            else:
                print("ERROR: BACKOFF - {} - Limit breached Args: '{}'".format(f.__name__, args))

            return f(*args, **kwargs)

        return f_retry  # true decorator

    return decorator_retry


# Allow mocking during For testing
def init_RekognitionCollectionController():
    rcc = RekognitionCollectionController(RVA_COLLECTION_CONTROL, 'COLLECTIONS')
    rcc.set_collection_size(RVA_COLLECTION_MAX_SIZE)

    return rcc


def publish_message(payload):
    lambda_client.invoke(
        FunctionName=RVA_IoT_publish_message_function,
        InvocationType='Event',
        LogType='None',
        Payload=payload
    )


def dynamodb_get_item_from_identifier(video_identifier):
    response = dynamodb_client.get_item(
        TableName=process_table.table_name,
        Key={"Identifier": {"S": video_identifier}},
        ConsistentRead=True,
        ReturnConsumedCapacity='NONE',
    )

    logger.debug("dynamodb_get_item_from_identifier\n{}".format(json.dumps(response)))

    return response


def dynamodb_update_item_by_identifier(video_identifier, status_val):
    process_table.update_item(
        Key={'Identifier': video_identifier},
        UpdateExpression="SET #st = :newvalue",
        ExpressionAttributeNames={
            '#st': 'Status'
        },
        ExpressionAttributeValues={':newvalue': status_val})


def notify_event(video_identifier, status):
    try:
        milestone = {
            "Identifier" : video_identifier,
            "Status" : status
        }

        response = sns_client.publish(
            TopicArn=RVA_SNS_MILESTONES_TOPIC_ARN,
            Message=json.dumps(milestone)
        )
    except Exception as e:
        logger.error("Error notifying event for video '{}'".format(video_identifier))
        logger.error(e)


def execute_lambda_process_photos(video_identifier, item):
    logger.debug("Executing with file '{}'".format(item))
    lambda_client.invoke(
        FunctionName=RVA_process_photos_function,
        InvocationType='Event',
        LogType='None',
        Payload=json.dumps({"Identifier": video_identifier, "Key": item})
    )


def lambda_handler(event, context):
    logger.debug("Received event: " + json.dumps(event))

    initial_percentage = 60.0
    final_percentage = 90.0
    results_array = []
    rcc = init_RekognitionCollectionController()

    for record in event['Records']:
        if record['eventName'] != 'REMOVE':
            try:
                row = record['dynamodb']['NewImage']
                video_identifier = row['Identifier']['S']

                logger.debug("Processing record:\n{}".format(json.dumps(row)))

                ddb_video_item = dynamodb_get_item_from_identifier(video_identifier)

                if 'Item' in ddb_video_item:
                    row = ddb_video_item['Item']

                iot_topic = row['Topic']['S']

                if row['Status']['S'] == 'PROCESSING':
                    # Check if we've already seen this iot topic to make sure we won't mess with other simultaneous process.
                    element = None

                    for v in results_array:
                        if v['iot_topic'] == iot_topic:
                            element = v
                            break

                    if element is None:
                        element = {'iot_topic': iot_topic, 'number_of_items': 0, 'max_completed_items': 0,
                                   'status': 'PROCESSING'}
                        results_array.append(element)

                    element['identifier'] = video_identifier
                    completed_sum = 0
                    processing_sum = 0
                    part_dict = row['Parts']['M']
                    element['number_of_items'] = len(part_dict)
                    number_of_items = len(part_dict)
                    parts = json.dumps(part_dict)
                    pending_list = []
                    logger.debug(parts)

                    for key, value in part_dict.iteritems():
                        if value['S'] == "COMPLETED":
                            completed_sum += 1
                        else:
                            if value['S'] == "PENDING":
                                pending_list.append(key)
                                #logger.debug("Item '{}' with status '{}' needs to be processed".format(key, value))
                            if value['S'] == 'PROCESSING':
                                processing_sum +=1
                                #logger.debug("Item '{}' with status '{}' is being processed".format(key, value))

                    logger.debug('Verifying completed items {}'.format(element))

                    try:
                        if completed_sum > element['max_completed_items']:
                            element['max_completed_items'] = completed_sum
                    except Exception as e:
                        logger.error(e, exc_info=True)
                        logger.error('-' * 10)
                        traceback.print_exc(file=sys.stdout)
                        logger.error('-' * 10)

                    logger.debug("Items completed so far: '{}'".format(element['max_completed_items']))

                    #  it means this is a fresh record
                    if completed_sum == 0  and processing_sum == 0:

                        collection_id = rcc.fetch_collection()

                        logger.debug("CollectionId: '{}'".format(collection_id))

                        videos_results_table.put_item(Item={
                            'Identifier': video_identifier,
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
                                             'SURPRISED': 0, 'CALM': 0}
                            },
                            'NumberFaceDetails': 0,
                            'DetectedLabels': {},
                            'Individuals': [],
                            'CollectionId': collection_id
                        })

                    if pending_list:
                        logger.debug("Pending list is: " + json.dumps(pending_list))

                        if processing_sum >= int(MAX_TPS):
                            logger.debug("You have '{}' items being processed".format(processing_sum))
                        else:
                            run_functions = int(MAX_TPS) - processing_sum
                            if run_functions > 0:
                                logger.debug("You have '{}' files to process".format(len(pending_list)))
                                logger.debug("You can run '{}' functions".format(run_functions))
                                for item in pending_list[:run_functions]:
                                    execute_lambda_process_photos(video_identifier, item)
                            else:
                                logger.debug("You can't run more functins at this time.")
                    elif number_of_items == completed_sum:
                        dynamodb_update_item_by_identifier(video_identifier, 'COMPLETED')

                # elif row['Status']['S'] == 'EXTRACTING_THUMBNAILS':
                #     element = {'iot_topic': iot_topic, 'status': 'EXTRACTING_THUMBNAILS'}
                #     results_array.append(element)
                #
                #     lambda_client.invoke(
                #         FunctionName=RVA_crop_orchestration_function,
                #         InvocationType='Event',
                #         LogType='None',
                #         Payload=json.dumps({"Identifier": video_identifier})
                #     )
                elif row['Status']['S'] == 'COMPLETED':
                    element = {'iot_topic': iot_topic, 'status': 'COMPLETED', 'identifier': video_identifier, 'max_completed_items': 0}
                    results_array.append(element)
                    notify_event(video_identifier, 'COMPLETED')

            except Exception as e:
                logger.error(e, exc_info=True)
                logger.error('-' * 10)
                traceback.print_exc(file=sys.stdout)
                logger.error('-' * 10)

    for element in results_array:
        if element['iot_topic'] != "none":
            # Check if the process is already finished
            if element['status'] == 'PROCESSING':
                if element['number_of_items'] != 0 and element['max_completed_items'] != 0:
                    calculated_value = int(round(initial_percentage + (
                        ((final_percentage - initial_percentage) / element['number_of_items']) * element['max_completed_items'])))
                    logger.debug("iot_topic:%s - %d" % (element['iot_topic'], calculated_value))

                    payload = json.dumps({
                        'topic': element['iot_topic'],
                        'type': 'status',
                        'payload': {'message': 'Analyzing frames', 'percentage': calculated_value}
                    })
                    publish_message(payload)
            elif element['status'] == 'CONSOLIDATING':
                payload = json.dumps({
                    'topic': element['iot_topic'],
                    'type': 'status',
                    'payload': {'message': 'Consolidating results', 'percentage': final_percentage}
                })
                publish_message(payload)
            elif element['status'] == 'EXTRACTING_THUMBNAILS':
                payload = json.dumps({
                    'topic': element['iot_topic'],
                    'type': 'status',
                    'payload': {'message': 'Extracting thumbnails from individuals', 'percentage': 95}
                })
                publish_message(payload)
            elif element['status'] == 'COMPLETED':
                payload = json.dumps({
                    'topic': element['iot_topic'],
                    'type': 'redirect',
                    'payload': {'identifier': element['identifier']}
                })
                publish_message(payload)

    logger.info('Successfully processed {} records.'.format(len(event['Records'])))

    return 'Successfully processed {} records.'.format(len(event['Records']))

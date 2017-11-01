from __future__ import print_function

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from decimal import Decimal
import json
import urllib
import re
import random, string
import threading
import sys, traceback
import os
import logging
import time
import base64
from multiprocessing.dummy import Pool
from functools import wraps
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

# CONSTANTS
RVA_COLLECTION_CONTROL = 'RVA_COLLECTION_CONTROL_TABLE'
THRESHOLD_FACEDETAILS_RESPONSE_CONFIDENCE = 95
THRESHOLD_FACEDETAILS_ATTRIBUTES_CONFIDENCE = 90
THRESHOLD_DETECTLABELS_ATTRIBUTES_CONFIDENCE = 90
DETECT_LABELS_MAX_NUMBER = 10
MAX_BACKOFF = 15 # seconds
MAX_RETRIES = 5
THREAD_POOL_SIZE = 4
RETRY_EXCEPTIONS = ('ProvisionedThroughputExceededException',
                    'ThrottlingException',
                    'ResourceNotFoundException') # Collection not found


# GLOBAL VARIABLES
video_identifier = ''
faces_indexed = 0
detect_labels_results = {}
detect_faces_results = {}

# LAMBDA VARIABLES
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
VIDEO_BUCKET = os.environ['VIDEO_BUCKET']

# SERVICES
rekognition = boto3.client('rekognition', region_name=os.environ['AWS_DEFAULT_REGION'], config=Config(max_pool_connections=30))
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
dynamodb_client = boto3.resource('dynamodb', config=Config(max_pool_connections=30))
process_table = dynamodb_client.Table('RVA_PROCESS_TABLE')
frames_results_table = dynamodb_client.Table('RVA_FRAMES_RESULTS_TABLE')
videos_results_table = dynamodb_client.Table('RVA_VIDEOS_RESULTS_TABLE')
videos_labels_table  = dynamodb_client.Table('RVA_VIDEOS_LABELS_TABLE')


if LOG_LEVEL == 'DEBUG':
    logger.setLevel(logging.DEBUG)

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

# --------------- Helper Functions to call Rekognition APIs ------------------

@retry(RETRY_EXCEPTIONS, 5, MAX_BACKOFF, logger)
def index_faces_call(bucket, key, video_identifier, coll_id):
    response = rekognition.index_faces(
        Image={"S3Object": {"Bucket": bucket, "Name": key}},
        CollectionId=coll_id,
        DetectionAttributes=["ALL", "DEFAULT"],
        ExternalImageId=extract_object_key(key)
    )

    return response


@retry(RETRY_EXCEPTIONS, 5, MAX_BACKOFF, logger)
def detect_labels_call(bucket, key, threshold_detectlabels_attributes_confidence, detect_labels_max_number):

    response = rekognition.detect_labels(Image={"S3Object": {"Bucket": bucket, "Name": key}},
        MinConfidence=threshold_detectlabels_attributes_confidence,
        MaxLabels=detect_labels_max_number)

    return response


def detect_faces(params):
    global faces_indexed
    global detect_faces_results

    #Unpacking params to support ThreadPools
    bucket = params[0]
    key = params[1]
    timestamp = params[2]
    coll_id = params[3]

    try:
        logger.debug("detect_faces: threadname: '{}' bucket: '{}' key: '{}' CollId:'{}'".format(threading.currentThread().getName(), bucket, key, coll_id))

        response = index_faces_call(bucket, key, video_identifier, coll_id)

        if 'FaceRecords' in response:
            req_id = response['ResponseMetadata']['RequestId']
            detect_faces_results[req_id] = response['FaceRecords']

            faces_found = len(response['FaceRecords'])
            faces_indexed += faces_found

            logger.debug("Faces found on the frame: '{}'".format(faces_found))

            # Let's check whether or not we have found faces in this picture, otherwise it wouldn't
            # make any sense to waste our resources to persist it.
            if faces_found > 0:
                # response_dynamodb_ready = json.loads(json.dumps(response), parse_float=Decimal)
                object_name = extract_object_key(key)

                object_path = "results/frames/{}/{}.json".format(str(video_identifier), str(object_name))
                object_body = json.dumps(response['FaceRecords']) #, parse_float=Decimal)

                saveObject = s3_resource.Object(bucket, object_path).put(Body=object_body,ServerSideEncryption='AES256')

                frames_results_table.put_item(
                    Item = {
                        'Identifier': video_identifier,
                        'Key': extract_object_key(key),
                        'S3Path': "s3://{}/{}".format(bucket, object_path),
                        'Time': timestamp
                    }
                )
    except Exception as e:
        logger.error("detect_faces - error processing file. Bucket: '{}' Key: '{}'".format(bucket, key))
        logger.error(e)
        logger.error('-' * 60)
        traceback.print_exc(file=sys.stdout)
        logger.error('-' * 60)


def store_labels_detected(key, timestamp, labels):

    if labels:
        logger.debug("Inserting labels... Key: '{}' Time: '{}' Labels: '{}' video_identifier: '{}'".format(key, timestamp, labels, video_identifier))

        response_dynamodb_ready = json.loads(json.dumps(labels))

        videos_labels_table.update_item(
            Key={'Identifier': video_identifier, 'Key': extract_object_key(key)},
            UpdateExpression='SET #labels = :val1, #timestamp = :val2',
            ExpressionAttributeNames={'#labels': 'Labels', '#timestamp': 'Time'},
            ExpressionAttributeValues={':val1': labels, ':val2': timestamp})


def detect_labels(params):
    #Unpacking params to support ThreadPool
    bucket = params[0]
    key = params[1]
    timestamp = params[2]

    global detect_labels_results
    update_expression_str_list = []
    expression_attr_values_dict = {}
    expression_attr_names_dict = {}
    labels = []

    try:
        logger.debug(
            'detect_labels: threadname: "{}" bucket: "{}" key: "{}"'.format(threading.currentThread().getName(), bucket, key))

        response = detect_labels_call(bucket, key, THRESHOLD_DETECTLABELS_ATTRIBUTES_CONFIDENCE, DETECT_LABELS_MAX_NUMBER)

        logger.debug("detect_labels resp: '{}'".format(response))

        if 'Labels' in response:
            req_id = response['ResponseMetadata']['RequestId']
            detect_labels_results[req_id] = response['Labels']

            for label_prediction in response['Labels']:
                labels.append(label_prediction['Name'])

            store_labels_detected(key, timestamp, labels)

    except Exception as e:
        logger.debug("update_expression_str_list: {} ".format(update_expression_str_list))
        logger.debug("expression_attr_values_dict: {} ".format(expression_attr_values_dict))
        logger.error(e)
        logger.error('-' * 60)
        traceback.print_exc(file=sys.stdout)
        logger.error('-' * 60)

    return labels

def index_faces(bucket, key):
    logger.debug('index_faces: threadname: {} bucket: {} key: {}'.format(threading.currentThread().getName(), bucket, key))
    try:
        # Calculate
        response = rekognition.index_faces(Image={"S3Object": {"Bucket": bucket, "Name": key}},
                                           CollectionId=video_identifier, DetectionAttributes=["ALL"])
        logger.debug(json.dumps(response))
    except Exception as e:
        logger.error(e)
        logger.error('-' * 60)
        traceback.print_exc(file=sys.stdout)
        logger.error('-' * 60)


# --------------- Misc helper functions ------------------

def extract_video_identifier(key):
    return extract_key_paths(key, 1)


def extract_object_key(key):
    return extract_key_paths(key, 2)


def extract_key_paths(key, group_number):
    p = re.compile('images/(.*)/(.*)')
    m = p.match(key)
    return m.group(group_number)


def update_process_table_completed_item(identifier, key):
    logger.debug("update_process_table_completed_item {} {}".format(identifier, key))

    process_table.update_item(
        Key={'Identifier': identifier},
        UpdateExpression="SET Parts.#bf = :newvalue",
        ExpressionAttributeNames={
            '#bf': key
        },
        ExpressionAttributeValues={':newvalue': 'COMPLETED'})

def evaluate_facedetail_attribute_boolean_type(service_response, attribute, output):
    if service_response['FaceDetail'][attribute]['Confidence'] > THRESHOLD_FACEDETAILS_ATTRIBUTES_CONFIDENCE:
        output['Positive' if service_response['FaceDetail'][attribute]['Value'] else 'Negative'] += 1


def evaluate_facedetail_attribute_gender_type(service_response, attribute, output):
    if service_response['FaceDetail'][attribute]['Confidence'] > THRESHOLD_FACEDETAILS_ATTRIBUTES_CONFIDENCE:
        output[service_response['FaceDetail'][attribute]['Value']] += 1


def evaluate_facedetail_attribute_emotions_type(service_response, attribute, output):
    selected_type = ''
    highest_value = 0.0
    for element in service_response['FaceDetail'][attribute]:
        if element['Confidence'] > THRESHOLD_FACEDETAILS_ATTRIBUTES_CONFIDENCE:
            if element['Confidence'] > highest_value:
                highest_value = element['Confidence']
                selected_type = element['Type']
    try:
        if selected_type != '':
            output[selected_type] += 1
    except Exception as e:
        logger.debug("Unsupported Face Details Type.")


def randomword(length):
    return ''.join(random.choice(string.lowercase) for i in range(length))


def get_collection_id(video_identifier):
    coll_id = ""

    r = videos_results_table.get_item(
        Key={'Identifier': video_identifier},
        AttributesToGet=['CollectionId']
    )

    #Wait until receiving a coll id
    if 'Item' in r:
        coll_id = r['Item']['CollectionId']
    else:
        raise "Collection not found"

    return coll_id


def update_collection_control(collection_id, faces_indexed):
    rcc = RekognitionCollectionController(RVA_COLLECTION_CONTROL, 'COLLECTIONS')
    rcc.increment_collection_count(collection_id, faces_indexed)


def check_file_processing_status(identifier, key):
    response = process_table.get_item(
        Key={"Identifier": identifier},
        ProjectionExpression="Parts.#file",
        ExpressionAttributeNames={ "#file": key},
        ConsistentRead=True,
        ReturnConsumedCapacity='NONE',
    )

    file_processing_status = response['Item']['Parts'][key]

    return file_processing_status


def change_status_to_processing(identifier, key):
    logger.debug(">change_status_to_processing Id: '{}' Key: '{}'".format(identifier, key))
    updated = False

    try:
        response = process_table.update_item(
            Key={ "Identifier": identifier },
            UpdateExpression="SET Parts.#file = :proc",
            ExpressionAttributeValues={
                ":proc" : "PROCESSING",
                ":pend" : "PENDING"
            },
            ExpressionAttributeNames={ "#file": key },
            ConditionExpression="Parts.#file = :pend",
            ReturnValues="UPDATED_NEW"
        )

        updated = True
    except Exception as e:
        logger.debug("Function in execution. Skipping...")

    return updated


def update_videos_results_table(summary_labels, summary_faces, faces_detected):
    update_labels(summary_labels)
    update_faces(summary_faces, faces_detected)


def update_faces(list_fd_attr, number_of_recognized_faces):
    update_expression_str_list = []
    expression_attr_values_dict = {}

    for item in list_fd_attr:
        if item['Type'] == 'Boolean':
            for item_state in ['Positive', 'Negative']:
                random_value_name = randomword(5)
                if item[item_state] > 0:
                    update_expression_str_list.append(
                        'FaceDetails.{0}.{1} = FaceDetails.{0}.{1} + :{2}'.format(item['Name'], item_state,
                                                                                    random_value_name))
                    expression_attr_values_dict[':{}'.format(random_value_name)] = item[item_state]
        elif item['Type'] == 'Gender':
            for item_state in ['Male', 'Female']:
                random_value_name = randomword(5)
                if item[item_state] > 0:
                    update_expression_str_list.append(
                        'FaceDetails.{0}.{1} = FaceDetails.{0}.{1} + :{2}'.format(item['Name'], item_state,
                                                                                    random_value_name))
                    expression_attr_values_dict[':{}'.format(random_value_name)] = item[item_state]
        elif item['Type'] == 'Emotions':
            for item_state in ['HAPPY', 'SURPRISED', 'DISGUSTED', 'ANGRY', 'SAD', 'CONFUSED', 'CALM']:
                random_value_name = randomword(5)
                if item[item_state] > 0:
                    update_expression_str_list.append(
                        'FaceDetails.{0}.{1} = FaceDetails.{0}.{1} + :{2}'.format(item['Name'], item_state,
                                                                                    random_value_name))
                    expression_attr_values_dict[':{}'.format(random_value_name)] = item[item_state]

    if len(update_expression_str_list) > 0:
        logger.debug("Updating faces. SET {}".format(', '.join(update_expression_str_list)))
        # Add number of faces recognized
        random_value_name = randomword(5)
        update_expression_str_list.append('NumberFaceDetails = NumberFaceDetails + :{0}'.format(random_value_name))
        expression_attr_values_dict[':{}'.format(random_value_name)] = number_of_recognized_faces

        videos_results_table.update_item(
            Key={'Identifier': video_identifier},
            UpdateExpression=('SET ' + ', '.join(update_expression_str_list)),
            ExpressionAttributeValues=expression_attr_values_dict)


def update_labels(summary):
    logger.debug(">update_labels {}".format(summary))

    update_expression_str_list = []
    expression_attr_values_dict = {}
    expression_attr_names_dict = {}

    for k, v in summary.iteritems():
        random_value_name = randomword(5)
        random_expr_attr_name = randomword(5)

        #DDB reserved names issue
        update_expression_str_list.append(
            'DetectedLabels.#{0} :{1}'.format(random_expr_attr_name, random_value_name))
        expression_attr_names_dict['#{}'.format(random_expr_attr_name)] = k
        expression_attr_values_dict[':{}'.format(random_value_name)] = v

    logger.debug('ADD ' + ', '.join(expression_attr_names_dict))
    logger.debug('ADD ' + ', '.join(update_expression_str_list))

    if len(update_expression_str_list) > 0:
        logger.debug('ADD ' + ', '.join(update_expression_str_list))
        logger.debug(expression_attr_values_dict)

        videos_results_table.update_item(
            Key={'Identifier': video_identifier},
            UpdateExpression=('ADD ' + ', '.join(update_expression_str_list)),
            ExpressionAttributeValues=expression_attr_values_dict,
            ExpressionAttributeNames=expression_attr_names_dict
        )


def summarize_labels(summary):
    logger.debug(">summarize_labels\n{}".format(json.dumps(detect_labels_results)))

    labels_consolidated = {}

    for a, v in summary.iteritems():
        if v:
            for element in v:
                label = element['Name']
                if label in labels_consolidated:
                    labels_consolidated[label] += 1
                else:
                    labels_consolidated[label] = 1

    return labels_consolidated

def summarize_faces(summary):
    logger.debug(">summarize_faces\n{}".format(json.dumps(summary)))

    number_of_recognized_faces = 0
    list_fd_attr = [
        {'Name': 'Eyeglasses', 'Type': 'Boolean', 'Positive': 0, 'Negative': 0},
        {'Name': 'Sunglasses', 'Type': 'Boolean', 'Positive': 0, 'Negative': 0},
        {'Name': 'EyesOpen', 'Type': 'Boolean', 'Positive': 0, 'Negative': 0},
        {'Name': 'Smile', 'Type': 'Boolean', 'Positive': 0, 'Negative': 0},
        {'Name': 'MouthOpen', 'Type': 'Boolean', 'Positive': 0, 'Negative': 0},
        {'Name': 'Mustache', 'Type': 'Boolean', 'Positive': 0, 'Negative': 0},
        {'Name': 'Beard', 'Type': 'Boolean', 'Positive': 0, 'Negative': 0},
        {'Name': 'Gender', 'Type': 'Gender', 'Male': 0, 'Female': 0},
        {'Name': 'Emotions', 'Type': 'Emotions', 'HAPPY': 0, 'SURPRISED': 0, 'DISGUSTED': 0, 'ANGRY': 0, 'SAD': 0,
            'CONFUSED': 0, 'CALM': 0}
    ]

    for a, v in summary.iteritems():
        if v:
            for faceDetail in v:
                if faceDetail['FaceDetail']['Confidence'] > THRESHOLD_FACEDETAILS_RESPONSE_CONFIDENCE:
                    evaluate_facedetail_attribute_boolean_type(faceDetail, list_fd_attr[0]['Name'], list_fd_attr[0])
                    evaluate_facedetail_attribute_boolean_type(faceDetail, list_fd_attr[1]['Name'], list_fd_attr[1])
                    evaluate_facedetail_attribute_boolean_type(faceDetail, list_fd_attr[2]['Name'], list_fd_attr[2])
                    evaluate_facedetail_attribute_boolean_type(faceDetail, list_fd_attr[3]['Name'], list_fd_attr[3])
                    evaluate_facedetail_attribute_boolean_type(faceDetail, list_fd_attr[4]['Name'], list_fd_attr[4])
                    evaluate_facedetail_attribute_boolean_type(faceDetail, list_fd_attr[5]['Name'], list_fd_attr[5])
                    evaluate_facedetail_attribute_boolean_type(faceDetail, list_fd_attr[6]['Name'], list_fd_attr[6])
                    evaluate_facedetail_attribute_gender_type(faceDetail, list_fd_attr[7]['Name'], list_fd_attr[7])
                    evaluate_facedetail_attribute_emotions_type(faceDetail, list_fd_attr[8]['Name'], list_fd_attr[8])
                    number_of_recognized_faces += 1

    logger.debug("list_fd_attr: '{}'".format(list_fd_attr))

    return list_fd_attr, number_of_recognized_faces

# --------------- Main handler ------------------

def lambda_handler(event, context):
    logger.debug("Received event: {}".format(json.dumps(event)))

    # Get the object from the event
    bucket = VIDEO_BUCKET
    identifier = event['Identifier']
    key = event['Key']

    updated = change_status_to_processing(identifier, key)
    
    if not updated:
        logger.error("File '{}' is already being processed. Nothing to do.".format(key))
        return "Done"

    contents = s3_client.get_object(Bucket=bucket, Key=key)['Body'].read()

    logger.debug("Video filename: {}".format(identifier))
    logger.debug("File to process: {}".format(key))

    global video_identifier
    global faces_indexed
    global detect_labels_results
    global detect_faces_results

    detect_labels_results = {}
    detect_faces_results = {}
    faces_indexed = 0

    video_identifier = extract_video_identifier(key)
    coll_id = get_collection_id(video_identifier)

    contents_array = contents.split(' ')

    pool = Pool(THREAD_POOL_SIZE)

    detect_labels_params = []
    detect_faces_params = []

    for image_file_meta in contents_array:
        metadata = image_file_meta.split(':')
        image_file = metadata[0].strip()
        timestamp = metadata[1].strip()
        logger.debug("Analyzing image file: '{}'".format(image_file))

        detect_faces_params.append((bucket, image_file, timestamp, coll_id))
        detect_labels_params.append((bucket, image_file, timestamp))

    pool.map(detect_faces, detect_faces_params)
    pool.map(detect_labels, detect_labels_params)

    pool.close()
    pool.join()

    # Consolidate results
    summary_labels = summarize_labels(detect_labels_results)
    summary_faces, faces_detected = summarize_faces(detect_faces_results)

    logger.debug("We are updating the tables")
    update_process_table_completed_item(video_identifier, key)
    update_videos_results_table(summary_labels, summary_faces, faces_detected)
    update_collection_control(coll_id, faces_indexed)

    return "OK. Time remaining: '{}' Faces indexed: '{}'".format(context.get_remaining_time_in_millis(), faces_indexed)

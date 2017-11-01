import boto3
import json
import os
import logging

# Lambda Variables
LOG_LEVEL = str(os.environ.get('LOG_LEVEL', 'INFO')).upper()

if LOG_LEVEL not in ['DEBUG', 'INFO','WARNING', 'ERROR','CRITICAL']:
    LOG_LEVEL = 'INFO'
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)

# Constants

# Global Variables

# Lambda Variables

# Services
s3 = boto3.resource('s3')
iot_data = boto3.client('iot-data', region_name=os.environ['AWS_DEFAULT_REGION'])


def lambda_handler(event, context):
    topic = event["topic"]
    payload = {"type": event['type'], 'payload': event['payload']}
    response = iot_data.publish(
        topic=topic,
        qos=1,
        payload=json.dumps(payload)
    )
    
    logger.debug(response)

    return 'OK'

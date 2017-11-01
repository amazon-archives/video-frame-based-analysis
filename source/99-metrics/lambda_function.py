from notify import Notify
from logger import Logger

import os
import json
import logging

# Lambda Variables
LOG_LEVEL = str(os.environ.get('LOG_LEVEL', 'INFO')).upper()

if LOG_LEVEL not in ['DEBUG', 'INFO','WARNING', 'ERROR','CRITICAL']:
    LOG_LEVEL = 'INFO'
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

def lambda_handler(event, context):
    # Instantiate Custom classes
    logger.info("Event: "+ json.dumps(event))
    notify = Notify(logger)

    # solution id and data must be updated every solution
    solution_id = os.environ['solution_id']
    data = event['Data']
    uuid = os.environ['uuid']  # from env variable
    send_data = os.environ['SEND_ANONYMOUS_DATA']
    # Metrics Account (Production)
    metrics_url = 'https://metrics.awssolutionsbuilder.com/generic'
    # Metrics Account (Dev1)
    #metrics_url = 'https://oszclq8tyh.execute-api.us-east-1.amazonaws.com/prod/generic'

    # Send Anonymous Metrics
    if send_data == "Yes":
        notify.metrics(solution_id, uuid, data, metrics_url)
        logger.debug(uuid+" : "+json.dumps(data))

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

# Description: This Lambda function sends an email notification to a given AWS SNS topic
#               when a DMS Task fails with a particular pattern and it is matched in the DMS logs.
#               The email message body of the SNS notification contains the full event details.
# Author: Mihir Rathwa

import json
from base64 import b64decode
from boto3 import client
from gzip import decompress
from json import loads
from logging import basicConfig, getLogger, INFO, DEBUG
from os import environ
from botocore.exceptions import ClientError

basicConfig(level=INFO)
logger = getLogger(__name__)


def read_payload(event):
    logger.setLevel(DEBUG)
    logger.debug(event['awslogs']['data'])
    compressed_payload = b64decode(event['awslogs']['data'])
    uncompressed_payload = decompress(compressed_payload)
    log_payload = loads(uncompressed_payload)
    return log_payload


def read_error_logs(payload):
    error_msg = ""
    log_events = payload['logEvents']
    logger.debug(payload)
    replication_instance = payload['logGroup']
    dms_task = payload['logStream']
    # lambda_func_name = replication_instance.split('/')
    logger.debug(f'LogGroup: {replication_instance}')
    logger.debug(f'Logstream: {dms_task}')
    # logger.debug(f'Function name: {lambda_func_name[3]}')
    logger.debug(log_events)
    for log_event in log_events:
        error_msg += log_event['message']
        logger.debug('Message: %s' % error_msg.split("\n"))

    return replication_instance, dms_task, error_msg


def publish_message(replication_instance, dms_task, error_msg):

    sns_arn = environ['snsARN']  # Getting the SNS Topic ARN passed in by the environment variables.
    sns_client = client('sns')

    try:
        message = ""
        message += "\nDMS Task Error Summary" + "\n\n"
        message += "##########################################################\n"
        message += "# Replication Instance: " + str(replication_instance) + "\n"
        message += "# DMS Task: " + str(dms_task) + "\n"
        message += "# Error Message: " + "\n"
        message += "# " + str(error_msg.split("\n")) + "\n"
        message += "##########################################################\n"

        # Sending SNS notification
        sns_client.publish(
            TargetArn=sns_arn,
            Subject=f'Error for DMS Task',
            Message=message
        )

    except ClientError as e:
        logger.error("An error occurred: %s" % e)


def lambda_handler(event, context):
    """
    Main Lambda function that reads logs from AWS CloudWatch, parses them and sends to AWS SNS.

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format
        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes
        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    try:
        log_payload = read_payload(event)
        repl_ins, dms_tsk, errmessage = read_error_logs(log_payload)
        publish_message(repl_ins, dms_tsk, errmessage)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "SNS Event notification was successful."
            })
        }

    except Exception as e:
        logger.error("There was error executing function.")
        raise e

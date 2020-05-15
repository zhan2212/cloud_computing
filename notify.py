# notify.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Add utility code here
import subprocess
import json
import os
import boto3
import botocore
from botocore.exceptions import ClientError
from configparser import SafeConfigParser

if __name__ == "__main__":
    # Get configuration
    config = SafeConfigParser(os.environ)
    config.read('notify_config.ini')
    # Boto 3 Docs 1.13.3 documentation. [Source Code]
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs.html
    # Connect to SQS and get the message queue
    try:
        sqs = boto3.resource('sqs', region_name=config['aws']['AwsRegionName'])
        que = sqs.get_queue_by_name(QueueName=config['aws']['AwsQueueName'])
    except ClientError as e:
        # report error
        print(e)
        print('Client error. Fail to get queue.')
    except botocore.exceptions.ConnectionTimeourError as e:
        print(e)
        print('Connetion timeout.')
    except botocore.exceptions.IncompleteReadError as e:
        print(e)
        print('Incomplete read.')
    except:
        print('Undefined error. Fail to get que from server.')
    
    # Poll the message queue in a loop 
    while True:  
        # Boto 3 Docs 1.13.3 documentation. [Source Code]
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs.html
        # receive messages from SQS using long poll
        try:
            messages = que.receive_messages(WaitTimeSeconds=int(config['aws']['WaitTimeSeconds']), 
                                        MessageAttributeNames=['string',],
                                        MaxNumberOfMessages=1)
        except ClientError as e:
            # report error
            print(e)
            print('Fail to receive messages.')
        except botocore.exceptions.ConnectionTimeourError as e:
            print(e)
            print('Connetion timeout.')
        except botocore.exceptions.IncompleteReadError as e:
            print(e)
            print('Incomplete read.')
        except:
            print('Fail to receive messages from queue.')
              
        for message in messages:
        	data = json.loads(message.body)
            data = json.loads(data['Message'])
            print(data)
            # extract parameters from json data
            job_id = data['job_id']
            user_id = data['user_id']
            user_name = data['user_name']
            user_email = data['user_email']

        	###
        	# Create a new SES resource and specify a region.
			client = boto3.client('ses',region_name=config['aws']['AwsRegionName'])
			# Try to send the email.
			try:
			    #Provide the contents of the email.
			    response = client.send_email(
			        Destination={
			            'ToAddresses': [
			                user_email,
			            ],
			        },
			        Message={
			            'Body': {
			                'Text': {
			                    'Charset': "UTF-8",
			                    'Data': "Dear " + user_name +":\n" + "Your job " + job_id + " is completed.",
			                },
			            },
			            'Subject': {
			                'Charset': "UTF-8",
			                'Data': "Job Completed",
			            },
			        },
			        Source=config['gas']['EmailDefaultSender'],
			        # If you are not using a configuration set, comment or delete the
			        # following line
			        ConfigurationSetName="ConfigSet",
			    )
			# Display an error if something goes wrong.	
			except ClientError as e:
			    print(e.response['Error']['Message'])
			else:
			    print("Email sent! Message ID:"),
			    print(response['MessageId'])
        	#####


### EOF
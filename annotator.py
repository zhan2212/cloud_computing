#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 23 20:51:04 2020

@author: yueyangzhang
"""

import subprocess
import json
import os
import boto3
import botocore
from botocore.exceptions import ClientError

if __name__ == "__main__":
    # Boto 3 Docs 1.13.3 documentation. [Source Code]
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs.html
    # Connect to SQS and get the message queue
    try:
        sqs = boto3.resource('sqs', region_name='us-east-1')
        que = sqs.get_queue_by_name(QueueName='zhan2212_job_requests')
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
            messages = que.receive_messages(WaitTimeSeconds=20, 
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
            UUID = data['job_id']
            input_file_name = data['s3_key_input_file']
            user_id = data['user_id']
            path = 'zhan2212/' + user_id + '/' + input_file_name
            try:
                # connect with database
                dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
                # identify the table
                ann_table = dynamodb.Table('zhan2212_annotations')
            except ClientError as e:
                # report error
                print(e)
                print('Cannot connect with the database.')
            except botocore.exceptions.DataNotFoundError as e:
                print(e)
                print('Data not found.')
            except:
                print('Fail to get table.')
        
            # generate the path to save file
            cwd = os.getcwd() # get current path
            saveFolder = os.path.join(cwd, "data", UUID)
            if not os.path.exists(saveFolder):
                # create folder to store output file
                os.mkdir(saveFolder)
            savePath = os.path.join(cwd, "data", UUID, input_file_name)
            # Downloading a File from an S3 Bucket. Boto 3 Docs 1.9.42 documentation [Source Code]
            # https://boto3.amazonaws.com/v1/documentation/api/1.9.42/guide/s3-example-download-file.html
            s3 = boto3.resource('s3') # connect with s3
            try:
                # download file from S3 bucket
                s3.Bucket('gas-inputs').download_file(path, savePath)
            except ClientError as e:
                # report error
                print(e)
                print('S3 object not found.')
            except botocore.exceptions.DataNotFoundError as e:
                print(e)
                print('Data not found.')
            except:
                print('Fail to download file.')
        
            # Launch annotation job as a background process
            try:
                # subprocess management [Source Code]
                # https://docs.python.org/3/library/subprocess.html
                # run sub process
                command = "python hw5_run.py data/" + UUID + "/" + input_file_name + " " + UUID
                print(command)
                process = subprocess.Popen(command, shell=True,stdout=subprocess.PIPE)
            except:
                # report launch error
                print('Fail to launch the annotator job.')
            
            try:
                # Condition Expressions. AWS Documentation. [Source Code]
                # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html
                # update the status of job from pending to running
                # check if the job is pending
                ann_table.update_item(
                        Key={
                        'job_id': UUID
                        },
                        UpdateExpression="set job_status = :r1",
                        ConditionExpression="job_status = :r2",
                        ExpressionAttributeValues={
                        ":r1": "RUNNING", ":r2": "PENDING"
                        }) 
            except ClientError as e:
                # report error
                print(e)
                print('The task is currently running or already completed.')
            except botocore.exceptions.DataNotFoundError as e:
                print(e)
                print('Data not found.')
            except:
                print('Fail to update the database.')
        
            # Boto 3 Docs 1.13.3 documentation. [Source Code]
            # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs.html
            # Delete the message from the queue, if job was successfully submitted
            try:
                message.delete()
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
                print('Fail to delete message from que.')

 

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 23 20:51:04 2020

@author: yueyangzhang
"""

import subprocess
import json
import os
from shutil import copyfile
import boto3
import botocore

if __name__ == "__main__":
    # Connect to SQS and get the message queue
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/127134666975/zhan2212_job_requests'
    
    # Poll the message queue in a loop 
    while True:  
        # Boto 3 Docs 1.13.3 documentation. [Source Code]
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-sending-receiving-msgs.html
        # receive messages from SQS using long poll
        response = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=[
                'SentTimestamp'
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=0,
            WaitTimeSeconds=20
        )
        # If the message is invalid, continue to next iteration
        if 'Messages' not in response:
            continue
        # read the message from response
        message = response['Messages'][0]
        # use json to load the data
        data = json.loads(message['Body'])
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
        except:
            # report error
            print('Cannot connect with the database.')
            
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
                    ConditionExpression="attribute_exists (job_status) and job_status = :r2",
                    ExpressionAttributeValues={
                    ":r1": "RUNNING", ":r2": "PENDING"
                    }) 
        except:
            # report error
            print('The task is currently running or already completed.')
    
        # generate the path to save file
        cwd = os.getcwd() # get current path
        savePath = os.path.join(cwd, "data", input_file_name)
        # Downloading a File from an S3 Bucket. Boto 3 Docs 1.9.42 documentation [Source Code]
        # https://boto3.amazonaws.com/v1/documentation/api/1.9.42/guide/s3-example-download-file.html
        s3 = boto3.resource('s3') # connect with s3
        try:
            # download file from S3 bucket
            s3.Bucket('gas-inputs').download_file(path, savePath)
        except botocore.exceptions.ClientError as e:
            # report error
            print(e)
            print('S3 object not found.')
    
        # Python Check If File or Directory Exists. Guru99
        # [Source Code] https://www.guru99.com/python-check-if-file-exists.html
        folderPath = os.path.join(cwd, "data", UUID)
        if not os.path.exists(folderPath):
            # create folder to store output file
            os.mkdir(folderPath)
            
        # How to move a file in Python. Stack Overflow [Source Code]
        # https://stackoverflow.com/questions/8858008/how-to-move-a-file-in-python
        srcDir = cwd+'/data/'+input_file_name
        destDir = cwd+'/data/'+UUID+'/'+input_file_name
        try:
            # copy raw input file to output folder
            copyfile(srcDir, destDir)
            os.remove(srcDir)
        # built-in exceptions [Source Code] https://docs.python.org/3/library/exceptions.html
        except FileNotFoundError as e:
            # report error
            print(e)
            print('Input file not found.')
    
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
    
        # Boto 3 Docs 1.13.3 documentation. [Source Code]
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-sending-receiving-msgs.html
        # Delete the message from the queue, if job was successfully submitted
        receipt_handle = message['ReceiptHandle']
        # Delete received message from queue
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )

 


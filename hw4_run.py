#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 24 16:29:29 2020

@author: yueyangzhang
"""

__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import sys
import time
import driver
import boto3
import os
from botocore.exceptions import ClientError
import shutil


"""A rudimentary timer for coarse-grained profiling
"""
class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")

if __name__ == '__main__':
	# Call the AnnTools pipeline
    if len(sys.argv) > 1:
        with Timer():
            driver.run(sys.argv[1], 'vcf')
            # Upload the results file
            filePath = sys.argv[1]
            # extract path and file name
            data = filePath.split('/') 
            path = ''
            for i in range(len(data)-1):
                path += data[i] + '/'
            fullFileName = data[-1]
            fileName = fullFileName.split('.vcf')[0]
            # get the current path
            cwd = os.getcwd()
            # connect with s3
            s3_client = boto3.client('s3')
            # path of the complete file
            completeFileName = fileName + ".annot.vcf"
            completeFile = os.path.join(cwd, path, completeFileName)
            try:
                # upload results file to s3 bucket
                response = s3_client.upload_file(completeFile, 'gas-results', 'zhan2212/UserX/' + completeFileName)
            except ClientError as e:
                print('Fail to upload results file!')
                print(e)
            
            # Upload the log file
            logFileName = fileName + ".vcf.count.log" 
            logFile = os.path.join(cwd, path, logFileName)
            try:
                # upload log file to s3 bucket
                response = s3_client.upload_file(logFile, 'gas-results', 'zhan2212/UserX/' + logFileName)
            except ClientError as e:
                print('Fail to upload log file!')
                print(e)
            
            # Clean up (delete) local job files
            try:
                # folder to save all the files
                folderPath = os.path.join(cwd, path)
                # remvve the folder
                shutil.rmtree(folderPath)
            except FileNotFoundError as e:
                print("Fail to find the directory")
                print(e)
            # extract job id (UUID) from the inputs
            UUID = sys.argv[2]
            try:
                # Step 3: Create, Read, Update, and Delete an Item with Python. 
                # AWS Documentation. [Source Code]
                # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.03.html
                # connect with database
                dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
                # identify the table
                ann_table = dynamodb.Table('zhan2212_annotations')
                # update the data for item
                ann_table.update_item(
                        Key={
                        'job_id': UUID
                        },
                        UpdateExpression="set s3_results_bucket = :r1, \
                                              s3_key_result_file = :r2, \
                                              s3_key_log_file = :r3, \
                                              complete_time = :r4, \
                                              job_status = :r5",
                        ExpressionAttributeValues={
                        ':r1': "gas-results",
                        ':r2': completeFileName,
                        ':r3': logFileName,
                        ':r4': int(time.time()),
                        ':r5': "COMPLETED"
                        }) 
            except ClientError as e:
                print('Fail to update the data in table!')
                print(e)
                
    else:
        print("A valid .vcf file must be provided as input to this program.")

### EOF

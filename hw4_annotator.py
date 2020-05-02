#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 23 20:51:04 2020

@author: yueyangzhang
"""

import subprocess
from flask import Flask, request, jsonify, make_response
import os
from shutil import copyfile
import boto3
import botocore

app = Flask(__name__)

@app.route('/annotations', methods=['POST'])
def request_annotation():
    # extract job_id, filename and path from the post data
    UUID = request.form['job_id']
    input_file_name = request.form['s3_key_input_file']
    user_id = request.form['user_id']
    path = 'zhan2212/' + user_id + '/' + input_file_name
    try:
        # connect with database
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        # identify the table
        ann_table = dynamodb.Table('zhan2212_annotations')
    except:
        # report error
        res = {'code': 404,
                'status': 'error',
                'message': 'Cannot connect with the database.' }
        # Return HTTP status code 201 in flask. Stack Overflow [Source  Code]
        # https://stackoverflow.com/questions/7824101/return-http-status-code-201-in-flask
        # make response and set headers
        response = make_response(jsonify(res), 404)
        response.headers['Content-Type'] = 'application/json'
        response.headers['Code'] = 404
        return response
        
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
        res = {'code': 409,
                'status': 'error',
                'message': 'The task is currently running or already completed.' }
        # Return HTTP status code 201 in flask. Stack Overflow [Source  Code]
        # https://stackoverflow.com/questions/7824101/return-http-status-code-201-in-flask
        # make response and set headers
        response = make_response(jsonify(res), 409)
        response.headers['Content-Type'] = 'application/json'
        response.headers['Code'] = 409
        return response
    
    # generate the path to save file
    cwd = os.getcwd() # get current path
    savePath = os.path.join(cwd, "data", input_file_name)
    # Downloading a File from an S3 Bucket. Boto 3 Docs 1.9.42 documentation [Source Code]
    # https://boto3.amazonaws.com/v1/documentation/api/1.9.42/guide/s3-example-download-file.html
    s3 = boto3.resource('s3') # connect with s3
    try:
        # download file from S3 bucket
        s3.Bucket('gas-inputs').download_file(path, savePath)
    except botocore.exceptions.ClientError:
        # report error
        res = {'code': 404,
                'status': 'error',
                'message': 'S3 object not found.' }
        # Return HTTP status code 201 in flask. Stack Overflow [Source  Code]
        # https://stackoverflow.com/questions/7824101/return-http-status-code-201-in-flask
        # make response and set headers
        response = make_response(jsonify(res), 404)
        response.headers['Content-Type'] = 'application/json'
        response.headers['Code'] = 404
        return response

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
    except FileNotFoundError:
        # report error
        res = {'code': 404,
                'status': 'error',
                'message': 'Input file not found.' }
        # Return HTTP status code 201 in flask. Stack Overflow [Source  Code]
        # https://stackoverflow.com/questions/7824101/return-http-status-code-201-in-flask
        # make response and set headers
        response = make_response(jsonify(res), 404)
        response.headers['Content-Type'] = 'application/json'
        response.headers['Code'] = 404
        return response

    try:
        # subprocess management [Source Code]
        # https://docs.python.org/3/library/subprocess.html
        # run sub process
        command = "python hw4_run.py data/" + UUID + "/" + input_file_name + " " + UUID
        print(command)
        process = subprocess.Popen(command, shell=True,stdout=subprocess.PIPE)
    except:
        # report launch error
        res = {'code': 500,
                'status': 'error',
                'message': 'Fail to launch the annotator job.'}
        # Return HTTP status code 201 in flask. Stack Overflow [Source  Code]
        # https://stackoverflow.com/questions/7824101/return-http-status-code-201-in-flask
        # make response and set headers
        response = make_response(jsonify(res), 500)
        response.headers['Content-Type'] = 'application/json'
        response.headers['Code'] = 500
        return response
        
    # dictionary to store output data
    res = {}
    res['code'] = 201
    res['data'] = {}
    res['data']['job_id'] = UUID
    res['data']['input_file'] = input_file_name
    # Return HTTP status code 201 in flask. Stack Overflow [Source  Code]
    # https://stackoverflow.com/questions/7824101/return-http-status-code-201-in-flask
    # make response and set headers
    response = make_response(jsonify(res), 201)
    response.headers['Content-Type'] = 'application/json'
    response.headers['Code'] = 201
    return response
#   
 
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)



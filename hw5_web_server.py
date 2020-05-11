#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 23 20:50:48 2020

@author: yueyangzhang
"""

from flask import Flask, request, jsonify, render_template, make_response
import boto3
import uuid
from botocore.exceptions import ClientError
import time
import json
import botocore


app = Flask(__name__)


@app.route("/annotate/job", methods=["GET"])
def annotate_job():
    # extract name from the URL
    path = request.args.get('key')
    fileName = path.split('/')[-1]
    input_file_name = fileName.split("~")[1]
    UUID = fileName.split("~")[0]
    # Create a job item and persist it to the annotations database
    # Step 3: Create, Read, Update, and Delete an Item with Python. 
    # AWS Documentation. [Source Code]
    # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.03.html
    # connect with database
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    # identify table
    ann_table = dynamodb.Table('zhan2212_annotations')
    # prepare the data for database
    data = {"job_id": UUID,
            "user_id": 'userX', 
            "input_file_name": input_file_name, 
            "s3_inputs_bucket": "gas-inputs",
            "s3_key_input_file": fileName, 
            "submit_time": int(time.time()),
            "job_status": "PENDING"
    }
    # insert the data into the table
    try:
        ann_table.put_item(Item=data) 
    except ClientError as e:
        print(e)
        res = {}
        res['code'] = 500
        res['status'] = 'error'
        res['messgae'] = 'Fail to save job in database'
        return jsonify(res)
    # JSON encoding error publishing sns message with boto3. Stack Overflow. [Source Code]
    # https://stackoverflow.com/questions/35071549/json-encoding-error-publishing-sns-message-with-boto3
    # connect to SNS server
    sns = boto3.client('sns')
    # Publish a simple message to the specified SNS topic
    response = sns.publish(
        TopicArn='arn:aws:sns:us-east-1:127134666975:zhan2212_job_requests',   
        Message = json.dumps(data) 
    )
    # dictionary to store output data
    res = {}
    res['code'] = 201
    res['data'] = {}
    res['data']['job_id'] = UUID
    res['data']['input_file'] = fileName
    # Return HTTP status code 201 in flask. Stack Overflow [Source  Code]
    # https://stackoverflow.com/questions/7824101/return-http-status-code-201-in-flask
    # make response and set headers
    response = make_response(jsonify(res), 201)
    response.headers['Content-Type'] = 'application/json'
    response.headers['Code'] = 201
    return response



@app.route('/annotate', methods=['GET'])
def annotate():
    print(request.url)
    # Define S3 policy fields and conditions
    fields = {"acl": "private", "success_action_redirect": str(request.url) + "/job"}
    # set up the redirect URL
    conditions = [
        {"acl": "private"},
        ["starts-with", "$success_action_redirect", str(request.url) + "/job"]
    ]
    # set the username
    userName = 'userX'
    try:
        # Please provide better documentation, and examples, for s3 `generate_presigned_post`
        # Github. [Source Code]
        # https://github.com/boto/boto3/issues/1857
        # connect to S3 server
        s3 = boto3.client('s3', config = botocore.config.Config(signature_version='s3v4'))
        # generate UUID
        UUID = str(uuid.uuid4())
        # Generate signed POST request
        # Boto 3 Docs 1.12.46 documentation [Source Code]
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
        post = s3.generate_presigned_post(
            Bucket = 'gas-inputs', # bucket name
            Key = 'zhan2212/' + userName + '/' + UUID + '~' + '${filename}', # path to upload
            Fields = fields, # fileds
            Conditions = conditions, # conditions
            ExpiresIn = 200 # expire time 200s
        )
    except ClientError:
        # report launch error
        res = {'code': 500,
                'status': 'error',
                'message': 'Fail to generate post!'}
        # Return HTTP status code 201 in flask. Stack Overflow [Source  Code]
        # https://stackoverflow.com/questions/7824101/return-http-status-code-201-in-flask
        # make response and set headers
        response = make_response(jsonify(res), 500)
        response.headers['Content-Type'] = 'application/json'
        response.headers['Code'] = 500
        return response

    # Render the upload form template and pass post to HTML
    return render_template("annotate.html", data = post, url = str(request.url) + "/job")

@app.route('/annotate/files', methods=['GET'])
def get_object_list():
    try:
        # connect to s3 
        s3 = boto3.client('s3')
        # list all the file ojects
        data = s3.list_objects(Bucket='gas-inputs', Prefix='zhan2212/')
        # make response
    except:
        # report error
        res = {'code': 404,
                'status': 'error',
                'message': 'S3 File not found.' }
        # Return HTTP status code 201 in flask. Stack Overflow [Source  Code]
        # https://stackoverflow.com/questions/7824101/return-http-status-code-201-in-flask
        # make response and set headers
        response = make_response(jsonify(res), 404)
        response.headers['Content-Type'] = 'application/json'
        response.headers['Code'] = 404
        return response
    
    res = {}
    res['code'] = 200
    res['data'] = {}
    res['data']['files'] = []
    for i in range(1, len(data['Contents'])):
        obj = data['Contents'][i]
        res['data']['files'].append(obj['Key'])
    # Return HTTP status code 201 in flask. Stack Overflow [Source  Code]
    # https://stackoverflow.com/questions/7824101/return-http-status-code-201-in-flask
    # make response and set headers
    response = make_response(jsonify(res), 201)
    response.headers['Content-Type'] = 'application/json'
    response.headers['Code'] = 201
    
    return response


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
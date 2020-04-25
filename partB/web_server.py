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

app = Flask(__name__)

@app.route('/annotate', methods=['GET'])
def annotate():
    # Define S3 policy fields and conditions
    fields = {"acl": "private"}
    # set up the redirect URL
    conditions = [
        {"acl": "private"},
        ["starts-with", "$success_action_redirect", "http://zhan2212-hw3-ann.ucmpcs.org:5000/annotations"]
    ]
    # generate id
    UUID = str(uuid.uuid4()) # generate uuid
    # set the username
    userName = 'userX'
    try:
        # connect to S3 server
        s3 = boto3.client('s3')
        # Generate signed POST request
        # Boto 3 Docs 1.12.46 documentation [Source Code]
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
        post = s3.generate_presigned_post(
            Bucket = 'gas-inputs', # bucket name
            Key = 'zhan2212/' + userName + '/' + UUID + '~${filename}', # path to upload
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
    return render_template("annotate.html", data = post)

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
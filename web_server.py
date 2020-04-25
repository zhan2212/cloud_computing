#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 23 20:50:48 2020

@author: yueyangzhang
"""

from flask import Flask, request, jsonify, render_template
import boto3
import uuid

app = Flask(__name__)

@app.route('/annotate', methods=['GET'])
def annotate():
    # Define S3 policy fields and conditions
    fields = {"acl": "private"}
    conditions = [
        {"acl": "private"},
        ["starts-with", "$success_action_redirect", "http://0.0.0.0:8000/annotations"]
    ]
    # generate id
    UUID = str(uuid.uuid4()) # generate uuid
    userName = 'userX'

    # Generate signed POST request
    s3 = boto3.client('s3')
    post = s3.generate_presigned_post(
        Bucket='gas-inputs',
        Key='zhan2212/' + userName + '/' + UUID + '~${filename}',
        Fields=fields,
        Conditions=conditions,
        ExpiresIn=200
    )

    # Render the upload form template
    return render_template("annotate.html", data=post, URL = '/annotate')

@app.route('/annotate/files', methods=['GET'])
def get_object_list():
    s3 = boto3.client('s3')
    data = s3.list_objects(Bucket='gas-inputs', Prefix='zhan2212/')
    res = {}
    res['code'] = 200
    res['data'] = {}
    res['data']['files'] = []
    for i in range(1, len(data['Contents'])):
        obj = data['Contents'][i]
        res['data']['files'].append(obj['Key'])
    print(res)
    
    return jsonify(res)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
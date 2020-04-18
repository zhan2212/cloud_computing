#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 17 10:53:35 2020

@author: yueyangzhang
"""

import subprocess
import uuid
from flask import Flask, request, jsonify
import os
from shutil import copyfile

app = Flask(__name__)

@app.route('/annotations', methods=["POST"])
def submit():
    fileName = str(request.args.get('input_file')) # get input file name
    # Generating Random id’s using UUID in Python. Geeks For Geeks [Source Code]
    # https://www.geeksforgeeks.org/generating-random-ids-using-uuid-python/
    UUID = str(uuid.uuid4()) # generate uuid
    folderName = fileName + "_" + UUID # create folder name with input file and uuid
    
    # Python Check If File or Directory Exists. Guru99
    # [Source Code] https://www.guru99.com/python-check-if-file-exists.html
    cwd = os.getcwd() # get current path
    path = os.path.join(cwd, "data", folderName)
    if not os.path.exists(path):
        # create folder to store output file
        os.mkdir(path)
        
    # How to move a file in Python. Stack Overflow [Source Code]
    # https://stackoverflow.com/questions/8858008/how-to-move-a-file-in-python
    srcDir = cwd+'/data/'+fileName+'.vcf'
    destDir = cwd+'/data/'+folderName+'/'+fileName+'.vcf'
    try:
        # copy raw input file to output folder
        copyfile(srcDir, destDir)
    # built-in exceptions [Source Code] https://docs.python.org/3/library/exceptions.html
    except FileNotFoundError:
        # report error
        return {'code': 404,
                'status': 'error',
                'message': 'Input file not found.' }

    try:
        # subprocess management [Source Code]
        # https://docs.python.org/3/library/subprocess.html
        # run sub process
        command = "python run.py data/" +folderName + "/" + fileName +".vcf"
        process = subprocess.Popen(command, shell=True,stdout=subprocess.PIPE)
    except:
        # report launch error
        return {'code': 500,
                'status': 'error',
                'message': 'Fail to launch the annotator job.'}
    # dictionary to store output data
    res = {}
    res['code'] = 201
    res['data'] = {}
    res['data']['job_id'] = UUID
    res['data']['input_file'] = fileName + '.vcf'
    
    return jsonify(res)
    
    
@app.route('/annotations/<job_id>', methods=["GET"])
def retrieve(job_id):
    cwd = os.getcwd() # get current path
    path = os.path.join(cwd, "data")
    # Python | os.listdir() method. Geeks For Geeks [Source Code]
    # https://www.geeksforgeeks.org/python-os-listdir-method/
    files = os.listdir(path) # get all files/folders inside the data folder
    destFolder = ""
    # find the folder with input job id
    for file in files:
        if job_id in file:
            destFolder = file
            break
    
    data = destFolder.split("_")
    fileName = ""
    for i in range(len(data)-1):    
        fileName += data[i]
    # path of the complete file
    completeFile = os.path.join(cwd, "data", destFolder,fileName + ".annot.vcf" ) 
    # dictionary to store result
    res = {}
    res['code'] = 200
    res['data'] = {}
    res['data']['job_id'] = job_id
    
    if os.path.exists(completeFile):
        res['data']['job_status'] = "completed"
    else:
        res['data']['job_status'] = "running"
        
    logFile = os.path.join(cwd, "data", destFolder,fileName + ".vcf.count.log" ) 
    try:
        # read content from the log file
        with open(logFile) as f:
            res['data']['log'] = f.read().replace('\n', '')
    except FileNotFoundError:
        # report log file not found error
        return {'code': 404,
                'status': 'error',
                'message': 'Log file not found.'}
    return res


@app.route('/annotations', methods=["GET"])
def retrieveList():
    cwd = os.getcwd() # get the current folder
    path = os.path.join(cwd, "data")
    files = os.listdir(path) # list all files in the folder
    # dictionary to store result
    res = {}
    res['code'] = 200
    res['data'] = {}
    res['data']['jobs'] = []
    
    for file in files:
        # iterate through all job folders
        if os.path.isdir(os.path.join(cwd, "data",file,'')):
            job_id = file.split("_")[1]
            url = "/annotaions/" + job_id
            res['data']['jobs'] = {}
            res['data']['jobs']['job_id'] = job_id
            res['data']['jobs']['job_details'] = url
    return res
       
 
if __name__ == "__main__":
    app.run(host="0,0,0,0", port=5000)



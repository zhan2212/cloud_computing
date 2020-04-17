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
    fileName = str(request.args.get('input_file'))
    UUID = str(uuid.uuid4())
    folderName = fileName + "_" + UUID
    
    cwd = os.getcwd()
    path = os.path.join(cwd, "data", folderName)
    if not os.path.exists(path):
        os.mkdir(path)
    
    srcDir = cwd+'/data/'+fileName+'.vcf'
    destDir = cwd+'/data/'+folderName+'/'+fileName+'.vcf'
    copyfile(srcDir, destDir)
    
    command = "python run.py data/" +folderName + "/" + fileName +".vcf"
    process = subprocess.Popen(command, shell=True,stdout=subprocess.PIPE)
    
    res = {}
    res['code'] = 201
    res['data'] = {}
    res['data']['job_id'] = UUID
    res['data']['input_file'] = fileName + '.vcf'
    
    return jsonify(res)
    #
    
    
@app.route('/annotations/<job_id>', methods=["GET"])
def retrieve(job_id):
    
    cwd = os.getcwd()
    path = os.path.join(cwd, "data")
    files = os.listdir(path)
    destFolder = ""
    
    for file in files:
        if job_id in file:
            destFolder = file
            break
        
    if destFolder == "":
        return jsonify("Error")
    
    data = destFolder.split("_")
    fileName = data[0]
    completeFile = os.path.join(cwd, "data", destFolder,fileName + ".annot.vcf" ) 
    
    res = {}
    res['code'] = 200
    res['data'] = {}
    res['data']['job_id'] = job_id
    
    if os.path.exists(completeFile):
        res['data']['job_status'] = "completed"
    else:
        res['data']['job_status'] = "running"
        
    logFile = os.path.join(cwd, "data", destFolder,fileName + ".vcf.count.log" ) 
    with open(logFile) as f:
        res['data']['log'] = f.read() #.replace('\n', '')
        
    return res



@app.route('/annotations', methods=["GET"])
def retrieveList():
    cwd = os.getcwd()
    path = os.path.join(cwd, "data")
    files = os.listdir(path)
    
    res = {}
    res['code'] = 200
    res['data'] = {}
    res['data']['jobs'] = []
    
    
    for file in files:
        if os.path.isdir(os.path.join(cwd, "data",file,'')):
            print(file)
            job_id = file.split("_")[1]
            url = "/annotaions/" + job_id
            res['data']['jobs'] = {}
            res['data']['jobs']['job_id'] = job_id
            res['data']['jobs']['job_details'] = url
            
    return res
        
    
    
    
if __name__ == "__main__":
    app.run()



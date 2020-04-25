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
            # 1. Upload the results file
            filePath = sys.argv[1]
            data = filePath.split('/')
            path = ''
            for i in range(len(data)-1):
                path += data[i] + '/'
            fileName = data[-1]
            fileName = fileName.split('.vcf')[0]
            
            cwd = os.getcwd()
            # path of the complete file
            completeFile = os.path.join(cwd, path, fileName + ".annot.vcf" ) #.annot
            try:
                response = s3_client.upload_file(completeFile, 'gas-results', 'zhan2212/' + fileName + ".annot.vcf")
            except ClientError as e:
                print(e)
            
            # 2. Upload the log file
            logFile = os.path.join(cwd, path, fileName + ".vcf.count.log" )
            try:
                response = s3_client.upload_file(completeFile, 'gas-results', 'zhan2212/' + fileName + ".annot.vcf")
            except ClientError as e:
                print(e)
            
            
            # 3. Clean up (delete) local job files

	else:
		print("A valid .vcf file must be provided as input to this program.")

### EOF
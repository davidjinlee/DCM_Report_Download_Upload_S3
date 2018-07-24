#!/usr/bin/python
#
# Copyright 2015 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import io
import argparse
import random
import sys
import time
import os
import datetime
import boto3

import dfareporting_utils
from googleapiclient import http
from oauth2client import client
from botocore.client import Config

#Initialize AWS Client
client = boto3.client('s3')

#AWS Credentials
ACCESS_KEY_ID = AWS_ACCESS_KEY_ID
ACCESS_SECRET_KEY = AWS_SECRET_KEY
BUCKET_NAME = BUCKET_NAME


# Declare command-line flags.
argparser = argparse.ArgumentParser(add_help=False)

argparser.add_argument(
    'execution_date', type=str,
    help='Task execution date')

# Chunk size to use when downloading report files. Defaults to 32MB.
CHUNK_SIZE = 64 * 1024 * 1024

# The following values control retry behavior while the report is processing.
# Minimum amount of time between polling requests. Defaults to 10 seconds.
MIN_RETRY_INTERVAL = 60
# Maximum amount of time between polling requests. Defaults to 10 minutes.
MAX_RETRY_INTERVAL = 10 * 60
# Maximum amount of time to spend polling. Defaults to 1 hour.
MAX_RETRY_ELAPSED_TIME = 60 * 60


def main(argv):
  # Retrieve command line arguments.
  flags = dfareporting_utils.get_arguments(argv, __doc__, parents=[argparser])

  # Authenticate and construct service.
  service = dfareporting_utils.setup(flags)

  execution_date = flags.execution_date
  profile_id = DCM_PROFILE_ID
  report_id = DCM_REPORT_ID

  criteria = {
  "floodlightCriteria": {
    "dateRange": {
      "startDate": execution_date,
      "endDate": execution_date}}}

  try:
    #Update the date range
    update_date = service.reports().patch(profileId=profile_id,reportId=report_id,body=criteria).execute()
    
    # Run the report.
    report_file = service.reports().run(profileId=profile_id,
                                        reportId=report_id).execute()
    print('File with ID %s has been created.' % (report_file['id']))

    # Wait for the report file to finish processing.
    # An exponential backoff strategy is used to conserve request quota.
    sleep = 0
    start_time = time.time()
    while True:
      report_file = service.files().get(reportId=report_id,
                                        fileId=report_file['id']).execute()

      status = report_file['status']
      if status == 'REPORT_AVAILABLE':
        print('File status is %s, ready to download.' % (status))
        # 2. (optional) Generate browser URL.
        #generate_browser_url(service, report_id, report_file['id'])

        # 3. Directly download the file.
        direct_download_file(service, report_id, report_file['id'])
        
        return
      elif status != 'PROCESSING':
        print('File status is %s, processing failed.' % (status))
        return
      elif time.time() - start_time > MAX_RETRY_ELAPSED_TIME:
        print('File processing deadline exceeded.')
        return

      sleep = next_sleep_interval(sleep)
      print('File status is %s, sleeping for %d seconds.' % (status, sleep))
      time.sleep(sleep)

  except client.AccessTokenRefreshError:
    print ('The credentials have been revoked or expired, please re-run the '
           'application to re-authorize')


def next_sleep_interval(previous_sleep_interval):
  """Calculates the next sleep interval based on the previous."""
  min_interval = previous_sleep_interval or MIN_RETRY_INTERVAL
  max_interval = previous_sleep_interval * 2 or MIN_RETRY_INTERVAL
  return min(MAX_RETRY_INTERVAL, random.randint(min_interval, max_interval))

def generate_browser_url(service, report_id, file_id):
  """Prints the browser download URL for the file."""
  report_file = service.files().get(
      reportId=report_id, fileId=file_id).execute()
  browser_url = report_file['urls']['browserUrl']

  print('File %s has browser URL: %s.' % (report_file['id'], browser_url))


def direct_download_file(service, report_id, file_id):
  """Downloads a report file to disk."""
  # Retrieve the file metadata.
  report_file = service.files().get(
      reportId=report_id, fileId=file_id).execute()

  if report_file['status'] == 'REPORT_AVAILABLE':
    # Prepare a local file to download the report contents to.
    out_file = io.FileIO(generate_file_name(report_file), mode='wb')

    # Create a get request.
    request = service.files().get_media(reportId=report_id, fileId=file_id)

    # Create a media downloader instance.
    # Optional: adjust the chunk size used when downloading the file.
    downloader = http.MediaIoBaseDownload(
        out_file, request, chunksize=CHUNK_SIZE)

    # Execute the get request and download the file.
    download_finished = False
    while download_finished is False:
      _, download_finished = downloader.next_chunk()

    print('File %s downloaded to %s' % (report_file['id'],
                                        os.path.realpath(out_file.name)))


def generate_file_name(report_file):
  """Generates a report file name based on the file metadata."""
  # If no filename is specified, use the file ID instead.
  file_name = FILE_PATH + report_file['fileName']
  extension = '.csv' if report_file['format'] == 'CSV' else '.xml'
  return file_name + extension

def s3_creation_upload(argv):

	execution_date = flags.execution_date
	data = open(file_name,"rb")

	s3 = boto3.resource(
	's3',
	aws_access_key_id=ACCESS_KEY_ID,
	aws_secret_access_key=ACCESS_SECRET_KEY)


	key = AWS_KEY

	s3.Bucket(BUCKET_NAME).put_object(Key=key,Body=data)

	print("Upload To S3 - Complete")

if __name__ == '__main__':
  main(sys.argv)


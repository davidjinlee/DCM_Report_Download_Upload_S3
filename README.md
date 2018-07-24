# DCM Report Download Then Upload To S3
This repository contains a Python script that extracts a performance report from DCM Reporting for a specified date. It then uploads it into AWS S3.

# Overview 
1. Authenticate using the "authenticate_using_user_account.py" script (it should publish a .dat file).
2. Create a report in DCM reporting and get the report ID and profile ID
3. Execute script in command line in this format: python "DCM_Report_Download_Upload_S3" [Date]

"AWS_ACCESS_KEY_ID" must be replaced with the AWS access key for your account

"AWS_SECRET_KEY" must be replaced with the AWS secret key for your account

"BUCKET_NAME" must be replaced with the S3 bucket name you desire

"AWS_KEY" must be replaced with the S3 key that you desire

"DCM_PROFILE_ID" must be replaced with the DCM profile ID for your account

"DCM_REPORT_ID" must be replaced with the DCM report ID for the report that you created in the reporting UI

"FILE_PATH" must be replaced with the location you want the file to be downloaded on your machine.

# Databricks notebook source
import urllib.request
import shutil
import os
from datetime import datetime, date, timezone
from dateutil.relativedelta import relativedelta

# Code to get current_month - 2 months programmatically
two_months_ago = date.today() - relativedelta(months=4)
formatted_date = two_months_ago.strftime('%Y-%m')

#Define directory and file path
dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{formatted_date}"
file_path = f"{dir_path}/yellow_tripdata_{formatted_date}.parquet"
file_name = file_path.split('/')[-1]
print(file_name)
print(file_path)

try:
    # Now lets check if the file is already downloaded
    dbutils.fs.ls(file_path)
    dbutils.jobs.taskValues.set(key="continue_downstream", value='no')
    print(f"File '{file_name}' already present. Aborting downstream task...")
except:
    try:
        # Target URL of the public csv file to download
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{formatted_date}.parquet"

        #Open a connection to remote URL and stream the remote
        response = urllib.request.urlopen(url)

        # Creating the local directory
        os.makedirs(dir_path, exist_ok=True)
        
        #save the streamed content to the local file in binary mode
        with open(file_path,'wb') as f:
            shutil.copyfileobj(response,f)

        # Now lets set current_process_month parameter so that it can be used in downstream task.
        dbutils.jobs.taskValues.set(key='current_process_month', value=formatted_date)
        print(f"The current process month: {formatted_date}")

        #Set continue_downstream parameter to yes to continue downstream tasks
        dbutils.jobs.taskValues.set(key="continue_downstream", value='yes')
        print(f"File '{file_name}' has been downloaded successfully")
    except Exception as e:
        #Set continue_downstream parameter to no to disbale downstream task if file is not downloaded
        dbutils.jobs.taskValues.set(key="continue_downstream", value='no')
        print(f"File download failed: {str(e)}")
finally:
    print("The yellow trip file ingest process has been completed!")     

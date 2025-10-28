# Databricks notebook source
import urllib.request
import shutil
import os
from datetime import datetime, date, timezone
from dateutil.relativedelta import relativedelta
import sys

# Go two levels to reach the project root
project_root =  os.path.abspath(os.path.join(os.getcwd(), "../../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

from modules.data_loader.file_downloader import download_file

# Code to get current_month - 2 months 
two_months_ago = date.today() - relativedelta(months=4)
formatted_date = two_months_ago.strftime('%Y-%m')


#Define directory and file path
dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{formatted_date}"
local_file_path = f"{dir_path}/yellow_tripdata_{formatted_date}.parquet"
file_name = local_file_path.split('/')[-1]

print(file_name)
print(local_file_path)

try:
    # Now lets check if the file is already downloaded
    dbutils.fs.ls(local_file_path)
    dbutils.jobs.taskValues.set(key="continue_downstream", value='no')
    print(f"File '{file_name}' already present. Aborting downstream task...")
except:
    try:
        # Target URL of the public csv file to download
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{formatted_date}.parquet"

        # Create the directory and store downloaded file
        download_file(url, dir_path, local_file_path)

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
    print("The yellow trip file ingest process is now complete!")     

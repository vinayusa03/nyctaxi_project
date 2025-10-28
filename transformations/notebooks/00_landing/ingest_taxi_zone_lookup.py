# Databricks notebook source
import os
import sys

# Go two levels to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)
print(project_root)
print(sys.path)

import urllib.request
import shutil
from modules.data_loader.file_downloader import download_file

try:
    # Target URL of the public csv file to download
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
 
    # Create the target directory to store downloaded csv file 
    lookup_dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/lookup"

    # Define a full local path (including filename) where the file will be saved
    local_file_path = f"{lookup_dir_path}/taxi_zone_lookup.csv"

    # Download the file 
    download_file(url, lookup_dir_path, local_file_path)

    dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
    print("File has benn successfully downloaded!")
except Exception as e:
    dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
    print(f" Lookup File download failed :{str(e)}")
finally:
    print("The lookup ingestion process is now Complete!")
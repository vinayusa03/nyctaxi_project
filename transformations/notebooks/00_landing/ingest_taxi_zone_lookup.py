# Databricks notebook source
import urllib.request
import shutil
import os

try:
    # Target URL of the public csv file to download
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    #Open a connection to remote URL and fetch  the csv file as a stream 
    response = urllib.request.urlopen(url)
    # Create the destination directory for stroing downloaded csv file 
    lookup_dir_path = "/Volumes/nyctaxi/00_landing/data_sources/lookup"
    os.makedirs(lookup_dir_path, exist_ok=True)
    # Define a full local path (including filename) where the file will be saved
    lookup_file_path = lookup_dir_path + "/taxi_zone_lookup.csv"
    # Write the contents of the response stream to the specified local file path
    with open(lookup_file_path, 'wb') as f:
      shutil.copyfileobj(response,f)
    dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
    print("File Has Been Successfully Downloaded")
except Exception as e:
    dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
    print(f"File Download Failed :{str(e)}")
finally:
    print("The Lookup Ingest Has Been Completed!")
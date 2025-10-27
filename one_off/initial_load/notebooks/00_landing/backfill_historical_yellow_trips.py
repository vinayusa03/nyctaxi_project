# Databricks notebook source
import urllib.request
import shutil
import os

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Download datasets from nyctaxi URL

# COMMAND ----------

# Lists the dates you want to download
# PLEASE UPDATE THIS FOR YOUR DATE RANGE, THIS COULD BE THE LAST SIX MONTHS  OF AVAILABLE DATA
dates_to_process = ['2024-12' ,'2025-01','2025-02', '2025-03', '2025-04', '2025-05']

for date in dates_to_process:
    # Target URL of the public csv file to download
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{date}.parquet"
    
    #Open a connection to remote URL and fetch the parquet file as a stream 
    response = urllib.request.urlopen(url)
    
    # Create the destination directory for stroing downloaded parquet file 
    dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{date}"
    os.makedirs(dir_path, exist_ok=True)
    
    # Define a full local path (including filename) where the file will be saved
    local_path = f"{dir_path}/yellow_tripdata_{date}.parquet"
    
    # Write the contents of the response stream to the specified local file path
    with open(local_path, 'wb') as f:
        shutil.copyfileobj(response, f)


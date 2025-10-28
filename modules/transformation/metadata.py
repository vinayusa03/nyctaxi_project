from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp

import urllib.request
import os
import shutil

def add_processed_timestamp(df: DataFrame) -> DataFrame:
    """
    Adds a 'processed_timestamp column to the DataFrame with the current timestamp.

    Parameters:
    df (DataFrame) : Input Spark DataFrame.
    process_month : current process month(current_month - 2)

    Returns: 
            DataFrame: The DataFrame with an additional 'processed_timestamp' column.
    """
    return df.withColumn('processed_timestamp', current_timestamp())
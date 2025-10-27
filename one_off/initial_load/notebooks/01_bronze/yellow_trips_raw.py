# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

df  = spark.read.format('parquet').load('/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/*')

# COMMAND ----------

audit_col_df = df.withColumn('processed_timestamp', F.current_timestamp())\
                    .withColumn('file_path', F.col('_metadata.file_path'))

# COMMAND ----------

display(audit_col_df.limit(10))

# COMMAND ----------

audit_col_df.write.mode('overwrite').saveAsTable('nyctaxi.01_bronze.yellow_trips_raw')
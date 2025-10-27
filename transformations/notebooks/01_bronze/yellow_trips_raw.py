# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

# Lets fetch current_process_month parameter value from ingest_yellow_trip notebook for further prcoessing
current_process_month = dbutils.jobs.taskValues.get(
                                                    taskKey='00_ingest_yellow_trips',
                                                    key='current_process_month'
                                                    )
print(f"The current process month: {current_process_month}")

# COMMAND ----------

# Read data from file and load into dataFrame
df  = spark.read.format('parquet').load(f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{current_process_month}")

# COMMAND ----------

df.agg(F.min(F.col('tpep_pickup_datetime'))).display()

# COMMAND ----------

# Add audit columns for audit and debug purpose.
audit_col_df = df.withColumn('processed_timestamp', F.current_timestamp())\
                    .withColumn('process_month', F.lit(current_process_month))\
                    .withColumn('file_path', F.col('_metadata.file_path'))



# COMMAND ----------

display(audit_col_df.limit(10))

# COMMAND ----------

# Write the data into Brone layer
(
    audit_col_df
    .write
    .mode('append')
    .option('mergeSchema','true')
    .saveAsTable('nyctaxi.01_bronze.yellow_trips_raw')
    )


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECt * FROM nyctaxi.01_bronze.yellow_trips_raw WHERE PROCESSED_TIMESTAMP like '2025-10-25%' and PROCESS_MONTH IS NULL;
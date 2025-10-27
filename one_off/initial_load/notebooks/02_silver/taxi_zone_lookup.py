# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType, IntegerType

# COMMAND ----------

lkpdf = spark.read.format('csv').option('header', 'true').load('/Volumes/nyctaxi/00_landing/data_sources/lookup/taxi_zone_lookup.csv')

# COMMAND ----------

df = lkpdf.select(
    F.col('LocationID').cast(IntegerType()).alias('location_id'),
    F.col('Borough').alias('borough'),
    F.col('Zone').alias('zone'),
    F.col('service_zone'),
    F.current_timestamp().alias('effective_date'),
    F.lit(None).cast(TimestampType()).alias('end_date'))

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.mode('overwrite').saveAsTable('nyctaxi.02_silver.taxi_zone_lookup')

# COMMAND ----------


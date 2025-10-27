# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType, IntegerType
from delta.tables import DeltaTable
from datetime import datetime, date

# COMMAND ----------

# Read the data from source layer
lkpdf = spark.read.format('csv').option('header', 'true').load('/Volumes/nyctaxi/00_landing/data_sources/lookup/taxi_zone_lookup.csv')

# COMMAND ----------

# Standardize datatypes and attribute name for loading into silver layer
# Add audit columns for audit and SCD-2 type purpose.
df = lkpdf.select(
    F.col('LocationID').cast(IntegerType()).alias('location_id'),
    F.col('Borough').alias('borough'),
    F.col('Zone').alias('zone'),
    F.col('service_zone'),
    F.current_timestamp().alias('effective_date'),
    F.lit(None).cast(TimestampType()).alias('end_date'),
    F.lit(1).cast(IntegerType()).alias('is_current')
    )

# COMMAND ----------

#Verify the data
display(df.limit(10))

# COMMAND ----------

# Using if condition programmatically handle Day 1 execution without manual intervention
is_first_run = not spark.catalog.tableExists("nyctaxi.02_silver.taxi_zone_lookup")

if is_first_run:

    # Write lookup data into silver table
    df.write.mode('append').saveAsTable('nyctaxi.02_silver.taxi_zone_lookup')

else:
    #Using 2 step approach to handle SCD Type-2

    # Delta table reference
    dt = DeltaTable.forName(spark,'nyctaxi.`02_silver`.taxi_zone_lookup')

    #STEP 1: Lets close the existing records in table if any changes to its non-key columns
    dt.alias('target').merge(
        df.alias('source'),
        "target.location_id = source.location_id AND target.is_current = 1"
    ).whenMatchedUpdate(
        condition = " source.end_date IS NULL AND (target.borough <> source.borough OR target.zone <> source.zone OR target.service_zone <> source.service_zone)",
        set = {
            "end_date" : "current_timestamp()",
            "is_current" : "0"
        }
    ).execute()

    #STEP 2: Let insert brand-new location_ids and updated location_ids
    dt.alias('t').merge(
        df.alias('s'),
        "t.location_id = s.location_id AND t.is_current = 1"
        ).whenNotMatchedInsert(
            values={
                "location_id": "s.location_id",
                "borough": "s.borough",
                "zone": "s.zone",
                "service_zone": "s.service_zone",
                "effective_date": "s.effective_date",
                "end_date": "s.end_date",
                "is_current": "s.is_current"  
            }
        ).execute()
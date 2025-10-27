# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.functions import datediff

# COMMAND ----------

# Lets fetch current_process_month parameter value from ingest_yellow_trip notebook for further prcoessing
current_process_month = dbutils.jobs.taskValues.get(
                                                    taskKey='00_ingest_yellow_trips',
                                                    key='current_process_month'
                                                    )
print(f"The current process month: {current_process_month}")

# COMMAND ----------

yelCleansedDF = spark.read.table('nyctaxi.`02_silver`.yellow_trips_cleansed')\
    .filter(F.col('process_month') == current_process_month)


# COMMAND ----------

display(yelCleansedDF.limit(2))

# COMMAND ----------

display(yelCleansedDF.limit(10))

# COMMAND ----------

taxiZoneLkpDF = spark.sql(''' SELECT location_id, borough, zone FROM nyctaxi.`02_silver`.taxi_zone_lookup WHERE end_date IS NULL''')

# COMMAND ----------

# Lets derive trip duration in minutes using pickup and dropoff column
yelTripMinDF = (
    yelCleansedDF.withColumn(
        'trip_duration_mins', 
        F.round(((F.unix_timestamp('tpep_dropoff_datetime') - F.unix_timestamp('tpep_pickup_datetime')) / 60),2))
)   

# COMMAND ----------

# Lets check for Null/Blank values
yelTripMinDF.select([F.count(F.when(F.col(c).isNull() | (F.col(c) == ""),1)).alias(c)  for c in ['pu_location_id', 'do_location_id']]).display()

# COMMAND ----------

# Function to fetch borough and zone for pickup and dropoff locations
def fetch_brough_zone(df, lkpdf, dfKey, lkpKey, borough_alias, zone_alias):
    return df.join(
        lkpdf.select(F.col(lkpKey),
                     F.col('borough').alias(borough_alias),
                     F.col('zone').alias(zone_alias)
                     ),
        F.col(dfKey) == F.col(lkpKey),
        'left'
    )

# COMMAND ----------

# Lets join with taxi zone lookup to fetch borough and zone for pickup and dropoff locations

## Pickup Borough and Zone
yelJoinedPuBougZoneDF = fetch_brough_zone(yelTripMinDF, taxiZoneLkpDF, 'pu_location_id', 'location_id', 'pu_borough', 'pu_zone').drop('location_id')

# COMMAND ----------

## fetch DropOff Borough and Zone
yelJoinedDoBougZoneDF = fetch_brough_zone(yelJoinedPuBougZoneDF, taxiZoneLkpDF, 'do_location_id', 'location_id', 'do_borough', 'do_zone').drop('location_id')

# COMMAND ----------

# Since taxi zone lookup is <10MB, it has been broadcasted automatically.
yelJoinedPuBougZoneDF.explain(mode='extended')

# COMMAND ----------

# Lets count after join
#yelJoinedDoBougZoneDF.count()

# COMMAND ----------

# Lets verify skew after join
#yelJoinedDoBougZoneDF.withColumn('partition_id', spark_partition_id)\
#    .groupBy('partition_id').count().display()

# COMMAND ----------

yelJoinedDoBougZoneDF.repartition(6).withColumn('process_month', F.lit(current_process_month))\
    .write.mode('append').option('mergeSchema','true').saveAsTable('nyctaxi.`02_silver`.yellow_trips_enriched')

# COMMAND ----------

spark.read.table('nyctaxi.`02_silver`.yellow_trips_enriched').display()
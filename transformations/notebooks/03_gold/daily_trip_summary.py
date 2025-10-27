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

# Lets read data from enriched table
yelEnrichedDF = spark.read.table('nyctaxi.`02_silver`.yellow_trips_enriched')\
    .filter(F.col('process_month') == current_process_month)

# COMMAND ----------

display(yelEnrichedDF.limit(2))

# COMMAND ----------

 # Lets calculate daily summary 
 dailySummaryDF = (
     yelEnrichedDF.groupBy(F.col('tpep_pickup_datetime').cast('date').alias('pickup_date'))
     .agg(
         F.count('*').alias('total_trips'),
         F.round(F.avg('passenger_count'),1).alias('avg_passengers_per_trip'),
         F.round(F.avg('trip_distance'),1).alias('avg_distance_per_trip'),
         F.round(F.avg('fare_amount'),1).alias('avg_fair_per_trip'),
         F.max('fare_amount').alias('max_fare'),
         F.min('fare_amount').alias('min_fare'),
         F.round(F.sum('total_amount'),2).alias('total_revenue')
         )
     .orderBy(F.col('total_revenue').desc())
)

# COMMAND ----------

# Write into gold layer
dailySummaryDF.write.mode('append').option('mergeSchema','true').saveAsTable('nyctaxi.`03_gold`.daily_trip_summary')

# COMMAND ----------



# COMMAND ----------


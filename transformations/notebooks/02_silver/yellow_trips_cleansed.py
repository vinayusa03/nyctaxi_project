# Databricks notebook source
import pyspark.sql.functions as F
from dateutil.relativedelta import relativedelta
from datetime import datetime

# COMMAND ----------

# Lets fetch current_process_month parameter value from ingest_yellow_trip notebook for further prcoessing
current_process_month = dbutils.jobs.taskValues.get(
                                                    taskKey='00_ingest_yellow_trips',
                                                    key='current_process_month'
                                                    )
print(f"The current process month: {current_process_month}")

# COMMAND ----------

start_of_the_month = (current_process_month + '-01')
start_of_the_next_month = (datetime.strptime(start_of_the_month, '%Y-%m-%d') + relativedelta(months=1)).strftime('%Y-%m-%d')
print(start_of_the_month)
print(start_of_the_next_month)

# COMMAND ----------

yelTriRawDF = spark.read.table('nyctaxi.`01_bronze`.yellow_trips_raw')\
    .filter(F.col('process_month') == current_process_month) # Predicate pruning

# COMMAND ----------

display(yelTriRawDF.limit(2))

# COMMAND ----------

# Lets check if we have any rows thats out of our 6 months window(Jan-June, 2025)
yelTriRawDF.agg(
    F.min(F.col('tpep_pickup_datetime')).alias('min_pickup_dttime'), 
    F.max(F.col('tpep_pickup_datetime')).alias('max_pickup_dttime'), 
    F.min(F.col('tpep_dropoff_datetime')).alias('min_dropoff_dttime'),
    F.max(F.col('tpep_dropoff_datetime')).alias('max_dropoff_dttime')).display()

# COMMAND ----------

# Lets filter rows which are out of 6 Months window
yelFilteredDF = yelTriRawDF.filter(f"""
    (tpep_pickup_datetime >= '{start_of_the_month}' AND tpep_pickup_datetime < '{start_of_the_next_month}') AND 
    (tpep_dropoff_datetime >= '{start_of_the_month}' AND tpep_dropoff_datetime < '{start_of_the_next_month}')"""
    )

# COMMAND ----------

yelFilteredDF.agg(
    F.min(F.col('tpep_pickup_datetime')).alias('min_pickup_dttime'), 
    F.max(F.col('tpep_pickup_datetime')).alias('max_pickup_dttime'), 
    F.min(F.col('tpep_dropoff_datetime')).alias('min_dropoff_dttime'),
    F.max(F.col('tpep_dropoff_datetime')).alias('max_dropoff_dttime')).display()

# COMMAND ----------

# Lets verify the count for each VendorID before we apply Vendor name logic
yelFilteredDF.groupBy('VendorID').count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Busniess logic to vendor name
# MAGIC  A code indicating the TPEP provider that provided the record.  
# MAGIC - 1 = Creative Mobile Technologies LLC
# MAGIC - 2 = Curb Mobility LLC
# MAGIC - 6 = Myle Technologies Inc
# MAGIC - 7 = Helix

# COMMAND ----------

# Lets apply above logic on vendor column
yelVendorNmDF = (
    yelFilteredDF.withColumn('vendor', 
                          F.when(F.col('VendorID') == 1, "Creative Mobile Technologies LLC")
                          .when(F.col('VendorID') == 2, "Curb Mobility LLC")
                          .when(F.col('VendorID') == 6, "Myle Technologies Inc")
                          .when(F.col('VendorID') == 7, "Helix")
                          .otherwise("unknown"))
    .drop('VendorID')
    )



# COMMAND ----------

# Lets verify the count for each VendorID after Vendor name logic
yelVendorNmDF.groupBy('vendor').count().display()

# COMMAND ----------

# Lets Analyze RateCodeID before we apply rate_type logic
yelVendorNmDF.groupBy('RateCodeID').count().display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###### Business logic to derive rate type
# MAGIC The final rate code in effect at the end of the trip. 
# MAGIC - 1 = Standard rate
# MAGIC - 2 = JFK
# MAGIC - 3 = Newark
# MAGIC - 4 = Nassau or Westchester
# MAGIC - 5 = Negotiated fare
# MAGIC - 6 = Group ride
# MAGIC - 99 = Null/unknown

# COMMAND ----------

#Lets derive rate_type using above logic
yelRateTypeDF = (
    yelVendorNmDF.withColumn('rate_type',
                             F.when(F.col('RateCodeID') == 1, "Standard rate")
                             .when(F.col('RateCodeID') == 2, "JFK")
                             .when(F.col('RateCodeID') == 3, "Newark")
                             .when(F.col('RateCodeID') == 4, "Nassau or Westchester")
                             .when(F.col('RateCodeID') == 5, "Negotiated fare")
                             .when(F.col('RateCodeID') == 6, "Group ride")
                             .otherwise('unknown')
                             ).drop('RateCodeID')

)

# COMMAND ----------

# Verify if applied logic has been effective
yelRateTypeDF.groupBy('rate_type').count().display()

# COMMAND ----------

# Lets analyze payment_type before applying logic to derive payment type name
yelRateTypeDF.groupBy('payment_type').count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Business logic to derive payment_type
# MAGIC A numeric code signifying how the passenger paid for the trip. 
# MAGIC - 0 = Flex Fare trip
# MAGIC - 1 = Credit card
# MAGIC - 2 = Cash 
# MAGIC - 3 = No charge
# MAGIC - 4 = Dispute
# MAGIC - 5 = Unknown
# MAGIC - 6 = Voided trip 

# COMMAND ----------

#Lets derive payment type using above logic
yelPaymentTypeDF = (
    yelRateTypeDF.withColumn('payment_type',
                             F.when(F.col('payment_type') == 0, "Flex Fare trip")
                             .when(F.col('payment_type') == 1, "Credit card")
                             .when(F.col('payment_type') == 2, "Cash")
                             .when(F.col('payment_type') == 3, "No charge")
                             .when(F.col('payment_type') == 4, "Dispute")
                             .when(F.col('payment_type') == 5, "Unknown")
                             .when(F.col('payment_type') == 6, "Voided trip")
                         )
)

# COMMAND ----------

# Verify if applied logic has been effective
yelPaymentTypeDF.groupBy('payment_type').count().display()

# COMMAND ----------

# lets drop file_path column and write data into cleansed delta table in silver layer
yellow_trips_cleansed_df = (
    yelPaymentTypeDF.select
    (
        yelPaymentTypeDF.vendor,
        yelPaymentTypeDF.tpep_pickup_datetime,
        yelPaymentTypeDF.tpep_dropoff_datetime,
        yelPaymentTypeDF.passenger_count,
        yelPaymentTypeDF.trip_distance,
        yelPaymentTypeDF.rate_type,
        yelPaymentTypeDF.store_and_fwd_flag,
        yelPaymentTypeDF.PULocationID.alias('pu_location_id'),
        yelPaymentTypeDF.DOLocationID.alias('do_location_id'),
        yelPaymentTypeDF.payment_type,
        yelPaymentTypeDF.fare_amount,
        yelPaymentTypeDF.extra,
        yelPaymentTypeDF.mta_tax,
        yelPaymentTypeDF.tip_amount,
        yelPaymentTypeDF.tolls_amount,
        yelPaymentTypeDF.improvement_surcharge,
        yelPaymentTypeDF.total_amount,
        yelPaymentTypeDF.congestion_surcharge,
        yelPaymentTypeDF.Airport_fee.alias('airport_fee'),
        yelPaymentTypeDF.cbd_congestion_fee,
        F.current_timestamp().alias('processed_timestamp')
        )
    )


# COMMAND ----------

# Write into silver cleansed table
yellow_trips_cleansed_df.withColumn('process_month', F.lit(current_process_month))\
    .write.mode("append").option('mergeSchema','true').saveAsTable("nyctaxi.`02_silver`.yellow_trips_cleansed")
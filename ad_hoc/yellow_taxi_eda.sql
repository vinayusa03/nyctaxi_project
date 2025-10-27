-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Which vendor makes the most revenue?
-- MAGIC

-- COMMAND ----------

SELECT VENDOR, ROUND(SUM(TOTAL_AMOUNT),2) AS TOTAL_REVENUE FROM nyctaxi.`02_silver`.yellow_trips_enriched
GROUP BY VENDOR
ORDER BY TOTAL_REVENUE DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### What is most popular pickup borough?

-- COMMAND ----------

SELECT PU_BOROUGH, COUNT(tpep_pickup_datetime) AS TOTAL_PICKUP FROM nyctaxi.`02_silver`.yellow_trips_enriched
GROUP BY PU_BOROUGH 
ORDER BY TOTAL_PICKUP DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### What is most common journey(borough to borough)?

-- COMMAND ----------

SELECT CONCAT(PU_BOROUGH,' -> ',DO_BOROUGH) AS JOURNEY, (COUNT(PU_BOROUGH)) AS TOTAL_TRIPS_BETWEEN_BOUROUGHS
 FROM nyctaxi.`02_silver`.yellow_trips_enriched
GROUP BY PU_BOROUGH, DO_BOROUGH
ORDER BY TOTAL_TRIPS_BETWEEN_BOUROUGHS DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create a time series chart showing the number of trips and total revenue per day?

-- COMMAND ----------

SELECT pickup_date, total_trips, total_revenue FROM  nyctaxi.`03_gold`.daily_trip_summary;
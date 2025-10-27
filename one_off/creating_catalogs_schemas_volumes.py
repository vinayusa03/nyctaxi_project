# Databricks notebook source
spark.sql("create catalog if not exists nyctaxi managed location 'abfss://unity-catalog-storage@dbstoragemaxs36v5rfmt2.dfs.core.windows.net/2175295891068395'")

# COMMAND ----------

spark.sql('CREATE SCHEMA IF NOT EXISTS nyctaxi.00_landing');
spark.sql('CREATE SCHEMA IF NOT EXISTS nyctaxi.01_bronze');
spark.sql('CREATE SCHEMA IF NOT EXISTS nyctaxi.02_silver');
spark.sql('CREATE SCHEMA IF NOT EXISTS nyctaxi.03_gold');

# COMMAND ----------

spark.sql(" create volume if not exists nyctaxi.00_landing.data_sources ")
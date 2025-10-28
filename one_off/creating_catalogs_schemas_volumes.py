# Databricks notebook source
spark.sql("create catalog if not exists nyctaxi managed location 'abfss://unity-catalog-storage@dbstorageuhw5o2avppt6c.dfs.core.windows.net/776520650149584'")

# COMMAND ----------

spark.sql('CREATE SCHEMA IF NOT EXISTS nyctaxi.00_landing');
spark.sql('CREATE SCHEMA IF NOT EXISTS nyctaxi.01_bronze');
spark.sql('CREATE SCHEMA IF NOT EXISTS nyctaxi.02_silver');
spark.sql('CREATE SCHEMA IF NOT EXISTS nyctaxi.03_gold');

# COMMAND ----------

spark.sql(" create volume if not exists nyctaxi.00_landing.data_sources ")

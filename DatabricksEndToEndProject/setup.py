# Databricks notebook source
# MAGIC %sql
# MAGIC create volume workspace.raw.rawVolume

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw/rawVolume/rawData/bookings")

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw/rawVolume/rawData/airports")
dbutils.fs.mkdirs("/Volumes/workspace/raw/rawVolume/rawData/customers")
dbutils.fs.mkdirs("/Volumes/workspace/raw/rawVolume/rawData/flights")

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema workspace.bronze;
# MAGIC create schema workspace.silver;
# MAGIC create schema workspace.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC create volume workspace.bronze.bronze_vol;
# MAGIC create volume workspace.silver.silver_vol;
# MAGIC create volume workspace.gold.gold_vol;
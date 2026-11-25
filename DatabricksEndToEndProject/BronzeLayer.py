# Databricks notebook source
# MAGIC %md
# MAGIC ## Incremental Data ingestion

# COMMAND ----------

dbutils.widgets.text("src","")

# COMMAND ----------

src_value = dbutils.widgets.get("src")
src_value

# COMMAND ----------

# DBTITLE 1,Cell 5
df_bookings = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","csv")\
            .option("cloudFiles.schemaLocation",f"/Volumes/workspace/bronze/bronze_vol/{src_value}/checkpoint")\
            .option ("cloudFiles.schemaEvolutionMode","rescue")\
            .load(f"/Volumes/workspace/raw/rawvolume/rawData/{src_value}/")

# COMMAND ----------

df_bookings.writeStream.format("delta")\
            .outputMode("append")\
            .trigger(once=True)\
            .option("checkpointLocation",f"/Volumes/workspace/bronze/bronze_vol/{src_value}/checkpoint")\
            .option("path",f"/Volumes/workspace/bronze/bronze_vol/{src_value}/data")\
            .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/Volumes/workspace/bronze/bronze_vol/flights/data`;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/Volumes/workspace/bronze/bronze_vol/flights/data`;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/Volumes/workspace/bronze/bronze_vol/airports/data`;
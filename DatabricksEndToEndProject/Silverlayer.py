# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df =  spark.read.format("delta").load("/Volumes/workspace/bronze/bronze_vol/bookings/data")
display(df)

# COMMAND ----------

df = df.withColumn("amount", col("amount").cast(DoubleType()))\
       .withColumn("modifiedDate", current_timestamp())\
       .withColumn("booking_date", to_date(col("booking_date")))\
       .drop("_rescued_data")
display(df)

# COMMAND ----------

df = df.drop("_rescued_data")\
        .withColumn("modifiedDate", current_timestamp())

# COMMAND ----------

##streaming table creation
@dlt.table(name="stage_bookings")
def stage_bookings():
    df = spark.readStream.format("delta").load("/Volumes/workspace/bronze/bronze_vol/bookings/data")
    return df

##streaming view
@dlt.view(name= "trans_bookings")
def trans_bookings():
    df = spark.readStream.table("stage_bookings")
    df = df.withColumn("amount", col("amount").cast(DoubleType()))\
           .withColumn("modifiedDate", current_timestamp())\
           .withColumn("booking_date", to_date(col("booking_date")))\
           .drop("_rescued_data")
    return df   
                 
@dlt.table(name ="silver_bookings")
def silver_bookings():
    df = spark.readStream.table("trans_bookings")
    return df




# COMMAND ----------

rules = {"rule1": "booking_id is not null",
        "rule2":"passenger_id is not null" }

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC * from workspace.silver.silver_bookings

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.silver.silver_bookings

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.silver.silver_flights

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.silver_scd2.silver_flights_scd2
# MAGIC /* I created a new dlt pipeline DLT_Silver_flight_SCD2 for flights to check scd2 in auto cdc flow for my reference and created silver_Scd2 schema also in workspace catalog, we have added file dim_flights_scd2.csv with few changes too to check the silver layer data */

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table workspace.silver.silver_business
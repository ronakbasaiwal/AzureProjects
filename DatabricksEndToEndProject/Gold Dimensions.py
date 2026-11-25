# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.silver.silver_flights limit 2;

# COMMAND ----------


# key columns
#dbutils.widgets.text("keycols","['flight_id']")

#cdc columns
#dbutils.widgets.text("cdccols","")

#back dated refresh
#dbutils.widgets.text("backdated_refresh","")

#source object
#dbutils.widgets.text("source_object","")
#source schema
#dbutils.widgets.text("source_schema","")

# COMMAND ----------

#keycols= eval(dbutils.widgets.get("keycols"))
#cdccols= eval(dbutils.widgets.get("cdccols"))
#backdaterefresh= eval(dbutils.widgets.get("backdated_refresh"))
#source_object = eval(dbutils.widgets.get("source_object"))
#source_schema = eval(dbutils.widgets.get("source_schema"))

keycols = "['flight_id']"
key_cols_list = eval(keycols)
cdc_cols = "modifiedDate"
backdate_refresh = ''
source_object = "silver_flights"
source_schema = "silver"
target_schema = "gold"
target_object = "DimFlights"
surrogate_key = "DimFlightsKey"
catalog = "workspace"


# COMMAND ----------

## Airport parameters
#keycols = "['airport_id']"
#key_cols_list = eval(keycols)
cdc_cols = "modifiedDate"
backdate_refresh = ''
#source_object = "silver_airports"
source_schema = "silver"
target_schema = "gold"
#target_object = "DimAirports"
#surrogate_key = "DimAirportsKey"
catalog = "workspace"

# COMMAND ----------

## Passengers parameters
#keycols = "['passenger_id']"
#key_cols_list = eval(keycols)
cdc_cols = "modifiedDate"
backdate_refresh = ''
#source_object = "silver_customers"
source_schema = "silver"
target_schema = "gold"
#target_object = "DimCustomers"
#surrogate_key = "DimCustomersKey"
catalog = "workspace"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental Data Ingestion

# COMMAND ----------

# DBTITLE 1,if len(backdate_refresh) == 0:
if len(backdate_refresh) == 0:
  if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    last_load = spark.sql(f"select max({cdc_cols}) from {catalog}.{target_schema}.{target_object}").collect()[0][0]
  else:
    last_load = "1900-01-01 00:00:00"
else:
  last_load  = backdate_refresh
last_load

# COMMAND ----------

df_src = spark.sql(f"select * from {catalog}.{source_schema}.{source_object} where {cdc_cols} > '{last_load}'")
df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Old vs New Records

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
 #key cols incremental load
 key_cols_string_incremental = ','.join(key_cols_list)
 df_trg =  spark.sql(f""" select {key_cols_string_incremental}, {surrogate_key}, create_date,update_date from {catalog}.{target_schema}.{target_object}""")
else:
    #key cols initial load 
    key_cols_string_init  =  [f" '' as {i}" for i in key_cols_list]
    key_cols_string_init = ','. join(key_cols_string_init)
    df_trg =  spark.sql(f"select {key_cols_string_init}, cast('0' as int) as {surrogate_key}, cast('1900-01-01 00:00:00' as timestamp) as create_date, cast('1900-01-01 00:00:00' as timestamp) as update_date where 1 = 0")

# COMMAND ----------

df_trg.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join Condition

# COMMAND ----------

join_condition = " and ".join([f"src.{i} = target.{i}" for i in key_cols_list])
join_condition

# COMMAND ----------

df_src.createOrReplaceTempView("src")
df_trg.createOrReplaceTempView("target")

df_join = spark.sql(f""" select src.*, target.{surrogate_key} as {surrogate_key}, 
                    target.create_date, target.update_date from src left join target on {join_condition} """)
df_join.display()

# COMMAND ----------

#old records
df_old = df_join.filter(col(f'{surrogate_key}').isNotNull())
#new records
df_new = df_join.filter(col(f'{surrogate_key}').isNull())

# COMMAND ----------

df_old.display()

# COMMAND ----------

df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enriching Dataframes 

# COMMAND ----------

# MAGIC %md
# MAGIC > Preparing old DFs

# COMMAND ----------

df_old_enr = df_old.withColumn('update_date', current_timestamp())
df_old_enr.display()

# COMMAND ----------

# MAGIC %md
# MAGIC > Preparing New DFs

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    max_surrogate_key = spark.sql(f""" select max({surrogate_key}) from 
                                  {catalog}.{target_schema}.{target_object} """).collect()[0][0]
    max_surrogate_key = int(max_surrogate_key)
    df_new_enr = df_new.withColumn(f'{surrogate_key}',lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
                        .withColumn('create_date',current_timestamp())\
                        .withColumn('update_date',current_timestamp() )
    #print (max_surrogate_key,'inside if ', lit(max_surrogate_key)+lit(1)+monotonically_increasing_id(), type(max_surrogate_key))
else:
    max_surrogate_key = 0
    df_new_enr = df_new.withColumn(f'{surrogate_key}',lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
                        .withColumn('create_date',current_timestamp())\
                        .withColumn('update_date',current_timestamp() )
                            
df_new_enr.display()             

# COMMAND ----------

print(max_surrogate_key)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unioning df_new_enr and df_old_enr

# COMMAND ----------

df_union  = df_old_enr.unionByName(df_new_enr)
df_union.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert

# COMMAND ----------

from delta.tables import DeltaTable
if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    dlt_obj = DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}")
    dlt_obj.alias("target")\
           .merge(df_union.alias("src"), f"target.{surrogate_key} = src.{surrogate_key}" ) \
           .whenMatchedUpdateAll(condition = f'src.{cdc_cols} >= target.{cdc_cols}')\
           .whenNotMatchedInsertAll().execute()
else:
    df_union.write.format("delta").mode("append")\
     .saveAsTable(f"{catalog}.{target_schema}.{target_object}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.gold.dimFlights ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.gold.dimAirports;
# Databricks notebook source
cdc_cols = "modifiedDate"
backdate_refresh = ''
source_object = "silver_bookings"
source_schema = "silver"
target_schema = "gold"
target_object = "FactBookings"
#surrogate_key = "DimFlightsKey"
catalog = "workspace"
fact_table = f"{catalog}.{source_schema}.{source_object}"
#fact key cols
fact_key_cols =["DimFlightsKey","DimAirportsKey","DimCustomersKey","booking_date"]

# COMMAND ----------

dimensions = [ 
               {"table":"workspace.gold.DimAirports" ,
                "alias":"airports",
                "join_keys":[("airport_id", "airport_id")]
                },
               {"table":"workspace.gold.DimCustomers" ,
                "alias":"customers",
                "join_keys":[("passenger_id", "passenger_id")]
                },
               {"table":"workspace.gold.DimFlights" ,
                "alias":"flights",
                "join_keys":[("flight_id", "flight_id")] #(fact_col, dim_col)
                } 
               ]
fact_columns =["amount","booking_date","modifiedDate"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Last Load Date

# COMMAND ----------

if len(backdate_refresh) == 0:
  if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    last_load = spark.sql(f"select max({cdc_cols}) from {catalog}.{target_schema}.{target_object}").collect()[0][0]
  else:
    last_load = "1900-01-01 00:00:00"
else:
  last_load  = backdate_refresh
last_load

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dynamic Fact query creation 

# COMMAND ----------

def generate_fact_query_incremental(fact_table, dimensions, fact_cols, cdc_col, last_load):
    fact_alias ='f'
    select_cols =  [f"{fact_alias}.{cols}" for cols in fact_cols]

    join_clause = []
    for dim in dimensions:
        dim_alias = dim['alias']
        dim_table = dim['table']
        join_keys = dim['join_keys']
        surrogate_key = f"{dim_alias}.{dim_table.split('.')[-1]}Key"
        select_cols.append(surrogate_key) #dim cols appended 

        #Build on clause
        on_condition = [f"{dim_alias}.{dk} = {fact_alias}.{fk}" for fk, dk in join_keys ]
        join_condition = f"Left join {dim_table} {dim_alias} on "+ " And ".join(on_condition)
        join_clause.append(join_condition)

    #Final select and join clause
    select_cols = " , ".join(select_cols)
    join_cols =  " \n ".join(join_clause)

    #where clause for incremental filtering

    where_clause = f"{fact_alias}.{cdc_col} >= '{last_load}' "

    #Final query
    return f"select {select_cols} from {fact_table} {fact_alias} {join_cols} \n where  {where_clause}"
    


# COMMAND ----------

query = generate_fact_query_incremental(fact_table, dimensions, fact_columns, cdc_cols, last_load)
print(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## df_Fact creation

# COMMAND ----------

df_fact = spark.sql(query)
df_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact key cols merge condition

# COMMAND ----------

fact_key_cols_str = " and ".join([f"src.{col} = target.{col}" for col in fact_key_cols])
fact_key_cols_str

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert query

# COMMAND ----------

from delta.tables import DeltaTable
if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    dlt_obj = DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}")
    dlt_obj.alias("target")\
           .merge(df_fact.alias("src"), fact_key_cols_str ) \
           .whenMatchedUpdateAll(condition = f'src.{cdc_cols} >= target.{cdc_cols}')\
           .whenNotMatchedInsertAll().execute()
else:
    df_fact.write.format("delta").mode("append")\
     .saveAsTable(f"{catalog}.{target_schema}.{target_object}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.gold.factbookings

# COMMAND ----------

# MAGIC %sql
# MAGIC select dimairportskey,dimflightskey,dimcustomerskey, count(*) as cnt  from workspace.gold.factbookings group by dimairportskey,dimflightskey,dimcustomerskey having cnt >1;
# MAGIC /* it has duplicates thats why we are adding booking date also in fact cols list*/
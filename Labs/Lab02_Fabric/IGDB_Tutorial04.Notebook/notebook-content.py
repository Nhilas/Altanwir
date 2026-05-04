# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "21686009-3b8b-4dac-a144-e9cf00d8b9cc",
# META       "default_lakehouse_name": "IGDBAnalytics",
# META       "default_lakehouse_workspace_id": "d1206eb3-2259-44b8-844a-409f1a63f284",
# META       "known_lakehouses": [
# META         {
# META           "id": "21686009-3b8b-4dac-a144-e9cf00d8b9cc"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# pull platform types from IGDB

import requests

igdb_client_id = ""  # set before running
igdb_token     = ""  # set before running

headers = {
    'Client-ID': igdb_client_id
    , 'Authorization': f'Bearer {igdb_token}'
}

query =  (
    'fields name, summary, created_at, updated_at;'
)

response = requests.post('https://api.igdb.com/v4/platforms', headers=headers, data=query)

if response.status_code == 200:
    df_raw = spark.createDataFrame(response.json())
    df_raw.show(n=10,truncate=True)
else:
    print(f'Error: {response.status_code} - {response.text}')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create temporary local view
sourcePlatforms = df_raw.createOrReplaceTempView("sourcePlatforms")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC with source as (
# MAGIC     select 
# MAGIC         created_at as created_at
# MAGIC         , id as id
# MAGIC         , name as name
# MAGIC         , updated_at as updated_at
# MAGIC         , summary as summary
# MAGIC         , md5(cast(concat_ws(',', name, summary, created_at, updated_at) as string)) as hash
# MAGIC     from sourcePlatforms
# MAGIC )
# MAGIC 
# MAGIC merge into bronze.platforms as t
# MAGIC using source as s
# MAGIC     on t.id = s.id
# MAGIC when matched and t.hash != coalesce(s.hash, '') then
# MAGIC     update set *
# MAGIC when not matched then
# MAGIC     insert *

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Test Broadcast join vs regular join vs salted join

# CELL ********************

# we are going to artifically inflate the games table to stress-test the pipeline so I don't have to wait to pull IGDB games
df_source = spark.read.table("bronze.games")

# Double it 5 times: 50k -> 100k -> 200k -> 400k -> 800k -> 1.6M
for _ in range(5):
    df_source = df_source.unionAll(df_source)

df_source.createOrReplaceTempView("synthGamesBig")
print(f"Stress-test row count: {df_source.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## sql join vs python broadcast join

# CELL ********************

# MAGIC %%sql
# MAGIC -- 1. Force Spark to look at the 'bronze' schema/database specifically
# MAGIC USE bronze;
# MAGIC 
# MAGIC create or replace temporary view tempSynthGamesPlatforms as
# MAGIC WITH synthGamePlatforms AS (
# MAGIC     SELECT 
# MAGIC         id AS gameId,
# MAGIC         name AS gameName,
# MAGIC         explode(platforms) AS platformId
# MAGIC     FROM synthGamesBig
# MAGIC )   
# MAGIC SELECT 
# MAGIC     gp.gameName
# MAGIC     , p.name AS platformName
# MAGIC FROM synthGamePlatforms AS gp
# MAGIC JOIN platforms AS p
# MAGIC     ON gp.platformId = p.id;
# MAGIC 
# MAGIC select * from tempSynthGamesPlatforms limit 10;
# MAGIC 
# MAGIC /* Notes
# MAGIC - This got so big that the job timed out after running for about 30 mins
# MAGIC - While I did get 10 rows displayed (thanks to Spark's Lazy Execution), the query behind the view never finished
# MAGIC - This is because the shuffle was so big and it had so many rows that they couldn't be indexed in my particular cluster's ram
# MAGIC */

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# let's use python and broadcast join and compare

from pyspark.sql import functions as f

df_synthGamesBig = df_source \
    .withColumn("platformId", f.explode("platforms")) \
    .join(f.broadcast(spark.table("bronze.platforms").alias("p")),
        f.col("platformId") == f.col("p.id"))

df_synthGamesBig.show(n=10)

# it uh. finished in 1 second. I need a minute to cry a bit. It's the most beautiful thing i've ever seen

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Let's test how this behaves with salting
# 
# ## What is salting
# 
# - Since the way spark handles joins is by assigning join keys to worker nodes, "specializing" them to minimize network traffic, we can get **data skew**
# - **Data Skew** is when there's a lot more values for one key over the rest. for example, the platform id distribution is definitely more skewed towards windows
# - the solution is **Salting**: artificially breaking up the distribution by assigning random values to the table in a new column. this will break the monopoly and help spread the computational load
# - it is done using pyspark.sql.functions.rand

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- let's see what our troublemakers are
# MAGIC 
# MAGIC with explodedPlatforms as (
# MAGIC     SELECT 
# MAGIC         id AS gameId,
# MAGIC         name AS gameName,
# MAGIC         explode(platforms) AS platformId
# MAGIC     FROM synthGamesBig
# MAGIC )
# MAGIC 
# MAGIC -- select count(*) from explodedPlatforms -- 2 652 608
# MAGIC 
# MAGIC select 
# MAGIC     platformId
# MAGIC     , count(*) as gameCount
# MAGIC from explodedPlatforms
# MAGIC group by platformId
# MAGIC order by gameCount desc
# MAGIC 
# MAGIC -- yeah, 6 is pretty crazy, as well as 14, 130, and 48

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as f

df_synthGamesExploded = df_source.withColumn("platformId", f.explode("platforms"))

# df_synthGamesExploded.show(n=10)

df_synthGamesSalted = df_synthGamesExploded.withColumn("salt",(f.rand()*10).cast("int"))

df_synthGamesSalted.show(n=10)

# df_synthGamesSalted.groupBy("salt").count().show()
# ohhh so rand is automagically kind of even

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# create a lookup bridge table for salt

df_saltLookup = spark.range(10).withColumnRenamed("id", "saltKey")

# df_saltLookup.show()

# and create a dataframe that cross applies the platforms with salt

df_platformsSalted = spark.table("bronze.platforms").crossJoin(df_saltLookup)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# TEMPORARILY turn off autoBroadcastJoin so I can see the performance impact
# spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) # Set to 100MB

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# let's join without salt first

df_unsaltedJoin = df_synthGamesSalted.alias("g") \
    .join(df_platformsSalted.alias("p") \
    ,(f.col("g.id") == f.col("p.id")))

df_unsaltedJoin.show(n=10)
# 20 seconds

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# let's join with salt now

df_saltedJoin = df_synthGamesSalted.alias("g") \
    .join(df_platformsSalted.alias("p") \
    ,(f.col("g.id") == f.col("p.id")) & (f.col("g.salt") == f.col("p.saltKey")))

df_saltedJoin.show(n=10)

# 12 seconds

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# turn autobroadcastjoin back on
# spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760) # Set to 10MB
# spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10m") # Set to 10MB

# Returns the value in bytes (e.g., '10485760' for 10MB)
print(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Testing Upsert logic for bronze.Platforms

# CELL ********************

-- %%sql

-- update bronze.platforms
-- set name = 'test', hash = 'test'
-- where id in (12, 150)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

-- %%sql

-- select * from bronze.platforms
-- where id in (12, 150)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

-- %%sql

-- delete from bronze.platforms
-- where id in ( 376, 510 )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

-- %%sql

-- select * from bronze.platforms
-- where id in ( 376, 510 )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

headers = {
    'Client-ID': 'IGDB_CLIENT_ID_REDACTED'
    , 'Authorization': 'Bearer IGDB_BEARER_TOKEN_REDACTED'
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

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- extract the platforms from bronze.games
# MAGIC 
# MAGIC select *
# MAGIC from bronze.games as g
# MAGIC lateral view explode(g.platforms) as platform_id
# MAGIC join bronze.platforms as p
# MAGIC     on platform_id = p.id

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- [A!M2] The "Sandbox" Pattern: Explode first, Join second.
# MAGIC -- This is much more stable than chaining LATERAL VIEW and JOIN in one block.
# MAGIC 
# MAGIC WITH exploded_games AS (
# MAGIC     SELECT 
# MAGIC         id AS game_id,
# MAGIC         name AS game_name,
# MAGIC         explode(platforms) AS platform_id
# MAGIC     FROM bronze.games
# MAGIC )
# MAGIC SELECT 
# MAGIC     eg.game_name,
# MAGIC     p.name AS platform_name
# MAGIC FROM exploded_games AS eg
# MAGIC JOIN bronze.platforms AS p
# MAGIC     ON eg.platform_id = p.id

# METADATA ********************

# META {
# META   "language": "sparksql",
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

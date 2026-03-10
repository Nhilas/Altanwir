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

# pull games from IGDB, 500 games at a time

import requests

headers = {
    'Client-ID': 'IGDB_CLIENT_ID_REDACTED'
    , 'Authorization': 'Bearer IGDB_BEARER_TOKEN_REDACTED'
}

query =  (
    'fields name, aggregated_rating, first_release_date, rating, platforms;'
)

limit = 50000
gamesList = []

for i in range(0, limit, 500):     # igdb limits to 500 results per request and 4 requests a second
    query =  (
        'fields name, aggregated_rating, first_release_date, rating, platforms;'
        'limit 500;'
        f'offset {i};'
    )
    print(f'Pulling games {i} to {i+500}...')

    response = requests.post('https://api.igdb.com/v4/games', headers=headers, data=query)

    if response.status_code != 200:
        print(f'Error: {response.status_code} - {response.text}')
        break

    gamesList.extend(response.json())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a dataframe and then a view on top of it for sql transformations later on

df_raw = spark.createDataFrame(gamesList)

# This was a one-time thing to create the table
# df_raw.write.mode("overwrite").saveAsTable("bronze.games")

# Create temporary local view
sourceGames = df_raw.createOrReplaceTempView("sourceGames")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC # ho boy, lots has changed. key notes: you can now use set * / insert *, but the condition is that source and target column names MUST BE IDENTICAL
# MAGIC # this is why i decided to write a source cte where i explicitly select and name the columns even if it is redundant
# MAGIC 
# MAGIC with source as (
# MAGIC     select 
# MAGIC         id as id
# MAGIC         , name as name
# MAGIC         , aggregated_rating as aggregated_rating
# MAGIC         , first_release_date as first_release_date
# MAGIC         , rating as rating
# MAGIC         , platforms as platforms
# MAGIC         , md5(cast(concat_ws(',', name, aggregated_rating, first_release_date, rating, platforms) as string)) as hash
# MAGIC     from sourceGames
# MAGIC )
# MAGIC 
# MAGIC merge into bronze.games as t
# MAGIC using source as s
# MAGIC     on t.id = s.id
# MAGIC when matched and t.hash != coalesce(s.hash, '') then
# MAGIC     update set *
# MAGIC when not matched then
# MAGIC     insert *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%sql

# select * from bronze.games limit 10

# CELL ********************

# %%sql

# update bronze.games
#     set name = 'temp update'
#         , hash = 'lol'
# where id in ( 152657, 360930 )

# CELL ********************

# %%sql

# select * from bronze.games
# where id in ( 152657, 360930 )

# CELL ********************

# %%sql

# delete from bronze.games
# where id in ( 51778, 29980 )

# CELL ********************

# %%sql

# select * from bronze.games
# where id in ( 51778, 29980 )

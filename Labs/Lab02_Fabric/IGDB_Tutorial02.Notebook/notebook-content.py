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

# Part 1 is creating a table. i'll use this opportunity to mess around with some more transformations
# Part 2 is creating another table and appending it to the first, to see what mergeSchema is like in action

import requests

igdb_client_id = ""  # set before running
igdb_token     = ""  # set before running

headers = {
    'Client-ID': igdb_client_id
    , 'Authorization': f'Bearer {igdb_token}'
}

query =  (
    'fields name, aggregated_rating, first_release_date, rating;'
    'where aggregated_rating != null & rating != null;'
    'limit 500;'    # igdb limits to 500 results per request
)

response = requests.post('https://api.igdb.com/v4/games', headers=headers, data=query)

if response.status_code == 200:
    df_raw = spark.createDataFrame(response.json())
    df_raw.show(n=100,truncate=True)
else:
    print(f'Error: {response.status_code} - {response.text}')



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, from_unixtime, round

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cast = df_raw.withColumn("first_release_date",from_unixtime(col("first_release_date")).cast("date")) \
    .withColumn("aggregated_rating",round(col("aggregated_rating"),2)) \
    .withColumn("rating",round(col("rating"),2))

df_cast.show(n=5,truncate=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cast.write.mode("overwrite").saveAsTable("sample.tempGames")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# now to get a slightly different table and see if it blows up

query2 =  (
    'fields name, aggregated_rating, first_release_date, rating, storyline;'
    'where aggregated_rating != null & rating != null;'
    'limit 500;'    # igdb limits to 500 results per request
)

response = requests.post('https://api.igdb.com/v4/games', headers=headers, data=query2)

if response.status_code == 200:
    df_raw2 = spark.createDataFrame(response.json())
    df_raw2.show(n=5,truncate=True)
else:
    print(f'Error: {response.status_code} - {response.text}')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cast2 = df_raw2.withColumn("first_release_date",from_unixtime(col("first_release_date")).cast("date")) \
    .withColumn("aggregated_rating",round(col("aggregated_rating"),2)) \
    .withColumn("rating",round(col("rating"),2))

df_cast2.show(n=5,truncate=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# it go bum?

# df_cast2.write.mode("append").saveAsTable("sample.tempGames")
# this yells at me that the schema is different

df_cast2.write.mode("append").option("mergeSchema", "true").saveAsTable("sample.tempGames")
# this will simply dump stuff, making the new column null for the old values

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("select * from sample.tempgames limit 5").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

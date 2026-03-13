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

# MARKDOWN ********************

# # Parameters and Configurations

# CELL ********************

from pyspark.sql import functions as f
from delta.tables import *

# CELL ********************

games_path = "abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Tables/bronze/games"

# MARKDOWN ********************

# ## silver.games

# CELL ********************

df = spark.read.format("delta").load(games_path).select("id", "name", "genres", "themes", "platforms", "first_release_date", "aggregated_rating", "aggregated_rating_count","rating", "rating_count", "total_rating", "total_rating_count")

df_cleaned = df.filter(df.id.isNotNull()) \
    .withColumn("gameId", f.col("id")) \
    .withColumn("gameName", f.trim(f.col("name"))) \
    .withColumn("releasedOn", f.from_unixtime(f.col("first_release_date")).cast("date")) \
    .withColumn("externalRating", f.round(f.col("aggregated_rating"),2)) \
    .withColumn("externalRatingSourceCount", f.col("aggregated_rating_count")) \
    .withColumn("igdbRating", f.round(f.col("rating"),2)) \
    .withColumn("igdbRatingSourceCount", f.col("rating_count")) \
    .withColumn("aggregatedRating", f.round(f.col("total_rating"),2)) \
    .withColumn("aggregatedRatingSourceCount", f.col("total_rating_count"))

# selecting renamed columns and hashing only them, except gameId
df_renamed = df_cleaned.select("gameId", "gameName", "genres", "themes", "platforms", "releasedOn", "externalRating", "externalRatingSourceCount", "igdbRating", "igdbRatingSourceCount", "aggregatedRating", "aggregatedRatingSourceCount")

columns_to_hash = list(df_renamed.columns)
columns_to_hash.remove("gameId")

df_hashed = df_renamed.withColumn("hash", f.md5(f.concat_ws(",", *[f.col(c) for c in columns_to_hash])))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("USE spark_catalog.silver")

# print(f"Current Database: {spark.catalog.currentDatabase()}")

if not spark.catalog.tableExists("silver.games"):
    df_hashed.write.format("delta").saveAsTable("silver.games")
else:    
    # 1. Define the target table
    targetTable = DeltaTable.forName(spark, "silver.games")

    # 2. Execute Merge
    targetTable.alias("t").merge(
        source = df_hashed.alias("s"),
        condition = "t.gameId = s.gameId"
    ).whenMatchedUpdateAll(
        # Optional: Only update if the content actually changed (using your hash column)
        condition = "t.hash != s.hash"
    ).whenNotMatchedInsertAll(
        # Inserts all columns from source to target
    ).execute()


# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select count(*) from silver.games

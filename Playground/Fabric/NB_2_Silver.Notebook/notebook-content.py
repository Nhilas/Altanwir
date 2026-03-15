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
from pyspark.sql.window import Window
from delta.tables import *

# CELL ********************

games_path = "abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Tables/bronze/games"
genres_path = "abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Tables/bronze/genres"
themes_path = "abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Tables/bronze/themes"
platforms_path = "abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Tables/bronze/platforms"
platform_types_path = "abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Tables/bronze/platform_types"
run_mode = "FULL" # FULL or INCREMENTAL

# MARKDOWN ********************

# # Loading Silver Tables
# 
# Sanitizations to be performed:
# - casting
#   - proper date on released_date
#   - rounding for ratings
# - no null ids
# - clean string values
#   - null is null
#   - remove invalid ids, or ids without names
# - deduplicate
# - create a surrogate key
# - trimming
# 
# Things I'm aware of that I'm not doing for now:
# - SCD
# - Schema Drift
# - Late Arrivals
# - Auditing

# MARKDOWN ********************

# ## To Do
# 
# - rewrite the initial select of columns and df_cleaned using selectExpr()
# ```python
# # example
# dataframe.selectExpr(
#     "original_column_name as new_column_name",
#     "trim(another_column) as trimmed_column"
# )
# ```
# 
# - rewrite the hashing part to use the new df_cleaned and the below
# ```python
# # The general pattern for filtering a list
# new_list = [item for item in original_list if some_condition_is_true]
# 
# # Applying it to your use case
# columns_to_hash = [c for c in df_cleaned.columns if c != 'gameId']
# ```

# MARKDOWN ********************

# ## silver.games

# CELL ********************

df = spark.read.format("delta").load(games_path).select("id", "name", "external_games", "genres", "themes", "platforms", "first_release_date", "created_at", "aggregated_rating", "aggregated_rating_count","rating", "rating_count", "total_rating", "total_rating_count")

# enforce nulls for game names and deduplicate by id
deduplicateBy = Window.partitionBy("id").orderBy(f.col("created_at").desc())

df_filtered = df \
    .withColumn("name"
        , f.when(f.col("name").isin("", "na", "n/a", "not available", "unknown", "null"), None)
        .otherwise(f.col("name"))
    ) \
    .withColumn("row_number"
        , f.row_number().over(deduplicateBy)
    ) \
    .filter(
        (f.col("id").cast("int").isNotNull())   # cast ensures we filter out any ids that are not numbers, for whatever reason
        & (f.col("name").isNotNull())
        & (f.col("row_number") == 1)
    )

df_cleaned = df_filtered \
    .withColumn("gameId", f.sha2(f.col("id").cast("string"), 256)) \
    .withColumn("gameName", f.trim(f.col("name"))) \
    .withColumn("genreId", f.col("genres")) \
    .withColumn("themeId", f.col("themes")) \
    .withColumn("platformId", f.col("platforms")) \
    .withColumn("externalGamesId", f.col("external_games")) \
    .withColumn("releasedOn", f.from_unixtime(f.col("first_release_date")).cast("date")) \
    .withColumn("externalRating", f.round(f.col("aggregated_rating"),2)) \
    .withColumn("externalRatingSourceCount", f.col("aggregated_rating_count")) \
    .withColumn("igdbRating", f.round(f.col("rating"),2)) \
    .withColumn("igdbRatingSourceCount", f.col("rating_count")) \
    .withColumn("aggregatedRating", f.round(f.col("total_rating"),2)) \
    .withColumn("aggregatedRatingSourceCount", f.col("total_rating_count"))

# selecting renamed columns and hashing only them, except gameId
df_renamed = df_cleaned.select("gameId", "gameName", "genreId", "themeId", "platformId", "releasedOn", "externalGamesId", "externalRating", "externalRatingSourceCount", "igdbRating", "igdbRatingSourceCount", "aggregatedRating", "aggregatedRatingSourceCount")

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

# recreate the table if run mode is full
## or create it if it doesn't exist
## or merge into it if it does exist
if (run_mode == "FULL"):
    spark.sql("drop table if exists silver.games")
    df_hashed.write.format("delta").saveAsTable("silver.games")    
elif (spark.catalog.tableExists("silver.games")):
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


# MARKDOWN ********************

# ## silver.genres

# CELL ********************

df = spark.read.format("delta").load(genres_path).select("id", "name", "created_at")

deduplicateBy = Window.partitionBy("id").orderBy(f.col("created_at").desc())

df_filtered = df \
    .withColumn("name"
        , f.when(f.col("name").isin("", "na", "n/a", "not available", "unknown", "null"), None)
        .otherwise(f.col("name"))
    ) \
    .withColumn("row_number"
        , f.row_number().over(deduplicateBy)
    ) \
    .filter(
        (f.col("id").cast("int").isNotNull())
        & (f.col("name").isNotNull())
        & (f.col("row_number") == 1)
    )

df_cleaned = df_filtered \
    .withColumn("genreId", f.sha2(f.col("id").cast("string"), 256)) \
    .withColumn("genreName", f.trim(f.col("name")))

df_renamed = df_cleaned.select("genreId", "genreName")


# CELL ********************

spark.sql("USE spark_catalog.silver")

if (run_mode == "FULL"):
    spark.sql("drop table if exists silver.genres")
    df_renamed.write.format("delta").saveAsTable("silver.genres")    
elif (spark.catalog.tableExists("silver.genres")):
    df_renamed.write.format("delta").saveAsTable("silver.genres")
else:    
    targetTable = DeltaTable.forName(spark, "silver.genres")

    targetTable.alias("t").merge(
        source = df_renamed.alias("s"),
        condition = "t.genreId = s.genreId"
    ).whenMatchedUpdateAll(
        condition = "t.genreName != s.genreName"
    ).whenNotMatchedInsertAll(
    ).execute()


# MARKDOWN ********************

# ## silver.themes

# CELL ********************

df = spark.read.format("delta").load(themes_path).select("id", "name", "created_at")

deduplicateBy = Window.partitionBy("id").orderBy(f.col("created_at").desc())

df_filtered = df \
    .withColumn("name"
        , f.when(f.col("name").isin("", "na", "n/a", "not available", "unknown", "null"), None)
        .otherwise(f.col("name"))
    ) \
    .withColumn("row_number"
        , f.row_number().over(deduplicateBy)
    ) \
    .filter(
        (f.col("id").cast("int").isNotNull())
        & (f.col("name").isNotNull())
        & (f.col("row_number") == 1)
    )

df_cleaned = df_filtered \
    .withColumn("themeId", f.sha2(f.col("id").cast("string"), 256)) \
    .withColumn("themeName", f.trim(f.col("name")))

df_renamed = df_cleaned.select("themeId", "themeName")


# CELL ********************

spark.sql("USE spark_catalog.silver")

if (run_mode == "FULL"):
    spark.sql("drop table if exists silver.themes")
    df_renamed.write.format("delta").saveAsTable("silver.themes")    
elif (spark.catalog.tableExists("silver.themes")):
    df_renamed.write.format("delta").saveAsTable("silver.themes")
else:    
    targetTable = DeltaTable.forName(spark, "silver.themes")

    targetTable.alias("t").merge(
        source = df_renamed.alias("s"),
        condition = "t.themeId = s.themeId"
    ).whenMatchedUpdateAll(
        condition = "t.themeName != s.themeName"
    ).whenNotMatchedInsertAll(
    ).execute()


# MARKDOWN ********************

# ## silver.platforms

# CELL ********************

df1 = spark.read.format("delta").load(platforms_path).select("id", "name", "platform_type", "summary", "created_at")
df2 = spark.read.format("delta").load(platform_types_path).select("id", "name", "created_at")


deduplicateBy = Window.partitionBy("id").orderBy(f.col("created_at").desc())

# platforms
df_filtered1 = df1 \
    .withColumn("name"
        , f.when(f.col("name").isin("", "na", "n/a", "not available", "unknown", "null"), None)
        .otherwise(f.col("name"))
    ) \
    .withColumn("row_number"
        , f.row_number().over(deduplicateBy)
    ) \
    .filter(
        (f.col("id").cast("int").isNotNull())
        & (f.col("name").isNotNull())
        & (f.col("row_number") == 1)
    )

df_cleaned1 = df_filtered1 \
    .withColumn("platformId", f.sha2(f.col("id").cast("string"), 256)) \
    .withColumn("platformName", f.trim(f.col("name"))) \
    .withColumn("platformTypeId", f.col("platform_type"))

# platformTypes
df_filtered2 = df2 \
    .withColumn("name"
        , f.when(f.col("name").isin("", "na", "n/a", "not available", "unknown", "null"), None)
        .otherwise(f.col("name"))
    ) \
    .withColumn("row_number"
        , f.row_number().over(deduplicateBy)
    ) \
    .filter(
        (f.col("id").cast("int").isNotNull())
        & (f.col("name").isNotNull())
        & (f.col("row_number") == 1)
    )

df_cleaned2 = df_filtered2 \
    .withColumn("platformTypeId", f.col("id")) \
    .withColumn("platformTypeName", f.trim(f.col("name")))

df_cleaned = df_cleaned1.alias("pl") \
    .join(df_cleaned2.alias("plt") \
        ,f.col("pl.platformTypeId") == f.col("plt.platformTypeId"))

df_renamed = df_cleaned.select("platformId", "platformName", "summary", "platformTypeName")

columns_to_hash = list(df_renamed.columns)
columns_to_hash.remove("platformId")

df_hashed = df_renamed.withColumn("hash", f.md5(f.concat_ws(",", *[f.col(c) for c in columns_to_hash])))

df_hashed.show(n=10)

# CELL ********************

spark.sql("USE spark_catalog.silver")

if (run_mode == "FULL"):
    spark.sql("drop table if exists silver.platforms")
    df_hashed.write.format("delta").saveAsTable("silver.platforms")    
elif (spark.catalog.tableExists("silver.platforms")):
    df_hashed.write.format("delta").saveAsTable("silver.platforms")
else:    
    targetTable = DeltaTable.forName(spark, "silver.platforms")

    targetTable.alias("t").merge(
        source = df_hashed.alias("s"),
        condition = "t.platformId = s.platformId"
    ).whenMatchedUpdateAll(
        condition = "t.has != s.hash"
    ).whenNotMatchedInsertAll(
    ).execute()


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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

games_path = "abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Tables/bronze/games"
genres_path = "abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Tables/bronze/genres"
themes_path = "abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Tables/bronze/themes"
platforms_path = "abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Tables/bronze/platforms"
platform_types_path = "abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Tables/bronze/platform_types"
external_games_path = "abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Tables/bronze/external_games"
external_game_sources_path = "abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Tables/bronze/external_game_sources"

silverGames_path = "abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Tables/silver/games"
silverGenres_path = "abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Tables/silver/genres"
silverThemes_path = "abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Tables/silver/themes"
silverPlatforms_path = "abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Tables/silver/platforms"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Parameters
# - **run_Mode**
#   - FULL - drops the target table and recreates it entirely
#   - INCREMENTAL - upsert via change detection

# MARKDOWN ********************


# PARAMETERS CELL ********************

run_mode = "INCREMENTAL" # FULL or INCREMENTAL

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Loading Silver Tables
# 
# Sanitizations performed:
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
# Lookup of surrogate keys is also performed, where applicable
# 
# Things I'm aware of that I'm not doing for now:
# - SCD
# - Schema Drift
# - Late Arrivals
# - Auditing

# MARKDOWN ********************

# ## silver.games

# CELL ********************

deduplicateBy = Window.partitionBy("id").orderBy(f.col("created_at").desc())

df = spark.read.format("delta").load(games_path)

df_filtered = df \
.withColumn("name"                          # collapse unknowns into nulls
    , f.when(f.col("name").isin("", "na", "n/a", "not available", "unknown", "null"), None)
    .otherwise(f.col("name"))
).withColumn("row_number"                   # sort by id and by created_at, which is an igdb field that represents when the record was added in the igdb
    , f.row_number().over(deduplicateBy)
).filter(
    (f.col("id").cast("int").isNotNull())   # cast ensures we filter out any ids that are not numbers, for whatever reason
    & (f.col("name").isNotNull())
    & (f.col("row_number") == 1)
)

df_cleaned = df_filtered \
.selectExpr(
    "sha2(cast(id as string), 256)                      as gameKey"
    , "trim(name)                                       as gameName"
    , "id                                               as gameId"
    , "genres                                           as genreId"
    , "themes                                           as themeId"
    , "platforms                                        as platformId"
    , "cast(from_unixtime(first_release_date) as date)  as releasedOn"
    , "round(aggregated_rating, 2)                      as externalRating"
    , "aggregated_rating_count                          as externalRatingSourcesCount"
    , "round(rating, 2)                                 as igdbRating"
    , "rating_count                                     as igdbRatingSourceCount"
    , "round(total_rating, 2)                           as aggregatedRating"
    , "total_rating_count                               as aggregatedRatingSourceCount"
)

columns_to_hash = [c for c in df_cleaned.columns if c != 'gameKey']

df_hashed = df_cleaned.withColumn("hash", f.md5(f.concat_ws(",", *[f.col(c) for c in columns_to_hash])))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("USE spark_catalog.silver")

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
        condition = "t.gameKey = s.gameKey"
    ).whenMatchedUpdateAll(
        # Optional: Only update if the content actually changed (using your hash column)
        condition = "t.hash != s.hash"
    ).whenNotMatchedInsertAll(
        # Inserts all columns from source to target
    ).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## silver.genres

# CELL ********************

deduplicateBy = Window.partitionBy("id").orderBy(f.col("created_at").desc())

df = spark.read.format("delta").load(genres_path)

df_filtered = df \
.withColumn("name"                          # collapse unknowns into nulls
    , f.when(f.col("name").isin("", "na", "n/a", "not available", "unknown", "null"), None)
    .otherwise(f.col("name"))
).withColumn("row_number"                   # sort by id and by created_at, which is an igdb field that represents when the record was added in the igdb
    , f.row_number().over(deduplicateBy)
).filter(
    (f.col("id").cast("int").isNotNull())   # cast ensures we filter out any ids that are not numbers, for whatever reason
    & (f.col("name").isNotNull())
    & (f.col("row_number") == 1)
)

df_cleaned = df_filtered \
.selectExpr(
    "sha2(cast(id as string), 256)                      as genreKey"
    , "id                                               as genreId"    
    , "trim(name)                                       as genreName"
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("USE spark_catalog.silver")

if (run_mode == "FULL"):
    spark.sql("drop table if exists silver.genres")
    df_cleaned.write.format("delta").saveAsTable("silver.genres")    
elif (spark.catalog.tableExists("silver.genres")):
    df_cleaned.write.format("delta").saveAsTable("silver.genres")
else:    
    targetTable = DeltaTable.forName(spark, "silver.genres")

    targetTable.alias("t").merge(
        source = df_cleaned.alias("s"),
        condition = "t.genreKey = s.genreKey"
    ).whenMatchedUpdateAll(
        condition = "t.genreName != s.genreName"
    ).whenNotMatchedInsertAll(
    ).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## silver.themes

# CELL ********************

deduplicateBy = Window.partitionBy("id").orderBy(f.col("created_at").desc())

df = spark.read.format("delta").load(themes_path)

df_filtered = df \
.withColumn("name"                          # collapse unknowns into nulls
    , f.when(f.col("name").isin("", "na", "n/a", "not available", "unknown", "null"), None)
    .otherwise(f.col("name"))
).withColumn("row_number"                   # sort by id and by created_at, which is an igdb field that represents when the record was added in the igdb
    , f.row_number().over(deduplicateBy)
).filter(
    (f.col("id").cast("int").isNotNull())   # cast ensures we filter out any ids that are not numbers, for whatever reason
    & (f.col("name").isNotNull())
    & (f.col("row_number") == 1)
)

df_cleaned = df_filtered \
.selectExpr(
    "sha2(cast(id as string), 256)                      as themeKey"
    , "id                                               as themeId"
    , "trim(name)                                       as themeName"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("USE spark_catalog.silver")

if (run_mode == "FULL"):
    spark.sql("drop table if exists silver.themes")
    df_cleaned.write.format("delta").saveAsTable("silver.themes")    
elif (spark.catalog.tableExists("silver.themes")):
    df_cleaned.write.format("delta").saveAsTable("silver.themes")
else:    
    targetTable = DeltaTable.forName(spark, "silver.themes")

    targetTable.alias("t").merge(
        source = df_cleaned.alias("s"),
        condition = "t.themeKey = s.themeKey"
    ).whenMatchedUpdateAll(
        condition = "t.themeName != s.themeName"
    ).whenNotMatchedInsertAll(
    ).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## silver.platforms

# CELL ********************

deduplicateBy = Window.partitionBy("id").orderBy(f.col("created_at").desc())

# Platforms
df1 = spark.read.format("delta").load(platforms_path)

df_filtered1 = df1 \
.withColumn("name"                          # collapse unknowns into nulls
    , f.when(f.col("name").isin("", "na", "n/a", "not available", "unknown", "null"), None)
    .otherwise(f.col("name"))
).withColumn("row_number"                   # sort by id and by created_at, which is an igdb field that represents when the record was added in the igdb
    , f.row_number().over(deduplicateBy)
).filter(
    (f.col("id").cast("int").isNotNull())   # cast ensures we filter out any ids that are not numbers, for whatever reason
    & (f.col("name").isNotNull())
    & (f.col("row_number") == 1)
)

df_cleaned1 = df_filtered1 \
.selectExpr(
    "sha2(cast(id as string), 256)                      as platformKey"
    , "id                                               as platformId"  
    , "trim(name)                                       as platformName"
    , "platform_type                                    as platformTypeId"
)

# Platform Types
df2 = spark.read.format("delta").load(platform_types_path)

df_filtered2 = df2 \
.withColumn("name"                          # collapse unknowns into nulls
    , f.when(f.col("name").isin("", "na", "n/a", "not available", "unknown", "null"), None)
    .otherwise(f.col("name"))
).withColumn("row_number"                   # sort by id and by created_at, which is an igdb field that represents when the record was added in the igdb
    , f.row_number().over(deduplicateBy)
).filter(
    (f.col("id").cast("int").isNotNull())   # cast ensures we filter out any ids that are not numbers, for whatever reason
    & (f.col("name").isNotNull())
    & (f.col("row_number") == 1)
)

# Consolidate
df_cleaned2 = df_filtered2 \
.selectExpr(
    "id                                                 as platformTypeId"
    , "trim(name)                                       as platformType"
)

df_cleaned = df_cleaned1.alias("pl") \
    .join(df_cleaned2.alias("plt") \
        ,f.col("pl.platformTypeId") == f.col("plt.platformTypeId")) \
    .drop("platformTypeId")

columns_to_hash = [c for c in df_cleaned.columns if c != 'platformKey']

df_hashed = df_cleaned.withColumn("hash", f.md5(f.concat_ws(",", *[f.col(c) for c in columns_to_hash])))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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
        condition = "t.platformKey = s.platformKey"
    ).whenMatchedUpdateAll(
        condition = "t.hash != s.hash"
    ).whenNotMatchedInsertAll(
    ).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## silver.externalGames
# 
# ### ToDo
# 
# - [ ]: add a lookup to insert the surrogate key for game id

# CELL ********************

deduplicateBy = Window.partitionBy("id").orderBy(f.col("created_at").desc())

# lookup tables
df_silverGames = spark.read.format("delta").load(silverGames_path).select("gameKey", "gameId", "genreId", "themeId", "platformId")
df_silverGenres = spark.read.format("delta").load(silverGenres_path).select("genreKey", "genreId")
df_silverThemes = spark.read.format("delta").load(silverThemes_path).select("themeKey", "themeId")
df_silverPlatforms = spark.read.format("delta").load(silverPlatforms_path).select("platformKey", "platformId")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# External Games
df1 = spark.read.format("delta").load(external_games_path)

df_filtered1 = df1 \
.withColumn("name"                          # collapse unknowns into nulls
    , f.when(f.col("name").isin("", "na", "n/a", "not available", "unknown", "null"), None)
    .otherwise(f.col("name"))
).withColumn("row_number"                   # sort by id and by created_at, which is an igdb field that represents when the record was added in the igdb
    , f.row_number().over(deduplicateBy)
).filter(
    (f.col("id").cast("int").isNotNull())   # cast ensures we filter out any ids that are not numbers, for whatever reason
    & (f.col("name").isNotNull())
    & (f.col("row_number") == 1)
)

# lookup gameKey and platformKey
df_lookups = df_filtered1.alias("eg") \
    .join(df_silverGames.alias("g") \
          , f.col("eg.game") == f.col("g.gameId"), "left") \
    .join(df_silverPlatforms.alias("p") \
          , f.col("eg.platform") == f.col("p.platformId"), "left")

df_cleaned1 = df_lookups \
.selectExpr(
    "sha2(cast(id as string), 256)                      as egKey"
    , "id                                               as egId"
    , "trim(name)                                       as egName"
    , "uid                                              as eId"
    , "gameKey"
    , "platformKey"
    , "external_game_source                             as egSourceId"
    , "game                                             as GameId"
    , "platform                                         as platformId"

)

# External Game Sources
df2 = spark.read.format("delta").load(external_game_sources_path)

df_filtered2 = df2 \
.withColumn("name"                          # collapse unknowns into nulls
    , f.when(f.col("name").isin("", "na", "n/a", "not available", "unknown", "null"), None)
    .otherwise(f.col("name"))
).withColumn("row_number"                   # sort by id and by created_at, which is an igdb field that represents when the record was added in the igdb
    , f.row_number().over(deduplicateBy)
).filter(
    (f.col("id").cast("int").isNotNull())   # cast ensures we filter out any ids that are not numbers, for whatever reason
    & (f.col("name").isNotNull())
    & (f.col("row_number") == 1)
)

df_cleaned2 = df_filtered2 \
.selectExpr(
    "id                                                 as egSourceId"
    , "trim(name)                                       as egSourceName"
)

# Consolidate external games with external game sources
df_cleaned = df_cleaned1.alias("eg") \
    .join(df_cleaned2.alias("egs") \
        ,f.col("eg.egSourceId") == f.col("egs.egSourceId")) \
    .drop("egSourceId")

columns_to_hash = [c for c in df_cleaned.columns if c != 'egKey']

df_hashed = df_cleaned.withColumn("hash", f.md5(f.concat_ws(",", *[f.col(c) for c in columns_to_hash])))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("USE spark_catalog.silver")

if (run_mode == "FULL"):
    spark.sql("drop table if exists silver.externalGames")
    df_hashed.write.format("delta").saveAsTable("silver.externalGames")    
elif (spark.catalog.tableExists("silver.externalGames")):
    df_hashed.write.format("delta").saveAsTable("silver.externalGames")
else:    
    targetTable = DeltaTable.forName(spark, "silver.externalGames")

    targetTable.alias("t").merge(
        source = df_hashed.alias("s"),
        condition = "t.platformId = s.platformId"
    ).whenMatchedUpdateAll(
        condition = "t.hash != s.hash"
    ).whenNotMatchedInsertAll(
    ).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Bridge Table Creation

# CELL ********************

spark.sql("USE spark_catalog.silver")

# Genres
df_explodedGenres = df_silverGames.alias("g") \
    .withColumn("genreIdFlattened", f.explode(f.from_json(f.col("g.genreId"), "array<int>"))) \
    .join(df_silverGenres.alias("gen") \
          , f.col("genreIdFlattened") == f.col("gen.genreId"), "left")

spark.sql("drop table if exists silver.bridgeGameGenres")
df_explodedGenres.select("gameKey", "genreKey").write.format("delta").mode("overwrite").saveAsTable("silver.bridgeGameGenres")

# Themes
df_explodedThemes = df_silverGames.alias("g") \
    .withColumn("themeIdFlattened", f.explode(f.from_json(f.col("g.themeId"), "array<int>"))) \
    .join(df_silverThemes.alias("t") \
          , f.col("themeIdFlattened") == f.col("t.themeId"), "left")

spark.sql("drop table if exists silver.bridgeGameThemes")
df_explodedThemes.select("gameKey", "themeKey").write.format("delta").mode("overwrite").saveAsTable("silver.bridgeGameThemes")

# Platforms
df_explodedPlatforms = df_silverGames.alias("g") \
    .withColumn("platformIdFlattened", f.explode(f.from_json(f.col("g.platformId"), "array<int>"))) \
    .join(df_silverPlatforms.alias("p") \
          , f.col("platformIdFlattened") == f.col("p.platformId"), "left")

spark.sql("drop table if exists silver.bridgeGamePlatforms")
df_explodedPlatforms.select("gameKey", "platformKey").write.format("delta").mode("overwrite").saveAsTable("silver.bridgeGamePlatforms")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

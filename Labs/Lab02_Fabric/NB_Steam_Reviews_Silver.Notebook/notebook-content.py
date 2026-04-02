# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c683a58d-3109-458d-8cb3-da991c23a31e",
# META       "default_lakehouse_name": "IGDBAnalytics_Dev",
# META       "default_lakehouse_workspace_id": "d1206eb3-2259-44b8-844a-409f1a63f284",
# META       "known_lakehouses": [
# META         {
# META           "id": "c683a58d-3109-458d-8cb3-da991c23a31e"
# META         },
# META         {
# META           "id": "21686009-3b8b-4dac-a144-e9cf00d8b9cc"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Imports

# CELL ********************

import json

from delta.tables import DeltaTable
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, ByteType, IntegerType, LongType, FloatType, BooleanType

# debug / exploration
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Parameters

# CELL ********************

environment = "dev"
load_type = "initial"

# MARKDOWN ********************

# ## Variables

# CELL ********************

lakehouse_name = "IGDBAnalytics" if environment == "prod" else "IGDBAnalytics_Dev"
lakehouse_info = notebookutils.lakehouse.get(lakehouse_name)
audit_schema = "dev" if environment == "dev" else "steam"
    
abfs_root = f"{lakehouse_info['properties']['abfsPath']}"

source_abfs = f"{abfs_root}/Tables/bronze/steamreviews"
source_path = f"{lakehouse_name}.bronze.steamreviews"
source_table = DeltaTable.forName(spark, source_path)

# target_path = f"{lakehouse_name}.silver.steamreviews"
# target_table = DeltaTable.forName(spark, target_path)

# MARKDOWN ********************

# ## Constants

# CELL ********************

audit_server = '22jgi2dsfxnu5lmyn6ifyaro5e-wnxcbukzek4ejbckicpruy7sqq.datawarehouse.fabric.microsoft.com'
audit_database = 'IGDBAudit'

# CELL ********************

print(f"Silver Steam Reviews ELT Initiated with load_type = '{load_type}'")
print(f"Environment = {environment}\n Lakehouse = {lakehouse_name}\n Audit = {audit_database}.{audit_schema}")
print(f"Loading from {source_path} into silver.steamreviews") # {target_path}")

# MARKDOWN ********************

# # EDA

# CELL ********************

df_raw = spark.read.format("delta").load(source_abfs).select("app_id", "recommendationid", "review_json")

df_top_10_games = df_raw.groupBy("app_id") \
    .agg(f.count("*").alias("total_reviews")) \
    .withColumn("rn", f.row_number().over(Window.orderBy(f.col("total_reviews").desc()))) \
    .filter(f.col("rn") <= 10 )

df_games_total_reviews = df_raw.join(
    f.broadcast(df_top_10_games)
    , "app_id"
    , "inner"
)


# CELL ********************

json_rdd = df_games_total_reviews.select("review_json").rdd.map(lambda row: row[0])

df_schema = spark.read.json(json_rdd).printSchema()

# CELL ********************

df_games_loaded = spark.read.json(json_rdd)

# Count nulls per column
df_games_loaded.select([
    f.count(f.when(f.col(c).isNull(), 1)).alias(f"{c}_nulls")
    for c in df_games_loaded.columns
]).show()

# MARKDOWN ********************

# - the most concerning thing about the approximate cardinality is that the # of authors seems to be different than the # of recommendations, suggesting multiple reviews per author?

# CELL ********************

# Approximate cardinality (NDV) per column
df_games_loaded.select([
    f.approx_count_distinct(c).alias(f"{c}_ndv")
    for c in df_games_loaded.columns
]).show()

# MARKDOWN ********************

# - brother how do i have non-english reviews when i filtered by english xD 

# CELL ********************

categorical_cols = [c for c, t in df_games_loaded.dtypes if t in ("string", "boolean")]
for c in categorical_cols[:5]:
    df_games_loaded.groupBy(c).count().orderBy(f.desc("count")).limit(20).show()

# CELL ********************

raw_review_schema = StructType([
    StructField("app_id", LongType(), False )
    , StructField("recommendationid", StringType(), False )
    , StructField("author", StructType([
        StructField("steamid", StringType(), False )
        , StructField("playtime_forever", LongType(), False)
        , StructField("playtime_last_two_weeks", LongType(), False)
        , StructField("playtime_at_review", LongType(), False)
        , StructField("last_played", LongType(), False)                      # must convert to timestamp later
    ]), False)
    , StructField("language", StringType(), False )
    , StructField("review_text", StringType(), False )
    , StructField("voted_up", BooleanType(), False )
    , StructField("votes_up", LongType(), False )
    , StructField("votes_funny", LongType(), False )
    , StructField("weighted_vote_score", StringType(), False )              # schema sample shows it as string. to convert to float (according to steam docu)
    , StructField("timestamp_created", LongType(), False )                   # must convert to timestamp later
    , StructField("timestamp_updated", LongType(), False )                   # must convert to timestamp later
    , StructField("written_during_early_access", BooleanType(), False )
])

print(raw_review_schema)


# MARKDOWN ********************

# # Main

# MARKDOWN ********************

# # Debug

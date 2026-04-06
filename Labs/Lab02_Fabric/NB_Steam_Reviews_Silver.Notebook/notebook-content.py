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
import emoji
import re

from delta.tables import DeltaTable
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, ByteType, IntegerType, LongType, FloatType, BooleanType, TimestampType

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

df_raw = spark.read.format("delta").load(source_abfs).select("app_id", "review_json")

df_top_10_games = df_raw.groupBy("app_id") \
    .agg(f.count("*").alias("total_reviews")) \
    .withColumn("rn", f.row_number().over(Window.orderBy(f.col("total_reviews").desc()))) \
    .filter(f.col("rn") <= 10 )

df_games_total_reviews = df_raw \
    .join(
        f.broadcast(df_top_10_games)
        , "app_id"
        , "inner"
    ) \
    .select("app_id", "review_json")


# MARKDOWN ********************

# **Schema**
# * Taken from top 10 games by total review counts

# CELL ********************

json_rdd = df_games_total_reviews.select("review_json").rdd.map(lambda row: row[0])

schema_struct = spark.read.json(json_rdd).schema

# CELL ********************

spark.read.json(json_rdd).printSchema()

# CELL ********************

df_games_loaded = df_games_total_reviews \
    .withColumn("parse_review", f.from_json(f.col("review_json"), schema=schema_struct)) \
    .select(
        f.col("app_id"),
        f.col("parse_review.*")
    )

# MARKDOWN ********************

# **Check Nulls**
# critical columns:
# * app_id
# * recommendationid
# * review

# CELL ********************

# Count nulls per column
df_games_loaded.select([
    f.count(f.when(f.col(c).isNull(), 1)).alias(f"{c}_nulls")
    for c in df_games_loaded.columns
]).show()

# MARKDOWN ********************

# * Nothing important is null, only hardware info and while that's super interesting it is, alas, out of scope

# MARKDOWN ********************

# **Empty Strings**

# CELL ********************

df_games_loaded.select([
    f.count(f.when(f.col(c[0]).isNull() | (f.trim(f.col(c[0])) == ""), 1)).alias(f"{c[0]}_empty")
    for c in df_games_loaded.dtypes if c[1] == 'string'
]).show()

# 7819 empty reviews, every other column is clean. lol

# CELL ********************

df_games_loaded.filter(f.col("review") == "").show()

# MARKDOWN ********************

# * yep legit have reviews with nothing inside. to remove

# MARKDOWN ********************

# **Useless Strings**

# CELL ********************

df_games_loaded.withColumn("review_length", f.char_length(f.trim(f.col("review")))) \
    .groupBy(f.col("review_length")).count().orderBy("review_length").limit(10).show()
    # .groupBy(f.col("review_length")).count().orderBy(f.desc("count")).limit(20).show()

# CELL ********************

df_games_loaded.withColumn("review_length", f.char_length(f.trim(f.col("review")))) \
    .filter(f.col("review_length") == 1).select("app_id", "recommendationid", "review").show(30)

# MARKDOWN ********************

# * need to either treat or at least acknowledge emojis somehow

# MARKDOWN ********************

# **Cardinality**

# CELL ********************

# cardinality (NDV) per column
df_games_loaded.select([
    f.count_distinct(c).alias(f"{c}_ndv")
    for c in df_games_loaded.columns
]).show()

# MARKDOWN ********************

# * totals - 1 813 986
# * author - 1 813 984
#   - fewer authors than total reviews? same guy did multiple reviews?
# * reviews - 1 813 986
# * timestamp_created - 1 801 244
# * timestamp_updated - 1 801 048

# CELL ********************

# check weird author thing
# df_games_loaded.printSchema()
# Find the exact author structs that appear more than once
df_author_counts = df_games_loaded.groupBy("author").count().filter(f.col("count") > 1)

# Join back to the main dataframe to see both rn=1 and rn=2 side-by-side
df_games_loaded.join(f.broadcast(df_author_counts).select("author"), on="author", how="inner") \
    .select("app_id", "recommendationid", "author.steamid", "author.playtime_forever") \
    .orderBy("author.steamid") \
    .show(truncate=False)


# MARKDOWN ********************

# * yep legit same author different reviews
# * it's a steam bug i think. reviews are identical, the recommendationid is sequential, it's proly double pressed or something. must deduplicate by author as well

# CELL ********************

# return the batch files with these reviews, just in case something went wrong when parsing them

raw_json_path = f"{abfs_root}/Files/Steam/Reviews/initial/025486e3-06f8-4674-9fad-743454406dce/00efd8f8-d1d3-50bb-b7c9-414019db8a91/*.json"

df_investigate = spark.read.option("multiLine", "true").json(raw_json_path) \
    .filter(f.col("recommendationid").isin("14208378", "14208379"))

df_investigate.select(
    f.input_file_name().alias("source_batch_file"), 
    f.col("recommendationid")
).show(truncate=False)

# nop it's legit from steam cool

# MARKDOWN ********************

# **Categories**

# CELL ********************

categorical_cols = [c for c, t in df_games_loaded.dtypes if t in ("string", "boolean")]
for c in categorical_cols:
    df_games_loaded.groupBy(c).count().orderBy(f.desc("count")).limit(20).show()

# MARKDOWN ********************

# - brother how do i have non-english reviews when i filtered by english xD to remove language != 'english'

# MARKDOWN ********************

# **weighted_vote_score**

# CELL ********************

df_games_loaded.select("weighted_vote_score").describe().show()

# MARKDOWN ********************

# * it's a string between 0.05 and 0.988 so i guess it's meant to be a percentage?
# * official docu says it should be float, so it must be converted
# * no idea what it means, but it's used by steam to assign review 'helpfulness'

# MARKDOWN ********************

# **reactions**

# CELL ********************

df_games_loaded.select("reactions").show(20)

# MARKDOWN ********************

# * it's some internal thing with values like {1, 14}. not going to extract what they mean so i will ignore this column

# MARKDOWN ********************

# # Review Text Cleaning

# CELL ********************

import re

test = "            [b]One of the best game I've played.\nAll my friends are obsessed with it              ![/b]"

print(test)

# kill bbcode tags
print(re.sub(r'\[.*?\]', '', test))

# kill \ characters (whitespace?)
print(re.sub(r'[\t\n\r\f\v]', '', test))

# trim
print(re.sub(r'\s+', ' ', test).strip())


# CELL ********************

test = "This game gets a :thumbs_up: from me!"

print(test)

print(re.sub(r'[:_:]', ' ', test))

# CELL ********************

test_demoji = "C:/ is a path and some_variable_name and :thumbs_up:"

print(test_demoji)

demoji = re.compile(':\w+:')

print(re.sub(r':(\w+):', r'\1', test_demoji))

print(re.sub(r':(\w+):', r'\1', test_demoji).replace('_', ' '))


# CELL ********************

test_ascii = """
  |\_/|        ****************************    (\_/)
 / @ @ \       *  "Purrrfectly pleasant"  *   (='.'=)
( > º < )      *       Poppy Prinz        *   (")_(")
 `>>x<<´       *   (pprinz@example.com)   *
 /  O  \       ****************************
"""

print(test_ascii)

partial_ascii = re.sub(r'[^\w]{3,}', ' ', test_ascii)

print(partial_ascii)

# CELL ********************

test_ascii = "Amazing game! 10/10 :)"

print(test_ascii)

partial_ascii = re.sub(r'[^\w\s]{3,}', ' ', test_ascii)

print(partial_ascii)

# CELL ********************

import emoji

test_emoji = "This game gets a 🥔 and a 👍 from me!"

demoji_text = emoji.demojize(test_emoji)

print(demoji_text)

# CELL ********************

%pip install emoji

# MARKDOWN ********************

# # Main

# CELL ********************

raw_review_schema = StructType([
    StructField("recommendationid", StringType(), False )
    , StructField("author", StructType([
        StructField("steamid", StringType(), False )
        , StructField("playtime_forever", LongType(), False)
        , StructField("playtime_at_review", LongType(), False)
    ]), False)
    , StructField("language", StringType(), False )
    , StructField("review", StringType(), False )
    , StructField("voted_up", BooleanType(), False )
    , StructField("votes_up", LongType(), False )
    , StructField("votes_funny", LongType(), False )
    , StructField("weighted_vote_score", StringType(), False )
    , StructField("timestamp_created", LongType(), False )
    , StructField("timestamp_updated", LongType(), False )
    , StructField("refunded", BooleanType(), False )
    , StructField("written_during_early_access", BooleanType(), False )
])

# print(raw_review_schema)

deduplicateBy = Window.partitionBy("app_id", "steamid").orderBy(f.col("timestamp_updated").desc())

# CELL ********************

df_bronze_raw = spark.read.format("delta").load(source_abfs).select("app_id", "review_json")

df_bronze_reviews = df_bronze_raw \
    .withColumn("parsed_review", f.from_json(f.col("review_json"), schema=raw_review_schema)) \
    .select(
        f.col("app_id")
        , f.col("parsed_review.recommendationid")
        , f.col("parsed_review.author.steamid")
        , f.col("parsed_review.language")
        , f.col("parsed_review.review")
        , f.col("parsed_review.voted_up")
        , f.col("parsed_review.votes_up")
        , f.col("parsed_review.votes_funny")
        , f.col("parsed_review.weighted_vote_score")
        , f.col("parsed_review.author.playtime_forever")
        , f.col("parsed_review.author.playtime_at_review")
        , f.col("parsed_review.timestamp_created")
        , f.col("parsed_review.timestamp_updated")
        , f.col("parsed_review.refunded")
        , f.col("parsed_review.written_during_early_access")
    )

# CELL ********************

# remove duplicates, non-english reviews, empty reviews, and enforce strict non-null critical columns
df_bronze_reviews_filtered = df_bronze_reviews \
    .withColumn("rn", f.row_number().over(deduplicateBy)) \
    .filter(
        (f.col("rn") == 1) \
        & (f.col("language") == 'english') \
        & (f.trim(f.col("review")) != "") \
        & (f.col("review").isNotNull()) \
        & (f.col("app_id").isNotNull()) \
        & (f.col("recommendationid").isNotNull())
    ) \

# cast stuff to the appropriate type
df_bronze_reviews_cast = df_bronze_reviews_filtered \
    .withColumn("weightedVoteScore", f.col("weighted_vote_score").cast(FloatType())) \
    .withColumn("playtimeForever", f.col("playtime_forever").cast(IntegerType())) \
    .withColumn("playtimeAtReview", f.col("playtime_at_review").cast(IntegerType())) \
    .withColumn("timestampCreated", f.from_unixtime(f.col("timestamp_created")).cast(TimestampType())) \
    .withColumn("timestampUpdated", f.from_unixtime(f.col("timestamp_updated")).cast(TimestampType())) 
    

# CELL ********************

df_bronze_reviews_cast.show(5)

# MARKDOWN ********************

# **to-do for review cleansing**
# 1. Strip BBCode          [b]:thumbs_up:[/b]  →  :thumbs_up:
# 2. Non-space whitespace  \n \t               →  (space)
# 3. Demojize              🥔                  →  :potato:
# 4. Strip :emoji:         :potato:            →  potato
# 5. Strip ASCII art       ****                →  (gone)
# 6. Collapse + trim       (multiple spaces)   →  single space

# MARKDOWN ********************

# # Debug

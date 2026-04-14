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
# META     },
# META     "environment": {
# META       "environmentId": "f87d9097-30b5-bb36-473c-1a6d5f085dde",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Imports

# CELL ********************

import struct
import pyodbc
import json
import emoji
import pandas as pd
import notebookutils

from delta.tables import DeltaTable

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType, BooleanType, TimestampType
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
load_type = "incremental"
run_id = "devIncrementalAndAudit1"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

external_games_path = f"{lakehouse_name}.silver.externalgames"

target_path = f"{lakehouse_name}.silver.steamreviews"
target_table = DeltaTable.forName(spark, target_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Constants

# CELL ********************

audit_server = '22jgi2dsfxnu5lmyn6ifyaro5e-wnxcbukzek4ejbckicpruy7sqq.datawarehouse.fabric.microsoft.com'
audit_database = 'IGDBAudit'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

sia_schema = StructType([
    StructField("pos", FloatType())
    , StructField("compound", FloatType())
    , StructField("neu", FloatType())
    , StructField("neg", FloatType())
])

deduplicateBy = Window.partitionBy("app_id", "steamid").orderBy(f.col("timestamp_updated").desc())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Silver Steam Reviews ELT Initiated with load_type = '{load_type}', for run_id = '{run_id}'")
print(f"Environment = {environment}\n Lakehouse = {lakehouse_name}\n Audit = {audit_database}.{audit_schema}")
print(f"Loading from {source_path} into {target_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Functions

# MARKDOWN ********************

# ## demojize

# CELL ********************

@f.pandas_udf(StringType())
def demojize_udf(s: pd.Series) -> pd.Series:
    return s.apply(emoji.demojize)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## sentiment_compound

# CELL ********************

@f.pandas_udf(sia_schema)
def sentiment_compound(r: pd.Series) -> pd.DataFrame:
    sia = SentimentIntensityAnalyzer()

    analysis_series = r.apply(lambda review: sia.polarity_scores(review) if review else {"pos": None, "compound": None, "neu": None, "neg": None})

    return pd.DataFrame(analysis_series.tolist())[["pos", "compound", "neu", "neg"]]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## connect_audit_wh

# CELL ********************

def connect_audit_wh():

    # token formation
    token = notebookutils.credentials.getToken("pbi")
    token_bytes = token.encode("utf-16-le")
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)

    # connection string + connection
    conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={audit_server};DATABASE={audit_database};Encrypt=yes;TrustServerCertificate=no"
    conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})

    return conn

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## check_version

# CELL ********************

def check_version(table_name):
    query = f"""
        select top 1
            latest_source_version
        from {audit_schema}.versionControl
        where table_name = ?
        order by
            commit_version desc
    """

    conn = connect_audit_wh()
    db_cursor = conn.cursor()

    try:
        db_cursor.execute(query, table_name)
        latest_source_version = db_cursor.fetchone()[0]

        print(f"Retrieved last source version for {table_name}: {latest_source_version}")        
        return latest_source_version
    except Exception as e:
        print(f"Failed to retrieve last source version for {table_name} from {audit_database}.{audit_schema}.versionControl: {e}")
    finally:
        db_cursor.close()
        conn.close()    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## insert_version

# CELL ********************

def insert_version(audit_row, latest_source_version):
    insert_query = f"""
        insert into {audit_schema}.versionControl (
            table_name
            , run_id
            , change_type
            , commit_version
            , commit_timestamp
            , rows_inserted
            , rows_updated
            , latest_source_version
            , audit_notes
        )
        values (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    full_audit_row = audit_row[0].asDict()
    audit_notes = json.dumps(full_audit_row, default=str)

    insert_parameters = [
        target_path
        , run_id
        , audit_row[0]['operation']
        , audit_row[0]['version']
        , audit_row[0]['timestamp']
        , int(audit_row[0]['operationMetrics']['numTargetRowsInserted'])
        , int(audit_row[0]['operationMetrics']['numTargetRowsUpdated'])
        , latest_source_version
        , audit_notes
    ]

    conn = connect_audit_wh()
    db_cursor = conn.cursor()

    try:
        db_cursor.execute(insert_query, insert_parameters)
        conn.commit()
    except Exception as e:
        print(f"Failed to insert version: {e}")
        conn.rollback()
    else:
        print(f"Successfully logged audit for {target_path} in {audit_schema}.versionControl with the audit_row = {audit_notes}")
    finally:
        db_cursor.close()
        conn.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Main

# CELL ********************

# Load data from Bronze based on load_type
if load_type == 'full':
    df_bronze_raw = spark.read.format("delta").load(source_abfs).select("app_id", "review_json")
elif load_type == 'incremental':
    latest_source_version = check_version(table_name=target_path)
    current_source_version = source_table.history(1).select("version").collect()[0][0]

    if latest_source_version is None:
        print(f"No previous source version found for {target_path} in audit. Defaulting to full load.")

        df_bronze_raw = spark.read.format("delta").load(source_abfs).select("app_id", "review_json")
    elif current_source_version == latest_source_version:
        print(f"No new version found for {source_path}. Latest version in audit: {latest_source_version}, current version in source: {current_source_version}. Shutting down.")
        notebookutils.notebook.exit("No new version to process")
    else:
        df_bronze_raw = spark.read.format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingVersion", latest_source_version+1) \
            .table(source_path) \
            .filter(f.col("_change_type").isin("insert", "update_postimage")) \
            .select("app_id", "review_json")
else:
    print(f"Invalid load_type: {load_type}! Shutting down")
    notebookutils.notebook.exit("Wrong load_type")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# basic parsing and flattening of the json review data, to get it ready for cleaning and enrichment
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
    .withColumn("eId", f.col("app_id").cast(StringType())) \
    .withColumn("weightedVoteScore", f.col("weighted_vote_score").cast(FloatType())) \
    .withColumn("playtimeForever", f.col("playtime_forever").cast(IntegerType())) \
    .withColumn("playtimeAtReview", f.col("playtime_at_review").cast(IntegerType())) \
    .withColumn("timestampCreated", f.from_unixtime(f.col("timestamp_created")).cast(TimestampType())) \
    .withColumn("timestampUpdated", f.from_unixtime(f.col("timestamp_updated")).cast(TimestampType())) \
    .withColumn("votesUp",
        f.when(f.col("votes_up") > 2147483647, 0 ).otherwise(f.col("votes_up").cast(IntegerType()))
    ) \
    .withColumn("votesFunny",
        f.when(f.col("votes_funny") > 2147483647, 0 ).otherwise(f.col("votes_funny").cast(IntegerType()))
    )    
    
# convert emoji icons into text so vader can read them    
df_bronze_reviews_demoji = df_bronze_reviews_cast \
    .withColumn("reviewDemoji", demojize_udf(f.col("review")))

# extensive cleaning of the review text to remove bbcode, links, hashtags, emojis, and other non-standard characters, while also trying to catch common censorship patterns (i.e. :heart_suit: in a row)
df_bronze_reviews_cleaned = df_bronze_reviews_demoji \
    .withColumn("reviewText",
        f.trim(
            f.regexp_replace(
                f.regexp_replace(
                    f.regexp_replace(
                        f.regexp_replace(
                            f.regexp_replace(
                                f.regexp_replace(
                                    f.col("reviewDemoji") # bbcode removal -> remove any [ ] pair with anything inside i.e. [b] [/b]
                                    , r"\[.*?\]"
                                    ,''
                                )
                                , r"[^\w!\?\s]{3,}|#|https?://\S+"  # ascii, hashtag, link removal ->  remove any non-alphanumeric non-! character that repeats at least 3 times in sequence, remove any #, remove anything that starts with https:// followed by any repeating non-whitespace character
                                , ''
                            )
                            , r"(:heart_suit:){7,}"     # replace a string of 7+ x :heart_suit: (the demojized version of the character steam uses for censorship)
                            , 'fucking'
                            )
                        , r"(:heart_suit:){2,}"    # catchall for any other censor
                        , 'fuck'
                        )
                    , r':(\w+):'
                    , r'$1'     # demoji removal of ':' -> replace any group found between two ::, of alphanumeric characters that repeat at least once, with itself (just without the :)
                )
                , r"_|\s+"    # replace _ OR any whitespace character that repeats at least once with a single space
                , ' '
            )
        )
    )  

# enhance the review data with additional features
df_bronze_reviews_enhanced = df_bronze_reviews_cleaned \
    .withColumn("reviewLength", f.length(f.col("reviewText"))) \
    .withColumn("wordCount", f.size(f.split(f.col("reviewText"), " "))) \
    .withColumn("vaderRatio", 
        f.when(f.col("reviewLength") == 0, 0.0).otherwise(
            f.length(f.regexp_replace(f.col("reviewText"), r'[^\x00-\x7F]', '')) / f.col("reviewLength"))       # ratio of non-standard characters outside of the main 126 ascii vs total length. if it's more than 41% exclude text from sentiment analysis
    ) \
    .withColumn("isUsableForVader", f.col("vaderRatio") >= 0.6) \
    .withColumn("containsBugReport", 
        f.col("reviewText").rlike(r'(?i)\b(bug|bugs|crash|error|lag|stuck)')                                    # find any case insensitive word that starts with those elements (i.e. bug, bugging, bugged)
    ) \
    .withColumn("emotionalIntensity", 
        f.when(f.col("reviewLength") == 0, 0.0).otherwise(
            (f.col("reviewLength") - f.length(f.regexp_replace(f.col("reviewText"), r'!{2,}|[A-Z]', ''))) / f.col("reviewLength"))      # ratio of '!' and caps vs total length
    ) \
    .withColumn("sentimentAnalysis", 
        f.when(f.col("isUsableForVader"), sentiment_compound(f.col("reviewText"))) \
            .otherwise(None)
     ) \
     .withColumn("salt",        # salting because i join with external_games and some eIds have way more reviews than others
        (f.rand()*10).cast("int")
     ) \
    .select(
        "eId"
        , "recommendationid"
        , "steamid"
        , "language"
        , 'review'
        , "reviewText"
        , "voted_up"
        , "votesUp"
        , "votesFunny"
        , "weightedVoteScore"
        , "playtimeForever"
        , "playtimeAtReview"
        , "timestampCreated"
        , "timestampUpdated"
        , "refunded"
        , "written_during_early_access"
        , "reviewLength"
        , "wordCount"
        , "vaderRatio"
        , "isUsableForVader"
        , "containsBugReport"
        , "emotionalIntensity"
        , "sentimentAnalysis.pos"
        , "sentimentAnalysis.compound"
        , "sentimentAnalysis.neu"
        , "sentimentAnalysis.neg"
    )

# join with external games to get the gameKey, and create a hash of all the review attributes to make it easier to identify changes in the review content in the future (since steam doesn't provide a unique id for each review and reviews can be updated)
df_external_games = spark.read.format("delta").table(external_games_path).select("eId", "gameKey").filter(f.col("egSourceName") == 'Steam')

df_bronze_lookup = df_bronze_reviews_enhanced.alias("reviews") \
    .join(f.broadcast(df_external_games).alias("eg"), "eId", "inner")

columns_to_hash = [c for c in df_bronze_lookup.columns if c not in ['eId', 'steamid'] ]

df_bronze_reviews_final = df_bronze_lookup \
    .withColumn("hash", f.sha2(f.concat_ws(",", *[f.col(c) for c in columns_to_hash]), 256)) \
    .selectExpr(
        "sha2(cast(concat_ws('|', eId, steamid) as string), 256)                        as reviewKey"
        , "gameKey                                                                      as gameKey"
        , "eId                                                                          as eId"
        , "recommendationid                                                             as recommendationId"
        , "steamid                                                                      as authorId"
        , "language                                                                     as language"
        , "review                                                                       as reviewRaw"
        , "reviewText                                                                   as reviewCleaned"
        , "voted_up                                                                     as votedUp"
        , "votesUp                                                                      as votesUp"
        , "votesFunny                                                                   as votesFunny"
        , "weightedVoteScore                                                            as weightedVoteScore"
        , "playtimeForever                                                              as playtimeForever"
        , "playtimeAtReview                                                             as playtimeAtReview"
        , "timestampCreated                                                             as timestampCreated"
        , "timestampUpdated                                                             as timestampUpdated"
        , "refunded                                                                     as refunded"
        , "written_during_early_access                                                  as writtenDuringEarlyAccess"
        , "reviewLength                                                                 as reviewLength"
        , "wordCount                                                                    as wordCount"
        , "vaderRatio                                                                   as vaderRatio"
        , "isUsableForVader                                                             as isUsableForVader"
        , "containsBugReport                                                            as containsBugReport"
        , "emotionalIntensity                                                           as emotionalIntensity"
        , "pos                                                                          as sentimentPositive"
        , "compound                                                                     as sentimentCompound"
        , "neu                                                                          as sentimentNeutral"
        , "neg                                                                          as sentimentNegative"
        , "hash                                                                         as hash"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Merge

# CELL ********************

if load_type == "full":
    print(f"Load type is '{load_type}', truncating table {target_path}...")    

    truncate_query = f"truncate table {target_path}"
    spark.sql(truncate_query)

    print("Truncate completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

version_before = target_table.history(1).collect()[0][0]

print(f"Merge target: {target_path}. Executing merge...")

target_table.alias("t").merge(
    df_bronze_reviews_final.alias("s"),
    "t.reviewKey = s.reviewKey"
).whenMatchedUpdate(
    condition="t.hash != s.hash",
    set={
        "gameKey":                  "s.gameKey"
        , "eId":                    "s.eId"
        , "recommendationId":       "s.recommendationId"
        , "authorId":               "s.authorId"
        , "language":               "s.language"
        , "reviewRaw":              "s.reviewRaw"
        , "reviewCleaned":          "s.reviewCleaned"
        , "votedUp":                "s.votedUp"
        , "votesUp":                "s.votesUp"
        , "votesFunny":             "s.votesFunny"
        , "weightedVoteScore":      "s.weightedVoteScore"
        , "playtimeForever":        "s.playtimeForever"
        , "playtimeAtReview":       "s.playtimeAtReview"
        , "timestampCreated":       "s.timestampCreated"
        , "timestampUpdated":       "s.timestampUpdated"
        , "refunded":               "s.refunded"
        , "writtenDuringEarlyAccess": "s.writtenDuringEarlyAccess"
        , "reviewLength":           "s.reviewLength"
        , "wordCount":              "s.wordCount"
        , "vaderRatio":             "s.vaderRatio"
        , "isUsableForVader":       "s.isUsableForVader"
        , "containsBugReport":      "s.containsBugReport"
        , "emotionalIntensity":     "s.emotionalIntensity"
        , "sentimentPositive":      "s.sentimentPositive"
        , "sentimentCompound":      "s.sentimentCompound"
        , "sentimentNeutral":       "s.sentimentNeutral"
        , "sentimentNegative":      "s.sentimentNegative"
        , "update_run_id":          f"'{run_id}'"
        , "hash":                   "s.hash"
    }
).whenNotMatchedInsert(
    values={
        "reviewKey":                "s.reviewKey"
        , "gameKey":                "s.gameKey"
        , "eId":                    "s.eId"
        , "recommendationId":       "s.recommendationId"
        , "authorId":               "s.authorId"
        , "language":               "s.language"
        , "reviewRaw":              "s.reviewRaw"
        , "reviewCleaned":          "s.reviewCleaned"
        , "votedUp":                "s.votedUp"
        , "votesUp":                "s.votesUp"
        , "votesFunny":             "s.votesFunny"
        , "weightedVoteScore":      "s.weightedVoteScore"
        , "playtimeForever":        "s.playtimeForever"
        , "playtimeAtReview":       "s.playtimeAtReview"
        , "timestampCreated":       "s.timestampCreated"
        , "timestampUpdated":       "s.timestampUpdated"
        , "refunded":               "s.refunded"
        , "writtenDuringEarlyAccess": "s.writtenDuringEarlyAccess"
        , "reviewLength":           "s.reviewLength"
        , "wordCount":              "s.wordCount"
        , "vaderRatio":             "s.vaderRatio"
        , "isUsableForVader":       "s.isUsableForVader"
        , "containsBugReport":      "s.containsBugReport"
        , "emotionalIntensity":     "s.emotionalIntensity"
        , "sentimentPositive":      "s.sentimentPositive"
        , "sentimentCompound":      "s.sentimentCompound"
        , "sentimentNeutral":       "s.sentimentNeutral"
        , "sentimentNegative":      "s.sentimentNegative"
        , "insert_run_id":          f"'{run_id}'"
        , "update_run_id":          "null"
        , "hash":                   "s.hash"
    }
).execute()

audit_row = target_table.history(1).collect()
version_after = audit_row[0][0]

if version_before == version_after:
    print("Merge executed. No rows affected")
else:
    current_source_version = source_table.history(1).collect()[0][0]
    insert_version(audit_row=audit_row, latest_source_version=current_source_version)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Optimize

# CELL ********************

if version_before != version_after:
    print(f"OPTIMIZE {target_path} to cluster new additions...")

    optimize_query = f"OPTIMIZE {target_path}"
    spark.sql(optimize_query)

    print(f"OPTIMIZE Completed!")    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

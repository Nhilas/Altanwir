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

import struct
import pyodbc
import notebookutils
import json

from pyspark.sql import functions as f
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Parameters

# PARAMETERS CELL ********************

environment = "dev"
load_type = "incremental"   # valid options: "full", "reload", "incremental", "targeted"
run_id = "gold_enhancement_04"

# threshold for salting; 9999999 effectively turns off salting. if a game ever gets 10 million reviews we have bigger problems anyway. lol
hot_key_threshold = 50000
salt_factor = 32

# format this as a list. only used if load_type = 'targeted'. only accepts gameKeys
targeted_reload = [ 'fee1882ab7f5f816b65f0cd5b277fb74c058352c5a95c6e302f07bc423aa717f', '9b82015126416c80cc13505a3f254f33336e37432509bab854553afd2b51f4fb', 'f6121e9cec01d2dc9c3f8762f2ed088c6e4b3cdf32b26a970a73e3eae5dd3351']

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

source_abfs = f"{abfs_root}/Tables/silver/steamreviews"
source_path = f"{lakehouse_name}.silver.steamreviews"
source_table = DeltaTable.forName(spark, source_path)

target_path = f"{lakehouse_name}.gold.factReviews"
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

community_weights = {
    'helpfulnessWeight': 0.45,
    'funninessWeight': 0.20,
    'commentWeight': 0.25,
    'reactionWeight': 0.1
}

influence_weights = {
    'w_community': 1.5,
    'w_length': 0.5,
    'w_emotional': 0.3,
    'w_playtime': 1.0,
    'w_sentiment': 1.0
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Gold Steam Reviews ETL Initiated with load_type = '{load_type}', for run_id = '{run_id}'")
print(f"Environment = {environment}\n Lakehouse = {lakehouse_name}\n Audit = {audit_database}.{audit_schema}")
print(f"\nLoading from {source_path} into {target_path}")
print("\nConfiguration:")
print(f"* Hot Key Threshold = {hot_key_threshold}")
print(f"* Salt Factor = {salt_factor}")
print(f"* Community Weights = {json.dumps(community_weights, indent=4)}")
print(f"* Influence Weights = {json.dumps(influence_weights, indent=4)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Functions

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

if load_type in ['full', 'reload']:
    from_clause = source_path
elif load_type == 'incremental':
    latest_source_version = check_version(table_name=target_path)
    current_source_version = source_table.history(1).select("version").collect()[0][0]

    if latest_source_version is None:
        print(f"No previous source version found for {target_path} in audit. Defaulting to full load_type.")

        from_clause = source_path
    elif current_source_version == latest_source_version:
        print(f"No new version found for {source_path}. Latest version in audit: {latest_source_version}, current version in source: {current_source_version}. Shutting down.")
        notebookutils.notebook.exit("No new version to process")
        
    else:
        cdf_query = f"""
            select distinct gameKey 
            from table_changes('{source_path}', {latest_source_version+1}) 
            where _change_type in ('insert', 'update_postimage')
        """
        changed_games = spark.sql(cdf_query).collect()

        gameKeys = [row.gameKey for row in changed_games]
        sep = "', '"
        predicate = f"'{sep.join(gameKeys)}'"

        print(f"Found {len(gameKeys)} games with new or updated reviews in {source_path}")
        from_clause = f"{source_path}\n\twhere gameKey in ({predicate})"

elif load_type == 'targeted' and targeted_reload:
    sep = "', '"
    gameKey_predicate = f"'{sep.join(targeted_reload)}'"
    from_clause = f"{source_path}\n\twhere gameKey in ({gameKey_predicate})"

else:
    print(f"Invalid load_type: {load_type}! Shutting down")
    notebookutils.notebook.exit("Wrong load_type")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

hot_keys_rows = spark.sql(f"select gameKey, count(*) as reviewCount from {from_clause} group by gameKey having reviewCount > {hot_key_threshold}").collect()
hot_keys = [row['gameKey'] for row in hot_keys_rows]

raw_query = f"""
select
    reviewKey
    , gameKey
    , reviewCleaned
    , votedUp
    , votesUp
    , votesFunny
    , commentCount
    , reactionCount
    , weightedVoteScore as steamWeightedVoteScore
    , playtimeAtReview
    , refunded
    , writtenDuringEarlyAccess
    , reviewLength
    , wordCount
    , wordLengthRatio
    , uniqueWordRatio
    , hasCredibleText
    , isVaderEligible    
    , containsBugReport
    , emotionalIntensity
    , sentimentCompound
from {from_clause}
"""

df_silver_raw = spark.sql(raw_query) \
    .withColumn("salt", 
        f.when(f.col("gameKey").isin(hot_keys), (f.rand() * salt_factor).cast("int"))
            .otherwise(f.lit(0))
    )

df_silver_game_stats_salted = df_silver_raw.groupBy("gameKey", "salt") \
    .agg(
        f.max("votesUp").alias("max_votesUp"),
        f.max("votesFunny").alias("max_votesFunny"),
        f.max("commentCount").alias("max_commentCount"),
        f.max("reactionCount").alias("max_reactionCount"),
        f.max(f.when(f.col("hasCredibleText") == True, f.col("reviewLength"))).alias("max_reviewLength"),
        f.sum("playtimeAtReview").alias("sum_playtime"),
        f.count("reviewKey").alias("reviewCount")
    ) \
    .select("gameKey", "max_votesUp", "max_votesFunny", "max_commentCount", "max_reactionCount", "max_reviewLength", "sum_playtime", "reviewCount")

df_silver_game_stats = df_silver_game_stats_salted.groupBy("gameKey") \
    .agg(
        f.max("max_votesUp").alias("max_votesUp"),
        f.max("max_votesFunny").alias("max_votesFunny"),
        f.max("max_commentCount").alias("max_commentCount"),
        f.max("max_reactionCount").alias("max_reactionCount"),
        f.max("max_reviewLength").alias("max_reviewLength"),
        (f.sum("sum_playtime") / f.sum("reviewCount")).alias("avg_playtime")
    ) \
    .select("gameKey", "max_votesUp", "max_votesFunny", "max_commentCount", "max_reactionCount", "max_reviewLength", "avg_playtime")

df_silver_game_max_stats = df_silver_raw \
    .join(f.broadcast(df_silver_game_stats), on= "gameKey", how="left") \
    .select(
        "reviewKey"
        , "gameKey"
        , "reviewCleaned"
        , "votedUp"
        , "votesUp"
        , "votesFunny"
        , "commentCount"
        , "reactionCount"
        , "steamWeightedVoteScore"
        , "playtimeAtReview"
        , "refunded"
        , "writtenDuringEarlyAccess"
        , "reviewLength"
        , "wordCount"
        , "wordLengthRatio"
        , "uniqueWordRatio"
        , "hasCredibleText"
        , "isVaderEligible"        
        , "containsBugReport"
        , "emotionalIntensity"
        , "sentimentCompound"
        , "max_votesUp"
        , "max_votesFunny"
        , "max_commentCount"
        , "max_reactionCount"
        , "max_reviewLength"
        , "avg_playtime"
    )

df_silver_game_max_stats.createOrReplaceTempView("silver_reviews_with_game_stats")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

merge_source_query = f"""
with aux_silver as (
    select
        *
        , percent_rank() over (partition by gameKey order by playtimeAtReview) as playtimePercentile
        , coalesce(log(votesUp + 1) / nullif(log(max_votesUp + 1),0),0) as helpfulnessRatio
        , coalesce(log(votesFunny + 1) / nullif(log(max_votesFunny + 1),0),0) as funninessRatio
        , coalesce(log(commentCount + 1) / nullif(log(max_commentCount + 1),0),0) as commentRatio
        , coalesce(log(reactionCount + 1) / nullif(log(max_reactionCount + 1),0),0) as reactionRatio
        , coalesce(log(reviewLength + 1) / nullif(log(max_reviewLength + 1),0),0) as lengthRatio
        , case when votedUp = True then 1 else -1 end as voteDirection
        , case
            when isVaderEligible and sentimentCompound <> 0 then sign(sentimentCompound)
            when isVaderEligible and sentimentCompound = 0 then
                case when votedUp = True then 1 else -1 end
        end as sentimentDirection
    from silver_reviews_with_game_stats
)

, influence_base as (
    select *
        , {community_weights['helpfulnessWeight']} * helpfulnessRatio 
            + {community_weights['funninessWeight']} * funninessRatio 
            + {community_weights['commentWeight']} * commentRatio 
            + {community_weights['reactionWeight']} * reactionRatio 
        as communitySignal
        , case when hasCredibleText = True then lengthRatio * uniqueWordRatio else NULL end as lengthSignal
        , case when hasCredibleText = True then least(emotionalIntensity, {influence_weights['w_emotional']}) / {influence_weights['w_emotional']} else NULL end as emotionalSignal
        , playtimePercentile as playtimeSignal
        , case when isVaderEligible = True then abs(sentimentCompound) else NULL end as sentimentSignal

        , case when hasCredibleText = True then {influence_weights['w_length']} else 0 end as w_length
        , case when hasCredibleText = True then {influence_weights['w_emotional']} else 0 end as w_emotional
        , case when avg_playtime > 0 then {influence_weights['w_playtime']} else 0 end as w_playtime
        , case when isVaderEligible = True then {influence_weights['w_sentiment']} else 0 end as w_sentiment
    from aux_silver
)

, influence_formula as (
    select *
        , ( {influence_weights['w_community']} * communitySignal
            + w_length * coalesce(lengthSignal, 0)
            + w_emotional * coalesce(emotionalSignal, 0)
            + w_playtime * playtimeSignal
            + w_sentiment * coalesce(sentimentSignal, 0) )
            / (  {influence_weights['w_community']} 
                + w_length
                + w_emotional
                + w_playtime
                + w_sentiment )
        as reviewInfluenceScore        
    from influence_base
)

select
    reviewKey
    , gameKey
    , reviewCleaned

    , votedUp
    , votesUp
    , votesFunny
    , commentCount
    , reactionCount
    , communitySignal

    , reviewLength
    , wordCount
    , wordLengthRatio
    , uniqueWordRatio
    , hasCredibleText
    , lengthSignal

    , playtimeAtReview
    , playtimeSignal

    , isVaderEligible
    , sentimentCompound
    , sentimentSignal
    , sentimentDirection

    , emotionalIntensity
    , emotionalSignal

    , voteDirection
    , reviewInfluenceScore
    , steamWeightedVoteScore

    , refunded
    , writtenDuringEarlyAccess
    , containsBugReport    
from influence_formula

"""

df_silver_reviews_processed = spark.sql(merge_source_query)

# hash excludes review raw and review cleaned for performance reasons; they can be quite large, and any change in the text is already captured by length, review metadata, ratios, sentiment and so on
columns_to_hash = [c for c in df_silver_reviews_processed.columns if c not in ['reviewKey', 'reviewCleaned'] ]

df_silver_reviews = df_silver_reviews_processed \
    .withColumn("hash", f.sha2(f.concat_ws("|", *[f.col(c) for c in columns_to_hash]), 256))

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
    df_silver_reviews.alias("s"),
    "t.reviewKey = s.reviewKey"
).whenMatchedUpdate(
    condition="t.hash != s.hash",
    set={
        "reviewKey":                  "s.reviewKey",
        "gameKey":                    "s.gameKey",
        "reviewCleaned":              "s.reviewCleaned",
        "votedUp":                    "s.votedUp",
        "votesUp":                    "s.votesUp",
        "votesFunny":                 "s.votesFunny",
        "commentCount":               "s.commentCount",
        "reactionCount":              "s.reactionCount",
        "communitySignal":            "s.communitySignal",
        "reviewLength":               "s.reviewLength",
        "wordCount":                  "s.wordCount",
        "wordLengthRatio":            "s.wordLengthRatio",
        "hasCredibleText":            "s.hasCredibleText",
        "uniqueWordRatio":            "s.uniqueWordRatio",
        "lengthSignal":               "s.lengthSignal",
        "playtimeAtReview":           "s.playtimeAtReview",
        "playtimeSignal":             "s.playtimeSignal",
        "isVaderEligible":            "s.isVaderEligible",
        "sentimentCompound":          "s.sentimentCompound",
        "sentimentSignal":            "s.sentimentSignal",
        "sentimentDirection":         "s.sentimentDirection",
        "emotionalIntensity":         "s.emotionalIntensity",
        "emotionalSignal":            "s.emotionalSignal",
        "voteDirection":              "s.voteDirection",
        "reviewInfluenceScore":       "s.reviewInfluenceScore",
        "steamWeightedVoteScore":     "s.steamWeightedVoteScore",
        "refunded":                   "s.refunded",
        "writtenDuringEarlyAccess":   "s.writtenDuringEarlyAccess",
        "containsBugReport":          "s.containsBugReport",    
        "t.update_run_id":            f"'{run_id}'",
        "t.hash":                     "s.hash",
    }
).whenNotMatchedInsert(
    values={
        "reviewKey":                "s.reviewKey",
        "gameKey":                  "s.gameKey",
        "reviewCleaned":            "s.reviewCleaned",
        "votedUp":                  "s.votedUp",
        "votesUp":                  "s.votesUp",
        "votesFunny":               "s.votesFunny",
        "commentCount":             "s.commentCount",
        "reactionCount":            "s.reactionCount",
        "communitySignal":          "s.communitySignal",
        "reviewLength":             "s.reviewLength",
        "wordCount":                "s.wordCount",
        "wordLengthRatio":          "s.wordLengthRatio",
        "hasCredibleText":          "s.hasCredibleText",        
        "uniqueWordRatio":          "s.uniqueWordRatio",
        "lengthSignal":             "s.lengthSignal",
        "playtimeAtReview":         "s.playtimeAtReview",
        "playtimeSignal":           "s.playtimeSignal",
        "isVaderEligible":          "s.isVaderEligible",
        "sentimentCompound":        "s.sentimentCompound",
        "sentimentSignal":          "s.sentimentSignal",
        "sentimentDirection":       "s.sentimentDirection",
        "emotionalIntensity":       "s.emotionalIntensity",
        "emotionalSignal":          "s.emotionalSignal",
        "voteDirection":            "s.voteDirection",
        "reviewInfluenceScore":     "s.reviewInfluenceScore",
        "steamWeightedVoteScore":   "s.steamWeightedVoteScore",
        "refunded":                 "s.refunded",
        "writtenDuringEarlyAccess": "s.writtenDuringEarlyAccess",
        "containsBugReport":        "s.containsBugReport",        
        "insert_run_id":            f"'{run_id}'",
        "update_run_id":            "null",
        "hash":                     "s.hash",
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

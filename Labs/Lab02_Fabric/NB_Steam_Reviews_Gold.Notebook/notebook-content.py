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

# CELL ********************

environment = "dev"
load_type = "incremental"   # valid options: "full", "reload", "incremental", "targeted"
run_id = "bugFix_null_playtime1"

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

target_path = f"{lakehouse_name}.gold.steamreviewstats"
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

print(f"Gold Steam Reviews ELT Initiated with load_type = '{load_type}', for run_id = '{run_id}'")
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
        from_clause = f"{source_path}\n\twhere gameKey in ( select distinct gameKey from table_changes('{source_path}', {latest_source_version+1}) )"
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

source_query = f"""
with raw_silver as (
    select
        reviewKey
        , gameKey
        , reviewCleaned
        , votedUp
        , votesUp
        , votesFunny
        , weightedVoteScore
        , playtimeAtReview
        , refunded
        , writtenDuringEarlyAccess
        , reviewLength
        , wordCount
        , containsBugReport
        , emotionalIntensity
        , isUsableForVader
        , sentimentCompound
    from {from_clause}
)

, game_stats as (
    select
        gameKey
        , max(votesUp) as max_votesUp
        , max(reviewLength) as max_reviewLength
    from raw_silver
    group by gameKey
)

, aux_silver as (
    select
        r.*
        , gs.max_votesUp
        , gs.max_reviewLength
        , round(percent_rank() over (partition by r.gameKey order by playtimeAtReview)*100) as playtimePercentile
        , coalesce(log(votesUp + 1) / nullif(log(max_votesUp + 1),0),0) as communityWeight
        , coalesce(log(reviewLength + 1) / nullif(log(max_reviewLength + 1),0),0) as lengthWeight
    from raw_silver as r
    left join game_stats as gs
        on r.gameKey = gs.gameKey
)

, enhanced_silver as (
    select 
        *
        , case 
            when playtimePercentile >= 67 then 'Hardcore'
            when playtimePercentile between 34 and 66 then 'Regular'
            when playtimePercentile <= 33 then 'Casual'
        end as playtimeBucket
        , case 
            when sentimentCompound is not NULL then
                case
                    when sentimentCompound >= 0.05 then 'Positive'
                    when sentimentCompound <= -0.05 then 'Negative'
                    else 'Neutral'
                end
            else NULL
        end as sentimentLabel
        , case when votedUp = True then 1 else -1 end as voteSignal
        , case
            when isUsableForVader and sentimentCompound <> 0 then sign(sentimentCompound)
            when isUsableForVader and sentimentCompound = 0 then
                case when votedUp = True then 1 else -1 end
        end as sentimentSignal
        , emotionalIntensity * 0.5 as emotionalWeight
        , case
            when isUsableForVader = True
                then ( playtimePercentile/100 + abs(sentimentCompound) + communityWeight + lengthWeight + emotionalWeight ) / 4.5
            else ( playtimePercentile/100 + communityWeight + emotionalWeight ) / 2.5
        end as reviewInfluenceScore
    from aux_silver
)

select
    reviewKey
    , gameKey
    , reviewCleaned

    , votedUp
    , votesUp
    , votesFunny
    , communityWeight

    , reviewLength
    , wordCount    
    , lengthWeight

    , playtimeAtReview
    , playtimePercentile
    , playtimeBucket

    , refunded
    , writtenDuringEarlyAccess
    , containsBugReport
    
    , sentimentCompound
    , sentimentLabel
    , emotionalIntensity
    , emotionalWeight

    , voteSignal
    , sentimentSignal
    , reviewInfluenceScore
    , weightedVoteScore
from enhanced_silver
"""

df_silver_reviews_processed = spark.sql(source_query)

columns_to_hash = [c for c in df_silver_reviews_processed.columns if c not in ['reviewKey'] ]

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
        "t.gameKey":                   "s.gameKey",
        "t.reviewCleaned":             "s.reviewCleaned",
        "t.votedUp":                   "s.votedUp",
        "t.votesUp":                   "s.votesUp",
        "t.votesFunny":                "s.votesFunny",
        "t.communityWeight":           "s.communityWeight",
        "t.reviewLength":              "s.reviewLength",
        "t.wordCount":                 "s.wordCount",
        "t.lengthWeight":              "s.lengthWeight",
        "t.playtimeAtReview":          "s.playtimeAtReview",
        "t.playtimePercentile":        "s.playtimePercentile",
        "t.playtimeBucket":            "s.playtimeBucket",
        "t.refunded":                  "s.refunded",
        "t.writtenDuringEarlyAccess":  "s.writtenDuringEarlyAccess",
        "t.containsBugReport":         "s.containsBugReport",
        "t.sentimentCompound":         "s.sentimentCompound",
        "t.sentimentLabel":            "s.sentimentLabel",
        "t.emotionalIntensity":        "s.emotionalIntensity",
        "t.emotionalWeight":           "s.emotionalWeight",
        "t.voteSignal":                "s.voteSignal",
        "t.sentimentSignal":           "s.sentimentSignal",
        "t.reviewInfluenceScore":      "s.reviewInfluenceScore",
        "t.weightedVoteScore":         "s.weightedVoteScore",
        "t.update_run_id":             f"'{run_id}'",
        "t.hash":                      "s.hash",
    }
).whenNotMatchedInsert(
    values={
        "reviewKey":                "s.reviewKey",
        "gameKey":                  "s.gameKey",
        "reviewCleaned":            "s.reviewCleaned",
        "votedUp":                  "s.votedUp",
        "votesUp":                  "s.votesUp",
        "votesFunny":               "s.votesFunny",
        "communityWeight":          "s.communityWeight",
        "reviewLength":             "s.reviewLength",
        "wordCount":                "s.wordCount",
        "lengthWeight":             "s.lengthWeight",
        "playtimeAtReview":         "s.playtimeAtReview",
        "playtimePercentile":       "s.playtimePercentile",
        "playtimeBucket":           "s.playtimeBucket",
        "refunded":                 "s.refunded",
        "writtenDuringEarlyAccess": "s.writtenDuringEarlyAccess",
        "containsBugReport":        "s.containsBugReport",
        "sentimentCompound":        "s.sentimentCompound",
        "sentimentLabel":           "s.sentimentLabel",
        "emotionalIntensity":       "s.emotionalIntensity",
        "emotionalWeight":          "s.emotionalWeight",
        "voteSignal":               "s.voteSignal",
        "sentimentSignal":          "s.sentimentSignal",
        "reviewInfluenceScore":     "s.reviewInfluenceScore",
        "weightedVoteScore":        "s.weightedVoteScore",
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

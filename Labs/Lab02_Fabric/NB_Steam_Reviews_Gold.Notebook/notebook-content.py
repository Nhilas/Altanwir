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
load_type = "incremental"
run_id = "devGoldSteamReviews1"

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
# target_table = DeltaTable.forName(spark, target_path)

# MARKDOWN ********************

# ## Constants

# CELL ********************

audit_server = '22jgi2dsfxnu5lmyn6ifyaro5e-wnxcbukzek4ejbckicpruy7sqq.datawarehouse.fabric.microsoft.com'
audit_database = 'IGDBAudit'

# CELL ********************

print(f"Gold Steam Reviews ELT Initiated with load_type = '{load_type}', for run_id = '{run_id}'")
print(f"Environment = {environment}\n Lakehouse = {lakehouse_name}\n Audit = {audit_database}.{audit_schema}")
print(f"Loading from {source_path} into {target_path}")

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

# MARKDOWN ********************

# # Main

# CELL ********************

test_sample = [ 'fee1882ab7f5f816b65f0cd5b277fb74c058352c5a95c6e302f07bc423aa717f', '9b82015126416c80cc13505a3f254f33336e37432509bab854553afd2b51f4fb', 'f6121e9cec01d2dc9c3f8762f2ed088c6e4b3cdf32b26a970a73e3eae5dd3351']

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
        , sentimentPositive
        , sentimentCompound
        , sentimentNeutral
        , sentimentNegative
    from {source_path}
    where gameKey in ('{"', '".join([game for game in test_sample])}')
)

, game_stats as (
    select
        gameKey
        , max(votesUp) as max_votesUp
        , max(reviewLength) as max_reviewLength
    from raw_silver
    where
        isUsableForVader
    group by gameKey
)

, aux_silver as (
    select
        r.*
        , gs.max_votesUp
        , gs.max_reviewLength
        , round(percent_rank() over (partition by r.gameKey order by playtimeAtReview)*100) as playtimePercentile
        , log(votesUp + 1) / log(max_votesUp + 1) as communityWeight
        , case
            when isUsableForVader then log(reviewLength + 1) / log(max_reviewLength + 1) 
            else NULL
        end as lengthWeight
        , emotionalIntensity * 0.3 as emotionalWeight
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
            when sentimentCompound >= 0.05 then 'Positive'
            when sentimentCompound <= -0.05 then 'Negative'
            else 'Neutral'
        end as sentimentLabel
        , case when votedUp then 1 else -1 end as voteSignal
        , case
            when isUsableForVader and sentimentCompound <> 0 then sign(sentimentCompound)
            else case when votedUp then 1 else -1 end
        end as sentimentSignal
        , case
            when isUsableForVader = True
                then ( playtimePercentile/100 + abs(sentimentCompound) + communityWeight + lengthWeight + emotionalWeight ) / 4.5
            else ( playtimePercentile/100 + communityWeight + emotionalWeight ) / 2.5
        end as reviewInfluenceScore
    from aux_silver
)

select *
from enhanced_silver
"""

spark.sql(source_query).createOrReplaceTempView("test")

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select
# MAGIC     case
# MAGIC         when votedUp = True
# MAGIC             then "yes"
# MAGIC         when votedUp = False
# MAGIC             then "no"
# MAGIC         when votedUp is NULL
# MAGIC             then "null"
# MAGIC         else "what the fuck" end as sparkCantSeeNullsLol
# MAGIC     , *
# MAGIC from test
# MAGIC where reviewKey = '856c577b70a3dfb134a205bc0883306a0843282f129088d986e36894b5154cbd'


# MARKDOWN ********************

# # Debug

# CELL ********************

query = f"select * from table_changes('{source_path}', 0) limit 10"

spark.sql(query).show()

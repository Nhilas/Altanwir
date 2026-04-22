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
# META           "id": "21686009-3b8b-4dac-a144-e9cf00d8b9cc"
# META         },
# META         {
# META           "id": "c683a58d-3109-458d-8cb3-da991c23a31e"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "d3a3471f-f92e-b08d-4eb0-c1c6e96451b1",
# META       "known_warehouses": [
# META         {
# META           "id": "d3a3471f-f92e-b08d-4eb0-c1c6e96451b1",
# META           "type": "Datawarehouse"
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Parameters

# CELL ********************

environment = "dev"

lakehouse_name = "IGDBAnalytics" if environment == "prod" else "IGDBAnalytics_Dev"
audit_schema = "dev" if environment == "dev" else "steam"

# Constants
audit_server = '22jgi2dsfxnu5lmyn6ifyaro5e-wnxcbukzek4ejbckicpruy7sqq.datawarehouse.fabric.microsoft.com'
audit_database = 'IGDBAudit'

print(f"Environment = {environment}\n Lakehouse = {lakehouse_name}\n Audit = {audit_database}.{audit_schema}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Delta DDL

# MARKDOWN ********************

# ## Tables

# MARKDOWN ********************

# ### Steam Reviews

# CELL ********************

# Bronze Reviews
ddl_query = f"""
create table if not exists {lakehouse_name}.bronze.steamReviews (
    app_id BIGINT
    , recommendationid STRING
    , review_json STRING
    , insert_execution_id STRING
    , update_execution_id STRING
)
USING DELTA
CLUSTER BY (recommendationid)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
)
"""

spark.sql(ddl_query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Silver Reviews
ddl_query = f"""
create table if not exists {lakehouse_name}.silver.steamReviews (
    reviewKey STRING
    , gameKey STRING
    , eId STRING
    , recommendationId STRING
    , authorId STRING
    , language STRING
    , reviewRaw STRING
    , reviewCleaned STRING
    , votedUp BOOLEAN
    , votesUp INT
    , votesFunny INT
    , commentCount INT
    , reactionTypeCount INT
    , reactionCount INT
    , weightedVoteScore FLOAT
    , playtimeForever INT
    , playtimeAtReview INT
    , timestampCreated TIMESTAMP
    , timestampUpdated TIMESTAMP
    , refunded BOOLEAN
    , writtenDuringEarlyAccess BOOLEAN
    , reviewLength INT
    , wordCount INT
    , wordLengthRatio FLOAT
    , hasCredibleText BOOLEAN
    , uniqueWordCount INT
    , uniqueWordRatio FLOAT
    , asciiRatio FLOAT
    , isVaderEligible BOOLEAN
    , containsBugReport BOOLEAN
    , emotionalIntensity FLOAT
    , sentimentPositive FLOAT
    , sentimentCompound FLOAT
    , sentimentNeutral FLOAT
    , sentimentNegative FLOAT
    , insert_run_id STRING
    , update_run_id STRING
    , hash STRING
)
USING DELTA
CLUSTER BY (reviewKey)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.parquet.vorder.enabled' = 'true',
    'delta.enableChangeDataFeed' = 'true'
)
"""

spark.sql(ddl_query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Gold

# CELL ********************

# Gold Fact Reviews
ddl_query = f"""
create table if not exists {lakehouse_name}.gold.factReviews (
    reviewKey STRING
    , gameKey STRING
    , reviewCleaned STRING

    , votedUp BOOLEAN
    , votesUp INT
    , votesFunny INT
    , commentCount INT
    , reactionCount INT
    , communitySignal FLOAT

    , reviewLength INT
    , wordCount INT
    , wordLengthRatio FLOAT
    , uniqueWordRatio FLOAT
    , hasCredibleText BOOLEAN
    , lengthSignal FLOAT

    , playtimeAtReview INT
    , playtimeSignal FLOAT

    , isVaderEligible BOOLEAN
    , sentimentCompound FLOAT
    , sentimentSignal FLOAT
    , sentimentDirection INT

    , emotionalIntensity FLOAT
    , emotionalSignal FLOAT

    , voteDirection INT
    , reviewInfluenceScore FLOAT
    , steamWeightedVoteScore FLOAT

    , refunded BOOLEAN
    , writtenDuringEarlyAccess BOOLEAN
    , containsBugReport BOOLEAN

    , insert_run_id STRING
    , update_run_id STRING
    , hash STRING
)
USING DELTA
CLUSTER BY (gameKey)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.parquet.vorder.enabled' = 'true',
    'delta.enableChangeDataFeed' = 'true'
)
"""

spark.sql(ddl_query)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gold Fact Game Scores
ddl_query = f"""
create table if not exists {lakehouse_name}.gold.factGameScores (
    gameKey STRING

	, pctIGDBRating DOUBLE
	, smoothedIGDBRating DOUBLE
	, IGDBSourceCount BIGINT

	, totalReviews BIGINT
	, sentimentReviews BIGINT

	, avgPlaytimeAtReview DOUBLE
	, avgWordCount DOUBLE
	, avgEmotionalIntensity DOUBLE

	, pctPositiveSentiment DOUBLE
	, pctNeutralSentiment DOUBLE
	, pctNegativeSentiment DOUBLE

	, sentimentVoteAlignment DOUBLE
	, weightedSentiment DOUBLE
	, pctWeightedSentiment DOUBLE
	, weightedSentimentRating DOUBLE

	, weightedVote DOUBLE
	, pctWeightedVote DOUBLE
	, weightedVoteRating DOUBLE

	, pctVotedUp DOUBLE
	, voteRating DOUBLE

	, pctEarlyAccess DOUBLE
	, pctBugReports DOUBLE
	, pctRefunded DOUBLE

    , insert_run_id STRING
    , update_run_id STRING
    , hash STRING
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.parquet.vorder.enabled' = 'true',
    'delta.enableChangeDataFeed' = 'true'
)
"""

spark.sql(ddl_query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Warehouse DDL

# MARKDOWN ********************

# ## Audit Tables

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

# CELL ********************

ddl_query = f"""
create table if not exists {audit_schema}.loadOrchestratorReviews (
    app_id INT not null
    , load_type STRING
    , priority STRING
    , load_status STRING
)
"""

conn = connect_audit_wh()
db_cursor = conn.cursor()

try:
    db_cursor.execute(ddl_query)
    conn.commit()
except Exception as e:
    print(f"Failed to create table: {e}")
else:
    print(f"Successfully created table {audit_database}.{audit_schema}.loadOrchestratorReviews")
finally:
    db_cursor.close()
    conn.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ddl_query = f"""
create table if not exists {audit_schema}.loadControlReviews (
    app_id INT NOT NULL
    , run_id STRING
    , execution_id STRING
    , execution_type STRING
    , execution_start_time TIMESTAMP
    , execution_end_time TIMESTAMP
    , execution_duration INT
    , execution_status STRING
    , retrieved_reviews INT
    , first_retrieved_timestamp BIGINT
    , last_retrieved_timestamp BIGINT
    , last_retrieved_cursor STRING
    , output_path STRING
)
"""

conn = connect_audit_wh()
db_cursor = conn.cursor()

try:
    db_cursor.execute(ddl_query)
    conn.commit()
except Exception as e:
    print(f"Failed to create table: {e}")
else:
    print(f"Successfully created table {audit_database}.{audit_schema}.loadControlReviews")
finally:
    db_cursor.close()
    conn.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ddl_query = f"""
create table {audit_schema}.versionControl (
    table_name VARCHAR(50)
    , run_id VARCHAR(50)
    , change_type VARCHAR(30)
    , commit_version BIGINT
    , commit_timestamp DATETIME2(3)
    , rows_inserted BIGINT
    , rows_updated BIGINT
    , latest_source_version BIGINT
    , audit_notes VARCHAR(MAX)
)
"""

conn = connect_audit_wh()
db_cursor = conn.cursor()

try:
    db_cursor.execute(ddl_query)
    conn.commit()
except Exception as e:
    print(f"Failed to create table: {e}")
else:
    print(f"Successfully created table {audit_database}.{audit_schema}.versionControl")
finally:
    db_cursor.close()
    conn.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Audit Views

# CELL ********************

ddl_query = f"""
create view {audit_schema}.vw_loadReviews as
with ordered_executions as (
        select
            *
            , ROW_NUMBER() over ( PARTITION by app_id, execution_type order by execution_start_time desc ) as rn
        from steam.loadControlReviews 
    )

, successful_executions as (
        select
            app_id
            , execution_type
            , execution_id
            , first_retrieved_timestamp
            , last_retrieved_cursor
            , ROW_NUMBER() over ( PARTITION by app_id, execution_type order by execution_start_time desc ) as rn
        from steam.loadControlReviews
        where execution_status = 'success'
    )

select
    o.app_id
    , o.load_type
    , o.priority
    , case 
        when o.load_status = 'retry' then -1
        when coalesce(e.execution_id, s.execution_id) is not null and o.load_status <> 'completed' then 0
        when o.load_status = 'completed' then 99
        when o.priority = 'High' then 1
        when o.priority = 'Medium' then 2
        when o.priority = 'Low' then 3
    end as priority_order
    , o.load_status
    , e.run_id
    , e.execution_id
    , e.execution_start_time
    , e.execution_end_time
    , e.execution_duration
    , e.execution_status
    , e.retrieved_reviews
    , cast(dateadd(second, e.first_retrieved_timestamp, '1970-01-01') as datetime2) as first_review_on
    , cast(dateadd(second, e.last_retrieved_timestamp, '1970-01-01') as datetime2) as last_review_on
    , e.output_path
    , s.first_retrieved_timestamp as high_water_mark
    , case
        when o.load_status in ('empty', 'failed', 'retry') and (e.last_retrieved_cursor is null or e.last_retrieved_cursor = '*' )
            then s.last_retrieved_cursor
        when o.load_status in ('completed', 'pending')
            then '*'
        else coalesce(e.last_retrieved_cursor, '*')
    end as last_retrieved_cursor
from steam.loadOrchestratorReviews as o
left join ordered_executions as e
    on o.app_id = e.app_id
    and o.load_type = e.execution_type
    and e.rn = 1
left join successful_executions as s
    on o.app_id = s.app_id
    and o.load_type = s.execution_type
    and s.rn = 1
"""

conn = connect_audit_wh()
db_cursor = conn.cursor()

try:
    db_cursor.execute(ddl_query)
    conn.commit()
except Exception as e:
    print(f"Failed to create table: {e}")
else:
    print(f"Successfully created table {audit_database}.{audit_schema}.loadReviews")
finally:
    db_cursor.close()
    conn.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ddl_query = f"""
create view {audit_schema}.vw_loadReviewStats as
WITH cteExecutions AS (
    SELECT
        run_id
        , execution_start_time
        , execution_end_time
        , retrieved_reviews
        , app_id
        , execution_status
        , datediff(minute, execution_start_time, execution_end_time) as exec_duration
        , min(datediff(minute, execution_start_time, execution_end_time)) over(partition by run_id) as min_duration
        , percentile_cont(0.25) within group (order by datediff(minute, execution_start_time, execution_end_time)) over(partition by run_id) as [25th_duration]
        , percentile_cont(0.75) within group (order by datediff(minute, execution_start_time, execution_end_time)) over(partition by run_id) as [75th_duration]
        , max(datediff(minute, execution_start_time, execution_end_time)) over(partition by run_id) as max_duration
    FROM dev.loadControlReviews
)
SELECT
    run_id
    , min(execution_start_time) as run_start_time
    , max(execution_end_time) as run_end_time        
    , datediff(minute, min(execution_start_time), max(execution_end_time)) as run_duration
    
    , max(min_duration) as min_duration
    , max([25th_duration]) as [25th_duration]
    , cast(avg(cast(exec_duration as decimal(10,2))) as decimal(10,2)) as avg_duration
    , max([75th_duration]) as [75th_duration]    
    , max(max_duration) as max_duration
    
    , sum(retrieved_reviews) as retrieved_reviews
    , count(distinct app_id) as processed_games
    , sum(case when execution_status = 'empty' then 1 else 0 end) as empty_games
    , sum(case when execution_status not in ( 'success', 'empty', 'in-progress' ) then 1 else 0 end) as failed_executions        
FROM cteExecutions
GROUP BY 
    run_id
"""

conn = connect_audit_wh()
db_cursor = conn.cursor()

try:
    db_cursor.execute(ddl_query)
    conn.commit()
except Exception as e:
    print(f"Failed to create table: {e}")
else:
    print(f"Successfully created table {audit_database}.{audit_schema}.loadReviews")
finally:
    db_cursor.close()
    conn.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

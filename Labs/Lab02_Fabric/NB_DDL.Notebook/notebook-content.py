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

# ## Steam Reviews

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
    , weightedVoteScore FLOAT
    , playtimeForever INT
    , playtimeAtReview INT
    , timestampCreated TIMESTAMP
    , timestampUpdated TIMESTAMP
    , refunded BOOLEAN
    , writtenDuringEarlyAccess BOOLEAN
    , reviewLength INT
    , wordCount INT
    , vaderRatio FLOAT
    , isUsableForVader BOOLEAN
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
CLUSTER BY (eId)
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

# ## Audit

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
    conn.rollback()
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
    conn.rollback()
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
    conn.rollback()
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

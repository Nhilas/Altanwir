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

import notebookutils
import struct
import pyodbc

from delta.tables import DeltaTable
from pyspark.sql import functions as f
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Parameters

# CELL ********************

environment = "dev"
load_type = 'initial'

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

print(f"Bronze Steam Reviews ELT Initiated with load_type = '{load_type}'")
print(f"Environment = {environment}\n Lakehouse = {lakehouse_name}\n Audit = {audit_database}.{audit_schema}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Functions

# MARKDOWN ********************

# ### connect_audit_wh

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

# ### update_audit

# CELL ********************

def update_audit (executions_list=[]):
    if not executions_list:
        return

    placeholders = ", ".join(["?"] * len(executions_list))

    update_query = f"""
        update {audit_schema}.loadControlReviews
        set is_loaded = 1
            where execution_id in ({placeholders})
    """

    print(f"Updating {audit_schema}.loadControlReviews...")

    conn = connect_audit_wh()
    db_cursor = conn.cursor()

    try:
        db_cursor.execute(update_query, executions_list)
        conn.commit()
    except Exception as e:
        print(f"Failed to update audit: {e}")
        conn.rollback()
    else:
        print(f"Successfully marked {len(executions_list)} executions as loaded in {audit_schema}.loadControlReviews")
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

conn = connect_audit_wh()
db_cursor = conn.cursor()
select_query = f"""
    select
        app_id as app_id
        , lower(execution_id) as execution_id
        , output_path as output_path
        , retrieved_reviews
    from {audit_schema}.loadControlReviews
    where execution_type = '{load_type}'
        and is_loaded = 0
        and retrieved_reviews > 0
"""

db_cursor.execute(select_query)
games_list = db_cursor.fetchall()
db_cursor.close()
conn.close()

print(f"{len(games_list)} valid executions found in {audit_schema}.loadControlReviews. Query used:")
print(select_query)

if len(games_list) == 0:
    print(f"No execution_ids marked unloaded in {audit_schema}.loadControlReviews. Execution halted")
else:
    try:
        # create an intermediary dataframe to hold the app_id, used later before the merge
        schema_interm = StructType([
            StructField("app_id", LongType(), True),
            StructField("execution_id", StringType(), True),
            StructField("retrieved_reviews", IntegerType(), True)
        ])
        df_interm = spark.createDataFrame([[row.app_id, row.execution_id, row.retrieved_reviews] for row in games_list],schema=schema_interm)

        # find the execution_id with the most amount of saved reviews and sample the schema. this is to avoid the query optimizer from hanging while trying to infer the schema from many folders and files
        sample_execution = df_interm.sort(f.col("retrieved_reviews"),ascending=False).limit(1).select("execution_id").collect()[0].execution_id
        sample_path = [f"{abfs_root}{row.output_path}/" for row in games_list if row.execution_id == sample_execution]
        print(f"Sampling from reviews found at {sample_path}...")
        sampled_schema = spark.read.json(sample_path, multiLine=True).schema
        print(f"Schema set as:\n{print(sampled_schema)}")

        # create a dataframe of all reviews in all the paths returned in games_list. multiLine is necessary because the reviews are saved using indentation, so there are multiple lines per record
        paths_list = [f"{abfs_root}{row.output_path}/" for row in games_list]
        df_raw = spark.read.schema(sampled_schema).json(paths_list,multiLine=True)

        # prepare relevant fields for the source: format review as json, extract execution_id from the path name and extract the recommendationid
        df_processed = df_raw.withColumn("review_json", f.to_json(f.struct("*"))) \
            .withColumn("execution_id", f.element_at(f.split(f.input_file_name(), "/"),-2))
        df_reviews = df_processed.select("execution_id", "recommendationid", "review_json")
        df_joined = df_reviews.join(f.broadcast(df_interm), "execution_id", "inner" )

        # merge
        target_path = f"{lakehouse_name}.bronze.steamreviews"
        target_table = DeltaTable.forName(spark, target_path)

        version_before = target_table.history(1).collect()[0][0]
        print(f"Merge target: {target_path}. Executing merge...")

        target_table.alias("t").merge(
            df_joined.alias("s")
            , 't.recommendationid = s.recommendationid'
        ).whenMatchedUpdate(
            condition = "t.review_json != s.review_json",
            set= {
                "review_json": "s.review_json"
                , "update_execution_id": "s.execution_id"
            }
        ).whenNotMatchedInsert(
            values = {
                "app_id": "s.app_id"
                , "recommendationid": "s.recommendationid"
                , "review_json": "s.review_json"
                , "insert_execution_id": "s.execution_id"
            }   
        ).execute()

        version_after = target_table.history(1).collect()[0][0]

        if version_before == version_after:
            print("Merge executed. No rows affected")
        else:
            audit_row = target_table.history(1).collect()
            audit_dict = audit_row[0].operationMetrics
            print(f"Merge executed. Statistics: {audit_dict}")
    except Exception as e:
        print(f"Catastropic error: {e}")
    else:
        execution_id_list = [row.execution_id.upper() for row in games_list]
        update_audit(execution_id_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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
import json

from delta.tables import DeltaTable
from pyspark.sql import functions as f
from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Parameters

# PARAMETERS CELL ********************

environment = "dev"
load_type = 'targeted'   # valid options: "full", "reload", "incremental", "targeted"
run_id = 'dev_targeted_1'

# format this as a list of string app_ids. only used if load_type = 'targeted', otherwise it's ignored. only accepts app_ids. example: ['271590', '292030', '45770']
targeted_reload = ['271590', '292030', '45770']

update_batch_size = 250

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

target_path = f"{lakehouse_name}.bronze.steamreviews"
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

deduplicateBy = Window.partitionBy("app_id", "steamid").orderBy(f.col("timestamp_updated").desc())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Bronze Steam Reviews ELT Initiated with load_type = '{load_type}'")
print(f"Environment = {environment}\n Lakehouse = {lakehouse_name}\n Audit = {audit_database}.{audit_schema}")
print(f"Loading from Data Lake ( {abfs_root} ) into {target_path}")

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
    
    max_batch = len(executions_list)

    conn = connect_audit_wh()
    db_cursor = conn.cursor()

    try:
        for batch in range(0, max_batch, update_batch_size):
        
            batch_executions = executions_list[batch:batch+update_batch_size]
            placeholders = ", ".join(["?"] * len(batch_executions))

            update_query = f"""
                update {audit_schema}.loadControlReviews
                set is_loaded = 1
                    where execution_id in ({placeholders})
            """

            print(f"Updating {audit_schema}.loadControlReviews for batch {batch+1} of {max_batch//update_batch_size+1}...")

            db_cursor.execute(update_query, batch_executions)
            conn.commit()

            print(f"Batch updated successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Failed to update audit for batch {batch+1} of {max_batch//update_batch_size+1}: {e}")
        raise
    else:
        print(f"Successfully marked {max_batch} executions as loaded in {audit_schema}.loadControlReviews")
    finally:
        db_cursor.close()
        conn.close()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### insert_version

# CELL ********************

def insert_version(audit_row):
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
        , None
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

if load_type in ('full', 'reload'):
    where_clause = "where retrieved_reviews > 0"

elif load_type == 'incremental':
    where_clause = "where execution_type = 'incremental' and is_loaded = 0 and retrieved_reviews > 0"

elif load_type == 'targeted' and targeted_reload:
    sep = "', '"
    gameKey_predicate = f"'{sep.join(targeted_reload)}'"    
    where_clause = f"where app_id in ({gameKey_predicate}) and retrieved_reviews > 0"

else:
    print(f"Invalid load_type: {load_type}! Shutting down")
    notebookutils.notebook.exit("Wrong load_type")

conn = connect_audit_wh()
db_cursor = conn.cursor()
select_query = f"""
    select
        app_id as app_id
        , lower(execution_id) as execution_id
        , output_path as output_path
        , retrieved_reviews
    from {audit_schema}.loadControlReviews
    {where_clause}
"""

db_cursor.execute(select_query)
games_list = db_cursor.fetchall()
db_cursor.close()
conn.close()

print(f"{len(games_list)} valid executions found in {audit_schema}.loadControlReviews. Query used:")
print(select_query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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
        df_reviews = df_processed.selectExpr("execution_id", "recommendationid", "author.steamid as steamid", "timestamp_updated", "review_json")

        df_joined = df_reviews.join(f.broadcast(df_interm), "execution_id", "inner" )

        # deduplicate by app_id and steamid, keeping the most recent review based on the timestamp_updated field
        df_deduplicated = df_joined.withColumn("row_number", f.row_number().over(deduplicateBy)) \
            .filter(f.col("row_number") == 1) \
            .drop("row_number", "steamid", "timestamp_updated")

        # merge
        version_before = target_table.history(1).collect()[0][0]
        print(f"Merge target: {target_path}. Executing merge...")

        target_table.alias("t").merge(
            df_deduplicated.alias("s")
            , 't.recommendationid = s.recommendationid'
        ).whenMatchedUpdate(
            condition = "t.review_json != s.review_json",
            set= {
                "review_json": "s.review_json"
                , "update_execution_id": "s.execution_id"
                , "update_run_id": f"'{run_id}'"
            }
        ).whenNotMatchedInsert(
            values = {
                "app_id": "s.app_id"
                , "recommendationid": "s.recommendationid"
                , "review_json": "s.review_json"
                , "insert_execution_id": "s.execution_id"
                , "insert_run_id": f"'{run_id}'"
                , "update_run_id": "null"                
            }   
        ).execute()

        audit_row = target_table.history(1).collect()
        version_after = audit_row[0][0]

        if version_before == version_after:
            print("Merge executed. No rows affected")
        else:
            insert_version(audit_row=audit_row)
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

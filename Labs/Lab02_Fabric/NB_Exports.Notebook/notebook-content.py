# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "21686009-3b8b-4dac-a144-e9cf00d8b9cc",
# META       "default_lakehouse_name": "IGDBAnalytics",
# META       "default_lakehouse_workspace_id": "d1206eb3-2259-44b8-844a-409f1a63f284",
# META       "known_lakehouses": [
# META         {
# META           "id": "21686009-3b8b-4dac-a144-e9cf00d8b9cc"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Tables

# CELL ********************

from pyspark.sql import functions as f

# config
schemas_to_export = ["gold", "silver", "bronze"]
skip_tables = {
    "bronze.steamreviews",  # too big, skip
    "gold.factgamescores" # already loaded

}

base_path = "abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse"

for schema in schemas_to_export:
    tables = spark.sql(f"SHOW TABLES IN IGDBAnalytics.{schema}").collect()
    
    for row in tables:
        table_name = row.tableName
        full_name = f"{schema}.{table_name}"
        
        if full_name in skip_tables:
            print(f"⊘ SKIP: {full_name}")
            continue
        
        src = f"{base_path}/Tables/{schema}/{table_name}"
        dst = f"{base_path}/Files/exports/{schema}/{table_name}"
        
        try:
            print(f"→ {full_name}...")
            spark.read.format("delta").load(src) \
                .write.mode("overwrite").parquet(dst)
            print(f"✓ {full_name}")
        except Exception as e:
            print(f"✗ {full_name}: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Views

# CELL ********************

from pyspark.sql import functions as f

# config
schemas_to_export = ["gold", "silver", "bronze"]

base_path = "abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse"

for schema in schemas_to_export:
    views = spark.sql(f"SHOW VIEWS IN IGDBAnalytics.{schema}").collect()
    
    for row in views:
        view_name = row.viewName
        full_name = f"{schema}.{view_name}"
        dst = f"{base_path}/Files/exports/views/{schema}/{view_name}"
        
        try:
            print(f"→ VIEW {full_name}...")
            spark.sql(f"SELECT * FROM IGDBAnalytics.{schema}.{view_name}") \
                .write.mode("overwrite").parquet(dst)
            print(f"✓ VIEW {full_name}")
        except Exception as e:
            print(f"✗ VIEW {full_name}: {e}")

print("Done.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Audit

# CELL ********************

import struct
import pyodbc
import pandas as pd

audit_server = '22jgi2dsfxnu5lmyn6ifyaro5e-wnxcbukzek4ejbckicpruy7sqq.datawarehouse.fabric.microsoft.com'
audit_database = 'IGDBAudit'

def connect_audit_wh():

    # token formation
    token = notebookutils.credentials.getToken("pbi")
    token_bytes = token.encode("utf-16-le")
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)

    # connection string + connection
    conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={audit_server};DATABASE={audit_database};Encrypt=yes;TrustServerCertificate=no"
    conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})

    return conn

queries = {
    "loadControlReviews": "SELECT * FROM steam.loadControlReviews",
    "versionControl": "SELECT * FROM steam.versionControl", 
    "loadOrchestratorReviews": "SELECT * FROM steam.loadOrchestratorReviews",
}

base_path = "abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Files/exports/audit"

conn = connect_audit_wh()
for name, query in queries.items():
    try:
        print(f"→ {name}...")
        df = pd.read_sql(query, conn)
        # save to parquet via spark since pandas->parquet locally is annoying
        spark.createDataFrame(df).write.mode("overwrite").parquet(f"{base_path}/{name}")
        print(f"✓ {name}: {len(df)} rows")
    except Exception as e:
        print(f"✗ {name}: {e}")
conn.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


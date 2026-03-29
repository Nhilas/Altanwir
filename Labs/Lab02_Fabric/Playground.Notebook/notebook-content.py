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
# META           "id": "78248bb5-e968-4f48-9eba-14f179318123"
# META         },
# META         {
# META           "id": "21686009-3b8b-4dac-a144-e9cf00d8b9cc"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

# We use %%sql to write T-SQL logic inside a Spark Notebook
# This IS allowed to perform DML
spark.sql("DELETE FROM sample.reviews WHERE app_id = 10100")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- DESCRIBE HISTORY sample.reviews
# MAGIC 
# MAGIC -- describe detail sample.reviews
# MAGIC 
# MAGIC -- RESTORE table sample.reviews to version as of 1
# MAGIC 
# MAGIC -- select count(*) from sample.reviews
# MAGIC 
# MAGIC select *
# MAGIC from sample.reviews VERSION as of 0
# MAGIC --where app_id = 10100
# MAGIC limit 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- DESCRIBE history sample.reviews
# MAGIC 
# MAGIC /*
# MAGIC select count(*)
# MAGIC from sample.reviews version as of 5
# MAGIC where is_positive = false
# MAGIC */

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Query the table as it existed at Version 0 (before the delete)
# df_v0 = spark.read.format("delta").option("versionAsOf", 1).table("sample.reviews").where("app_id = 10100")
# display(df_v0)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# create a new column in sample.reviews

## spark.sql("ALTER TABLE sample.reviews ADD COLUMNS (is_positive BOOLEAN)")
## spark.sql("UPDATE sample.reviews set is_positive = false where review_score = -1")

# unfuck parquet files

# spark.sql("OPTIMIZE sample.reviews")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

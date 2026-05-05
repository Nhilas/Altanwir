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

# CELL ********************

from pyspark.sql import functions as f


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # SQL Raw

# MARKDOWN ********************

# ## Detail

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DESCRIBE detail IGDBAnalytics.bronze.steamreviews

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DESCRIBE detail IGDBAnalytics.silver.steamreviews

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DESCRIBE detail IGDBAnalytics.gold.factreviews

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DESCRIBE detail IGDBAnalytics.gold.factgamescores

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## History

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DESCRIBE HISTORY IGDBAnalytics.bronze.steamreviews

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DESCRIBE HISTORY IGDBAnalytics.silver.steamreviews

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DESCRIBE HISTORY IGDBAnalytics.gold.factreviews

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DESCRIBE HISTORY IGDBAnalytics.gold.factgamescores

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## CDF

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT
# MAGIC     'IGDBAnalytics.bronze.steamreviews' as table_name,
# MAGIC     _change_type,
# MAGIC     _commit_version,
# MAGIC     _commit_timestamp,
# MAGIC     COUNT(*) AS rows_affected
# MAGIC FROM table_changes('IGDBAnalytics.bronze.steamreviews', 0)
# MAGIC GROUP BY _change_type, _commit_version, _commit_timestamp
# MAGIC ORDER BY _commit_version DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT
# MAGIC     'IGDBAnalytics.silver.steamreviews' as table_name,
# MAGIC     _change_type,
# MAGIC     _commit_version,
# MAGIC     _commit_timestamp,
# MAGIC     COUNT(*) AS rows_affected
# MAGIC FROM table_changes('IGDBAnalytics.silver.steamreviews', 0)
# MAGIC GROUP BY _change_type, _commit_version, _commit_timestamp
# MAGIC ORDER BY _commit_version DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT
# MAGIC     'IGDBAnalytics.gold.factreviews' as table_name,
# MAGIC     _change_type,
# MAGIC     _commit_version,
# MAGIC     _commit_timestamp,
# MAGIC     COUNT(*) AS rows_affected
# MAGIC FROM table_changes('IGDBAnalytics.gold.factreviews', 0)
# MAGIC GROUP BY _change_type, _commit_version, _commit_timestamp
# MAGIC ORDER BY _commit_version DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Python Formatted

# MARKDOWN ********************

# ## Detail

# CELL ********************

details = spark.sql("DESCRIBE DETAIL IGDBAnalytics.bronze.steamreviews")
display(
    details.select(
        f.lit("bronze.steamreviews").alias("tableName"),
        "format", "partitionColumns", "clusteringColumns", "numFiles", 
        (f.round(f.col("sizeInBytes")/1000000000,2)).alias("sizeGB"),
        f.col("properties")["delta.enableChangeDataFeed"].alias("CDF"),
        f.col("properties")["delta.autoOptimize.autoCompact"].alias("autoCompact"),
        f.col("properties")["delta.autoOptimize.optimizeWrite"].alias("optimizeWrite")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

details = spark.sql("DESCRIBE DETAIL IGDBAnalytics.silver.steamreviews")
display(
    details.select(
        f.lit("silver.steamreviews").alias("tableName"),
        "format", "partitionColumns", "clusteringColumns", "numFiles", 
        (f.round(f.col("sizeInBytes")/1000000000,2)).alias("sizeGB"),
        f.col("properties")["delta.enableChangeDataFeed"].alias("CDF"),
        f.col("properties")["delta.autoOptimize.autoCompact"].alias("autoCompact"),
        f.col("properties")["delta.autoOptimize.optimizeWrite"].alias("optimizeWrite"),
        f.col("properties")["delta.parquet.vorder.enabled"].alias("vOrder")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

details = spark.sql("DESCRIBE DETAIL IGDBAnalytics.gold.factreviews")
display(
    details.select(
        f.lit("gold.factreviews").alias("tableName"),
        "format", "partitionColumns", "clusteringColumns", "numFiles", 
        (f.round(f.col("sizeInBytes")/1000000000,2)).alias("sizeGB"),
        f.col("properties")["delta.enableChangeDataFeed"].alias("CDF"),
        f.col("properties")["delta.autoOptimize.autoCompact"].alias("autoCompact"),
        f.col("properties")["delta.autoOptimize.optimizeWrite"].alias("optimizeWrite"),
        f.col("properties")["delta.parquet.vorder.enabled"].alias("vOrder")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

details = spark.sql("DESCRIBE DETAIL IGDBAnalytics.gold.factgamescores")
display(
    details.select(
        f.lit("gold.factgamescores").alias("tableName"),
        "format", "partitionColumns", "clusteringColumns", "numFiles", 
        (f.round(f.col("sizeInBytes")/1000000000,2)).alias("sizeGB"),
        f.col("properties")["delta.enableChangeDataFeed"].alias("CDF"),
        f.col("properties")["delta.autoOptimize.autoCompact"].alias("autoCompact"),
        f.col("properties")["delta.autoOptimize.optimizeWrite"].alias("optimizeWrite"),
        f.col("properties")["delta.parquet.vorder.enabled"].alias("vOrder")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## History

# CELL ********************


hist = spark.sql("DESCRIBE HISTORY IGDBAnalytics.bronze.steamreviews")
display(
    hist.select(
        f.lit("bronze.steamreviews").alias("tableName"),
        "version", "operation",
        f.col("operationMetrics")["numOutputRows"].alias("rows"),
        f.col("operationMetrics")["numTargetRowsInserted"].alias("inserted"),
        f.col("operationMetrics")["numTargetRowsUpdated"].alias("updated"),
        f.round((f.col("operationMetrics")["executionTimeMs"]/1000/60),2).alias("duration_minutes")
    ).orderBy(f.col("version").desc())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


hist = spark.sql("DESCRIBE HISTORY IGDBAnalytics.silver.steamreviews")
display(
    hist.select(
        f.lit("silver.steamreviews").alias("tableName"),
        "version", "operation",
        f.col("operationMetrics")["numOutputRows"].alias("rows"),
        f.col("operationMetrics")["numTargetRowsInserted"].alias("inserted"),
        f.col("operationMetrics")["numTargetRowsUpdated"].alias("updated"),
        f.round((f.col("operationMetrics")["executionTimeMs"]/1000/60),2).alias("duration_minutes")
    ).orderBy(f.col("version").desc())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


hist = spark.sql("DESCRIBE HISTORY IGDBAnalytics.gold.factreviews")
display(
    hist.select(
        f.lit("gold.factreviews").alias("tableName"),
        "version", "operation",
        f.col("operationMetrics")["numOutputRows"].alias("rows"),
        f.col("operationMetrics")["numTargetRowsInserted"].alias("inserted"),
        f.col("operationMetrics")["numTargetRowsUpdated"].alias("updated"),
        f.round((f.col("operationMetrics")["executionTimeMs"]/1000/60),2).alias("duration_minutes")
    ).orderBy(f.col("version").desc())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


hist = spark.sql("DESCRIBE HISTORY IGDBAnalytics.gold.factgamescores")
display(
    hist.select(
        f.lit("gold.factgamescores").alias("tableName"),
        "version", "operation",
        f.col("operationMetrics")["numOutputRows"].alias("rows"),
        f.col("operationMetrics")["numTargetRowsInserted"].alias("inserted"),
        f.col("operationMetrics")["numTargetRowsUpdated"].alias("updated"),
        f.round((f.col("operationMetrics")["executionTimeMs"]/1000/60),2).alias("duration_minutes")
    ).orderBy(f.col("version").desc())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

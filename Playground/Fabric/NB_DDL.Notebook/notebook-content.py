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

# # Table DDL
# 
# ## Control

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC use control;
# MAGIC 
# MAGIC drop table if exists loadOrchestratorReviews;
# MAGIC 
# MAGIC create table loadOrchestratorReviews (
# MAGIC     app_id INT not null
# MAGIC     , priority VARCHAR(10)
# MAGIC     , schedule varchar(10)
# MAGIC     , load_status varchar(10)
# MAGIC     , most_recent_execution int
# MAGIC );
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC USE control;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS loadControlReviews;
# MAGIC 
# MAGIC CREATE TABLE loadControlReviews (
# MAGIC     execution_id STRING,
# MAGIC     app_id INT NOT NULL,
# MAGIC     execution_start_time TIMESTAMP,
# MAGIC     execution_duration INT,
# MAGIC     execution_type STRING,
# MAGIC     execution_status STRING,
# MAGIC     retrieved_reviews INT,
# MAGIC     last_retrieved_timestamp BIGINT,
# MAGIC     last_retrieved_cursor STRING,
# MAGIC     output_path STRING
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- select * from control.loadOrchestratorReviews
# MAGIC select * from control.loadControlReviews

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

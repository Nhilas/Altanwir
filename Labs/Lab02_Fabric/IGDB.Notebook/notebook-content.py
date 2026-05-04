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

# test out igdb api connection

import requests
    # this is a library that specializes in http requests, among other things

igdb_client_id = ""  # set before running
igdb_token     = ""  # set before running

headers = {
    'Client-ID': igdb_client_id
    , 'Authorization': f'Bearer {igdb_token}'
}
    # credentials injected at runtime — do not hardcode here

query = 'fields name, total_rating; limit 10;'
    # this is written in a language called apicalypse

response = requests.post("https://api.igdb.com/v4/games",headers=headers,data=query)
    # this sends a POST request to the igdb api and saves it in a variable called response
    # the result of this request is a json object

df_api = spark.createDataFrame(response.json())
    # Create a DataFrame from the API response
    # this is kind of like a table, if a table was an object that temporarily lives in memory

display(df_api) 
    # Display the DataFrame in a tabular format
# df_api.printSchema() 
    # Print the schema of the DataFrame

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# create a temp view to run sql queries on
# the view lives only in the current spark session. you can create a global view using createGlobalTempView instead, but it will be shared across all sessions

df_api.createOrReplaceTempView("tGames")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("select * from tGames").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# load data from a csv file, then play with some transformation logic. use this to practice typical transform operations
# then compare the run time between transformations and transformations plus saving the data into a table

# saving the path in this variable to work with it in vs code. swap the variable out when in fabric

abfssPath = "abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Files/Bronze/sampleReviews.csv"
oneLakePath = "Files/Bronze/sampleReviews.csv"

df_csv = spark.read.options(header='True',inferSchema='True').csv(abfssPath)

# display(df_csv)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# this is a side-quest to observe data shuffling by forcing a wide transformation (i.e. a transformation that requires nodes to exchange data between them aka shuffling)

df_grouped = df_csv.groupBy("app_id").count().show(truncate=True)

# to see the DAG, click on the description of the job that took the longest > stage that took the longest

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# transformation playground

from pyspark.sql.functions import col, current_timestamp

df_csvTransformed = df_csv.withColumn("review_votes",col("review_votes").cast("integer")) \
    .withColumn("isPositive",col("review_score") == 1) \
    .withColumn("isVoted",col("review_votes") == 1) \
    .withColumn("processedAt",current_timestamp())

df_csvTransformed.show(n=20,truncate=True)
# df_csvTransformed.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# save the csv table

df_csvTransformed.write.mode("overwrite").saveAsTable("sample.Reviews")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("select * from sample.reviews limit 10").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import requests
    # this is a library that specializes in http requests, among other things

headers = {
    'Client-ID': 'IGDB_CLIENT_ID_REDACTED'
    , 'Authorization': 'Bearer IGDB_BEARER_TOKEN_REDACTED'
}
    # my credentials. usually you'd use variables here to keep them a secret

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

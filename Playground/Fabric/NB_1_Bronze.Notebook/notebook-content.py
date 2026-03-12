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

# ## Configuration and Parameters

# CELL ********************

import requests
from pyspark.sql import functions as f

# CELL ********************

# Parameters
## set as FULL, INCREMENTAL, or NONE
run_mode = "FULL"
maxLimit = 500 # acts as the limit for partial / incremental loads

# Yes bad practice I'm aware
headers = {
    'Client-ID': 'IGDB_CLIENT_ID_REDACTED'
    , 'Authorization': 'Bearer IGDB_BEARER_TOKEN_REDACTED'
}

baseURL = 'https://api.igdb.com/v4/'

# CELL ********************

# set this session to allow autoMerge, meaning new columns will be added to the bronze tables if the source provides them
spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

# a list of dictionaries used in the main request loop. contains:
## endpoint: appended to the igdb api url in the request function
## target_table: used in the merge into statement
## exclude: used to exclude deprecated columns as per the igdb endpoint guide. null means we're taking every column
table_configs = [
    {
        "endpoint": "games",
        "target_table": "bronze.games",
        "exclude": ["category", "collection", "follows", "status"]
    },
    {
        "endpoint": "genres",
        "target_table": "bronze.genres",
        "exclude": []
    },
    {
        "endpoint": "themes",
        "target_table": "bronze.themes",
        "exclude": []
    },
    {
        "endpoint": "platforms",
        "target_table": "bronze.platforms",
        "exclude": ["category"]
    },
    {
        "endpoint": "platform_types",
        "target_table": "bronze.platform_types",
        "exclude": []
    }    
]

# MARKDOWN ********************

# ### Functions

# CELL ********************

# API Request function
def requestData(endpoint, run_mode, incremental_limit, exclude = []):
    
    tableURL  = f"{baseURL}{endpoint}"
    print(f'API URL: {tableURL}')

    count_response = requests.post(url=f"{tableURL}/count", headers=headers)

    records_to_fetch = 0
    if count_response.status_code != 200:
        print(f'Error getting count: {count_response.status_code} - {count_response.text}')
        # We'll fetch 0 records if we can't get a count
    else:
        api_count = count_response.json()['count']
        
        # If we're loading everything then we'll loop until the total # of records is reached
        ## Otherwise we load either the available # of records, or maxLimit, whichever is smallest
        if run_mode == 'FULL':
            records_to_fetch = api_count
        else:
            records_to_fetch = min(api_count, incremental_limit)

    if records_to_fetch == 0:
        print(f"Skipping {endpoint}, 0 records to fetch.")
        return []

    resultList = []

    for i in range(0, records_to_fetch, 500):     # igdb limits to 500 results per request and 4 requests a second

        print(f'Pulling records {i} to {i+500}...')

        query = (                                                   # this uses apicalypse
            f'fields *;'                                            ## select *
            + (f'exclude {",".join(exclude)};' if exclude else '')  ## remove these columns from the select, if exclude exists
            + 'limit 500;'                                          ## 500 records at a time
            + f'offset {i};'                                        ## without the previous 500 records
        )

        response = requests.post(url=tableURL, headers=headers, data= query)

        if response.status_code != 200:
            print(f'Error: {response.status_code} - {response.text}')
            break

        resultList.extend(response.json())

    print(f'Processed source for {endpoint}; found {len(resultList)} records')
    return resultList

# MARKDOWN ********************

# ## Table Loop

# CELL ********************

# this is just a temporary cell to show me the record counts in the source api
for current_config in table_configs:

    tableURL = f"{baseURL}{current_config['endpoint']}/count"

    response = requests.post(url=tableURL, headers=headers)

    if response.status_code != 200:
        print(f'Error: {response.status_code} - {response.text}')
    else:
        totalCount = response.json()
        print(f"{current_config['endpoint']}: {totalCount['count']}")

# CELL ********************

if run_mode == 'NONE':
    print("run_mode is 'NONE'. Skipping all processing.")
else:
    for current_config in table_configs:
        print(f"Processing {current_config['endpoint']}...")

        # returns the table with all columns except the excluded ones. the response is a json
        ## it will check if there are more records than the limit. if no, then it will only request until it gets the records
        df_raw = spark.createDataFrame(
            requestData(
                current_config['endpoint'],
                run_mode,
                maxLimit, 
                current_config['exclude']
            )
        )

        if df_raw.rdd.isEmpty():
            print(f"No data returned for {current_config['endpoint']}. Skipping to next table.")
            continue

        # create the hash inside the dataframe
        ## get the list of all columns inside df_raw. then exclude 'id'
        columns_to_hash = list(df_raw.columns)
        columns_to_hash.remove("id")

        ## hash the concatenation of all columns. concat_ws ignores/skips nulls
        df_hashed = df_raw.withColumn("hash", f.md5(f.concat_ws(",", *[f.col(c) for c in columns_to_hash])))

        # create a temporary view to use in the merge as a source
        df_hashed.createOrReplaceTempView("hashedView")    

        if run_mode == 'FULL':
            # yolo
            print(f"Recreating {current_config['target_table']}...")
            spark.sql(f"drop table if exists {current_config['target_table']}")
            df_hashed.write.mode("overwrite").saveAsTable(f"{current_config['target_table']}")
            print(f"{current_config['target_table']} recreated successfully!")
        else:
            # write and execute the merge
            print(f"Merging {current_config['target_table']}...")
            merge_sql = f"""
            merge into {current_config['target_table']} as t
            using hashedView as s
                on t.id = s.id
            when matched and coalesce(t.hash, '') != coalesce(s.hash, '') then
                update set *
            when not matched then
                insert *
            """
            df_merge_results = spark.sql(merge_sql)

            # Get the first (and only) row from the results DataFrame
            results_row = df_merge_results.first()

            # Access the values from the row by their column name
            inserted_rows = results_row['num_inserted_rows']
            updated_rows = results_row['num_updated_rows']

            print(f"Merge ended for {current_config['target_table']}. Inserted: {inserted_rows}, Updated: {updated_rows}")

# MARKDOWN ********************

# # Temporary ghetto table formation area

# CELL ********************

# endpoint = 'genres'
# exclude = []
# # exclude : ["category", "collection", "follows", "status"]

# # returns the table with all columns except the excluded ones. the response is a json
# df_raw = spark.createDataFrame(requestData(endpoint, maxLimit, exclude))

# # create the hash inside the dataframe
# ## get the list of all columns inside df_raw. then exclude 'id'
# columns_to_hash = list(df_raw.columns)
# columns_to_hash.remove("id")

# ## hash the concatenation of all columns. concat_ws ignores/skips nulls
# df_hashed = df_raw.withColumn("hash", f.md5(f.concat_ws(",", *[f.col(c) for c in columns_to_hash])))

# df_hashed.show(n=5)

# CELL ********************

# spark.sql("drop table if exists bronze.games")

# df_hashed.write.mode("overwrite").saveAsTable("bronze.games")

# CELL ********************

# %%sql

# select * from bronze.games limit 10

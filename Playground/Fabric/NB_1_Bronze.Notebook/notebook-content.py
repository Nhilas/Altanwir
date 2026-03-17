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
import json
import time

from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Parameters

## run_Mode
### FULL - drops the target table and recreates it entirely
### INCREMENTAL - upsert via change detection
### NONE - debugging purposes only
run_mode = "FULL"

## maxLimit
### acts as a hard limit to the # of records. useful for testing. 
### set to 0 to extract everything
maxLimit = 0

# a list containing the tables to be loaded
table_load = ["games", "genres", "themes", "platforms", "platform_types", "external_games", "external_game_sources"]

# Yes bad practice I'm aware
headers = {
    'Client-ID': 'IGDB_CLIENT_ID_REDACTED'
    , 'Authorization': 'Bearer IGDB_BEARER_TOKEN_REDACTED'
    , 'Content-Type': 'text/plain'
}

baseURL = 'https://api.igdb.com/v4/'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# set this session to allow autoMerge, meaning new columns will be added to the bronze tables if the source provides them
spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

# a list of dictionaries used in the main request loop.
## endpoint: appended to the igdb api url in the request function
## target_table: used in the merge into statement
## fields: used if specific columns must be extracted (or it's more efficient to pull specific columns than * exclude)
## exclude: used to exclude deprecated columns as per the igdb endpoint guide. null means we're taking every column
table_configs = [
    {
        "endpoint": "games",
        "target_table": "bronze.games",
        # "fields" : [
        #     "age_ratings", "aggregated_rating", "aggregated_rating_count", "alternative_names", "dlcs", 
        #     "expanded_games", "expansions", "external_games", "first_release_date", "game_modes", 
        #     "game_status", "game_type", "genres", "involved_companies", "name", 
        #     "parent_game", "platforms", "rating", "rating_count","standalone_expansions", 
        #     "storyline", "summary", "themes", "total_rating", "total_rating_count"
        # ],
        "fields": [],
        # "exclude": []
        "exclude": ["category", "collection", "follows", "status"]
    },
    {
        "endpoint": "genres",
        "target_table": "bronze.genres",
        "fields": [],
        "exclude": []
    },
    {
        "endpoint": "themes",
        "target_table": "bronze.themes",
        "fields": [],
        "exclude": []
    },
    {
        "endpoint": "platforms",
        "target_table": "bronze.platforms",
        "fields": [],
        "exclude": ["category"]
    },
    {
        "endpoint": "platform_types",
        "target_table": "bronze.platform_types",
        "fields": [],
        "exclude": []
    },
    {
        "endpoint": "external_games",
        "target_table": "bronze.external_games",
        "fields": [],
        "exclude": ["category", "media"]
    },
    {
        "endpoint": "external_game_sources",
        "target_table": "bronze.external_game_sources",
        "fields": ["id", "name", "created_at", "updated_at"],
        "exclude": []
    }            
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

response = requests.post('https://api.igdb.com/v4/external_game_sources', headers=headers, data='fields checksum,created_at,name,updated_at;')
print ("response: %s" % str(response.json()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Functions

# CELL ********************

# API Request function
def requestData(endpoint, limit, fields = [], exclude = []):
    
    print(f'\tSTART: Processing source data')

    tableURL  = f"{baseURL}{endpoint}"
    print(f'\t\tAPI: {tableURL}')

    # Initialize the Session Context Manager
    ## This automatically closes the connection when the indented block ends
    with requests.Session() as session:    

        # Every request made with 'session.' will now have these headers automatically
        session.headers.update(headers)

        count_response = session.post(url=f"{tableURL}/count", headers=headers)

        records_to_fetch = 0
        if count_response.status_code != 200:
            print(f'\t\tERROR: {count_response.status_code} - {count_response.text} \n while getting count')
            # We'll fetch 0 records if we can't get a count
        else:
            api_count = count_response.json()['count']
            
            # If we're loading everything then we'll loop until the total # of records is reached
            ## Otherwise we load either the available # of records, or maxLimit, whichever is smallest
            if limit == 0:
                records_to_fetch = api_count
            else:
                records_to_fetch = min(api_count, limit)

        if records_to_fetch == 0:
            print(f"\t\tSKIP: {endpoint}, 0 records to fetch.")
            return []

        print(f'\t\tSTART: Loading {records_to_fetch} total records. Found {api_count} records.')

        resultList = []

        for i in range(0, records_to_fetch, 500):     # igdb limits to 500 results per request and 4 requests a second

            query = (                                                               # this uses apicalypse
                (f'fields {",".join(fields)};' if fields else 'fields *;')          ## select *, or select [fields]
                + (f'exclude {",".join(exclude)};' if exclude else '')              ## remove these columns from the select, if exclude exists
                + 'limit 500;'                                                      ## 500 records at a time
                + f'offset {i};'                                                    ## without the previous 500 records
            )

            print(f'\t\tQuery sent: {query}')

            response = session.post(url=tableURL, data=query)            

            if response.status_code != 200:
                print(f'\t\ERROR: {response.status_code} - {response.text}')
                break

            resultList.extend(response.json())

            print(f'\t\t\tLoaded {endpoint} records {i} to {i+500}...')

            # IGDB allows 4 requests per second (1 request every 0.25s).
            # We sleep 0.26s to be safe. This doesn't run that fast, but best to be sure
            time.sleep(0.26)            

    print(f'\tEND: source processing; Loaded {len(resultList)} total {endpoint} records')
    return resultList

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Table Loop

# CELL ********************

if run_mode == 'NONE':
    print("SKIP: run_mode is 'NONE'. Skipping all processing.")
else:
    print(f"START: Processing the following endpoints: {table_load}...")
    print(f'PARAMETERS:\n - run_mode = {run_mode}')
    print(f' - maxLimit = {maxLimit}') if maxLimit != 0  else ' - no maxLimit set, loading everything'
    
    for current_config in table_configs:

        # continue is an unintuitive word for what this command does: if the endpoint isn't in the table_load, skip it
        current_endpoint = current_config['endpoint']
        if current_endpoint not in table_load:
            continue        
        
        print(f"START: Processing {current_config['endpoint']}")

        # extract data from igdb api
        raw_data = requestData(
                current_config['endpoint'],
                maxLimit, 
                current_config['fields'],
                current_config['exclude']
            )

        if not raw_data:
            print(f"\tSKIP: No data returned for {current_config['endpoint']}.")
            continue

        # Use read.json to scan ALL rows and infer the complete schema (merging keys from sparse data)
        df_raw = spark.read.json(spark.sparkContext.parallelize([json.dumps(r) for r in raw_data]))        

        # Convert ArrayType columns to string to avoid Fabric limitations with complex types
        for field in df_raw.schema.fields:
            if isinstance(field.dataType, ArrayType):
                df_raw = df_raw.withColumn(field.name, f.col(field.name).cast("string"))

        # create the hash inside the dataframe
        ## get the list of all columns inside df_raw. then exclude 'id'
        columns_to_hash = list(df_raw.columns)
        columns_to_hash.remove("id")

        ## hash the concatenation of all columns. concat_ws ignores/skips nulls
        df_hashed = df_raw.withColumn("hash", f.md5(f.concat_ws(",", *[f.col(c) for c in columns_to_hash])))

        print(f'\tSTART: SQL Processing for {current_config["target_table"]}')

        # create a temporary view to use in the merge as a source
        df_hashed.createOrReplaceTempView("hashedView")    

        if run_mode == 'FULL':
            # yolo
            print(f"\t\tSTART: Recreating {current_config['target_table']}...")

            spark.sql(f"drop table if exists {current_config['target_table']}")
            df_hashed.write.mode("overwrite").saveAsTable(f"{current_config['target_table']}")

            print(f"\t\tEND: {current_config['target_table']} recreated successfully!")
        else:
            # write and execute the merge
            print(f"\t\tSTART: Merging {current_config['target_table']}...")
            
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

            print(f"\t\tEND: Merge ended for {current_config['target_table']}. Inserted: {inserted_rows}, Updated: {updated_rows}")

        print(f'\tEND: Ended SQL Processing for {current_config["target_table"]}')
        print(f"\END: Processed {current_config['endpoint']}!")
    print(f"END: Processed endpoints: {table_load}!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

import requests
import json
import time
import datetime
import uuid
import notebookutils
import sys

from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, LongType
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type

# CELL ********************

path_orchestrator = 'abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Tables/control/loadorchestratorreviews'
path_control = 'abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Tables/control/loadcontrolreviews'

NAMESPACE_ALTANWIR = uuid.UUID('f81d4fae-7dec-11d0-a765-00a0c91e6bf6')

game_limit = 5
cursor_limit = 10
retry_limit = 3
wait_config = {
    "multiplier": 1
    , "min": 2
    , "max": 60
}

# MARKDOWN ********************

# # Original Tracer Bullet Script
# 
# Don't touch it, it's sacred

# CELL ********************

import requests
import time # We need to sleep between requests so Steam doesn't get mad

app_id = "105600" # Terraria
base_url = f"https://store.steampowered.com/appreviews/{app_id}?json=1"

# Steam uses '*' to mean "give me the very first page"
params = {
    "filter": "recent",
    "num_per_page": 100,
    "cursor": "*" 
}

my_tracer_reviews = []
max_pages = 3 # Hard limit so we don't sit here all day

for page in range(max_pages):
    print(f"Fetching page {page + 1}...")
    response = requests.get(base_url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        
        # Grab the reviews array from the response
        reviews = data.get("reviews", [])
        my_tracer_reviews.extend(reviews)
        
        # Grab the cursor for the next page
        next_cursor = data.get("cursor")
        print(f" -> Grabbed {len(reviews)} reviews. Next cursor is: {next_cursor[:10]}...")
        
        # If no cursor is returned, or we got 0 reviews, we hit the end!
        if not next_cursor or len(reviews) == 0:
            print("No more reviews to fetch.")
            break
            
        # Update our parameters dictionary with the new cursor for the next loop iteration
        params["cursor"] = next_cursor
        
        # Sleep for 1.5 seconds to respect the API rate limits
        time.sleep(1.5)
    else:
        print(f"Failed! Status Code: {response.status_code}")
        break

print(f"\nSuccess! We collected {len(my_tracer_reviews)} reviews for our tracer bullet.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Ghetto
# 
# A dumpster of ideas, to be cleaned out later. The workspace / scratchpad

# CELL ********************

# %%sql

# use control;

# drop table if exists loadOrchestratorReviews;

# create table loadOrchestratorReviews (
#     app_id INT not null
#     , priority STRING
#     , schedule STRING
#     , load_type STRING
#     , load_status STRING
#     , last_execution_id STRING
#     , last_execution_time TIMESTAMP
# );



# CELL ********************

# %%sql

# USE control;

# DROP TABLE IF EXISTS loadControlReviews;

# CREATE TABLE loadControlReviews (
#     execution_id STRING,
#     app_id INT NOT NULL,
#     execution_start_time TIMESTAMP,
#     execution_duration INT,
#     execution_type STRING,
#     execution_status STRING,
#     retrieved_reviews INT,
#     last_retrieved_timestamp TIMESTAMP,
#     last_retrieved_cursor STRING,
#     output_path STRING
# );

# MARKDOWN ********************

# ## Insert Test Values
# 
# Taste cases covered:
# - A game with 2 executions, to test ranking
#   - 39500: Gothic 3
# - A game marked as skip, to test it gets ignored
#   - 48231: Might & Magic: Heroes VI - Pirates of the Savage Sea Adventure Pack
# - A game with no executions, to test if it gets initialised
#   - 323190: Frostpunk

# CELL ********************

# # spark.sql("use control")

# spark.sql("truncate table control.loadorchestratorreviews")

# spark.sql("""insert into control.loadorchestratorreviews (app_id, load_type, load_status)
#         values ( 39500, 'initial', 'pending'),
#           ( 48231, 'initial', 'skip'),
#           ( 323190, 'initial', 'pending')""")

# # spark.sql("truncate table control.loadcontrolreviews")

# # spark.sql("""insert into control.loadcontrolreviews (execution_id, app_id, execution_start_time, last_retrieved_cursor)
# #           values (1, 39500, '2026-03-10 10:00:01.123', 'cursor1'),
# #             (2, 39500, '2026-03-10 10:05:01.223', 'cursor2'),
# #             (1, 48231, '2026-03-10 10:00:01.771', 'cursor1')""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Memory Trials

# CELL ********************

def memoryTrial():
    # Get memory per executor
    executor_mem = spark.conf.get("spark.executor.memory")

    # Get number of executors
    num_executors = int(spark.conf.get("spark.executor.instances"))

    # Get memory for the driver
    driver_mem = spark.conf.get("spark.driver.memory")

    print(f"MEMORY TRIAL ========================== Driver Memory: {driver_mem} ========================== MEMORY TRIAL")
    print(f"MEMORY TRIAL ========================== Executor Memory: {executor_mem} ========================== MEMORY TRIAL")
    print(f"MEMORY TRIAL ========================== Number of Executors: {num_executors} ========================== MEMORY TRIAL")

# Note: This doesn't tell you "free" memory, but it gives you the configured limits to work against.

# CELL ********************

# Initialise run
execution_type = 'initial'

df_orchestrator = spark.read.format("delta").load(path_orchestrator) \
    .select("app_id") \
    .limit(game_limit) \
    .sort(f.asc("last_execution_time")) \
    .filter(f.col("load_status").isin("pending", "in-progress"))
games_row = df_orchestrator.collect()
games_list = [row['app_id'] for row in games_row]

schema = StructType([
    StructField("app_id", LongType(), True)
    , StructField("recommendationid", StringType(), True)
    , StructField("review_json", StringType(), True)
])

data_list = []

print(f"Processing up to {cursor_limit*100} reviews for each of the following games: {games_list}")

for game in games_list:
    # Initialise loop and the audit dictionary
    execution_start_time = datetime.datetime.now()
    execution_id_base = str(game) + str(execution_start_time)
    execution_id = uuid.uuid5(NAMESPACE_ALTANWIR,execution_id_base)
    audit = {
        "app_id": game
        , "execution_id": str(execution_id)
        , "execution_start_time": execution_start_time
        , "execution_duration": 0
        , "execution_type": execution_type
        , "execution_status": 'in-progress'
        , "load_status": 'in-progress'
        , "retrieved_reviews": None
        , "last_retrieved_timestamp": None
        , "last_retrieved_cursor": None
        , "output_path": None
    }

    reviews_list = []

    batch = 1

    print(f"{execution_start_time}: Start processing app_id {game}. Assigned id {execution_id}.")

    audit["last_retrieved_cursor"] = checkControl(app_id=game)

    try:
        with requests.Session() as session:
            print(f"Session opened for app_id {audit['app_id']}")

            while (batch <= cursor_limit):
                print(f"Start batch {batch} of {cursor_limit}...")

                data = requestSteamReviews(
                        app_id=audit["app_id"]
                        , last_cursor=audit["last_retrieved_cursor"]
                        , session_instance=session
                    )
                print(f"END requestSteamReviews for appId {audit['app_id']}, cursor {audit['last_retrieved_cursor']}")

                reviews = data.get("reviews", [])
                audit["last_retrieved_cursor"] = data.get("cursor", [])

                if not audit["last_retrieved_cursor"] or len(reviews) == 0:
                    audit["load_status"] = "complete"
                    print(f"Reached end of reviews after {batch} batches for app_id {audit['app_id']}. load_type {audit['execution_type']} is now Complete.")
                    break    

                reviews_list.extend(reviews)

                size_in_bytes = sys.getsizeof(reviews_list)

                print(f"MEMORY TRIAL ========================== Size in MB for {len(reviews_list)} reviews = {size_in_bytes/8} ========================== MEMORY TRIAL")
                
                print(f"Processed batch {batch} of {cursor_limit}!")

                batch += 1
                time.sleep(1)

            audit["execution_status"] = "success"
            print(f"Reached end of batches for app_id {audit['app_id']}")
                
    except Exception as e:
        audit.update(
            {"execution_status": f"Failed: {str(e)}"
            , "load_status": "failed"}
        )
        print(f"Failed at app_id {audit['app_id']}, batch {batch}, execution {audit['execution_id']} with: {audit['execution_status']}")

    finally:
        if not reviews_list:
            audit.update(
                {"execution_status": "empty"
                , "load_status": "empty"}
            )    
        else:
            json_data = json.dumps(reviews_list, indent=4)
            date_str = audit["execution_start_time"].strftime("%Y%m%d") # simple formatter to yyyymmdd
            file_path = f"Files/Steam/Reviews/{audit['execution_type']}/{audit['app_id']}/{date_str}/{audit['execution_id']}.json"

            audit.update(
                {"retrieved_reviews": len(reviews_list)
                , "last_retrieved_timestamp": reviews_list[-1]["timestamp_created"]
                , "output_path": file_path}
            )

        duration = datetime.datetime.now() - audit["execution_start_time"]
        audit["execution_duration"] = duration.total_seconds()
            
        print(f"Audit state saved. {audit}")

    data_to_load = [(game, review['recommendationid'], json.dumps(review)) for review in reviews_list]
    size_in_bytes_df_list = sys.getsizeof(data_to_load)

    print(f"MEMORY TRIAL ========================== Size in MB for 'data_to_load' = {size_in_bytes_df_list/8} ========================== MEMORY TRIAL")

    data_list.extend(data_to_load)

    print(f"End processing execution {audit['execution_id']} for game {game}")

print(f"Processing complete for games {games_list}")

memoryTrial()

df_reviews = spark.createDataFrame(data=data_list, schema=schema)
# todo cache

memoryTrial()


# CELL ********************

schema = StructType([
    StructField("app_id", LongType(), True)
    , StructField("recommendationid", StringType(), True)
    , StructField("review_json", StringType(), True)
])
memoryTrial()

df_reviews = spark.createDataFrame(data=data_list, schema=schema)

df_reviews.show(n=5, truncate=False)

memoryTrial()

# CELL ********************

memoryTrial()

df_reviews.cache()

memoryTrial()

df_reviews.count()

# CELL ********************

df_reviews.unpersist()

# MARKDOWN ********************

# ## Audit Warehouse Implementation

# CELL ********************

import pyodbc
import struct
import notebookutils

server = '22jgi2dsfxnu5lmyn6ifyaro5e-wnxcbukzek4ejbckicpruy7sqq.datawarehouse.fabric.microsoft.com'
database = 'IGDBAudit'

token = notebookutils.credentials.getToken("pbi")
token_bytes = token.encode("utf-16-le")
token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)

# --- 3. Connection String ---
conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};Encrypt=yes;TrustServerCertificate=no"

print(token)
print(token_bytes)
print(token_struct)
print(conn_str)

try:
    conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
    cursor = conn.cursor()
    
    print("Successfully connected to Warehouse!")
    
    # --- 5. Example Execution ---
    # cursor.execute("UPDATE Orchestration SET load_status = 'in-progress' WHERE app_id = ?", (570,))
    # conn.commit()
    
except Exception as e:
    print(f"Connection failed: {e}")
finally:
    if 'conn' in locals():
        conn.close()


# MARKDOWN ********************

# # Ghetto Deluxe
# 
# I'm actually trying here

# MARKDOWN ********************

# ## Attempt 2: Audit Dictionary

# CELL ********************

import requests
import json
import time
import datetime
import uuid
import notebookutils

from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type

# MARKDOWN ********************

# ### Parameters

# CELL ********************

path_orchestrator = 'abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Tables/control/loadorchestratorreviews'
path_control = 'abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Tables/control/loadcontrolreviews'

NAMESPACE_ALTANWIR = uuid.UUID('f81d4fae-7dec-11d0-a765-00a0c91e6bf6')

game_limit = 5
cursor_limit = 100
retry_limit = 3
wait_config = {
    "multiplier": 1
    , "min": 2
    , "max": 60
}

# MARKDOWN ********************

# ### Functions

# CELL ********************

# the retry_state parameter is an object of tenacity's RetryCallState class
# it contains all the information about the current state of the retries, such as attempt number and wait time
def log_retry_state(retry_state):
    attempt = retry_state.attempt_number
    wait_time = retry_state.next_action.sleep
    print(f"Rate limit hit on attempt {attempt}/{retry_limit}. Waiting {wait_time:.2f} seconds before retrying...")

# this is am empty class that simply defines the custom exception 'SteamRateLimit' by inheriting from the base 'Exception' class
class SteamRateLimit(Exception):
    pass

# this wraps around requestSteamReviews and is only called if the exception is raised as a class called 'SteamRateLimit'
# it will attempt to connect to steam following the random_exponential pattern, for a limited amount of times
# right before it retries, it calls log_retry_state to print out some data for transparency
@retry(
        stop=stop_after_attempt(retry_limit)
        , wait=wait_random_exponential(**wait_config)
        , retry=retry_if_exception_type(SteamRateLimit)
        , before_sleep=log_retry_state
)

def requestSteamReviews(app_id, last_cursor, session_instance):

    base_url = f"https://store.steampowered.com/appreviews/{app_id}?json=1"

    params = {
        "filter": "recent",
        "num_per_page": 100,
        "cursor": last_cursor
    }

    print(f"START requestSteamReviews for appId {app_id}, cursor = {last_cursor}")

    response = session_instance.get(base_url, params=params)

    # raises a custom exception SteamRateLimit which triggers @retry
    if response.status_code == 429:
        raise SteamRateLimit("Throttled by Steam")

    # this does nothing if the response is between 200-299, otherwise it raises an exception
    response.raise_for_status()

    return response.json()


# CELL ********************

def updateOrchestrator(app_id, load_type = None, load_status = None, last_execution_id = None, last_execution_time = None):
        
    where_predicates = [
        f"app_id = {app_id}",
        f"load_type = '{load_type}'" if load_type else ''
    ]
    where_clause = " and ".join([pred for pred in where_predicates if pred])

    update_predicates = [
            f"load_status = '{load_status}'" if load_status else '',
            f"last_execution_id = '{last_execution_id}'" if last_execution_id else '',
            f"last_execution_time = '{last_execution_time}'" if last_execution_time else '',
    ]
    update_clause = ", ".join([pred for pred in update_predicates if pred])

    query = f"""
            update control.loadorchestratorreviews
            set {update_clause}
            where {where_clause}
            """

    df_results = spark.sql(query)

    results_row = df_results.first()
    affected_rows = results_row['num_affected_rows']


    print(f"Updated control.loadOrchestratorReviews for {where_clause}; {affected_rows} affected")

# CELL ********************

def checkControl (app_id, execution_id = None):

    where_predicates = [
        f"app_id = {app_id}",
        f"execution_id = '{execution_id}'" if execution_id else '',
        "last_retrieved_cursor <> '*'",
        "execution_status not in ('in-progress')"
    ]
    where_clause = " and ".join([pred for pred in where_predicates if pred])

    query = f"""
        select last_retrieved_cursor
        from control.loadcontrolreviews
        where {where_clause}
        order by execution_start_time desc
        limit 1
        """

    result_row = spark.sql(query).first()
    cursor = result_row[0] if result_row and result_row[0] is not None else '*'

    print(f"Determined cursor for app_id = {app_id}: {cursor}")
    return cursor

# MARKDOWN ********************

# ### Main Loop

# CELL ********************

# Initialise run
execution_type = 'initial'

df_orchestrator = spark.read.format("delta").load(path_orchestrator) \
    .select("app_id") \
    .limit(game_limit) \
    .sort(f.asc("last_execution_time")) \
    .filter(f.col("load_status").isin("pending", "in-progress"))
games_row = df_orchestrator.collect()
games_list = [row['app_id'] for row in games_row]

print(f"Processing up to {cursor_limit*100} reviews for each of the following games: {games_list}")

for game in games_list:
    # Initialise loop and the audit dictionary
    execution_start_time = datetime.datetime.now()
    execution_id_base = str(game) + str(execution_start_time)
    execution_id = uuid.uuid5(NAMESPACE_ALTANWIR,execution_id_base)
    audit = {
        "app_id": game
        , "execution_id": str(execution_id)
        , "execution_start_time": execution_start_time
        , "execution_duration": 0
        , "execution_type": execution_type
        , "execution_status": 'in-progress'
        , "load_status": 'in-progress'
        , "retrieved_reviews": None
        , "last_retrieved_timestamp": None
        , "last_retrieved_cursor": None
        , "output_path": None
    }
    reviews_list = []
    batch = 1

    print(f"{execution_start_time}: Start processing app_id {game}. Assigned id {execution_id}.")

    audit["last_retrieved_cursor"] = checkControl(app_id=game)

    try:
        with requests.Session() as session:
            print(f"Session opened for app_id {audit['app_id']}")

            while (batch <= cursor_limit):
                print(f"Start batch {batch} of {cursor_limit}...")

                data = requestSteamReviews(
                        app_id=audit["app_id"]
                        , last_cursor=audit["last_retrieved_cursor"]
                        , session_instance=session
                    )
                print(f"END requestSteamReviews for appId {audit['app_id']}, cursor {audit['last_retrieved_cursor']}")

                reviews = data.get("reviews", [])
                audit["last_retrieved_cursor"] = data.get("cursor", [])

                if not audit["last_retrieved_cursor"] or len(reviews) == 0:
                    audit["load_status"] = "complete"
                    print(f"Reached end of reviews after {batch} batches for app_id {audit['app_id']}. load_type {audit['execution_type']} is now Complete.")
                    break    

                reviews_list.extend(reviews)
                print(f"Processed batch {batch} of {cursor_limit}!")

                batch += 1
                time.sleep(1)

            audit["execution_status"] = "success"
            print(f"Reached end of batches for app_id {audit['app_id']}")
                
    except Exception as e:
        audit.update(
            {"execution_status": f"Failed: {str(e)}"
            , "load_status": "failed"}
        )
        print(f"Failed at app_id {audit['app_id']}, batch {batch}, execution {audit['execution_id']} with: {audit['execution_status']}")

    finally:
        if not reviews_list:
            audit.update(
                {"execution_status": "empty"
                , "load_status": "empty"}
            )    
        else:
            json_data = json.dumps(reviews_list, indent=4)
            date_str = audit["execution_start_time"].strftime("%Y%m%d") # simple formatter to yyyymmdd
            file_path = f"Files/Steam/Reviews/{audit['execution_type']}/{audit['app_id']}/{date_str}/{audit['execution_id']}.json"

            audit.update(
                {"retrieved_reviews": len(reviews_list)
                , "last_retrieved_timestamp": reviews_list[-1]["timestamp_created"]
                , "output_path": file_path}
            )

            # notebookutils to write the file to the Lakehouse
            # .put creates the directories if they do not exist
            # True will overwrite the file if it exists
            notebookutils.fs.put(file_path, json_data, True)

            print(f"Saved {len(reviews_list)} to {file_path}")

        duration = datetime.datetime.now() - audit["execution_start_time"]
        audit["execution_duration"] = duration.total_seconds()
            
        df_audit = spark.createDataFrame([audit])
        df_audit = df_audit.drop("load_status") \
            .withColumn("app_id", f.col("app_id").cast(IntegerType())) \
            .withColumn("execution_duration", f.col("execution_duration").cast(IntegerType())) \
            .withColumn("retrieved_reviews", f.col("retrieved_reviews").cast(IntegerType())) \
            .withColumn("last_retrieved_timestamp", f.from_unixtime(f.col("last_retrieved_timestamp")).cast("timestamp"))
        df_audit.write.format("delta").mode("append").save(path_control)

        updateOrchestrator(
            app_id= audit["app_id"]
            , load_type=audit["execution_type"]
            , load_status=audit["load_status"]
            , last_execution_id=audit["execution_id"]
            , last_execution_time=audit["execution_start_time"]
        )
        print(f"Audit state saved. {audit}")

    print(f"End processing execution {audit['execution_id']} for game {game}")

print(f"Processing complete for games {games_list}")


# MARKDOWN ********************

# ### Debug

# CELL ********************

# %%sql

# truncate table control.loadcontrolreviews;

# update control.loadorchestratorreviews
# set load_status = 'pending', last_execution_id = NULL, last_execution_time = NULL
# where app_id in ( 323190, 39500 )

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from control.loadorchestratorreviews;
# MAGIC 
# MAGIC select * from control.loadcontrolreviews order by app_id, execution_start_time desc

# CELL ********************

# checkControl(39500) #, '292b5ad1-024e-565f-9c88-449e51867499')

# MARKDOWN ********************

# ## Attempt 3: Multi-thread and Audit Warehouse

# CELL ********************

import pyodbc
import struct
import notebookutils
import requests
import json
import time
import uuid
import concurrent.futures
from datetime import datetime

from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type

# MARKDOWN ********************

# ### Parameters

# CELL ********************

NAMESPACE_ALTANWIR = uuid.UUID('f81d4fae-7dec-11d0-a765-00a0c91e6bf6')
audit_server = '22jgi2dsfxnu5lmyn6ifyaro5e-wnxcbukzek4ejbckicpruy7sqq.datawarehouse.fabric.microsoft.com'
audit_database = 'IGDBAudit'

path_root = 'Files/Steam/Reviews/'
run_id = 'Dev1'
load_type = 'initial'
game_limit = 1
batch_limit = 2
retry_limit = 3
wait_config = {
    "multiplier": 1
    , "min": 2
    , "max": 60
}

# MARKDOWN ********************

# ### Functions

# MARKDOWN ********************

# #### connect_audit_wh

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

# MARKDOWN ********************

# #### save_reviews

# CELL ********************

def save_reviews_to_path(path, batch, reviews):
    json_data = json.dumps(reviews, indent=4)
    save_path = f"{path}/batch_{batch}.json"

    notebookutils.fs.put(save_path, json_data, True)

    print(f"SAVED: {len(reviews)} reviews to {save_path}")

# MARKDOWN ********************

# #### request_steam_reviews

# CELL ********************

# the retry_state parameter is an object of tenacity's RetryCallState class
# it contains all the information about the current state of the retries, such as attempt number and wait time
def log_retry_state(retry_state):
    attempt = retry_state.attempt_number
    wait_time = retry_state.next_action.sleep
    print(f"RETRY: Rate limit hit on attempt {attempt}/{retry_limit}. Waiting {wait_time:.2f} seconds before retrying...")

# this is am empty class that simply defines the custom exception 'SteamRateLimit' by inheriting from the base 'Exception' class
class SteamRateLimit(Exception):
    pass

# this wraps around requestSteamReviews and is only called if the exception is raised as a class called 'SteamRateLimit'
# it will attempt to connect to steam following the random_exponential pattern, for a limited amount of times
# right before it retries, it calls log_retry_state to print out some data for transparency
@retry(
        stop=stop_after_attempt(retry_limit)
        , wait=wait_random_exponential(**wait_config)
        , retry=retry_if_exception_type(SteamRateLimit)
        , before_sleep=log_retry_state
)

def request_steam_reviews(app_id, last_cursor, session_instance):

    base_url = f"https://store.steampowered.com/appreviews/{app_id}?json=1"

    params = {
        "filter": "recent",
        "num_per_page": 100,
        "cursor": last_cursor
    }

    print(f"GET: reviews for appId {app_id}, cursor = {last_cursor}")

    response = session_instance.get(base_url, params=params)

    # raises a custom exception SteamRateLimit which triggers @retry
    if response.status_code == 429:
        raise SteamRateLimit("Throttled by Steam")

    # this does nothing if the response is between 200-299, otherwise it raises an exception
    response.raise_for_status()

    reviews = response.json().get("reviews", [])
    cursor = response.json().get("cursor", [])

    return reviews, cursor


# MARKDOWN ********************

# #### process_batch

# CELL ********************

def process_batch(app_id, load_type, high_water_mark, start_cursor):
    print(f"[{app_id}] Thread started.")
    
    # Initialize audit dictionary
    start_time = datetime.now()
    id_base = str(app_id) + str(start_time)
    execution_id = uuid.uuid5(NAMESPACE_ALTANWIR,id_base)    
    audit = {
        "app_id": app_id,
        "run_id": run_id,
        "execution_id": execution_id,
        "execution_type": load_type,
        "execution_start_time": start_time,
        "execution_end_time": None,
        "execution_duration": 0,
        "execution_status": "in-progress",
        "load_status": "in-progress",
        "retrieved_reviews": 0,
        "first_retrieved_timestamp": None,
        "last_retrieved_timestamp": None,
        "last_retrieved_cursor": start_cursor,
        "output_path": f"{path_root}/{load_type}/{run_id}/{execution_id}"
    }
    
    batch = 1
    cursor = start_cursor

    print(f"GAME: Start processing load {load_type} for game {app_id}, using cursor = {cursor} and watermark {high_water_mark}")
    
    try:
        with requests.Session() as session:
            while batch <= batch_limit:
                print(f"BATCH: Start batch {batch} of {batch_limit} for game {app_id}...")
                
                # 1. Fetch data
                reviews, cursor = request_steam_reviews(app_id, cursor, session) 
                
                if batch == 1: 
                    audit["first_retrieved_timestamp"] = reviews[0].get('timestamp_created')
                    print(f"BATCH: First timestamp saved: {audit['first_retrieved_timestamp']}")

                # 2. Stop condition: End of reviews
                if not reviews or cursor is None:
                    audit["load_status"] = "completed"
                    audit["execution_status"] = "success"
                    print(f"BATCH: Reached the end of reviews after {batch} batches for game {app_id}")
                    break
                
                # 3. Stop condition: Incremental Watermark reached
                if load_type != 'initial' and high_water_mark:
                    first_in_batch = reviews[0].get('timestamp_created')
                    if first_in_batch <= high_water_mark:
                        audit["load_status"] = "completed"
                        audit["execution_status"] = "success"
                        print(f"BATCH: Reached the start of last load after {batch} batches for game {app_id}")
                        break
                
                # 4. Save to Lakehouse path
                save_reviews_to_path(audit["output_path"], batch, reviews)
                
                # 5. Update audit state
                audit["retrieved_reviews"] += len(reviews)
                audit["last_retrieved_timestamp"] = reviews[-1].get('timestamp_created')
                audit["last_retrieved_cursor"] = cursor
                
                print(f"BATCH: Processed batch {batch} for game {app_id}")

                batch += 1
                time.sleep(1.5) # Courtesy sleep
                
    except Exception as e:
        audit["execution_status"] = f"failed: {str(e)}"
        audit["load_status"] = "failed"
        print(f"BATCH: Error {app_id} failed with {e}")
        
    finally:
        # 6. Database Check-in (Thread-safe because it's local to this function)
        if audit["retrieved_reviews"] == 0:
            audit["execution_status"] = "empty"
            audit["load_status"] = "empty"
            print(f"GAME: No reviews found for game {app_id}")
        else:
            end_time = datetime.now()
            duration = end_time - audit["execution_start_time"]
            audit["execution_status"] = 'success'
            audit["execution_end_time"] = end_time
            audit["execution_duration"] = duration.total_seconds()

    print(f"GAME: Load type {load_type} processed a total of {audit['retrieved_reviews']} reviews for game {app_id}")
                
    return app_id, audit

# MARKDOWN ********************

# ## Main

# CELL ********************

# 1. Form the list of games to load
conn = connect_audit_wh()
db_cursor = conn.cursor()
queryGames = f"""
    select top {game_limit}
        app_id
        , high_water_mark
        , isnull(last_retrieved_cursor, '*')
    from steam.loadReviews
    where load_status in ('pending', 'in-progress')
        and load_type = ?
    order by execution_start_time asc
"""

db_cursor.execute(queryGames, load_type)
games_list = db_cursor.fetchall()

conn.close()

print(games_list)
print(f"Starting orchestration for {len(games_list)} games...")

# 2. Spin up the Thread Pool
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    
    # Dictionary comprehension to map the Future object back to the app_id
    # This immediately submits all jobs to the pool.
    future_to_game = {
        executor.submit(
            process_batch, 
            game[0], 
            load_type, 
            game[1], 
            game[2]
        ): game[0] 
        for game in games_list
    }

    # 3. Process results as they finish
    # as_completed() yields the futures as soon as they are done, regardless of start order.
    for future in concurrent.futures.as_completed(future_to_game):
        app_id = future_to_game[future]
        try:
            returned_app_id, audit_dict = future.result()

            print(f"BATCH: Logging audit for game {app_id}: {audit}")

            conn = connect_audit_wh()
            db_cursor = conn.cursor()
            
            queryControl = """
                insert into steam.loadControlReviews (
                    app_id
                    , run_id
                    , execution_id
                    , execution_type
                    , execution_start_time
                    , execution_end_time
                    , execution_duration
                    , execution_status
                    , retrieved_reviews
                    , first_retrieved_timestamp
                    , last_retrieved_timestamp
                    , last_retrieved_cursor
                    , output_path
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            control_audit = dict(audit_dict)
            del control_audit["load_status"]
            argsControl = list(control_audit.values())

            queryOrchestrator = """
                update steam.loadOrchestratorReviews
                set load_status = ?
                where app_id = ?
                    and load_type = ?
            """
            argsOrhcestrator = [
                audit["load_status"]
                , audit["app_id"]
                , audit["execution_type"]
            ]

            # update the audit tables
            db_cursor.execute(queryControl,argsControl)
            db_cursor.execute(queryOrchestrator,argsOrhcestrator)
            
            conn.commit()
            print(f"MAIN: Game {returned_app_id} finished with execution status = {audit["execution_status"]}, load status = {audit["load_status"]}")
        except Exception as exc:
            # This catches catastrophic thread failures that your worker's Try/Except missed
            print(f"MAIN: Thread for Game {app_id} generated an unhandled exception: {exc}")
        finally:
            if 'conn' in locals():
                conn.close()

print("All threads completed. Orchestrator shutdown.")

# MARKDOWN ********************

# ## Debug

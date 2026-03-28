# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
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

# # Steam Rerviews
# 
# ## Considerations
# - The steam storefront api is a public facing endpoint where reviews can be obtained on a per-game basis
# - This endpoint is not documented in terms of rate limits, but it seems a decent margin is 200 requests every 5 minutes (meaning 20k reviews total)
# - Steam provides a cursor that can be used for pagination
# 
# ## Limitations
# - I chose to sort reviews by the date they were created. Steam provides them in chronologically descending order, up to 100 reviews per batch
# - I am running this on a fabric trial capacity, meaning I only have 1 cluster. Therefore I went out of my way to not spool up any cluster for review retrieval
# - This is the main reason why I chose to create a fabric warehouse called IGDBAudit for logging and pagination
# 
# ## Expected Behaviour
# - This process relies on IGDBAnalytics.steam.loadReviews, a view over an audit table (steam.loadControlReviews) and a manual table (steam.loadOrchestratorReviews)
# - The manual table contains the games I wanted to extract reviews for. Fields of note include priority (highly rated games got the highest priority) and 'load_status'
# - This script will extract the top {game_limit} highest priority games, sorted by the oldest extractions and prioritizing on-going loads. It will then save batches of up to 100 games, for {batch_limit} batches, saving them in {path_root} and logging the cursor in the control table
# - There are up to {max_threads} extractions at the same time using concurrency
# - This script is expected to run several times per hour
# - A separate notebook runs every so often and ingests the raw .json files in bronze.steamReviews


# CELL ********************

import pyodbc
import struct
import notebookutils
import requests
import json
import time
import uuid
import concurrent.futures
import random
from datetime import datetime
from urllib.parse import urlencode

from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Parameters

# PARAMETERS CELL ********************

# Constants
NAMESPACE_ALTANWIR = uuid.UUID('f81d4fae-7dec-11d0-a765-00a0c91e6bf6')
audit_server = '22jgi2dsfxnu5lmyn6ifyaro5e-wnxcbukzek4ejbckicpruy7sqq.datawarehouse.fabric.microsoft.com'
audit_database = 'IGDBAudit'
lakehouse_info = notebookutils.lakehouse.get("IGDBAnalytics")
abfs_root = f"{lakehouse_info['properties']['abfsPath']}"   # necessary for pipeline runs, can be commented for dev runs

# Run Configurations
run_id = 'Dev3'
load_type = 'initial'
max_threads = 6

# Limits
game_limit = 40         # games per run
batch_limit = 60       # batch per game. each batch is 100 reviews
retry_limit = 5         # how many retries in case of a 429 code from steam
jitter = 5              # maximum amount of seconds waiting between each batch

# Retry Configurations
wait_fixed = 10         # minimum retry wait of 10 seconds
wait_config = {
    "multiplier": 1     # the exponent
    , "max": 600        # max wait value
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Functions

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
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### save_reviews

# CELL ********************

def save_reviews_to_path(app_id, path, batch, reviews):
    if batch == 1:
        notebookutils.fs.mkdirs(abfs_root + path)
    
    json_data = json.dumps(reviews, indent=4)
    save_path = f"{abfs_root}{path}/batch_{batch}.json"

    notebookutils.fs.put(save_path, json_data, True)

    print(f"[{app_id}]\t\tSAVED: {len(reviews)} reviews to {path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### request_steam_reviews

# CELL ********************

# the retry_state parameter is an object of tenacity's RetryCallState class
# it contains all the information about the current state of the retries, such as attempt number and wait time
def log_retry_state(retry_state):
    attempt = retry_state.attempt_number
    wait_time = retry_state.next_action.sleep
    # Get app_id from the arguments passed to the function being retried",
    app_id = retry_state.args[0] if retry_state.args else "UNKNOWN_APP_ID"
    print(f"[{app_id}]\t\t\tRETRY: Rate limit hit on attempt {attempt}/{retry_limit}. Waiting {wait_time:.2f} seconds before retrying...")

# this is am empty class that simply defines the custom exception 'SteamRateLimit' by inheriting from the base 'Exception' class
class SteamRateLimit(Exception):
    pass

class SteamError(Exception):
    pass    

# this wraps around requestSteamReviews and is only called if the exception is raised as a class called 'SteamRateLimit'
# it will attempt to connect to steam following the random_exponential pattern, for a limited amount of times
# right before it retries, it calls log_retry_state to print out some data for transparency
@retry(
        stop=stop_after_attempt(retry_limit)
        , wait=wait_fixed + wait_random_exponential(**wait_config)
        , retry=retry_if_exception_type(SteamRateLimit)
        , before_sleep=log_retry_state
)

def request_steam_reviews(app_id, last_cursor, session_instance):

    base_url = f"https://store.steampowered.com/appreviews/{app_id}?json=1"

    params = {
        "filter": "recent",
        "purchase_type": "all",
        "language": "english",
        "num_per_page": 100,
        "cursor": last_cursor
    }

    query_string = urlencode(params)
    print(f"[{app_id}]\t\t\tGET: reviews for {base_url}&{query_string}")

    response = session_instance.get(base_url, params=params)

    # raises a custom exception SteamRateLimit which triggers @retry
    if response.status_code == 429:
        raise SteamRateLimit("Throttled by Steam")

    # this does nothing if the response is between 200-299, otherwise it raises an exception
    response.raise_for_status()

    data = response.json()
    reviews = data.get("reviews", [])
    cursor = data.get("cursor")

    if data.get("success") != 1:
        error_message = data.get('error', 'Unknown Steam Error')
        raise SteamError(f"Steam API Returned an error: {error_message}")

    return reviews, cursor


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### process_batch

# CELL ********************

def process_batch(app_id, load_type, high_water_mark, start_cursor):
    print(f"[{app_id}]\tGAME: Thread started.")
    
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
        "output_path": f"/Files/Steam/Reviews/{load_type}/{run_id}/{execution_id}"
    }
    
    batch = 1
    cursor = start_cursor

    print(f"[{app_id}]\tGAME: Start processing load {load_type}, using cursor = {cursor} and watermark {high_water_mark}")
    
    try:
        with requests.Session() as session:
            while batch <= batch_limit:
                # 1. Fetch data
                reviews, cursor = request_steam_reviews(app_id, cursor, session) 

                # 2. Stop condition: End of reviews
                if not reviews or cursor is None:
                    audit["load_status"] = "completed"
                    audit["execution_status"] = "success"
                    audit["last_retrieved_cursor"] = "*"
                    print(f"[{app_id}]\t\tBATCH: Reached the end of reviews after {batch} batches")
                    break

                # save the first timestamp only in the first batch
                if batch == 1: 
                    audit["first_retrieved_timestamp"] = reviews[0].get('timestamp_created')
                    print(f"[{app_id}]\t\tBATCH: First timestamp saved: {audit['first_retrieved_timestamp']}")
                else:
                    wait = random.randint(1,jitter)
                    print(f"[{app_id}]\t\tBATCH: Start batch {batch} of {batch_limit}.  Courtesy wait: {wait} seconds. Waiting...")
                    time.sleep(wait) # Courtesy sleep                    
                
                # 3. Stop condition: Incremental Watermark reached
                if load_type != 'initial' and high_water_mark:
                    first_in_batch = reviews[0].get('timestamp_created')
                    if first_in_batch <= high_water_mark:
                        audit["load_status"] = "completed"
                        audit["execution_status"] = "success"
                        print(f"[{app_id}]\t\tBATCH: Reached the start of last load after {batch} batches")
                        break
                
                # 4. Save to Lakehouse path
                save_reviews_to_path(app_id, audit["output_path"], batch, reviews)
                
                # 5. Update audit state
                audit["retrieved_reviews"] += len(reviews)
                audit["last_retrieved_timestamp"] = reviews[-1].get('timestamp_created')
                audit["last_retrieved_cursor"] = cursor
                
                print(f"[{app_id}]\t\tBATCH: Processed batch {batch}")

                batch += 1

    except Exception as e:
        audit["execution_status"] = f"failed: {str(e)}"
        audit["load_status"] = "failed"
        print(f"[{app_id}]\t\tBATCH: Failed with {e}")
    else:
        if audit["retrieved_reviews"] == 0:
            audit["execution_status"] = "empty"
            audit["load_status"] = "empty"
            audit["retrieved_reviews"] = 0
            print(f"[{app_id}]\t\tGAME: No reviews found")
        else:
            audit["execution_status"] = "success"
    finally:
        end_time = datetime.now()
        duration = end_time - audit["execution_start_time"]
        audit["execution_end_time"] = end_time
        audit["execution_duration"] = duration.total_seconds()

    print(f"[{app_id}]\t\tGAME: Load type {load_type} ended with status {audit['execution_status']}, load status is {audit['load_status']}. Processed a total of {audit['retrieved_reviews']} reviews.")
                
    return app_id, audit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Main

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
    order by priority_order asc, execution_start_time asc
"""

db_cursor.execute(queryGames, load_type)
games_list = db_cursor.fetchall()

conn.close()

print(f"MAIN: Starting orchestration for {len(games_list)} games, load_type = '{load_type}'")
print(f"MAIN: Game list is: {games_list}")
print(f"MAIN: Setup is: \n\tmax threads = {max_threads} \n\tgame_limit = {game_limit} \n\tbatch_limit = {batch_limit}x100 reviews \n\tjitter between 1 and {jitter} seconds")

# 2. Spin up the Thread Pool
with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
    
    # Dictionary comprehension to map the Future object back to the app_id
    # This immediately submits all jobs to the pool.
    future_to_game = {
        executor.submit(
            process_batch, 
            game[0],        # app_id
            load_type, 
            game[1],        # high_water_mark
            game[2]         # last_cursor
        ): game[0] 
        for game in games_list
    }

    # 3. Process results as they finish
    # as_completed() yields the futures as soon as they are done, regardless of start order.
    for future in concurrent.futures.as_completed(future_to_game):
        app_id = future_to_game[future]
        try:
            returned_app_id, audit_dict = future.result()

            print(f"[{app_id}]\tMAIN: Logging audit...")

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
            argsControl = [
                audit_dict["app_id"]
                , run_id
                , audit_dict["execution_id"]
                , load_type
                , audit_dict["execution_start_time"]
                , audit_dict["execution_end_time"]
                , audit_dict["execution_duration"]
                , audit_dict["execution_status"]
                , audit_dict["retrieved_reviews"]
                , audit_dict["first_retrieved_timestamp"]
                , audit_dict["last_retrieved_timestamp"]
                , audit_dict["last_retrieved_cursor"]
                , audit_dict["output_path"]
            ]

            queryOrchestrator = """
                update steam.loadOrchestratorReviews
                set load_status = ?
                where app_id = ?
                    and load_type = ?
            """
            argsOrchestrator = [
                audit_dict["load_status"]
                , audit_dict["app_id"]
                , audit_dict["execution_type"]
            ]

            # update the audit tables
            db_cursor.execute(queryControl,argsControl)
            db_cursor.execute(queryOrchestrator,argsOrchestrator)
            
            conn.commit()
            print(f"[{app_id}]\tMAIN: Game finished with execution status = '{audit_dict['execution_status']}', load status = '{audit_dict['load_status']}'")
        except Exception as exc:
            print(f"[{app_id}]\tMAIN: Thread generated an unhandled exception: {exc}")
        finally:
            if 'conn' in locals(): conn.close()            

print("MAIN: All threads completed. Orchestrator shutdown.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

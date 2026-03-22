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

# # Original Tracer Bullet Script
# 
# Don't touch it, it's sacred

# CELL ********************

import requests
import json
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

# ## Ghetto
# 
# A dumpster of ideas, to be cleaned out later. The workspace / scratchpad

# CELL ********************

import requests
import json
import time

from pyspark.sql import functions as f
from pyspark.sql.window import Window

# CELL ********************

path_orchestrator = 'abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Tables/control/loadorchestratorreviews'
path_control = 'abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Tables/control/loadcontrolreviews'

# MARKDOWN ********************

# ## Insert Test Values
# 
# Taste cases covered:
# - A game with 2 executions, to test ranking
#   - 39500: Gothic 3
# - A game marked as skip, to test it gets ignored
#   - 48231: Might & Magic: Heroes VI - Pirates of the Savage Sea Adventure Pack
# - A game with no executions, to test if it gets initialised
#   - 184571: Frostpunk

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
# MAGIC     , load_type varchar(10)
# MAGIC     , load_status varchar(10)
# MAGIC     , last_execution_id int
# MAGIC     , last_execution_time TIMESTAMP
# MAGIC );
# MAGIC 


# CELL ********************

# spark.sql("use control")

# spark.sql("truncate table control.loadorchestratorreviews")

# spark.sql("""insert into control.loadorchestratorreviews (app_id, load_type, load_status)
#         values ( 39500, 'initial', 'pending'),
#           ( 48231, 'initial', 'skip'),
#           ( 184571, 'initial', 'pending')""")

# spark.sql("truncate table control.loadcontrolreviews")

# spark.sql("""insert into control.loadcontrolreviews (execution_id, app_id, execution_start_time, last_retrieved_cursor)
#           values (1, 39500, '2026-03-10 10:00:01.123', 'cursor1'),
#             (2, 39500, '2026-03-10 10:05:01.223', 'cursor2'),
#             (1, 48231, '2026-03-10 10:00:01.771', 'cursor1')""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_orchestrator = spark.read.format("delta").load(path_orchestrator)
df_control = spark.read.format("delta").load(path_control)


# CELL ********************

df_orchestrator.show()
df_control.show()

# MARKDOWN ********************

# ## Joining to get last_cursor
# 
# 1) Remove runs w/ no cursor; this means the run is full?
# 2) Rank executions, grouped by app_id and ordered in descending execution

# CELL ********************

df_control = df_control \
    .filter(df_control.last_retrieved_cursor.isNotNull()) \
    .withColumn("rowNumber", f.row_number().over(Window.partitionBy("app_id").orderBy(f.col("execution_start_time").desc())))

df_joined = df_orchestrator.alias("o").filter(f.col("o.load_status") == 'pending') \
    .join(df_control.alias("c").filter(f.col("c.rowNumber") == 1) \
        ,f.col("o.app_id") == f.col("c.app_id") \
        ,"left"
    ) \
    .withColumn("steam_id", f.col("o.app_id"))  # idk how to solve the ambiguity of app_id existing in both dataframes so vOv

# CELL ********************

df_control.show()
df_joined.show()

# MARKDOWN ********************

# ## Main loop playground
# 
# ### Definitions
# - requestSteamReviews = function to get reviews from steam
# - insertControl = function to insert a line in the control table. returns inserted execution_id
# - checkControl = function to retrieve the most recent valid last_retrieved_cursor
# - updateOrchestrator = function to update orchestrator table
# - updateControl = function to save execution details in the control table
# - {path} = Fabric/Files/Steam/Reviews/{Daily, Weekly, Monthly, Initial}/{app_id}/{execution_id}/{batch_no}.json
# - **x** is a # we agree to use to extract reviews for a game at a rate of our chosing
# - **game loop** = games with a 'pending' (i.e. to start) or 'in-progress' (i.e. partially loaded') load_status
# - **y** = # of games processed at a time
# 
# ### Functions
# 
# #### requestSteamReviews
# 
# **Parameters**
# - session
# - app_id
# - cursor
# 
# **Returns**
# - reviews_list
# - (optional) response_message
# 
# **Pseudocode**
# - if valid response
#   - returns the response object
# - if no valid response
#   - response_message = 'failed + error code + error message'
# - courtesy wait    
# 
# #### updateOrchestrator
# 
# **Parameters**
# - app_id
# - (optional) load_type
# - (optional) load_status
# - (optional) last_execution_id
# - (optional) last_execution_time
# 
# **Returns**
# - (optional) last_execution_id
# - (optional) last_execution_time
# 
# **Pseudocode**
# - literally just updates control.loadorhcestratorreviews
# - sets load_status = in-progress / completed where app_id = app_id and load_type = load_type (if it exists)
# - if last_execution_id is given as a parameter
#   - sets it for app_id
#   - sets last_execution
# - else
#   - returns last_execution_id & last_execution_time, if it exists
# 
# #### insertControl
# 
# **Parameters**
# - app_id
# - execution_type
# - execution_start_time
# 
# **Returns**
# - execution_id
# 
# **Pseudocode**
# - sha2 hash the concatenation of app_id & execution_start_time
# - turn the hash into an uuid
# - path = Fabric/Files/Steam/Reviews/{execution_type}/{app_id}/{execution_id}
# - insert into control.loadcontrolreviews
#   - execution_id
#   - app_id
#   - execution_start_time
#   - execution_type
#   - execution_status = 'in_progress'
#   - path
# 
# #### checkControl
# 
# **Parameters**
# - app_id
# - (optional) execution_id
# 
# **Returns**
# - last_retrieved_cursor
# 
# **Pseudocode**
# - Searches for the app_id and optionally execution_id in control.loadcontrolreviews
# - returns the most recent last_retrieved_cursor
#   - where execution_status != 'running' / 'in-progress'
# 
# #### updateControl
# 
# **Parameters**
# - app_id
# - execution_id
# - (optional) execution_duration
# - (optional) execution_status
# - (optional) retrieved_reviews
# - (optional) last_retrieved_timestamp
# - (optional) last_retrieved_cursor
# 
# **Returns**
# - nothing
# 
# **Pseudocode**
# - simply updates control.loadcontrolreviews using parameters
# 
# ### Orchestration Loop
# 
# - open session and get session object
# - for games in game loop, limit [y] (top y from control.loadorchestratorreviews where load_status in ('pending', 'in-progress') order by last_execution_time asc )
#   - set execution_start_time, load_type = 'initial'
#   - *insertControl* (app_id, execution_type, execution_start_time) (returns execution_id)
#     - app_id / execution_start_time
#     - execution_start_time
#     - type = 'initial'
#     - status = 'in-progress'
#     - path = Fabric/Files/Steam/Reviews/{execution_type}/{app_id}/{execution_id}
#   - *updateOrchestrator* (app_id, load_type, load_status = 'in-progress', last_execution_id, last_execution_time)    
#   - if *checkControl* (app_id) (returns last_retrieved_cursor) is null (i.e. it did not find a cursor)
#     - set cursor = "*" else cursor = last_retrieved_cursor
#   - init execution_duration, execution_status, reviews_list, x starts at 1 up until the defined rate limit
#   - while [x]
#     - try 
#       - *requestSteamReviews*(app_id, cursor) (returns reponse, response_message (only if there is an error))
#       - if response_message exists ( **case: the function returned an error** )
#         - *updateControl*(app_id, execution_id) (execution_status = response_message)
#         - break 
# 
#       - data = response.json()
#       - reviews = data.get("reviews", [])
#       - reviews_list.extend(reviews)      
#       - get retrieved_reviews, last_retrieved_timestamp, last_retrieved_cursor from reviews
#         - retrieved_reviews = len(reviews_list)
#         - last_retrieved_timestamp = the timestamp_created of the last review in reviews
#         - last_retrieved_cursor = data.get("cursor",[])
# 
#       - if retrieved_reviews > 0 & last_retrieved_cursor is not null ( **case: the loop is valid and it continues** )
#         - save reviews in path/{x}.json
#         - set execution_duration
#         - *updateControl*(app_id, execution_id) (execution_duration, execution_status = 'running', retrieved_reviews, last_retrieved_timestamp, last_retrieved_cursor)
#         - x += 1
#         - cursor = last_retrieved_cursor
# 
#       - if retrieved_reviews == 0 or last_retrieved_cursor is null ( **case: the loop has ended** )     
#         - *updateOrchestrator* (app_id, load_type, load_status = 'completed)   
#         - break
# 
#     - catch
#       - set execution_duration
#       - *updateControl*(app_id, cursor) (execution_duration, execution_status = 'failed + error')
#       - break
#   - *updateControl*(app_id, execution_id) (execution_status = 'success')


# CELL ********************

games = df_joined.select("steam_id", "most_recent_execution", "last_retrieved_timestamp", "last_retrieved_cursor").collect() # filter(df_orchestrator.load_status == 'pending').collect()

print(games)

for game in games: 
    app_id = game['steam_id']
    last_cursor = game['last_retrieved_cursor']

    print(app_id)
    print(last_cursor)


# MARKDOWN ********************

# # Ghetto Deluxe
# 
# I'm actually trying here

# MARKDOWN ********************

# ## Functions

# CELL ********************

def requestSteamReviews(app_id, last_cursor, session_instance):

    base_url = f"https://store.steampowered.com/appreviews/{app_id}?json=1"

    params = {
        "filter": "recent",
        "num_per_page": 100,
        "cursor": {last_cursor}
    }

    print(f"START requestSteamReviews for appId {app_id}, cursor = {last_cursor}")

    response = session_instance.requests.get(base_url, params=params)

    if response.status_code == 200:
        time.sleep(1)
        print(f"END requestSteamReviews for appId {app_id}")

        return response
    else:
        response_message = f"Failed: {response.status_code} -- {response.text}"
        print(f"STOP requestSteamReviews: {response_message}")
        return response_message


# CELL ********************

def updateOrchestrator(app_id, load_type = None, load_status = None, last_execution_id = None, last_execution_time = None):
    
    set_predicates = [
            f"load_status = '{load_status}'" if load_status else '',
            f"last_execution_id = {last_execution_id}" if last_execution_id else '',
            f"last_execution_time = '{last_execution_time}'" if last_execution_time else '',
    ]
    set_list = [pred for pred in set_predicates if pred]

    query = f"""
            update control.loadorchestratorreviews
            set {", ".join(set_list)}
            where app_id = {app_id}
                {("and load_type = '" + load_type + "'") if load_type else ''}
            """
    spark.sql(query)

#    df_query_results = spark.sql(query)

#    return df_query_results

updateOrchestrator(39500, 'initial', 'wip', '1', '2026-03-22 10:10:01.392')


# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from control.loadorchestratorreviews

# MARKDOWN ********************

# ## Main Loop

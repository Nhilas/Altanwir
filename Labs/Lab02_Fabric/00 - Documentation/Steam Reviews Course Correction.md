# Conversation Summary: Project Altanwir - Architecting the Steam API Ingestion Engine

## 🎯 Core Objective & Main Goal

The primary objective of this engineering sync was to architect a robust, highly parallelized ingestion pipeline to extract millions of historical and incremental game reviews from the public Steam API. The underlying goal is to elevate the user—a senior SQL/SSIS developer—into a cloud-native DataOps paradigm on Microsoft Fabric, establishing the architectural maturity required to command a 15k RON/month individual contributor salary. The system must prioritize idempotency, handle aggressive rate limiting (HTTP 429s), and execute without manual babysitting.

## 🔑 Key Insights & Discoveries

The Small File Problem (The Storage Killer): We identified that saving thousands of individual JSON files to a data lake Landing Zone creates massive HTTP overhead. Insight: Write the raw API payloads directly into a Bronze Delta Table.

The Immutable Ledger Pattern: Instead of shredding the nested JSON during ingestion (the legacy ETL way), the Bronze table should store the raw JSON string in a single column. Spark's from_json function handles this effortlessly downstream in the Silver layer.

Generators over Returns (yield): To prevent Out Of Memory (OOM) errors during massive extractions, Python functions must act like SQL Table-Valued Functions, yielding rows one by one rather than returning massive arrays.

The "Bazooka" Realization: “Using PySpark's distributed workers (mapPartitions) for an I/O-bound task on a small dataset is massive overkill.” We realized that teaching a massive Spark cluster to make simple HTTP requests on a free Fabric capacity was over-engineering. Standard Python threading is the right tool for this specific network-bound extraction.

## ✅ Key Decisions Made

Pivot to ThreadPoolExecutor: The extraction engine will use standard Python threading (8 concurrent lanes) to parallelize the API requests across different games, replacing both the single-threaded loop and the over-engineered Spark mapPartitions approach.

Bronze Table Grain: The target Bronze Delta Table will be modeled at the app_id x recommendation_id grain (1 row = 1 review).

Idempotent Upserts: Data will be written to Bronze using a SQL MERGE INTO statement on the recommendation_id to guarantee no duplicate records are stored, regardless of how many times a pipeline fails and retries.

Decoupled Auditing: The architecture separates configuration from logging:

Control Table: Holds configuration (which games to run, priorities).

Execution Ledger: Records the High-Water Marks and rows extracted per run.

Fabric Pipeline: Manages the overarching RunId to eliminate the need for custom "master orchestrator" logging.

## Actionable items & next steps

Design the Micro-Batch Flush: Engineer a mechanism within the ThreadPoolExecutor logic that safely flushes data to the Spark Delta table without losing the batch-by-batch progress tracking the user originally designed.

Integrate the Pipeline RunId: Pass the @pipeline().RunId from the Fabric orchestrator dynamically into the Python notebook to stamp the lineage on the Bronze records.

Implement High-Water Marking: Finalize the logic that reads the maximum created_timestamp from the Execution Ledger to strictly pull incremental data from the Steam API.

## 🤔 Unresolved Questions & Open Loops

The Progress-Saving Dilemma: The user heavily criticized the proposed "Continuous Flush" design because it grouped 5 games at a time before saving to the database. The user requires a failsafe where each individual batch of 100 reviews has its cursor/state saved to prevent catastrophic data loss if a thread crashes mid-game. How can we write to Delta/Audit logs batch-by-batch without triggering the Small File Problem or killing performance?

The Spark-Python Bridge: There is lingering confusion on how to efficiently pass data from the Python threads into a Spark DataFrame for the MERGE INTO operation without bottlenecking the Driver's memory.

## 📝 General Observations & Synopsis

This conversation represents a classic, messy transition from on-premise RDBMS thinking to distributed Lakehouse engineering. We successfully mapped out the theoretical best practices (Idempotency, Medallion Architecture, API backoff), but hit a wall on the practical implementation.

We initially over-engineered the solution using PySpark's mapPartitions, which alienated the user. We course-corrected to Python's ThreadPoolExecutor, but failed to respect the user's ironclad requirement for granular, batch-level fault tolerance. The user's engineering instincts are sharp—they correctly identified memory vulnerabilities and state-loss risks in the proposed code. The next phase must focus strictly on delivering a threaded Python script that respects batch-level cursors while safely bridging that data into a Spark Delta Table.

## Saved Schematics for the PySpark partition implementation

### Proposed Flush Design

- *This is as far as we got into using ThreadPoolExecutor before I realised we're no longer correctly state tracking. I do not like this design at all*

```python
import concurrent.futures

# - 1. THE BRIDGE: Spark -> Python

df_games = spark.sql("SELECT app_id, last_retrieved_timestamp FROM control.loadorchestratorreviews WHERE load_status = 'pending'")
games_to_process = [row.asDict() for row in df_games.collect()]

# - Buffer variables to hold data temporarily

buffer_data = []
buffer_audit = []
CHUNK_SIZE = 5 # Flush to database every 5 games to save memory and progress

# - 2. Execute the ThreadPool

with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:

    # Submit all games to the executor
    futures = [executor.submit(process_single_game, game) for game in games_to_process]
    
    # as_completed yields the future the second it finishes, regardless of order
    for future in concurrent.futures.as_completed(futures):
        try:
            data, audit = future.result()
            buffer_data.extend(data)
            buffer_audit.append(audit)
            
            # 3. THE MICRO-BATCH FLUSH
            if len(buffer_audit) >= CHUNK_SIZE:
                print(f"Flushing {len(buffer_audit)} games to Bronze...")
                
                # A. Write Data to Bronze (Python -> Spark Bridge)
                if buffer_data:
                    df_incoming = spark.createDataFrame(buffer_data, schema=["app_id", "rec_id", "created_ts", "raw_json"])
                    df_incoming.createOrReplaceTempView("vw_incoming")
                    
                    # Execute SQL MERGE to safely upsert
                    # spark.sql(""" MERGE INTO bronze_reviews ... """)
                
                # B. Update Control Table with new watermarks
                # Loop through buffer_audit and run SQL updates for each app_id
                
                # C. Clear the buffers to free up RAM!
                buffer_data.clear()
                buffer_audit.clear()
                
        except Exception as e:
            print(f"A game failed catastrophically: {e}")
            # Optional: Log the failure to the control table so it can be retried later

# - 4. FINAL FLUSH
# - Catch any remaining games in the buffer that didn't neatly hit the CHUNK_SIZE

if len(buffer_audit) > 0:
    print(f"Flushing final {len(buffer_audit)} games to Bronze...")
    # Repeat the Spark DataFrame creation and MERGE logic here
```    

### Orchestration Loop 1

- *This is an incomplete pseudocode that I stopped when I realized we should try to use python threads instead of spark for this task.*
- create a new function called *processReviews* that takes the generator function as a parameter

- def *processReviews*(partition_data):
  - 'partition_data' is an Iterator injected by Spark; It contains a chunk of rows (e.g., 250 games and their watermarks).
  - with requests.Session() as session
    - Open ONE session per Worker. Loop through the specific games assigned to this Worker
    - for row in partition_data:
      - try:
        - app_id = row['app_id']
        - high_water_mark = row['last_retrieved_timestamp']
        - cursor = row['last_retrieved_cursor']
        - create execution_id from app_id and execution_start_time
        - set audit dictionary = {'app_id': app_id, 'execution_id': execution_id, 'execution_start_time': datetime, 'execution_duration': 0, 'execution_type': 'initial', 'execution_status': 'in-progress', 'retrieved_reviews': None, 'last_retrieved_timestamp': high_water_mark, 'last_retrieved_cursor': cursor}

        - while [x] and not stop_fetching:
          - *requestSteamReviews*(app_id, cursor) (returns reponse.json())
          - reviews = reponse.get("reviews", [])
          - cursor = reponse.get("cursor")
  
          - if len(reviews) == 0 or cursor is null ( **case: you reached the end of the reviews** )
            - audit.execution_status = 'completed'
            - break
  
          - Loop through the 'reviews' array returned by Steam
          - for review in reviews_array:
            - If review['timestamp_created'] is older than high_water_mark: (**case: you found old data** )
              - stop_fetching = True
              - break
            - yield ( (Instead of appending to a list, yield the Tuple; This pauses the function, sends the row to Spark, and resumes.)
              - "DATA", (this tags it to help distinguish from auditing later)
              - app_id,
              - review['recommendationid'],
              - json.dumps(review) # The raw envelope
            - )
  
          - total_reviews += len(reviews)
          - if x = 1: new_water_mark = reviews[0]['timestamp_created']
          - x += 1
          - time.sleep(1)
      - except:
        - audit.execution_status = 'failed' + error
      - finally:
        - audit.execution_status if audit.execution_status else 'success' (prioritize marking the run as complete or failed over success)
        - audit.execution_duration = (datetime.now() - audit.execution_start_time).total_seconds()
        - audit.retrieved_reviews = total_reviews
        - audit.last_retrieved_timestamp = new_water_mark
        - audit.last_retrieved_cursor = cursor
      - yield (
        - "AUDIT"
        - audit
       )

- main
  - create games df
    - df_games must have app_id, last_retrieved_timestamp, last_retrieved_cursor obtained from joining and ordering loadOrchestratorReviews with loadControlReviews
    - this replaces *checkControl* (including the logic of assigning the default cursor)
  - initialise audit:
    - global_execution_start
    - create an orchestration_id (uuid from global execution start?)
    - *updateOrchestrator* (audit.app_id, audit.execution_type, audit.execution_status, audit.execution_id, audit.execution_start_time)
  - try
    - rdd_results = df_games.repartition(8).rdd.mapPartitions(processReviews)
    - df_results = spark.createDataFrame(rdd_results, schema=schema)
    - df_raw_reviews = df_results.filter(col("tag") == "DATA")  
    - standard delta table merge logic into bronze.steamReviews
  - except:
  - finally:
    - df_audit = df_results.filter(col("tag") == "AUDIT")
    - df_audit.write.format("delta").mode("append").save("Tables/control/loadcontrolreviews")
    - *updateOrchestrator* (audit.app_id, audit.execution_type, audit.execution_status)

- ! BIG PROBLEM: The above does not save anything until the finally block, yet i save the last cursor. Meh

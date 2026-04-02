# Steam Reviews

## Fields

- `recommendationid` (steam's unique id for the reviwe)
- `playtime_forever` (total playtime of the game)
- `playtime_last_two_weeks`
- `playtime_at_review`
- `review` (text)
- `timestamp_created`
- `timestamp_updated`
- `voted_up` (boolean)
- `weighted_vote_score` helpfulness score
- `votes_up` / `votes_funny` / `comment_count` metrics to see community favorite reviews
- `refunded`
- `written_during_early_access`

## Bronze

### API

- [Official Documentation](https://partner.steamgames.com/doc/store/getreviews)

#### Considerations

- The steam storefront api is a public facing endpoint where reviews can be obtained on a per-game basis
- This endpoint is not documented in terms of rate limits, but it seems a decent margin is 200 requests every 5 minutes (meaning 20k reviews total)
- Steam provides a cursor that can be used for pagination

#### Limitations

- I chose to sort reviews by the date they were created. Steam provides them in chronologically descending order, up to 100 reviews per batch
- I am running this on a fabric trial capacity, meaning I only have 1 cluster. Therefore I went out of my way to not spool up any cluster for review retrieval
- This is the main reason why I chose to create a fabric warehouse called IGDBAudit for logging and pagination

#### Expected Behaviour

- This process relies on IGDBAnalytics.steam.loadReviews, a view over an audit table (steam.loadControlReviews) and a manual table (steam.loadOrchestratorReviews)
- The manual table contains the games I wanted to extract reviews for. Fields of note include priority (highly rated games got the highest priority) and 'load_status'
- This script will extract the top {game_limit} highest priority games, sorted by the oldest extractions and prioritizing on-going loads. It will then save batches of up to 100 games, for {batch_limit} batches, saving them in {path_root} and logging the cursor in the control table
- There are up to {max_threads} extractions at the same time using concurrency
- This script is expected to run hourly
- A separate notebook runs every so often and ingests the raw .json files in bronze.steamReviews

#### Incremental

- For each **Daily** game in **Orchestration Table**, get the last review timestamp from the bronze layer
- Call the Steam API to get reviews for the game, using the cursor for pagination, sorted by recent
  - break out of the api loop when the review timestamp gets older than the last review timestamp from bronze

#### Initial Load

- Hourly
- Take {game_limit} games:
  - sorted by priority and 'oldest' executions
  - with **pending load_status** in **Orchestration Table**
- Call the Steam API to get reviews for each game, using the cursor for pagination, sorted by recent
  - Perform {batch_limit} batches
  - Save details in **Control Table**

### Tables

#### Orchestration Table

| column | type | description |
|---|---|---|
| ``app_id`` | int | The Steam Application ID for the game. |
| ``priority`` | string | The priority of the game for review retrieval (e.g., 'high', 'medium', 'low'). |
| ``schedule`` | string | The frequency at which the game's reviews should be checked (e.g., 'daily', 'weekly', 'monthly'). |
| ``load_type`` | string | Type of load (e.g., 'initial', 'incremental'). |
| ``load_status`` | string | Status of the initial load (e.g., 'complete', 'in-progress', 'pending', 'skip', 'empty') |
| ``last_execution_id`` | int | The last successful ``execution_id`` |
| ``last_execution_time`` | datetime | Timestamp of the last successful execution. |

#### Control Table

| column | type | description |
|---|---|---|
| ``execution_id`` | int | A simple incremental key |
| ``app_id`` | int | The Steam Application ID for the game. |
| ``execution_start_time`` | datetime | Timestamp when the data retrieval process started for this app_id. |
| ``execution_duration`` | int | Duration in seconds for the data retrieval process for this app_id. |
| ``execution_type`` | string | Type of execution (e.g., 'initial', 'incremental', 'full'). |
| ``execution_status`` | string | Status of the last execution (e.g., 'success', 'failed + error message', 'in-progress', 'empty'). |
| ``retrieved_reviews`` | int | Number of reviews retrieved in the last execution. |
| ``last_retrieved_timestamp`` | unix | The timestamp of the most recent review retrieved for this app_id. |
| ``last_retrieved_cursor`` | string | The cursor value from the Steam API for the last retrieved batch of reviews. |
| ``output_path`` | string | Path where the raw review data was stored. |

#### Table Values

| value | column | table | meaning |
|---|---|---|---|
| ``pending`` | ``load_status`` | Orchestration | The game is awaiting its initial load of reviews. |
| ``in-progress`` | ``load_status`` | Orchestration | The game's reviews are currently being loaded. |
| ``completed`` | ``load_status`` | Orchestration | The game's initial load of reviews has finished. |  
| ``skip`` | ``load_status`` | Orchestration | Game not considered for this load type. |  
| ``empty`` | ``load_status`` | Orchestration | An execution was run but it retrieved no reviews. |
| ``failed`` | ``load_status`` | Orchestration | An execution was run but it failed. |  
| ``retry`` | ``load_status`` | Orchestration | Manual status set when I debugged a game and want to reload it. |  
| ``success`` | ``execution_status`` | Control | Execution was successful. |  
| ``failed + {message}`` | ``execution_status`` | Control | Execution failed + captured failure message. |  
| ``aborted`` | ``execution_status`` | Control | Run encountered 403 and was aborted to prevent IP locking. |  
| ``in-progress`` | ``execution_status`` | Control | Execution currently running. |  
| ``empty`` | ``execution_status`` | Control | Execution returned no reviews. |  
| ``initial`` | ``execution_type``/``load_type`` | Control/Orchestration | Initial historical load |  
| ``full`` | ``execution_type``/``load_type`` | Control/Orchestration | Full load (todo: up until a specific time?) |  
| ``incremental`` | ``execution_type``/``load_type`` | Control/Orchestration | Incremental load |  

### Documentation

- <https://partner.steamgames.com/doc/store/getreviews>

## Silver

- For my goals I don't need to care about review history, so I will be SCD 1
- Heavy emphasis on cleaning reviews in preparation for nlp (not sure what this looks like yet)

## Gold

- I will have a fact table for reviews. This might need some extra stuff from steam tbd
  - I also really wanna play with NLP
- Could treat reviews that are in the top percentiles of playtime / voted_up / voted_helpful as more important
- I'll enrich either the obt or a new table with what aggregates i can get
  - for example: total reviews, % positive or maybe % ratio (good to bad)
- Pay extra attention to data skew. Almost guaranteed to need to salt joins here

## Modeling

- Might need to model game version and updates (not sure if igdb has this). This is in order to help with reviews tied to moments in time (i.e.: grr this patch sucks)

- any modeling i do should support the addition of further stats, especially stuff like "concurrent users" or whatnot because, well, of course. lol. but so far that's out of scope

- what are my facts and dimensions? so far I have
- perhaps i should create an 'aggregate scores' fact? where the logic of the aggregate scores lives (the one from the obt table)
  - do i enrich it with fact review aggregates or do i keep that for the obt?
  
| fact | dimension |
|---|---|
| (todo)review | game |
| (maybe)scores | genre |
| | theme |
| | platform |
| | (todo lol)date |

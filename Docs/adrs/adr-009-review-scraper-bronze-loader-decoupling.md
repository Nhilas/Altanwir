# ADR-009: Review scraper / Bronze loader decoupling via landing-zone queue

**Date:** 2026-04-13

## Context

The Steam Reviews pipeline has two stages with different operational profiles. The extractor (`NB_Steam_Reviews` running inside `pl_Steam_API`) is a plain-Python API client against the Steam Storefront endpoint with no DataFrame work and no Spark dependency. The Bronze loader (`NB_Steam_Reviews_Bronze` running inside `pl_Steam_Reviews_Medallion_Incremental`) is a Spark upsert against the `bronze.steamReviews` Delta table.

Three operational constraints push these stages apart:

- First, Steam's Storefront API does not document its rate limits, so the pipeline treats the budget as resetting hourly and scrapes on that cadence. The parameter cell (`game_limit`, `batch_limit`, `max_threads`, `jitter`) gives knobs for winging-it calibration when the contract is silent. The upsert wants the opposite: daily is enough.
- Second, under one-cluster-at-a-time trial capacity, every Spark spin-up costs two to four minutes of wall clock. Bundling the API fetch into the same notebook as the upsert would force a cluster spin-up on the extractor's hourly cadence, which is wasteful given the API fetch never touches Spark.
- Third, the two stages have different failure profiles. The extractor needs aggressive retry on rate-limits and a 403 kill switch (Steam treats 403 as IP-suspect). The upsert needs Spark-side concurrency control and Delta MERGE semantics. Coupling them means a Steam-side flap can take down the Bronze MERGE schedule, and vice versa.

A direct hand-off (extractor writes Delta, loader reads Delta) would force the extractor onto a Spark cluster. The opposite hand-off (extractor calls the loader inline) collapses the cadence advantage and chains the failure modes together.

## Decision

Decouple the two stages through a landing zone plus an audit-queue handshake:

- The extractor writes batched JSON to OneLake under `/Files/Steam/Reviews/{load_type}/{run_id}/{execution_id}/` and records the execution in `IGDBAudit.steam.loadControlReviews` with `is_loaded = 0`.
- The Bronze loader polls `loadControlReviews WHERE execution_type = 'incremental' AND is_loaded = 0 AND retrieved_reviews > 0`, upserts the corresponding JSON batches into `bronze.steamReviews` (using `whenMatchedUpdate` keyed on `recommendationid` plus `whenNotMatchedInsert`), and flips the flag to `is_loaded = 1` only after a successful upsert.
- The queue lives in the Fabric SQL Warehouse audit plane (`IGDBAudit`). Both stages access it through pyodbc; neither read nor write requires a Spark cluster.

The two stages share no Spark session and rendezvous only through the `is_loaded` flag.

## Rationale

The landing zone works as a queue. The JSON files are the durable hand-off; the `is_loaded` flag is the synchronisation primitive. That split keeps *what was scraped* (data, in OneLake) separate from *what has been loaded* (state, in the audit warehouse), so each stage evolves its own concurrency, retry, and replay logic without shared compute.

Putting `is_loaded` in the audit warehouse rather than as a Delta column matters for the same reason ADR-002 keeps the CDF watermark off Delta. An extractor that needs a Spark cluster just to mark its work done would erase the cluster-avoidance benefit; pyodbc against the Fabric SQL Warehouse cuts the audit read/write to a few hundred milliseconds.

Schema resilience comes in two pieces. The landing zone holds raw JSON files with no DDL contract at all, so the extractor lands evolving Steam payloads without coordination. The Bronze table absorbs the same churn by storing the whole record as a single `review_json STRING` column. Parsing is deferred to Silver, where `from_json` against a managed schema turns an unexpected field into a recoverable failure at the Silver boundary instead of a Bronze upsert break.

## Trade-offs

**Gained.** Independent cadences: hourly scrape, daily upsert. No Spark spin-up on the extractor side. Schema-resilient Bronze that absorbs Steam API churn. Replay is trivial: zeroing `is_loaded` for a set of executions re-runs the upsert without re-scraping. Failure isolation runs both ways. Extractor outages do not block the Bronze pipeline from clearing the existing queue, and loader outages let the extractor keep scraping into the landing zone.

**Lost.** Two-stage complexity. A failed flip-to-1 leaves a partially-loaded execution in an ambiguous state, reconciled by inspection or re-replay. It stays observable in `loadControlReviews`, so silent corruption is unlikely. Eventual consistency: a review scraped at hour H is queryable only at the next daily upsert, up to ~24h later. Storage cost: raw JSON files pile up until cleaned, and cleanup is unbuilt. Operational surface grows by one audit table (`loadControlReviews`) and one rendezvous semantic to learn.

## Reversibility

Moderate. The pattern is local to the Steam pipeline. Reverting would mean consolidating the extractor and loader into one Spark notebook and pushing the queue into a Delta column. The audit-warehouse rendezvous would retire with no upstream or downstream contract changes. ADR-002 covers the CDF-watermark half of the same audit plane (separate concern, same warehouse) and would survive any reversal here.

## References

- Extractor: `Fabric/NB_Steam_Reviews.Notebook/notebook-content.py`
- Loader: `Fabric/NB_Steam_Reviews_Bronze.Notebook/notebook-content.py`
- Queue view: `Fabric/IGDBAudit.Warehouse/dev/Views/loadReviews.sql`
- Pipeline narrative: [`../architecture/overview.md`](../architecture/overview.md) §Layer contracts §Bronze, §Engineering patterns §Extraction
- Watermark half of the audit plane: [`adr-002-cdf-incremental-audit-warehouse.md`](adr-002-cdf-incremental-audit-warehouse.md)

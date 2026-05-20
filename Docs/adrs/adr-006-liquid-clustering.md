# ADR-006: Liquid Clustering over Hive Partitioning and Z-Order

**Date:** 2026-04-14

## Context

Bronze, Silver, and Gold review Delta tables need physical data organisation that serves both the MERGE write path (keyed on `recommendationid` or `reviewKey`) and the Gold read path (`GROUP BY gameKey` for `factGameScores`). The `app_id` / `eId` dimension has ~15,000 distinct values with heavy skew. Counter-Strike alone accounts for 2.5M of 71M reviews.

## Decision

Use liquid clustering on all review-layer Delta tables with cluster keys aligned to the dominant predicate:

- `bronze.steamReviews` → `recommendationid` (MERGE key)
- `silver.steamReviews` → `reviewKey` (MERGE key; SHA-256 hash distributes evenly for liquid clustering)
- `gold.factReviews` → `gameKey` (read path: `factGameScores` group-by)
- `gold.factGameScores` → no clustering (~30k rows; clustering buys nothing)

`CLUSTER BY (col)` must appear before `TBLPROPERTIES` in the DDL. Spark SQL's parser enforces this ordering. OPTIMIZE runs as a separate scheduled maintenance notebook (see [decisions.md](../decisions.md)), not inlined with MERGE.

## Rationale

Hive partitioning on `app_id` would create ~15,000 partition folders, most containing tiny files, the classic small-files anti-pattern that degrades both metadata operations and read performance. Z-Order was rejected because it rewrites *all* files on every OPTIMIZE, regardless of which rows changed; with incremental CDF writes touching a few thousand rows per run, the rewrite cost is disproportionate. Liquid clustering is incremental: `OPTIMIZE` only touches unclustered files since the last run, and new writes automatically cluster into the right file groups. The tightness of the cluster decays as ~1/∛N across additional columns, but independent columns (like a SHA-256 hash and a game key) tile cleanly in N-D space without wasting budget.

## Trade-offs

**Gained.** MERGE on `reviewKey` reads only the file groups that contain the matching keys. No full-table scan. Gold `factGameScores` GROUP BY on `gameKey` benefits from file-level skipping. OPTIMIZE is incremental, not a full rewrite. Physical table state at trial expiry: Bronze 24.7 GB / 30 files, Silver 33.9 GB / 110 files, Gold factReviews 20.8 GB / 31 files. File counts are consistent with liquid clustering compaction.

**Lost.** Liquid clustering is a Databricks/Fabric-specific feature and does not port to vanilla Delta OSS. `ALTER TABLE ... CLUSTER BY` is unsupported in Fabric; clusters are immutable post-create. Changing a cluster key needs dropping and recreating the table. Cluster-key choice at create time is therefore a commitment.

## Reversibility

Moderate. Switching to Z-Order means removing `CLUSTER BY` from DDL and adding `ZORDER BY` to OPTIMIZE calls, but OPTIMIZE would then rewrite all files on every run. Switching to Hive partitioning needs a full table rebuild with `PARTITIONED BY`. The underlying data and MERGE logic are unaffected by any of these changes; the cost is the physical reorganisation.

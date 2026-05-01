---
name: CDF Incremental Pattern — Silver Reviews
description: Design decisions for Change Data Feed incremental mode on Silver Steam Reviews, version control table architecture, and Bronze audit logging
type: project
originSessionId: b99bf017-1050-4c23-adf7-f86fdd706201
---

# STATUS: TBD

## CDF Incremental Mode (implemented 2026-04-13)

### Version Control Table (`versionControl` in IGDBAudit warehouse)

DDL (T-SQL, not Spark SQL — table lives in Fabric Warehouse):
- `table_name VARCHAR(50)` — stores Silver target path (e.g., `IGDBAnalytics_Dev.silver.steamreviews`)
- `run_id VARCHAR(50)`
- `change_type VARCHAR(30)` — from Delta history `operation` field
- `commit_version BIGINT` — Silver table version after MERGE
- `commit_timestamp DATETIME2(3)` — from Delta history `timestamp`
- `rows_inserted BIGINT`, `rows_updated BIGINT` — from `operationMetrics`, cast with `int()`
- `latest_source_version BIGINT` — **control column**: current Bronze version at time of Silver run
- `audit_notes VARCHAR(MAX)` — full Delta history row as `json.dumps(row.asDict(), default=str)`

**Key design decision:** This table serves dual purpose — audit (what happened to Silver) AND control (where to start reading Bronze next time). The `latest_source_version` column is the CDF bookmark.

**Why:** `DESCRIBE HISTORY` provides audit data, but there's no built-in way to track "last processed source version" across tables. The control column bridges this gap.

### CDF Read Pattern

```python
if load_type == 'incremental':
    latest_source_version = check_version(table_name=target_path)  # queries by SILVER table name
    current_source_version = source_table.history(1).select("version").collect()[0][0]

    if latest_source_version is None:
        # First incremental with no audit history → fall back to full read
    elif current_source_version == latest_source_version:
        notebookutils.notebook.exit("No new version to process")  # clean exit, pipeline sees "succeeded"
    else:
        df = spark.read.format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingVersion", latest_source_version + 1) \  # +1 because inclusive
            .table(source_path) \
            .filter(f.col("_change_type").isin("insert", "update_postimage"))
```

### Key Gotchas Encountered

1. **CDF is not retroactive** — enabling via `ALTER TABLE SET TBLPROPERTIES` only records changes from that version onward. Older versions have no change files.
2. **`startingVersion` is inclusive** — must add +1 to the last processed version, otherwise reprocesses.
3. **`_change_type` filter required** — without it, `update_preimage` (old values) are included alongside `update_postimage` (new values). Dedup window handles this by accident (older timestamp loses), but explicit filter is cleaner.
4. **`notebookutils.notebook.exit()`** — clean exit for "nothing to process" case. Shows as succeeded in Fabric pipeline.
5. **Version stored is Bronze's version, queried by Silver's name** — `insert_version` writes `table_name = target_path` but stores `source_table.history(1)` version as `latest_source_version`. `check_version` queries by `target_path` and returns `latest_source_version`.

### Bronze Audit Logging (also 2026-04-13)

`insert_version` function added to Bronze notebook (`NB_Steam_Reviews_Bronze`) — logs Delta history metadata to `versionControl` after each MERGE. Same table, same pattern.

- `audit_row = target_table.history(1).collect()` — Row object, access fields via `['fieldName']`
- `operationMetrics` is a Map type — nested access: `audit_row[0]['operationMetrics']['numTargetRowsInserted']`
- Values come back as strings — cast with `int()` for BIGINT columns
- `datetime.datetime` from `.collect()` works directly with pyodbc — no string conversion needed
- Cannot use `?` placeholders for schema/table identifiers in pyodbc — only for data values (use f-string)

### gameKey Lookup (added 2026-04-13)

- `gameKey` added to Silver Reviews via inner join with `silver.externalGames` on `eId`
- `gameKey` included in hash — if IGDB remaps a game (e.g., early access → final), Silver self-heals on next run
- Join uses string form (`"eId"`) which auto-deduplicates the join column
- `f.broadcast(df_external_games)` — small dimension table, broadcast join avoids shuffle

**Why:** Simplifies Gold OBT — `gameKey` is already in Silver, no re-lookup needed.
**How to apply:** gameKey is in the hash. Don't exclude it from `columns_to_hash`.

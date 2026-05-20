# Spark and Delta references

_Last updated: 2026-05-04_

## `table_changes()` subquery parser workaround

**What:** In Spark SQL, column references inside a subquery that reads `table_changes()` resolve against the outer table, not `table_changes()`. Aliasing the subquery does not help.

**Why it bites:** CDF reads from Silver to Gold use `table_changes(source_path, startingVersion)` to get changed `gameKey` values. A subquery like `SELECT DISTINCT gameKey FROM table_changes(...)` silently reads `gameKey` from the outer table instead, returning the wrong set of keys for the incremental MERGE predicate.

**What to do:** Collect `gameKey` values via a separate `spark.sql()` call into Python, then build an explicit `WHERE gameKey IN (...)` predicate as a string. Two round-trips to Spark. The IN-list scaling limit has not been stress-tested beyond ~15k keys.

---

## CDF is not retroactive

**What:** Enabling CDF via `ALTER TABLE SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')` only records changes from that version onward. Pre-existing versions have no change files.

**Why it bites:** After enabling CDF on an existing table, a `readChangeFeed` with `startingVersion = 0` fails. There are no CDF files for historical versions. The first incremental run must fall back to a full read.

**What to do:** The notebooks handle this: when `check_version()` returns NULL (no audit history), the code falls back to a full read. When enabling CDF on an existing table, run a full load first to establish the baseline version in `versionControl`.

---

## `startingVersion` on CDF reads is inclusive

**What:** `spark.read.format("delta").option("startingVersion", N)` includes version N in the result set.

**Why it bites:** If the last processed version was N, reading with `startingVersion = N` reprocesses version N. Without the `+1`, the CDF pipeline double-processes the last batch on every run.

**What to do:** Always read with `startingVersion = latest_source_version + 1`. The `check_version()` / `insert_version()` pattern in the audit warehouse stores the last-processed source version; the `+1` is applied at read time.

---

## `pandas_udf` StructType column-order is positional, not by name

**What:** When a `pandas_udf` returns a `StructType`, Spark maps the returned columns by position in the `[[...]]` selector, not by column name.

**Why it bites:** If the `StructType` defines `{pos, compound, neu, neg}` but the pandas DataFrame returns columns in a different order (e.g., `{compound, neg, neu, pos}`), the values are silently swapped. VADER sentiment `pos` would be read as `compound`.

**What to do:** Ensure the column order in the `[[col1, col2, ...]]` selector on the returned pandas DataFrame exactly matches the order of fields in the `StructType` definition. No reordering or renaming logic exists. Position is the contract.

---

## `DESCRIBE` is not subqueryable in Spark SQL

**What:** `DESCRIBE TABLE` cannot be used as a subquery in Spark SQL. It is a metadata command, not a table-returning expression.

**Why it bites:** Attempting to extract specific metadata columns (e.g., `operationMetrics` from Delta history) via a `SELECT ... FROM (DESCRIBE HISTORY ...)` pattern fails with a parse error.

**What to do:** Use `createOrReplaceTempView` on the result of the metadata command, then query the temp view. Alternatively, skip SQL entirely and use the returned DataFrame's `.select()` with map column access: `f.col("operationMetrics")["numOutputRows"]`.

# ADR-002: CDF Incremental with Audit Warehouse as Control Plane

**Date:** 2026-04-13

> **Scope boundary:** this ADR covers the CDF watermark in `versionControl` only. The landing-zone + `is_loaded` audit-queue half of the same pipeline is in [ADR-009](adr-009-review-scraper-bronze-loader-decoupling.md).

## Context

Silver and Gold notebooks need to know where to start reading the upstream Delta table on each incremental run. Delta Lake has no native cross-table watermark — `DESCRIBE HISTORY` exposes audit data for the table that was written, but not which version of the *source* table was last consumed. The Fabric trial cluster allows only one cluster at a time; spinning up Spark just to read a single integer watermark wastes 2–4 minutes of cluster startup.

## Decision

Store the CDF bookmark in `IGDBAudit.steam.versionControl`, a Fabric SQL Warehouse table accessed via pyodbc + PBI OAuth bearer token — not a Delta table in the Lakehouse. The `versionControl` table serves a dual purpose: audit record (what happened to Silver/Gold on each MERGE) **and** control bookmark (`latest_source_version` stores the Bronze/Silver version that was last processed). Every notebook run calls `check_version(table_name=target_path)` via pyodbc before touching Spark, and calls `insert_version()` after each MERGE to write the new watermark plus full Delta history metadata as `audit_notes` JSON.

CDF reads use `spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", latest_source_version + 1)`, filtered to `('insert', 'update_postimage')`. When no new version exists, the notebook exits cleanly via `notebookutils.notebook.exit()`, which shows as "succeeded" in the pipeline.

## Rationale

Three alternatives were considered for watermark storage: a Delta table in the Lakehouse (requires Spark to read), a Spark config/session variable (lost between runs), or the audit warehouse (reachable via pyodbc without a cluster). Under the one-cluster-at-a-time trial constraint, eliminating a cluster spin-up for every watermark check was load-bearing — especially in the incremental pipeline, which chains four notebooks with 120-second "poverty waits" between each. The dual-purpose design of `versionControl` avoids maintaining separate audit and control tables for what is fundamentally the same event: "this target processed up to this source version."

## Trade-offs

**Gained:** No cluster spin-up for watermark check or audit writes. Audit and control data queryable from T-SQL tooling independently of Spark. IN-clause chunking at ~1,000 rows stays under SQL Server's 2,100-parameter ceiling. CDF incremental ran 15,505 inserts in 2.85 minutes vs 5.74 minutes for the 70.9M-row full load — roughly 50× faster at Gold.

**Lost:** Cross-system dependency — if the warehouse is unavailable, incremental cannot proceed. Pyodbc adds a driver dependency and credential-plumbing step (PBI bearer token encoded as UTF-16-LE struct via `attrs_before`). The `latest_source_version` column stores the *source* version keyed by *target* path name, which is non-obvious to a reader encountering it cold.

## Reversibility

Moderate. Switching to a Delta-based watermark table requires replacing every `check_version` / `insert_version` call with Spark reads/writes and accepting the cluster spin-up cost. The `versionControl` DDL and pyodbc connection code live in a shared utility function — changes propagate to all notebooks from one place.

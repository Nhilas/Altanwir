# ADR_003: Dimensional Gold over Array-Shaped OBT

**Status:** Accepted
**Date:** 2026-04-16
**Project:** Altanwir — Steam Reviews × IGDB analytics on Microsoft Fabric

## Context

The Gold layer exposes game-level analytics enriched with M:M dimensions (genres, platforms, themes). Initial design followed the cloud DWH OBT convention: one row per game, dimensions stored as arrays via `collect_list` from Silver bridge tables. This pattern is idiomatic in Snowflake, BigQuery, and Databricks — engines that surface complex types natively.

During implementation, the array columns failed to materialize in the SQL analytics endpoint. Microsoft Learn confirms: complex types (`ARRAY`, `STRUCT`, `MAP`, `VARIANT`) are not currently supported in Fabric's SQL analytics endpoint. The arrays exist in Delta storage; T-SQL consumers cannot see them.

## Decision

Replace the array-shaped OBT with a dimensional model in Gold:

- `gold.factGameScores` — fact at game grain, all flat columns
- `gold.aggGamesByGenre` / `Platform` / `Theme` — pre-aggregated marts for headline single-dim queries
- `gold.dim*` and `gold.bridge*` — pass-through views over Silver (single source of truth, no duplication)
- `gold.vwGameCatalog` — exploded game × dim catalog for filter-then-aggregate workflows
- `gold.factReviews` (review grain) — unchanged

## Rationale

Workarounds were considered and rejected. Materialized Cartesian product inflates row counts and forces `COUNT(DISTINCT gameKey)` on every aggregation. JSON-string-plus-`OPENJSON` loses columnar performance and indexing. Bypassing the SQL endpoint cuts off T-SQL and Power BI Direct Lake consumers. Dimensional modeling is the textbook answer to M:M analytics, and the bridge tables in Silver were already shaped for it.

## Trade-offs

**Gained:** Pure T-SQL surface. Power BI Direct Lake compatible. Clean separation of fact / dim / bridge. Per-dim marts answer the most common analytical questions with zero joins. Bridges remain in Silver as single source of truth.

**Lost:** More physical Gold objects (5 tables + 7 views vs. 1 OBT). Cross-dim queries require an explicit CTE pattern via `vwGameCatalog` joining to `factGames`.

**Reversibility:** High. Underlying Silver model is unchanged. Gold can be rebuilt to a different shape if the platform constraint lifts in a future Fabric release.
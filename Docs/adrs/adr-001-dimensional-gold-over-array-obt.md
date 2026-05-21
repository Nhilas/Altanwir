# ADR_001: Dimensional Gold over Array-Shaped OBT

**Date:** 2026-04-16

## Context

The Gold layer exposes game-level analytics enriched with M:M dimensions (genres, platforms, themes). The initial design was OBT: one row per game, dimensions stored as arrays via `collect_list` from the Silver bridge tables. That pattern is idiomatic in Snowflake, BigQuery, and Databricks, engines that surface complex types natively.

During implementation, the array columns failed to materialize in the SQL analytics endpoint. Microsoft Learn confirms: complex types (`ARRAY`, `STRUCT`, `MAP`, `VARIANT`) are not currently supported in Fabric's SQL analytics endpoint. The arrays exist in Delta storage; T-SQL consumers cannot see them.

## Decision

Replace the array-shaped OBT with a dimensional model in Gold:

- `gold.factGameScores`: fact at game grain, all flat columns
- `gold.factReviews` (review grain): unchanged
- `gold.vw_agg*`: view marts for single-dim queries
- `gold.vw_dim*` and `gold.vw_game*`: pass-through views over Silver dims and bridges, mapping games to dimensions
- `gold.vw_gameCatalogue`: exploded game × dim catalog for filter-then-aggregate workflows

## Rationale

Three workarounds didn't survive review. A materialized Cartesian product inflates row counts and forces `COUNT(DISTINCT gameKey)` on every aggregation. JSON-string-plus-`OPENJSON` loses columnar performance and indexing. Bypassing the SQL endpoint cuts off T-SQL and Power BI Direct Lake consumers. Dimensional modeling was the move; the bridge tables in Silver were already shaped for it.

## Trade-offs

**Gained.** Pure T-SQL surface. Power BI Direct Lake compatible. Fact / dim / bridge stay separated. Bridges stay in Silver as single source of truth.

**Lost.** More physical Gold objects (2 tables + 13 views vs. 1 OBT). Cross-dim queries need the explicit CTE pattern via `vw_gameCatalogue` joining to `gold.factGameScores`.

## Reversibility

High. The underlying Silver model is unchanged. Gold can be rebuilt to a different shape if the platform constraint lifts in a future Fabric release.

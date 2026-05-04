# ADR-004: Percentile Columns Excluded from Fact Tables — Computed in Views

**Status:** Accepted
**Date:** 2026-04-16
**Project:** Altanwir — Steam Reviews × IGDB analytics on Microsoft Fabric

## Context

Gold game-grain analytics include percentile-rank columns (`IGDBRatingPercentile`, `weightedSentimentPercentile`, `weightedVotePercentile`) and tier bands (S/A/B/C/D/F) derived from them. Early builds materialised percentiles directly in `gold.factGameScores`. A `WHERE` clause against a fact table containing materialised percentiles produces percentile-of-percentile — silently wrong results, because the percentile was computed over the full population but is now being read over a filtered subset.

## Decision

Percentile and tier columns live exclusively in `gold.vw_factGameScores` (and analogous serving views), not in the `factGameScores` Delta table. The fact stores raw smoothed scores at full precision; the view computes `PERCENT_RANK()` windows and tier bands at read time.

## Rationale

Percentiles are non-additive measures in the Kimball sense: they are cohort-dependent and become meaningless after any filter that changes the population. The `targeted` load type — which merges a small subset of games — proved this concretely: `percent_rank()` windows ran over the filtered subset (e.g., 3 games), producing percentile values that bore no relation to the full ~30k-game population. If percentiles were in the fact, every targeted reload would silently corrupt them for the affected games until a full reload recalculated the windows. Moving percentiles to the view means they are always computed over whatever population the consumer's query addresses — correct by construction.

## Trade-offs

**Gained:** No silent percentile corruption from partial loads. Consumers can filter before the window runs, getting cohort-appropriate percentiles. Tier bands recalibrate to any subset without ETL intervention.

**Lost:** View must be refreshed (Spark view, so read-time cost). Consumers cannot pre-compute cross-percentile joins against the materialised fact. The view is Spark-only — the SQL analytics endpoint requires a separate T-SQL definition of the same logic.

## Reversibility

Low cost. Moving percentiles back into the fact requires adding the columns to `gold.factGameScores`, recomputing on every MERGE (including targeted), and accepting the percentile-of-percentile risk on filtered queries. The view logic already exists and would simply be duplicated into the MERGE CTE.

# ADR-008: Store Wide, Expose Narrow via View

**Date:** 2026-04-16

## Context

During development, the full set of analytical questions the model would need to answer was unknown. Gold fact tables evolved iteratively (new signal columns, ratio variants, intermediate values), and each iteration raised the question of what to persist in the Delta table versus what to compute at read time. Adding a column to a Delta table is a schema operation that triggers a full table rewrite on the next OPTIMIZE; removing a column is worse.

## Decision

`gold.factGameScores` stores all raw and intermediate values at full precision. No rounding, no scaling, no labels, no tier assignments. Presentation logic lives only in `gold.vw_factGameScores`: `× 100` scaling for percentages, `ROUND()` for display, `CASE WHEN` for S/A/B/C/D/F tier bands, (for example [`steamRatingLabel`](../../Fabric/IGDBAnalytics.SQLEndpoint/gold/Views/vw_factGameScores.sql#L75) ), and Capital Case formatting for `sentimentLabel`. Labelling and presentation-logic decisions (#21 in triage) follow directly from this pattern: `sentimentDirection` in the fact is NULL-aware and direction-only; the human-readable tiers and Steam labels exist only in views.

## Rationale

Schema changes on Delta tables are expensive. Column additions widen every Parquet file, and column removals force a rewrite. View columns cost nothing to add, rename, or remove. On a trial cluster with no rollback safety net, the wide-fact/narrow-view split meant presentation experiments (different tier thresholds, different label schemas, different rounding) never touched the underlying data. The view layer became the cheapest experimentation surface, and the fact stayed stable and analytically complete.

## Trade-offs

**Gained.** The fact table is a complete analytical record. Any future question can be answered from the raw values without ETL changes. Presentation changes (label wording, tier boundaries, scaling, column selection) are view-only, zero-downtime, zero-cost. Twelve columns were dropped from the OBT design during the Apr 14–16 pivot (see [overview.md §Data model](../architecture/overview.md#data-model)). None of those would have been needed if the fact had been wide from the start.

**Lost.** A consumer must use the view (or replicate its logic) to get human-readable output. The fact table's raw values are not directly presentable. The wide table includes intermediate values (e.g., `weightedSentiment`, `weightedVote`, `pctWeightedSentiment`, `pctWeightedVote`) that are useful for auditing the formula but irrelevant to most consumers.

## Reversibility

High. The fact table's schema is additive; no information is lost. Moving any view column into the fact needs the column added to the MERGE, a simple schema change. Moving the other direction (fact → view) needs a check that no downstream consumer reads the column from the fact before removing it.

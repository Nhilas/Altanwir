# ADR-005: Adaptive Salting on Hot Keys

**Date:** 2026-04-21

## Context

Gold review-grain processes groups on `gameKey` to compute per-game normalisation statistics (`max_votesUp`, `max_reviewLength`, `playtimeSignal = percent_rank()`). Counter-Strike has 2.5 million reviews; the median game has a few hundred. Without intervention, the `GROUP BY gameKey` shuffle concentrates Counter-Strike's 2.5M rows onto a single Spark task while thousands of small-game tasks finish in milliseconds. The result is pathological skew that stalls the stage.

## Decision

Apply compute-side salting only on hot keys: when a `gameKey` exceeds `salt_threshold` (default 50,000 reviews, pipeline-tunable), assign `salt = floor(rand() * salt_factor)` (default 32). Per-game statistics aggregate first on `(gameKey, salt)`, then re-aggregate on `gameKey`. Cold keys (the vast majority) are unsalted and pay no shuffle cost. Write-side file skew in `gold.factReviews` (1.1 GB vs 180 MB files) is left alone. It reflects real data distribution, and liquid clustering handles read-time skipping regardless.

## Rationale

Uniform salting (salt every key) would scatter games with 12 reviews across 32 near-empty partitions, multiplied by ~30k games. The shuffle cost would exceed the skew it prevents. The threshold-based approach targets only the keys that actually cause skew. Per-key adaptive salting (salt factor proportional to each key's cardinality) is deferred to silver_v2; the current threshold-based approach handles the ~50 hot keys well enough at 71M scale.

## Trade-offs

**Gained.** Counter-Strike's 2.5M-review GROUP BY completes without stalling the stage. `salt_threshold` and `salt_factor` are tunable at pipeline level (`gold_reviews_salt_threshold`, `gold_reviews_salt_factor`). No code change to adjust.

**Lost.** Salting reduces but does not eliminate skew. Post-salting, the `factReviews` MERGE (Stage 41) still shows a 24× max/median per-task ratio in Spark UI. Fabric auto-diagnostics independently flagged it as a data-skew stage. The two-pass aggregation (salt-level → game-level) adds a shuffle stage for hot keys. Silver GROUP BY shuffles more than necessary for non-skewed keys. Per-key adaptive salting would address this, but it is deferred.

## Reversibility

High. Removing salting means deleting the conditional salt assignment and the two-pass aggregation CTE in `NB_Steam_Reviews_Gold`, then re-running the ~71M-row Gold MERGE. The pipeline parameters (`salt_threshold`, `salt_factor`) would become unused.

# ADR-004: Tier Bands Computed in Serving Views, Not Materialised in the Fact

**Date:** 2026-04-16

## Context

Gold game-grain analytics expose tier bands (S/A/B/C/D/F) and rating labels over the smoothed scores in `gold.factgamescores`. These bands can be materialised as columns in the fact or computed at read time in the serving view. The choice decides whether a threshold change forces an ETL re-MERGE, and whether the band stays correct when a consumer filters the population.

## Decision

Tier and label columns live exclusively in `gold.vw_factGameScores` (and analogous serving views), not in the `factGameScores` Delta table. The fact stores raw smoothed scores at 0-1 precision; the view scales them to 0-100 and derives `IGDBRatingTier`, `weightedSentimentTier`, `weightedVoteTier`, and `steamRatingLabel` with `CASE` thresholds at read time.

## Rationale

Tier bands are a presentation concern, not a stored measure. Materialising them in the fact would couple the persistence layer to the display thresholds: a single threshold change (say, S at 93 instead of 95) would force a recompute and re-MERGE across the ~30k-game fact. In the view, the same change is a view redefinition with no data movement.

The bands use absolute thresholds on the smoothed rating rather than percentile rank. A percentile is cohort-dependent: `percent_rank()` over a filtered subset bears no relation to the rank over the full ~30k-game population, so a `targeted` load merging a handful of games would compute percentile-of-percentile and silently corrupt the bands. An absolute threshold gives a game the same tier whether it is read alone or alongside the full population.

## Trade-offs

**Gained.** No presentation logic baked into the fact. Threshold changes stay view-only, with no ETL re-MERGE. Bands stay correct under any filter because the threshold is absolute, not cohort-relative. The fact keeps full 0-1 precision.

**Lost.** The view recomputes scaling and the `CASE` bands on every read, a read-time cost paid per query rather than once at write. Consumers cannot read a pre-materialised tier straight off the fact.

## Reversibility

High. Materialising bands back into the fact means adding the columns to `gold.factgamescores`, computing the `CASE` logic in the Gold MERGE, and re-running it on every threshold change. The view logic already exists and would move into the MERGE.

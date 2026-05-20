# ADR-003: Empirical Bayes Priors Derived from Data

**Date:** 2026-04-18

## Context

Game-grain rating columns (`smoothedIGDBRating`, `weightedSentimentRating`, `weightedVoteRating`) apply shrinkage so that small-sample games don't outrank well-reviewed titles on noise alone. Every smoothed column started at `prior = 0.5`, the textbook uninformative prior for a [0, 1] value. The resulting `smoothedIGDBRating` distribution was flat: nearly every game clustered between 57–62 regardless of genre, source count, or actual quality signal.

## Decision

Derive priors from the observed population means of the dataset, not from a textbook default. `smoothedIGDBRating` uses `prior ≈ 0.67` (mean `aggregatedRating / 100` across IGDB-rated games); `weightedSentimentRating` uses `prior ≈ 0.84` (influence-weighted mean `(weightedSentiment + 1) / 2`); `weightedVoteRating` uses `prior ≈ 0.89` (influence-weighted mean `(weightedVote + 1) / 2`). `steamVoteRating` keeps `prior = 0.5`. It scores `pctVotedUp`, a binary signal where 0.5 is a genuine indifference point (equal up- and down-votes), not the population mean.

The smoothing formula `observed - (observed - prior) × pow(2, -log10(N + 1))` and tier calibration are documented in [scoring-model.md](../architecture/scoring-model.md).

## Rationale

The flat 57–62 distribution was the prior doing exactly what it was told, not a rendering bug. Shrinking to 0.5 when the population mean is 0.68 pulls every small-sample game to a midpoint 18 points below where the population actually lives. The genre-independent clustering proved the prior was wrong: the smoothing curve was biased low, and the symptom was a tier system where everything landed in C. Recalibrating the tier thresholds without fixing the prior would have masked the problem; the prior was the root cause. Binary vote keeps 0.5 because a small-sample 100%-positive game shrinking toward 0.89 would imply a confidence the sample doesn't support.

## Trade-offs

**Gained.** Tier distribution spreads across the observed range. S-tier at `totalReviews ≥ 1,000` surfaces small curated games (A Short Hike, Tiny Glade, Fields of Mistria); Portal, Stardew Valley, and Hades land in A-tier (regression to mean with volume, not miscalibration). The 0.5 exception on binary vote preserves statistical honesty.

**Lost.** Priors are dataset-dependent. A substantially different corpus (e.g., filtered to a single genre) would need re-derivation. Per-genre priors are deferred to silver_v2.

## Reversibility

High. Priors are computed at runtime from `gold.factReviews` aggregates, not hard-coded. Switching back to 0.5 or adopting per-genre priors needs changing the prior-computation CTE in `NB_Game_Scores_Gold` and re-running the ~30k-row `factGameScores` MERGE. A sub-minute operation.

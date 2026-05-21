# ADR-007: Per-Game Normalisation for `reviewInfluenceScore` Components

**Date:** 2026-04-18

## Context

`reviewInfluenceScore` in `gold.factReviews` is a weighted blend of five signals (`communitySignal`, `lengthSignal`, `emotionalSignal`, `playtimeSignal`, `sentimentSignal`) that determines each review's contribution to game-grain aggregates. Several of these signals need a normalisation ceiling: `communitySignal` divides `log(votesUp + 1)` by `log(max_votesUp + 1)`, `lengthSignal` divides `log(reviewLength + 1)` by `log(max_reviewLength + 1)`, and `playtimeSignal` uses `percent_rank()`. The question is whether the max values and percentile window should be global (across all 71M reviews) or per-game.

## Decision

All normalisation in the `reviewInfluenceScore` formula is scoped per `gameKey`. `max_votesUp` and `max_reviewLength` are computed in the [`aux_silver`](../../Fabric/NB_Steam_Reviews_Gold.Notebook/notebook-content.py#L437) CTE; `playtimeSignal = percent_rank() OVER (PARTITION BY gameKey ORDER BY playtimeAtReview)`. The normalisation ceiling for each signal is the maximum within the game's own review population, not the global corpus.

The full formula shape lives in [scoring-model.md](../architecture/scoring-model.md).

## Rationale

Counter-Strike has 2.5 million reviews; a niche indie has 12. A global `max_votesUp` would be dominated by Counter-Strike's single most helpful review. Every indie's most helpful review would then normalise to near-zero, and small-audience reviews would go negligible in the influence-weighted aggregates. Per-game normalisation keeps reviews comparable within their own context. A 10-hour player of a short indie is a veteran; the same playtime on Skyrim is a tourist. The `COALESCE(... / NULLIF(log(max_col + 1), 0), 0)` guard covers the edge case where every review for a game has zero votes.

## Trade-offs

**Gained.** Small-game reviews are fairly weighted within their own context. The influence score is analytically meaningful within a game: a review's influence reads as how prominent that voice is among the game's own reviewers.

**Lost.** `reviewInfluenceScore` is not directly comparable across games of different scales. A 0.9 for a 12-review indie is not the same as a 0.9 for Counter-Strike. Per-game normalisation introduces CDF write-amplification on incremental loads. When 15,505 new reviews land, `max_votesUp` and `playtimeSignal = percent_rank()` shift for every affected game. The content hash then changes on every existing review of those games, and the MERGE rewrites them all. One CDF run ingesting 15,505 new reviews **rewrote 131,800 prior rows** in `gold.factReviews`. The cost is accepted: global normalisation would distort the entire small-game population. It also introduces salting complexity for hot keys (see [ADR-005](adr-005-adaptive-salting.md)).

## Reversibility

Moderate. Switching to global normalisation needs the `aux_silver` CTE changed to compute across the full `gold.factReviews` table, and the `PARTITION BY gameKey` removed from the percentile window. The write-amplification cost disappears (global max changes rarely), but the distortion for small games returns. Re-running the full Gold MERGE (~71M rows, ~12 minutes) applies the change.

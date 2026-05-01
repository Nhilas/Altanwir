---
name: gold.factGameScores build decisions
description: Design and data decisions for gold.factGameScores (game grain fact) built across 2026-04-16/17 — table/view architecture, label tiers, load type behaviors
type: project
originSessionId: 96b9a1a2-2602-445b-8de0-07bda3f0c54e
---
# STATUS: TBD

## Table name: factGameScores (notebook renamed to NB_Game_Scores_Gold on 2026-04-17)

`gold.factGameScores` is the game-grain fact. Merge key is `gameKey`. Primary feed to future marts.
## sentimentLabel values are Capital Case in gold.factReviews

Queries against `gold.factReviews.sentimentLabel` must use `'Positive'`, `'Neutral'`, `'Negative'` — NOT lowercase.

**Why:** Confirmed by the user during build. CLAUDE.md documents lowercase for Silver but Gold stores Capital Case values. Filter strings must match exactly or all pct sentiment columns silently return 0.

**How to apply:** Any query filtering on `sentimentLabel` must use Capital Case string literals.

---

## refunded column exists in silver and gold

`refunded` (boolean) is present in `silver.steamReviews` and passes through to `gold.factReviews`. Safe to reference in `factGameScores` aggregation as `pctRefunded`. Now documented in CLAUDE.md Silver columns and factReviews grain sections.

---

## voteLabel lower boundary is < 20, not <= 19

The "Very Negative" tier uses `voteRating < 20` to avoid a gap at 19.01–19.99.

**Why:** `<= 19` leaves a NULL gap for fractional values between 19 and 20. `< 20` closes it cleanly.

**How to apply:** All future CASE expressions over continuous float ranges should use `<` / `>` for boundary conditions, not `=` on a rounded value.

---

## Schema philosophy: store wide, expose narrow via view

`gold.factGames` stores raw intermediate values (e.g. `weightedSentiment` in -1/1 range, percentiles as 0–1 floats, raw counts). A serving view handles rounding, scaling (*100), casting, labeling, and column selection.

**Why:** Bottom-up development — the analytical questions weren't known upfront, so the table captures everything. Adding a column to a view is free; adding a column to a Delta table requires a notebook change + schema change + full rerun. Wide table buys flexibility during development.

**How to apply:** Don't suggest pruning raw/intermediate columns from the base table during development. The view is the contract; the table is the store. Inline SQL comments in the query document column semantics — this is intentional, not noise.

---

## voteLabel is tiered by totalReviews (Steam pattern)

`voteLabel` now branches on `totalReviews` before applying the voteRating boundaries — matches Steam's actual labelling behaviour which never shows "Overwhelmingly Positive" on a game with 12 reviews.

| totalReviews | Available labels |
|---|---|
| >= 500 | Overwhelmingly Positive (>=95), Very Positive, Mostly Positive, Mixed, Mostly Negative, Overwhelmingly Negative (<20) |
| 50–499 | Very Positive (>=80), Mostly Positive, Mixed, Mostly Negative, Very Negative |
| 10–49 | Positive (>=80), Mostly Positive, Mixed, Mostly Negative, Negative |
| < 10 | 'Insufficient Data' |

**Why:** Small-sample games shouldn't qualify for superlative labels. Steam's own heuristic is a good mental model — mirror it.

**How to apply:** Any label-over-continuous-metric decision should consider sample size as a gate, not just the metric value.

---

## targeted load type produces wrong percentile values

For `load_type = 'targeted'`, the `percent_rank()` windows in `game_ratings` and `percentile_review_stats` compute over the filtered subset (e.g. 3 games), not the full dataset. The percentile columns written for targeted rows are meaningless relative to the rest of the table.

**Why:** The window functions have no PARTITION BY and operate over the SELECT's source rows. Filtering in `silver_games` restricts the window's dataset.

**How to apply:** `targeted` is safe for testing merge mechanics only. For correct data, use `full` or `reload`. If targeted ever becomes a production need, percentiles would need to be recomputed against the full dataset separately.

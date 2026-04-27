---
name: gold.steamReviewStats design decisions
description: Key design decisions made during build of gold.steamReviewStats (2026-04-15) — normalization scope, VADER separation of concerns, score ceiling calibration
type: project
originSessionId: 2e995913-e814-48b3-965f-21dcdcc8339e
---
# STATUS: TBD

## Normalization scope: per-game, not global

`max_votesUp` and `max_reviewLength` in `game_stats` CTE are scoped per `gameKey`, not globally.

**Why:** Games have vastly different audience sizes. Counter-Strike's max votesUp would dwarf Terra Nil's, making niche game reviews appear negligible in absolute terms. Normalization should be relative to a game's own universe to be fair and meaningful.

**How to apply:** Do not suggest reverting to global max. The per-game decision is settled and reasoned.

---

## game_stats: no isUsableForVader filter

`max_votesUp` and `max_reviewLength` computed from ALL reviews, not filtered to `isUsableForVader = true`.

**Why:** The max is a normalization ceiling — it has nothing to do with VADER eligibility. Filtering would make the ceiling artificially low and could cause `communityWeight` or `lengthWeight` to silently exceed 1.0 if a non-VADER review has more votes/length than any VADER-usable one.

---

## Zero-division guard: COALESCE(NULLIF) pattern

Both `communityWeight` and `lengthWeight` use:

```sql
COALESCE(log(col + 1) / NULLIF(log(max_col + 1), 0), 0)
```

**Why:** If all reviews for a game have 0 votesUp, `log(1)/log(1) = 0/0 = NaN`. NULLIF converts the zero denominator to NULL, COALESCE converts the resulting NULL to 0.

---

## lengthWeight: no isUsableForVader filter

`lengthWeight` is now computed for all reviews — no `CASE WHEN isUsableForVader` guard. It is stored as a column for all reviews but is only included in `reviewInfluenceScore` for VADER-usable reviews (the score formula's non-VADER branch omits it).

**Why:** Cleaner separation of concerns. The VADER quality gate only gates `sentimentCompound`. Accepting ASCII art / non-VADER content getting a `lengthWeight` column value is an accepted trade-off.

---

## sentimentLabel: NULL guard for NULL sentimentCompound

`sentimentLabel` returns NULL when `sentimentCompound IS NULL`, not 'Neutral'.

**Why:** A NULL compound means VADER didn't run (non-usable review). Labelling it 'Neutral' would be a false signal. NULL is the honest representation.

---

## sentimentSignal: NULL for non-VADER reviews

`sentimentSignal` is NULL when `isUsableForVader = false`. It does NOT fall back to `voteSignal`.
When `isUsableForVader = true AND sentimentCompound = 0`, falls back to votedUp as tiebreaker.

**Why:** Separation of concerns — `sentimentSignal` is a VADER-derived column. `voteSignal` is the separate vote-based signal. Conflating them would muddy what each column means downstream.

---

## reviewInfluenceScore ceiling calibrated to 1.0

`emotionalIntensity` weight = 0.5. Final denominators: **4.5 (VADER branch), 2.5 (non-VADER branch)**.

Max sum check:

- VADER: (1.0 + 1.0 + 1.0 + 1.0 + 0.5) / 4.5 = 1.0 ✓
- non-VADER: (1.0 + 1.0 + 0.5) / 2.5 = 1.0 ✓

---

## load_type: full vs reload distinction

- `full` = TRUNCATE target + full merge (clean slate, handles hard deletes from Silver)
- `reload` = full source read + merge WITHOUT truncate (recompute + upsert, no deletes)
- `incremental` = game-level CDF reload
- `targeted` = explicit gameKey list

No hard deletes propagated in incremental/targeted/reload modes by design. SCD Type 1, no history.

---

## reviewInfluenceScore = 0 concern — DEFERRED to coach claude

Some reviews legitimately score 0 (min playtime, 0 votes, 0 emotional intensity). This causes them to contribute 0 weight in `gamingAnalytics` weighted aggregates:

```
weightedSentiment = sum(sentimentCompound * reviewInfluenceScore) / sum(reviewInfluenceScore)
```

Zero-weight reviews are mathematically excluded from weighted averages — correct behaviour, but whether that's *desired* is a business question. Also: if ALL reviews for a game score 0, denominator = 0 → division by zero in gamingAnalytics. A `NULLIF(sum(reviewInfluenceScore), 0)` guard will be needed there regardless.

**How to apply:** When building gamingAnalytics weighted aggregates, always guard the denominator with NULLIF. The floor question (add minimum weight to all reviews?) is pending coach claude.

---

## Hash separator and column casing

- Hash uses `|` separator (not `,`) to reduce collision risk with free-text `reviewCleaned`
- Fixed casing inconsistency: `SentimentCompound` → `sentimentCompound` in sentimentLabel CASE

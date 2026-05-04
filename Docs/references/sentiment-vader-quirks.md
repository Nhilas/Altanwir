# Sentiment and VADER — references

_Last updated: 2026-05-04_

## `sentimentSignal` NULL means "not scored" — no fallback to vote

**What:** `sentimentSignal = abs(sentimentCompound)` when `isVaderEligible`; NULL otherwise. There is no fallback to `voteSignal`.

**Why it bites:** NULL propagates into downstream weighted-sentiment aggregates (`weightedSentiment` in `gold.factGameScores`). Without `NULLIF(SUM(reviewInfluenceScore), 0)` guards, a game where every review scores 0 influence produces a division-by-zero. Conflating NULL (VADER didn't run) with 0.0 (VADER scored it as neutral) would muddle two semantically distinct signals — `sentimentVoteAlignment` depends on this distinction being clean.

**What to do:** Use `NULLIF` on the denominator of every influence-weighted aggregate. Filter to `sentimentCompound IS NOT NULL` (not `!= 0`) when computing sentiment-only metrics. `sentimentLabel = NULL` means VADER did not run — do not label it 'Neutral'.

**Source:** silver-merged.md §Data model → Gold review-grain logic, lines 82, 86, 89. [ADR-007](../adrs/adr-007-per-game-normalisation.md) §Trade-offs.

---

## VADER eligibility gates

**What:** `isVaderEligible = (reviewLength > 1) AND ((asciiRatio >= 0.15) OR (uniqueWordRatio == 1)) AND (uniqueWordRatio >= 0.1) AND (hasCredibleText)`. `hasCredibleText = (reviewLength > 0) AND (wordLengthRatio BETWEEN 2 AND 15)`.

**Why it bites:** Steam mislabels some non-English reviews as English. Many reviews are emoji-only, template-heavy (BBCode blocks), or 8000-character `GoatGoat...` strings with no spaces. A naive `language == 'english'` filter passes all of these. Without the eligibility gates, VADER scores noise — producing compound scores that look plausible but represent gibberish.

**What to do:** Always check `isVaderEligible` before consuming VADER output. The `hasCredibleText` sub-flag catches the "Goat review" pattern: `wordLengthRatio` outside 2–15 indicates a single token or no tokens (typical English ranges 4–12). The eligibility flags are computed in Silver and persist to Gold `factReviews` for the `reviewInfluenceScore` dual-branch formula.

**Source:** silver-merged.md §Data model → Silver engineered columns, lines 71–72. adr-triage.md #18. Full formula in [scoring-model.md §VADER and eligibility](../architecture/scoring-model.md#vader-and-eligibility).

---

## `votesFunny` and `votesUp` uint32-overflow guard

**What:** Steam occasionally sends `4294967295` (UINT32_MAX, `0xFFFFFFFF`) for `votesFunny` and `votesUp` counts — an invalid/sentinel value, not a real count.

**Why it bites:** Without a guard, a single review with 4.3 billion funny votes dominates every normalisation and aggregation that touches the column. `max_votesUp` in the per-game `game_stats` CTE would be pinned to UINT32_MAX, making every other review's `communitySignal` normalise to ≈ 0.

**What to do:** Silver applies `when(col > 2147483647, 0).otherwise(col.cast(IntegerType()))` before any downstream computation. The threshold is INT32_MAX (`2^31 - 1`); any value above it is treated as an invalid sentinel and zeroed.

**Source:** silver-merged.md §Data model, line 68.

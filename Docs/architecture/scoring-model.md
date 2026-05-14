# Scoring model

This is the analytical-engineering subdocument behind [overview.md](overview.md). It covers everything between "we have clean review text in Silver" and "we expose a tier label in `vw_factGameScores`": VADER eligibility and text preparation, the `reviewInfluenceScore` formula, influence-weighted aggregation at game grain, Bayesian shrinkage with empirically-derived priors, tier calibration, and the sentiment-vote alignment metric that surfaces the headline analytical finding.

Every formula here exists in code under `Fabric/`. This doc explains *why* — the constraint or analytical observation that forced each shape.

---

## VADER and eligibility

> **Code:** `Fabric/NB_Steam_Reviews_Silver.Notebook` — text cleaning, engineered quality columns (`isVaderEligible`, `hasCredibleText`, `wordLengthRatio`, `asciiRatio`, `uniqueWordRatio`), VADER `pandas_udf` invocation.

Sentiment columns are produced with [VADER](https://github.com/cjhutto/vaderSentiment), a lexicon-based scorer for short, social-media-style English. Two design choices wrap it: a multi-stage text-cleaning chain in Silver, and a hard eligibility gate that drops noisy reviews before scoring.

### Text-cleaning chain (Silver)

VADER scores tokens against a curated lexicon. Steam reviews come laden with emoji shortcodes, BBCode `[quote]` blocks, ASCII art, hashtags, URLs, and Steam's heart-suit censorship glyph. Without preparation, VADER would score "spam" inputs that bear no relation to the underlying sentiment. VADER also is capable of interpreting explicit profanities in context. The cleaning chain has eight steps — a separate `withColumn` for `demojize` plus seven nested `regexp_replace` calls — in this order:

1. `demojize` — emoji → `:emoji_name:` text (separate `withColumn`)
2. BBCode + non-ASCII strip — `\[.*?\]|[^\x00-\x7F]`
3. ASCII-art / hashtag / URL strip — runs of 3+ non-alphanumeric chars, hashtags, URLs
4. Steam censored heart-suit long-run — `(:heart_suit:){7,}` → `fucking`
5. Steam censored heart-suit short-run — `(:heart_suit:){2,}` → `fuck`
6. Demoji-colon strip — turn `:smiling_face:` into `smiling_face` so the lexicon hits
7. Collapse underscore + whitespace runs
8. Trim

The order matters: BBCode strip before ASCII art (otherwise the `[` brackets get caught), demojize before colon-strip (otherwise emojis are silently lost).

### Eligibility gate

Even cleaned, many reviews are not worth scoring. Steam mislabels some non-English reviews as English; some users post 8000-character `GoatGoat...` strings with no spaces; some reviews are pure emoji or template noise. The `isVaderEligible` flag in Silver gates VADER:

```
isVaderEligible =
    (reviewLength > 1)
  AND ((asciiRatio >= 0.15) OR (uniqueWordRatio == 1))
  AND (uniqueWordRatio >= 0.1)
  AND (hasCredibleText)
```

`hasCredibleText` is an additional check: `reviewLength > 0` AND `wordLengthRatio = reviewLength / wordCount` between 2 and 15 (typical English ranges 4–12). It catches the "Goat reviews" — long single-token strings (`reviewLength` in the thousands, `wordCount = 1`, ratio in the thousands) that pass simpler ASCII or length filters.

VADER then runs as a `pandas_udf` returning `struct{pos, compound, neu, neg}` over Arrow batches. For ineligible reviews, it returns NULL — preserved as a real distinction from a `0.0` ("neutral") score. **There is no fallback to `voteDirection`.** A NULL means VADER didn't score it; a 0.0 means VADER scored it as neutral. Conflating the two would muddle two semantically distinct signals.

The full eligibility logic and the NULL-no-fallback contract for downstream aggregates live in [quirks/vader-quirks.md](../quirks/vader-quirks.md).

---

## `reviewInfluenceScore` — per-review weighted blend

> **Code:** `Fabric/NB_Steam_Reviews_Gold.Notebook` — derives `communitySignal`, `lengthSignal`, `emotionalSignal`, `playtimeSignal`, `sentimentSignal`, and assembles `reviewInfluenceScore`. Per-game `max_*` aggregations live in the `game_stats` CTE.

Each review carries a single `reviewInfluenceScore` in `gold.factReviews` that weights its contribution to all downstream game-grain aggregates. Five signals contribute, each gated by a quality flag:

| Signal | Weight | Gate | Logic |
|---|---|---|---|
| `communitySignal` | 1.5 | always | `0.45 × helpfulnessRatio + 0.20 × funninessRatio + 0.25 × commentRatio + 0.10 × reactionRatio`, each per-game log-normalised |
| `playtimeSignal` | 1.0 | `avg_playtime > 0` for the game | `percent_rank() OVER (PARTITION BY gameKey ORDER BY playtimeAtReview)` |
| `lengthSignal` | 0.5 | `hasCredibleText` | `lengthRatio × uniqueWordRatio`, where `lengthRatio = log(reviewLength + 1) / log(max_reviewLength + 1)` per-game |
| `emotionalSignal` | 0.3 | `hasCredibleText` | `least(emotionalIntensity, 0.3) / 0.3` — caps the input at 0.3, then normalises to 0–1 |
| `sentimentSignal` | 1.0 | `isVaderEligible` | `abs(sentimentCompound)` |

`reviewInfluenceScore = sum(active weighted signals) / sum(active weights)`. Each of the five weights resolves to its full value when its gate passes, 0 when it fails — five independent gates, not a binary branch. The numerator and denominator both adapt; the score ceilings at 1.0. Maximum denominator (all gates pass): `1.5 + 1.0 + 0.5 + 0.3 + 1.0 = 4.3`. Practical floor for a normal game (no `hasCredibleText`, not VADER-eligible, but the game has playtime): `1.5 + 1.0 = 2.5`. Absolute floor for DLC, where Steam does not capture playtime at the DLC level so the playtime gate fails for every review: `1.5`.

### Direction is not in the score

`reviewInfluenceScore` is magnitude only. Direction lives in two separate columns. `voteDirection` is `votedUp ? 1 : -1`, never NULL. `sentimentDirection` is `sign(sentimentCompound)` when VADER-eligible AND compound ≠ 0; falls back to `voteDirection` **only** when eligible AND compound is exactly 0 (a rare but real case where VADER ran and returned exactly neutral); NULL when not eligible — the outer CASE has no ELSE branch, so the "VADER did not run" distinction is preserved. Mixing direction into the influence score would conflate "how much should this review count" with "what is it saying" — two different downstream questions.

### Per-game normalisation

Three of the five signals are per-game-scoped, not global. `communitySignal` uses per-game maxima (`max_votesUp`, `max_votesFunny`, `max_commentCount`, `max_reactionCount`); `lengthSignal` uses per-game `max_reviewLength`; `playtimeSignal` uses per-game `percent_rank()`. Counter-Strike has 2.5M reviews; an indie has 12. A global `max_votesUp` would dwarf the indie's most helpful review into nothing. Per-game scoping keeps small-audience reviews comparable within their own context, even if it means the score isn't directly comparable across games of wildly different scales ([adr-007](../adrs/adr-007-per-game-normalisation.md)).

### Write-amplification cost

Per-game normalisation has a real CDF cost. When 15,505 new reviews land for an established game, `max_votesUp` and `playtimeSignal = percent_rank()` shift slightly, the hash on every existing review of that game changes, and the MERGE rewrites them all. Observed in production: a CDF run that ingested 15,505 new reviews **rewrote 131,800 prior rows** in `gold.factReviews`. This is acceptable — the alternative (global normalisation) would distort small games — but worth knowing if anyone reads incremental-load metrics expecting near-zero update counts.

---

## Game-grain aggregation — influence-weighted

> **Code:** `Fabric/NB_Game_Scores_Gold.Notebook` — game-grain aggregation, prior derivation, MERGE into `gold.factGameScores`.

Game-grain aggregates in `gold.factGameScores` are influence-weighted, not unweighted means. Each review contributes proportionally to its `reviewInfluenceScore`:

```
weightedSentiment =
    SUM(sentimentDirection × reviewInfluenceScore)
    / NULLIF(SUM(reviewInfluenceScore), 0)
```

The numerator is over VADER-eligible reviews only (ineligible rows have `sentimentDirection = NULL`, and `NULL × influence = NULL` drops them out). The denominator is over **all** reviews. That asymmetry is load-bearing: low VADER coverage attenuates the value toward 0 rather than producing a misleadingly confident average over a small eligible subset. The `NULLIF` guard handles the legitimate case where every review for a small game scores 0 (minimum playtime + zero votes + no emotional intensity).

`weightedVote` follows the same shape over all reviews. Influence weighting barely moves a binary vote signal — the up/down thumb has no internal magnitude for the influence weight to amplify, so `weightedVote` tracks closely to raw `pctVotedUp`. What's interesting is the *difference* between the influence-weighted text signal and the vote signal, which is the alignment metric below.

---

## Bayesian shrinkage with empirically-derived priors

Game-grain rating columns are smoothed with a SteamDB-style empirical-Bayes formula:

```
smoothed = observed - (observed - prior) * pow(2, -log10(N + 1))
```

The shrinkage approaches the prior at low `N` and approaches the observed value as `N` grows. Without it, a game with 3 reviews and 100% positive would outrank a game with 50,000 reviews and 95% positive — the small-sample noise dominates ([adr-003](../adrs/adr-003-empirical-bayes-priors.md)).

### Priors are derived from the dataset, not chosen

An early build of `smoothedIGDBRating` used `prior = 0.5` (the textbook "no information" choice for a 0–1 scaled value). The result was a flat distribution clustered between 57–62 across every genre. The fix was empirical: compute priors from the actual population means of the dataset.

| Column | Prior | Confidence denominator (`N`) |
|---|---|---|
| `smoothedIGDBRating` | ~0.67 (avg `aggregatedRating / 100` across rated IGDB games) | `IGDBSourceCount` |
| `weightedSentimentRating` | ~0.84 (`(weightedSentiment + 1) / 2`, influence-weighted across reviews) | `sentimentReviews` (count where `sentimentDirection IS NOT NULL`; ≈ VADER-eligible count, but a VADER-eligible review with compound = 0 still contributes via the vote-direction fallback) |
| `weightedVoteRating` | ~0.89 (`(weightedVote + 1) / 2`, influence-weighted across reviews) | `totalReviews` |
| `voteRating` (raw `pctVotedUp`) | **0.5** | `totalReviews` |

`voteRating` is the deliberate exception: it scores `pctVotedUp`, a binary signal where 0.5 is a *genuine* indifference point (equal up- and down-votes) rather than the population mean. Shrinking toward 0.89 there would be wrong — a small-sample 100%-positive game would shrink toward 0.89, which carries a meaning that the sample doesn't support.

---

## Tier calibration (S–F)

> **Code:** `View-DDL-Lakehouse.xlsx → vw_factGameScores` (Spark view, defined via Fabric SQL editor). Tier banding is presentation-layer logic — see [adr-004](../adrs/adr-004-percentiles-in-views.md), [adr-008](../adrs/adr-008-store-wide-expose-narrow.md).

`vw_factGameScores` exposes three independent tier columns — `IGDBRatingTier`, `weightedSentimentTier`, `weightedVoteTier` — all on the same scale:

| Tier | Threshold |
|---|---|
| S | ≥ 95 |
| A | ≥ 87 |
| B | ≥ 78 |
| C | ≥ 68 |
| D | ≥ 55 |
| F | otherwise |
| Insufficient Data | < 10 source samples |

Thresholds were **recalibrated upward** from an earlier 90 / 80 / 70 / 60 / 50 scheme. The reason ties back to the empirical priors above: once the smoothed columns shrink toward population means of 0.84–0.89, the bulk of the rated population sits well above the textbook 0.5 midpoint. Old thresholds would have produced an A-or-B-grade for ~95% of games. The recalibration spreads tiers across the actual observed distribution.

S-tier is rare by construction (the ≥ 95 threshold sits well above even the elevated population means). It is dominated by small curated experiences — A Short Hike (96.75), Fields of Mistria (96.47), Tiny Glade (96.21), Chants of Sennaar (95.86), The Room VR (96.41). Higher-volume titles regress toward the mean: Stardew Valley lands in A-tier (93.27), not S; Cuphead lands in B-tier (82.81). This is signal, not a calibration bug.

A separate `steamRatingLabel` column applies a `totalReviews`-bucketed Steam-style label scale ("Overwhelmingly Positive" → "Mostly Positive" → … → "Overwhelmingly Negative"), with the "Overwhelmingly" qualifiers gated to `≥ 500` reviews. Tier and label are independent axes — a game can be S-tier on `weightedSentimentTier` but only "Mostly Positive" by Steam-label volume.

---

## `sentimentVoteAlignment` — the divergence metric

The most analytically interesting column in the model is the difference between text sentiment and recommend-votes:

```
sentimentVoteAlignment = weightedSentimentRating - weightedVoteRating
```

Computed on confidence-adjusted (post-shrinkage) values, so small-sample games don't generate misleading divergences.

### What the tails look like

At `totalReviews ≥ 50,000`:

- **Negative tail (text more negative than votes)** — Doom **−21.16**, Doom Eternal −18.31, Sekiro −17.23, Phasmophobia −16.27, Lethal Company −15.64, Project Zomboid −14.84, RimWorld −14.13, Dark Souls III −13.76. The pattern is **punishing-difficulty + survival-horror sandbox**: players write angry-positive reviews. They recommend the game and write text dripping with negative sentiment.
- **Positive tail (text more positive than votes)** — Starfield **+16.64** (with 15.30% bug-report rate), Borderlands 4 +11.87 (22.73%), Ark: Survival Ascended +10.67 (26.01%), Battlefield 2042 +10.39 (16.18%), Lost Ark +9.87 (3.33%), Monster Hunter Wilds +8.86 (14.56%). The pattern is **disappointment AAAs and live-service launches**: players don't recommend the game but write text milder than the down-vote suggests.

The pattern replicates at aggregate grain. In `vw_aggThemes`, **Horror** sits at the bottom by `sentimentVoteAlignment ≈ −9.23` (2,506 games), Thriller at −8.21, Survival at −6.26 — same shape as Phasmophobia at the per-game grain.

### Why "funny ≠ positive" matters here

`votesFunny` and `sentimentLabel` are independent axes. Two confirming examples from the data: *Day One: Garry's Incident* — 6,573 funny votes, `votedUp = false`, sentiment Negative. *Counter-Strike: Global Offensive* — 22,190 funny votes, `votedUp = true`, sentiment Positive. The funniness axis cannot substitute for either alignment dimension.

### Why this matters more for popular games

A natural intuition: "if a game has 100k+ reviews, both vote and sentiment scores have plenty of data, so they should mostly agree." The data refuses to cooperate. Bucketing `vw_factGameScores` by review volume (values on the 0-100 view scale):

| Volume bucket | mean text sentiment | mean vote rating |
|---|---|---|
| 0–99 reviews | 79.92 | 78.14 |
| 100k+ reviews | 82.53 | 88.23 |

Both ratings drift upward as volume grows (well-reviewed games are, on average, well-liked). But **vote rating climbs ~4× more than sentiment rating does** (10.1 points vs 2.6 points across the buckets) — at high volume, vote scores cluster tightly around the population mean while sentiment scores stay more spread out. The practical consequence: among popular games, vote alone can no longer separate "good popular game" from "disappointing popular game"; sentiment still can.

This is precisely why `sentimentVoteAlignment` is interesting at the tails. Both Phasmophobia (426k reviews, alignment −16.27) and Starfield (116k reviews, alignment +16.64) sit in the volume bucket where votes have gone flat — and both show double-digit alignment gaps in opposite directions. The metric is doing real work exactly where the simpler vote signal has stopped.

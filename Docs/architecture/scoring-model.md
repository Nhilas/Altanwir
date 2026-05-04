# Scoring model

This is the analytical-engineering subdocument behind [overview.md](overview.md). It covers everything between "we have clean review text in Silver" and "we expose a tier label in `vw_factGameScores`": VADER eligibility and text preparation, the `reviewInfluenceScore` formula, influence-weighted aggregation at game grain, Bayesian shrinkage with empirically-derived priors, tier calibration, and the sentiment-vote alignment metric that surfaces the headline analytical finding.

Every formula here exists in code under `Labs/Lab02_Fabric/`. This doc explains *why* — the constraint or analytical observation that forced each shape.

---

## VADER and eligibility

> **Code:** `Labs/Lab02_Fabric/NB_Steam_Reviews_Silver.Notebook` — text cleaning, engineered quality columns (`isVaderEligible`, `hasCredibleText`, `wordLengthRatio`, `asciiRatio`, `uniqueWordRatio`), VADER `pandas_udf` invocation.

Sentiment columns are produced with [VADER](https://github.com/cjhutto/vaderSentiment), a lexicon-based scorer for short, social-media-style English. Two design choices wrap it: a multi-stage text-cleaning chain in Silver, and a hard eligibility gate that drops noisy reviews before scoring.

### Text-cleaning chain (Silver)

VADER scores tokens against a curated lexicon. Steam reviews come laden with emoji shortcodes, BBCode `[quote]` blocks, ASCII art, hashtags, URLs, and Steam's heart-suit censorship glyph. Without preparation, VADER would score "spam" inputs that bear no relation to the underlying sentiment. The cleaning chain (eight nested `regexp_replace` calls) runs in this order:

1. `demojize` — emoji → `:emoji_name:` text
2. BBCode strip — `\[.*?\]`
3. ASCII art / hashtag / URL strip — `[^\w\s!\?]{3,}|#|https?://\S+`
4–5. Steam heart-suit substitution (uncensors common patterns)
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

`hasCredibleText` is an additional check: `wordLengthRatio = reviewLength / wordCount` must sit between 2 and 15 (typical English ranges 4–12). It catches the "Goat reviews" — long single-token strings (`reviewLength` in the thousands, `wordCount = 1`, ratio in the thousands) that pass simpler ASCII or length filters.

VADER then runs as a `pandas_udf` returning `struct{pos, compound, neu, neg}` over Arrow batches. For ineligible reviews, it returns NULL — preserved as a real distinction from a `0.0` ("neutral") score. **There is no fallback to `voteSignal`.** A NULL means VADER didn't score it; a 0.0 means VADER scored it as neutral. Conflating the two would muddle two semantically distinct signals.

The full eligibility logic and the NULL-no-fallback contract for downstream aggregates live in [references/sentiment-vader-quirks.md](../references/sentiment-vader-quirks.md).

---

## `reviewInfluenceScore` — per-review weighted blend

> **Code:** `Labs/Lab02_Fabric/NB_Steam_Reviews_Gold.Notebook` — derives `communitySignal`, `lengthSignal`, `emotionalSignal`, `playtimeSignal`, `sentimentSignal`, and assembles `reviewInfluenceScore`. Per-game `max_*` aggregations live in the `game_stats` CTE.

Each review carries a single `reviewInfluenceScore` in `gold.factReviews` that weights its contribution to all downstream game-grain aggregates. Five signals contribute, each gated by a quality flag:

| Signal | Weight | Gate | Logic |
|---|---|---|---|
| `communitySignal` | 1.5 | always | `0.45 × helpfulnessRatio + 0.20 × funninessRatio + 0.25 × commentRatio + 0.10 × reactionRatio`, each per-game log-normalised |
| `playtimeSignal` | 1.0 | always | `percent_rank() OVER (PARTITION BY gameKey ORDER BY playtimeAtReview)` |
| `lengthSignal` | 0.5 | `hasCredibleText` | `log(reviewLength + 1) / log(max_reviewLength + 1)` per-game |
| `emotionalSignal` | 0.3 | `hasCredibleText` | `emotionalIntensity × 0.5`, capped at 0.3 |
| `sentimentSignal` | 1.0 | `isVaderEligible` | `abs(sentimentCompound)` |

`reviewInfluenceScore = sum(active weighted signals) / sum(active weights)`. Both branches (VADER-eligible vs not) ceiling at 1.0; the denominators differ (4.5 vs 2.5) precisely so they normalise cleanly.

### Direction is not in the score

`reviewInfluenceScore` is magnitude only. Direction lives in two separate columns: `sentimentDirection` (`sign(sentimentCompound)` when VADER-eligible, else `votedUp ? 1 : -1`) and `voteDirection` (`votedUp ? 1 : -1`). Mixing them into the influence score would conflate "how much should this review count" with "what is it saying" — two different downstream questions.

### Per-game normalisation

`communitySignal` and `lengthSignal` use per-game maxima, not global. Counter-Strike has 2.5M reviews; an indie has 12. A global `max_votesUp` would dwarf the indie's most helpful review into nothing. Per-game `max_votesUp` keeps small-audience reviews comparable within their own context, even if it means the score isn't directly comparable across games of wildly different scales ([adr-007](../adrs/adr-007-per-game-normalisation.md)).

### Write-amplification cost

Per-game normalisation has a real CDF cost. When 15,505 new reviews land for an established game, `max_votesUp` and `playtimeSignal = percent_rank()` shift slightly, the hash on every existing review of that game changes, and the MERGE rewrites them all. Observed in production: a CDF run that ingested 15,505 new reviews **rewrote 131,800 prior rows** in `gold.factReviews`. This is acceptable — the alternative (global normalisation) would distort small games — but worth knowing if anyone reads incremental-load metrics expecting near-zero update counts.

---

## Game-grain aggregation — influence-weighted

> **Code:** `Labs/Lab02_Fabric/NB_Game_Scores_Gold.Notebook` — game-grain aggregation, prior derivation, MERGE into `gold.factGameScores`.

Game-grain aggregates in `gold.factGameScores` are influence-weighted, not unweighted means. Each review contributes proportionally to its `reviewInfluenceScore`:

```
weightedSentiment =
    SUM(sentimentCompound × reviewInfluenceScore)
    / NULLIF(SUM(reviewInfluenceScore), 0)
```

…over VADER-eligible reviews only. The `NULLIF` guard handles the legitimate case where every review for a small game scores 0 (minimum playtime + zero votes + no emotional intensity).

`weightedVote` follows the same shape over all reviews. An empirical observation: `weightedVote ≈ pctVotedUp` (Pearson r = 0.9938 across the corpus, mean shift 0.14 percentage points). Influence weighting barely moves a binary vote signal — what's interesting is the *difference* between the influence-weighted text signal and the vote signal, which is the alignment metric below.

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
| `weightedSentimentRating` | ~0.84 (`(weightedSentiment + 1) / 2`, influence-weighted across reviews) | `sentimentReviews` (VADER-eligible count) |
| `weightedVoteRating` | ~0.89 (`(weightedVote + 1) / 2`, influence-weighted across reviews) | `totalReviews` |
| `voteRating` (raw `pctVotedUp`) | **0.5** | `totalReviews` |

`voteRating` is the deliberate exception: it scores `pctVotedUp`, a binary signal where 0.5 is a *genuine* indifference point (equal up- and down-votes) rather than the population mean. Shrinking toward 0.89 there would be wrong — a small-sample 100%-positive game would shrink toward 0.89, which carries a meaning that the sample doesn't support.

---

## Tier calibration (S–F)

> **Code:** `View-DDL-Lakehouse.xlsx → vw_factGameScores` (Spark view, defined via Fabric SQL editor). Tier banding is presentation-layer logic — see [adr-006](../adrs/adr-006-percentiles-in-views.md), [adr-020](../adrs/adr-020-store-wide-expose-narrow.md).

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

Observed S-tier population (from `vw_factGameScores`, excluding Insufficient Data): 83 games out of 15,395 rated, or **0.5%**. F-tier holds ~1.3%. Roughly 60% of all games land in F-tier *before* the source-count gate — a long-tail distribution, mathematically correct rather than miscalibrated.

S-tier is dominated by small curated experiences (A Short Hike, Fields of Mistria, Tiny Glade, Chants of Sennaar, The Room VR). Higher-volume titles regress toward the mean — also expected. Portal, Stardew Valley, Hades, Cuphead, and Tunic land in A-tier, not S; this is signal, not a calibration bug.

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

- **Negative tail (text more negative than votes)** — Ultrakill **−23.41**, Noita −22.97, Doom −21.16, People Playground −18.84, Doom Eternal −18.31, Darkest Dungeon −18.30, Sekiro −17.23, Phasmophobia −16.27. The pattern is **rage / horror / Doom**: players write angry-positive reviews. They recommend the game and write text dripping with negative sentiment.
- **Positive tail (text more positive than votes)** — Starfield **+16.64** (with 15.30% bug-report rate), Borderlands 4 +11.87 (22.73%), Ark: Survival Ascended +10.67 (26.01%), Battlefield 2042 +10.39 (16.18%), Lost Ark +9.87 (3.33%), Monster Hunter Wilds +8.86 (14.56%). The pattern is **disappointment AAAs and live-service launches**: players don't recommend the game but write text milder than the down-vote suggests.

The pattern replicates at aggregate grain. In `vw_aggThemes`, **Horror** sits at the bottom by `avgSentimentVoteAlignment ≈ −9.23` (1,641 games), Thriller at −8.21, Survival at −6.26 — same shape as Ultrakill at the per-game grain.

### Why "funny ≠ positive" matters here

`votesFunny` and `sentimentLabel` are independent axes. Two confirming examples from the data: *Day One: Garry's Incident* — 6,573 funny votes, `votedUp = 0`, sentiment Negative. *Sakura Angels* — 4,745 funny votes, `votedUp = 1`, sentiment Positive. The funniness axis cannot substitute for either alignment dimension.

### Why this matters more for popular games

A natural intuition: "if a game has 100k+ reviews, both vote and sentiment scores have plenty of data, so they should mostly agree." The data refuses to cooperate. Bucketing `vw_factGameScores` by review volume:

| Volume bucket | `avg_sentiment_rating` | `avg_vote_rating` |
|---|---|---|
| 0–99 reviews | 0.798 | 0.777 |
| 100k+ reviews | 0.825 | 0.882 |

Both ratings drift upward as volume grows (well-reviewed games are, on average, well-liked). But **vote rating climbs roughly 3× more than sentiment rating does** — at high volume, vote scores cluster tightly around the population mean while sentiment scores stay more spread out. The practical consequence: among popular games, vote alone can no longer separate "good popular game" from "disappointing popular game"; sentiment still can.

This is precisely why `sentimentVoteAlignment` is interesting at the tails. Both Ultrakill (156k reviews) and Starfield (115k reviews) sit in the volume bucket where votes have gone flat — and both show double-digit alignment gaps in opposite directions. The metric is doing real work exactly where the simpler vote signal has stopped.

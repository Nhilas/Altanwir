# Altanwir Gold — agent orientation primer

> **Agent: read [`query-rules.md`](query-rules.md) before composing any query against `gold.*` views.** The patterns there are mandatory, not suggestions. Skipping them produces wrong results or impractical run times. Read that file first, then return here for vocabulary.

**Note for human readers.** This doc is written for an agent dropping into context with no prior knowledge of the Altanwir lakehouse. It defines column meanings, naming conventions, and architecture orientation so the agent can write correct queries without re-deriving terms from the code.

**Why this exists.** Used to audit and correct findings during ad-hoc analytics work — ensuring the meaning behind fields and entities stayed consistent, and that the agent understood what to query and how. The human-facing project story lives in [`../Docs/architecture/overview.md`](../Docs/architecture/overview.md); the agent loop that produced the findings is described in [agentic-analytics.md](agentic-analytics.md) next to this file.

---

A canonical reference for ad-hoc analytics over the Altanwir Steam-reviews × IGDB lakehouse via DuckDB on the Gold parquet exports. Built for future agents dropping the doc into context to query without rediscovery; readable end-to-end by a hiring manager who has never seen the project.

**Database file**: `G:\Work\Altanwir-scratch\Lab03_duckdb_gold\altanwir-gold.duckdb` (gitignored, OneDrive-synced).
**Parquet root**: `G:/Work/IGDB-Blitz/IGDB-exports/<schema>/<table>/part-*.snappy.parquet`.
**Harness**: `g:\Work\Altanwir\DuckDB\init.duckdb.sql`.
**Source-of-truth view bodies**: `g:\Work\Altanwir\Fabric\IGDBAnalytics.SQLEndpoint\gold\Views\*.sql`.
**Source-of-truth notebooks**: `g:\Work\Altanwir\Fabric\NB_*.Notebook\notebook-content.py`.

The `.duckdb` file holds only the catalog (schemas + view definitions). All data lives in the parquet exports; DuckDB scans them on every query.

---

## 1. What this doc is for

A single source of truth for column meanings, locations, types, scales, and derivations across the Altanwir lakehouse. Use it when:

- You need to know what column to query and where it lives.
- You need the plain-English meaning of a column or morpheme before writing a query.
- You need the formula behind a derived column to sanity-check a value.
- You need to know which view materialises an expensive join.

**In scope**: 9 silver objects + 2 gold base tables + 13 gold serving views (24 objects, 258 columns total in the schema oracle). Every silver-side derivation, every gold-side signal/influence/shrinkage formula, every view-layer rounding/scaling/tier rule.

**Out of scope** (handled by other systems, not in this doc):

- **IGDBAudit Fabric warehouse** (`steam.loadControlReviews`, `steam.versionControl`, `steam.loadOrchestratorReviews`, `steam.vw_loadReviews*`). The audit warehouse is a separate Fabric SQL Warehouse for orchestration and CDF watermarks; it is not queryable through the DuckDB harness.
- **Bronze tables** (raw JSON ingest). The DuckDB harness only exposes silver and gold; bronze structure is documented in `Docs/architecture/overview.md`.
- **Pipeline orchestration** (DataFactory pipelines, run history, scheduling). See `Docs/architecture/overview.md` §Pipelines.
- **ADR rationale and history**. See `Docs/adrs/`.

**Where to look when this doc is wrong**: §10 Source-of-truth pointers. The Fabric notebooks and view DDL files are absolute truth; if this doc disagrees with them, the code wins.

---

## 2. Glossary

Two parts. **§2a Conventions** is the morpheme decoder: each suffix or prefix that carries semantic meaning across the schema, with its scale/grain/gate contract. A reader who learns "rating means smoothed 0-100 game-grain measure" can decode any new `*Rating` column without re-reading. **§2b Terms** is the specific-column lookup: each named column or concept with its plain meaning, layer, and pointer to the canonical formula.

### 2a. Conventions (morphemes)

| Element | Plain meaning | Scale | Grain | Gate | Examples |
|---|---|---|---|---|---|
| `signal` | a 0-1 per-review component score measuring one quality dimension; each signal has a quality gate and feeds the influence-score weighted blend | 0-1 | review | required (per signal) | `communitySignal`, `playtimeSignal`, `lengthSignal`, `emotionalSignal`, `sentimentSignal` |
| `score` | a per-review composite or steam-side measure; not normalized to a fixed range across columns | 0-1 (current schema) | review | always (none) | `reviewInfluenceScore`, `steamWeightedVoteScore`, `weightedVoteScore` (silver, Steam-side) |
| `rating` | a per-game numeric measure on a smoothed scale; the headline score a reader compares between games | 0-1 (fact) / 0-100 (view) | game | source data must exist | `weightedSentimentRating`, `weightedVoteRating`, `voteRating`, `smoothedIGDBRating`, `IGDBRating` (view), `steamVoteRating` (view), `weightedIGDBRating` (agg) |
| `tier` | a letter-grade (S/A/B/C/D/F) classification derived from a `*Rating` column; computed in the view layer only | categorical | game | sample-count >= 10 | `IGDBRatingTier`, `weightedSentimentTier`, `weightedVoteTier` |
| `label` | a human-readable categorical string derived from a numeric score; computed in the view layer only | categorical | game or review | source metric and review-count gate | `steamRatingLabel`, `sentimentLabel` |
| `direction` | a signed integer (-1 or 1) encoding the polarity of a vote or VADER score; acts as a multiplier in weighted aggregates | -1 / +1 | review | varies | `voteDirection`, `sentimentDirection` |
| `ratio` | a 0-1 proportion computed from two related counts or lengths; used for quality gating and signal inputs | 0-1 | review | denominator > 0 (else 0.0, not NULL) | `wordLengthRatio`, `uniqueWordRatio`, `asciiRatio` |
| `bucket` | a named categorical bin derived from a continuous signal; used for human-readable segmentation | categorical | review | always | `playtimeBucket` |
| `count` | an integer tally of discrete items; grain varies (review-level item counts vs game-level review counts vs game-level source counts) | non-negative integer | varies | always (zero-coalesced) | `IGDBSourceCount`, `commentCount`, `wordCount`, `reactionCount`, `uniqueWordCount`, `aggregatedRatingSourceCount` |
| `weighted` (prefix) | a measure that incorporates `reviewInfluenceScore` as a per-review weight, so higher-quality reviews contribute more | varies | varies | always (NULLIF guard on denominator) | `weightedSentiment`, `weightedVote`, `weightedSentimentRating`, `weightedVoteRating`, `pctWeightedSentiment`, `pctWeightedVote` |
| `smoothed` (prefix) | a Bayesian-shrinkage-adjusted version of a rating, pulled toward the global prior so low-evidence games regress to the mean | preserves base scale | game | source data exists | `smoothedIGDBRating` |
| `pct` (prefix) | a proportion stored as 0-1 in `gold.factgamescores`, multiplied by 100 and rounded for presentation in views | 0-1 (fact) / 0-100 (view) | game | totalReviews or sentimentReviews > 0 | `pctIGDBRating`, `pctPositiveSentiment`, `pctNegativeSentiment`, `pctNeutralSentiment`, `pctEarlyAccess`, `pctBugReports`, `pctRefunded`, `pctVotedUp`, `pctWeightedSentiment`, `pctWeightedVote` |
| `avg` (prefix) | an arithmetic mean computed at game grain from per-review values; views may rescale (minutes → hours, fraction → percent) | varies | game | at least one review | `avgPlaytimeAtReview` (minutes), `avgPlaytimeAtReviewHours` (view), `avgWordCount`, `avgEmotionalIntensity` |
| `is` / `has` (prefix) | a BOOLEAN flag computed from other review columns; gates downstream signal/sentiment computations | boolean | review | always | `isVaderEligible`, `hasCredibleText` |
| `vw_` (prefix) | a SQL view in `gold` schema; presentation layer over Delta facts (scale conversions, rounding, label/tier derivation, dim joins, agg rollups) | n/a | varies | n/a | all 13 `gold.vw_*` views |
| `fact*` (object prefix) | a Delta table storing the core measurable facts; CDF-enabled; the persistence layer | n/a | review or game | n/a | `gold.factreviews` (review), `gold.factgamescores` (game) |
| `dim*` (object prefix) | a dimension view presenting descriptive attributes of a slowly-changing entity; thin projection over silver | n/a | one row per entity | n/a | `vw_dimGames`, `vw_dimGenre`, `vw_dimPlatform`, `vw_dimTheme` |
| `bridge*` (object prefix) | a many-to-many junction table between games and a dimension; silver Delta tables only (gold equivalents drop the prefix) | n/a | game-dim pair | n/a | `silver.bridgegamegenres`, `silver.bridgegameplatforms`, `silver.bridgegamethemes` |
| `agg*` (object prefix) | a view that rolls game-grain measures up to genre/theme/platform grain using weighted averages | n/a | one row per dim member | n/a | `vw_aggGenres`, `vw_aggThemes`, `vw_aggPlatforms` |

#### Per-morpheme YAML contracts

For tools or strict-validation agents that want a parseable contract per morpheme.

```yaml
signal:
  scale: 0-1
  grain: review
  gate: required (per signal)
  null_when: gate-fails (NULL, not 0)
  lifecycle: gold.factreviews; surfaced rounded(., 4) in gold.vw_factReviews

score:
  scale: 0-1 (in current schema; the suffix does not strictly enforce a range)
  grain: review
  gate: always populated
  null_when: never
  lifecycle: gold.factreviews and gold.vw_factReviews

rating:
  scale: 0-1 in storage (gold.factgamescores); 0-100 in views (round(., 2))
  grain: game
  gate: source data exists (per column)
  null_when: no reviews or no IGDB data
  smoothing_formula: observed - (observed - prior) * pow(2, -log10(N + 1))
  lifecycle: gold.factgamescores; views apply *100

tier:
  scale: categorical (S, A, B, C, D, F, "Insufficient Data")
  grain: game
  thresholds: S>=95, A>=87, B>=78, C>=68, D>=55, F<55 (on 0-100 view scale)
  insufficient_data_gate: sample-count < 10
  null_when: source rating column is NULL
  lifecycle: gold.vw_factGameScores only (not stored in fact)

label:
  scale: categorical
  grain: varies (game for steamRatingLabel, review for sentimentLabel)
  gate: source metric + review-count gate
  null_when: gate not met
  lifecycle: view layer only

direction:
  scale: -1 or +1 (integer)
  grain: review
  gate: per column (voteDirection always; sentimentDirection only when isVaderEligible)
  null_when: sentimentDirection NULL when not isVaderEligible
  lifecycle: gold.factreviews

ratio:
  scale: 0-1
  grain: review
  gate: denominator > 0 (else 0.0)
  null_when: never (zero-coalesced)
  lifecycle: silver.steamreviews; promoted to gold.factreviews; rounded(., 4) in vw_factReviews

bucket:
  scale: categorical
  grain: review
  gate: always (source signal is always populated)
  null_when: never
  lifecycle: view layer only (gold.vw_factReviews)

count:
  scale: non-negative integer (BIGINT for game-grain totals)
  grain: varies
  gate: always (zero-coalesced)
  null_when: never
  lifecycle: silver origin; promoted to gold

weighted:
  method: sum(value * reviewInfluenceScore) / NULLIF(sum(reviewInfluenceScore), 0)
  exception_in_agg: weightedIGDBRating uses sum(value * IGDBSourceCount) / NULLIF(sum(IGDBSourceCount), 0)
  grain: varies (review-grain weight, game-grain output)
  null_when: NULLIF guard fires when total weight is 0
  lifecycle: gold.factgamescores; surfaced in views

smoothed:
  formula: observed - (observed - prior) * pow(2, -log10(evidence_count + 1))
  prior_source: runtime-computed global mean (sentiment, vote) OR hardcoded 0.5 (voteRating)
  grain: game
  null_when: source rating column is NULL
  lifecycle: gold.factgamescores; views rescale to 0-100

pct:
  scale_in_storage: 0-1
  scale_in_views: 0-100 (round(., 2))
  grain: game
  gate: denominator > 0
  null_when: source aggregation NULL
  lifecycle: gold.factgamescores; views rescale to 0-100

avg:
  scale: depends on input column (minutes for playtime, words for word count)
  grain: game
  gate: at least one review
  null_when: no reviews
  lifecycle: gold.factgamescores; views may rescale (minutes → hours via /60; fraction → percent via *100)

is_has:
  dtype: BOOLEAN
  grain: review
  gate: always populated (deterministic from other review columns)
  null_when: never
  lifecycle: silver.steamreviews; promoted to gold.factreviews and gold.vw_factReviews
```

#### Morpheme exceptions (non-decoder cases)

A few columns bear a morpheme but break its contract. Documented here so a reader does not assume the convention always holds.

| Column | Bears morpheme | Exception | Reason |
|---|---|---|---|
| `voteRating` | `rating` (smoothed contract) | uses hardcoded prior 0.5 instead of a runtime-computed global mean | `pctVotedUp` is binary (thumb up / down); 0.5 is the genuine indifference midpoint, not a sample mean |
| `IGDBRating` (view) | `rating` (smoothed contract) | not smoothed | the view exposes both `IGDBRating` (raw) and `smoothedIGDBRating` (shrunk); the absence of the `smoothed` prefix is the signal |
| `weightedIGDBRating` (agg views) | `weighted` (influence-weighted contract) | weighted by `IGDBSourceCount`, not `reviewInfluenceScore` | the agg-view context is genre/theme/platform grain over IGDB-rated games; influence weighting is review-grain |
| `pctIGDBRating` | `pct` (proportion-of-count contract) | unit conversion (`aggregatedRating / 100`), not a head-count fraction | IGDB's native scale is 0-100; `pct` here signals "stored as a fraction" rather than "fraction of a total count" |
| `pctWeightedSentiment` / `pctWeightedVote` | `pct` (proportion-of-count contract) | linear shift `(x+1)/2`, not a head-count fraction | maps a `[-1, 1]` weighted mean into `[0, 1]` for downstream shrinkage |
| `sentimentDirection` | `direction` (always-populated contract) | NULL when not `isVaderEligible` | preserves the "VADER did not run" distinction from "VADER ran and returned 0" |
| `steamWeightedVoteScore` / `weightedVoteScore` (silver) | `weighted` (influence-weighted contract) | Steam's own native field, not weighted by `reviewInfluenceScore` | passthrough from the Steam API; `steam` prefix in the gold rename signals the origin |
| `avgPlaytimeAtReviewHours` (in agg views) | `avg` (mean of inputs contract) | unweighted mean of per-game means, not a review-weighted mean | known approximation; games with one review contribute equally to games with 100k reviews |
| `wordLengthRatio` | `ratio` (0-1 proportion contract) | a physical chars-per-word division, typically 4-12 for English; pathologically high for Goat-shape reviews (the `hasCredibleText` gate caps the practical range to 2-15) | not a normalised proportion, an actual length-over-count quotient |

### 2b. Terms (specific columns and concepts)

Every term that appears anywhere in `Docs/architecture/`, `Docs/findings/`, or this doc, with its canonical column name, layer, and plain meaning. New terms defined elsewhere must land here too (see §2c).

| Term | Canonical column | Layer | Plain meaning |
|---|---|---|---|
| **VADER** | n/a (library, not a column) | silver | a lexicon-based sentiment scorer for short, social-media-style English; reads a review's words and returns a number from -1 (very negative) to +1 (very positive) |
| compound score | `sentimentCompound` | silver, gold | VADER's overall sentiment score; positive ≥ 0.05, negative ≤ -0.05, neutral in between; NULL when not eligible |
| VADER-eligible | `isVaderEligible` | silver, gold | per-review BOOLEAN; true when length, ASCII, unique-word, and credible-text gates all pass |
| credible text | `hasCredibleText` | silver, gold | per-review BOOLEAN; true when `reviewLength > 0` and `wordLengthRatio` between 2 and 15 |
| word-length ratio | `wordLengthRatio` | silver, gold | average characters per word; English prose typically 4-12; Goat-shape reviews hit the thousands |
| ASCII ratio | `asciiRatio` | silver only | fraction of raw review characters in the 0-127 ASCII range |
| unique-word ratio | `uniqueWordRatio` | silver, gold | ratio of distinct words to total words; 1.0 means every word different |
| emotional intensity | `emotionalIntensity` | silver, gold | fraction of review characters that are repeated `!` or uppercase letters |
| contains bug report | `containsBugReport` | silver, gold | regex flag for `bug`, `bugs`, `crash`, `error`, `lag`, `stuck`, `glitch` |
| influence score | `reviewInfluenceScore` | gold (factreviews) | per-review 0-1 weight; weighted blend of five signals; how much this review counts in game-grain rollups |
| community signal | `communitySignal` | gold (factreviews) | per-review 0-1 score; sub-blend of helpful/funny/comment/reaction votes (weights 0.45 / 0.20 / 0.25 / 0.10), each log-normalised against per-game max |
| playtime signal | `playtimeSignal` | gold (factreviews) | per-game `percent_rank` of `playtimeAtReview`; 0 = lowest playtime in the game's distribution, 1 = highest |
| length signal | `lengthSignal` | gold (factreviews) | `lengthRatio * uniqueWordRatio` per-game-normalised; NULL when `hasCredibleText` is false |
| emotional signal | `emotionalSignal` | gold (factreviews) | `least(emotionalIntensity, 0.3) / 0.3`; caps at 1.0; NULL when `hasCredibleText` is false |
| sentiment signal | `sentimentSignal` | gold (factreviews) | `abs(sentimentCompound)`; NULL when not VADER-eligible |
| vote direction | `voteDirection` | gold (factreviews) | `+1` if `votedUp = true`, `-1` otherwise; never NULL |
| sentiment direction | `sentimentDirection` | gold (factreviews) | `sign(sentimentCompound)` when eligible and non-zero; falls back to vote direction when eligible and compound is exactly 0; NULL when not eligible |
| Steam weighted vote score | `steamWeightedVoteScore` | gold (factreviews); silver as `weightedVoteScore` | Steam's own per-review helpfulness score; passthrough from the Steam API |
| sentiment vote alignment | `sentimentVoteAlignment` | gold (factgamescores), views | per-game `weightedSentimentRating - weightedVoteRating`; positive means text is milder than the votes suggest, negative means text is angrier |
| weighted sentiment | `weightedSentiment` | gold (factgamescores) | sum of `sentimentDirection × reviewInfluenceScore` for eligible reviews divided by `sum(reviewInfluenceScore)` over all reviews; range -1 to 1. Low VADER coverage attenuates the value. |
| weighted vote | `weightedVote` | gold (factgamescores) | influence-weighted mean of `voteDirection` across all reviews; range -1 to 1 |
| weighted sentiment rating | `weightedSentimentRating` | gold (factgamescores), views | smoothed `pctWeightedSentiment` on 0-1 (fact) / 0-100 (view) scale |
| weighted vote rating | `weightedVoteRating` | gold (factgamescores), views | smoothed `pctWeightedVote` on 0-1 / 0-100 |
| Steam vote rating | `steamVoteRating` | views | smoothed `pctVotedUp` on 0-100; view rename of `voteRating × 100` |
| smoothed IGDB rating | `smoothedIGDBRating` | gold (factgamescores), views | empirical-Bayes-shrunk `pctIGDBRating`; pulls toward `igdb_rating_prior` (~0.67) when `IGDBSourceCount` is low |
| IGDB rating | `IGDBRating` | views | `pctIGDBRating × 100`; raw, unsmoothed; co-exists with `smoothedIGDBRating` in `vw_factGameScores` |
| empirical-Bayes shrinkage | n/a (technique) | gold | `smoothed = observed - (observed - prior) * pow(2, -log10(N + 1))`; pulls low-evidence games toward the prior so a 5-review indie doesn't outrank a 50k-review behemoth |
| prior | n/a (parameter) | gold | the global mean toward which a smoothed score is pulled; three derived from data (igdb, sentiment, vote) plus one hardcoded (0.5 for voteRating) |
| total reviews | `totalReviews` | gold (factgamescores), views, agg views | per-game count of all reviews in `gold.factreviews` |
| sentiment reviews | `sentimentReviews` | gold (factgamescores), views, agg views | per-game count of reviews where `sentimentDirection` is not NULL |
| IGDB source count | `IGDBSourceCount` | gold (factgamescores), views, agg views | per-game count of external critic and IGDB user sources contributing to the IGDB aggregated rating |
| rated games | `ratedGames` | agg views | per-genre/theme/platform count of games with a non-NULL `smoothedIGDBRating` |
| reviewed games | `reviewedGames` | `vw_aggGenres`, `vw_aggThemes` | per-genre/theme count of games with at least one Steam review |
| early access | `pctEarlyAccess`, `writtenDuringEarlyAccess` | silver, gold | reviews written while the game was sold-but-still-in-development (Steam's *Early Access* tag) |
| refund rate | `pctRefunded`, `refunded` | silver, gold | share of reviews written by accounts that refunded the game |
| bug-mention rate | `pctBugReports`, `containsBugReport` | silver, gold | share of reviews matching the bug-keyword regex |
| Steam label | `steamRatingLabel` | views | Steam's own bucket: Overwhelmingly Positive / Very Positive / Mostly Positive / Mixed / Mostly Negative / Overwhelmingly Negative; thresholds depend on `totalReviews` |
| sentiment label | `sentimentLabel` | views (factReviews) | per-review Positive / Negative / Neutral; NULL preserved when `sentimentCompound` is NULL |
| playtime bucket | `playtimeBucket` | views (factReviews) | per-review Hardcore (≥ 0.67) / Regular (≥ 0.34) / Casual (< 0.34) on `playtimeSignal` |
| tier | `IGDBRatingTier`, `weightedSentimentTier`, `weightedVoteTier` | views (factGameScores) | S/A/B/C/D/F letter grade; thresholds 95/87/78/68/55; "Insufficient Data" when sample-count < 10 |
| review key | `reviewKey` | silver, gold | SHA-256 surrogate key per review: `sha2(cast(concat_ws('\|', eId, steamid) as string), 256)` |
| game key | `gameKey` | silver, gold | SHA-256 surrogate key per game: `sha2(cast(id as string), 256)` over the IGDB game id |
| run id | `insert_run_id`, `update_run_id` | silver, gold | pipeline run id propagated end-to-end; lineage column on every row; insert side carries `insert_run_id`, updates carry `update_run_id` |
| MERGE | n/a (operation) | silver, gold | Delta upsert; SCD Type 1; `whenMatchedUpdate(t.hash != s.hash)` so unchanged rows are no-ops |
| CDF | n/a (mechanism) | silver→gold | Delta Change Data Feed; reads only `_change_type IN ('insert', 'update_postimage')` rows since the watermark stored in the audit warehouse |
| broadcast join | n/a (technique) | silver | Spark hint forcing the small side of an N-vs-71M join to be replicated rather than shuffled; used for `silver.externalgames` and audit-execution lookups |
| `Unknown` | sentinel value | views (game-bridge views) | placeholder produced by `LEFT JOIN + COALESCE(..., 'Unknown')` so every game appears in every bridge view, even those with no IGDB metadata |

### 2c. Drift discipline

**Rule**: any term defined in `Docs/architecture/`, `Docs/findings/`, or this doc must appear in §2b above. Adding a new finding or scoring-model section that introduces a term means adding the term here in the same change. The Conventions table (§2a) covers morphemes, not specific columns; it grows only when a new naming convention is adopted across multiple columns.

If the source code (`Fabric/`) and this doc disagree, the code wins. File and update this doc accordingly. The verification workflow that built this doc lives in `G:\Work\Altanwir-scratch\sll-pro-workspace\` (schema oracle + per-domain specs + audit reports).

---

## 3. Architecture orientation

| Layer | Plain meaning | Role | Persistence |
|---|---|---|---|
| Bronze | the 'land it raw' layer; Steam reviews stored as `review_json STRING`, IGDB tables stored verbatim with arrays cast to STRING | schema-resilient ingest | Delta tables (out of scope for this doc; see `overview.md`) |
| Silver | the 'clean, enrich, score' layer; Steam reviews get parsed, deduplicated, regex-cleaned, VADER-scored; IGDB metadata gets dim + bridge expansion | structured truth for analytics | Delta tables under `silver.*`; surfaced as DuckDB views in this harness |
| Gold (base) | the 'derive and aggregate' layer; review-grain signals + game-grain weighted aggregates land in two flat fact tables | analytics-ready facts | Delta tables `gold.factreviews`, `gold.factgamescores`; surfaced as DuckDB views |
| Gold (views) | the 'present and label' layer; facts get rounded and joined to dims. `vw_factGameScores` additionally scales percentages and ratings to 0-100 and appends tier and `steamRatingLabel` columns; `vw_factReviews` rounds signals to 4 decimal places (0-1 preserved) and appends `playtimeBucket` and `sentimentLabel`. This is the surface analytics agents query. | consumer-facing measures | T-SQL views in Fabric SQL Endpoint, mirrored in DuckDB via `init.duckdb.sql` |

The DuckDB harness is a two-step bake. First, `silver` and `gold` schemas are created and base-table views are defined as `SELECT * FROM read_parquet(...)` over the parquet folders. Second, each Fabric T-SQL view file is `.read` in dependency order: dims → game-bridge views → facts → aggs + catalogue. Re-baking propagates any Fabric edit. The view definitions are not duplicated in the harness; the Fabric SQL files are the single source of truth.

Dependency tiers for the views (from `init.duckdb.sql`):

1. **Dims** depend on silver only: `vw_dimGames`, `vw_dimGenre`, `vw_dimPlatform`, `vw_dimTheme`.
2. **Game-bridge views** depend on tier 1 plus silver bridges: `vw_gameGenres`, `vw_gamePlatforms`, `vw_gameThemes`.
3. **Facts** depend on gold base plus tier 1: `vw_factGameScores`, `vw_factReviews`.
4. **Aggs + catalogue** depend on tiers 2/3: `vw_aggGenres`, `vw_aggThemes`, `vw_aggPlatforms`, `vw_gameCatalogue`.

---

This is a slice of a longer operational reference kept in personal working notes. The Architecture orientation section is where this slice ends; the rest (ERD, derivations, gotchas, query patterns) is operational scratchpad rather than portfolio material — happy to walk through on request.

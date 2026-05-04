# Altanwir — architecture overview

> **Altanwir** — a production-shaped Medallion lakehouse on Microsoft Fabric over Steam reviews × IGDB. **71M reviews ingested end-to-end in 2h 28m** on an 8-core trial cluster; CDF incremental thereafter (15,505 new reviews land in under 3 minutes). The headline analytical finding: text sentiment and recommend-votes diverge by up to ±20 points — Ultrakill players write angry-positive reviews; Starfield players are politely upvoted. The headline engineering story: **Fabric's SQL analytics endpoint can't surface complex types**, which forced dimensional modelling all the way down ([adr-001](../adrs/adr-001-dimensional-gold-over-array-obt.md)).

## Architecture

Altanwir is a Medallion lakehouse (Bronze → Silver → Gold) on Microsoft Fabric over two source systems: the Steam Reviews API (custom multi-threaded extractor) and the IGDB metadata catalog. Four Data Factory pipelines orchestrate ingestion; a separate Fabric SQL Warehouse sits off the Spark cluster as the audit and load-control plane (see [adr-002](../adrs/adr-002-cdf-incremental-audit-warehouse.md)).

### Pipelines

| Pipeline | Stages | Notes |
|---|---|---|
| `pl_IGDB_Medallion` | `NB_1_Bronze` → `NB_2_Silver` → `NB_Game_Scores_Gold` | IGDB reload; Silver/Gold gated on Bronze's `processed_any = true` exit signal |
| `pl_Steam_API` | `NB_Steam_Reviews` (extractor only) | Plain Jupyter (`python3.11`), not Spark — no cluster spin-up for the API fetch ([adr-002](../adrs/adr-002-cdf-incremental-audit-warehouse.md)). Writes batched JSON to OneLake `/Files/Steam/Reviews/` and marks every execution in `steam.loadControlReviews`. |
| `pl_Steam_Reviews_Medallion` | `NB_Steam_Reviews_Bronze` → `_Silver` → `_Gold` → `NB_Game_Scores_Gold` | Full reload of a game cohort |
| `pl_Steam_Reviews_Medallion_Incremental` | `NB_Steam_Reviews` → Bronze → Silver → Gold → GameScores | Default `load_type = incremental`; CDF-driven |

`NB_Game_Scores_Gold` is shared between the two terminal pipelines and idempotent (~30k rows, MERGE-on-hash) — re-running it from either chain costs nothing when no upstream rows changed. That property is what makes the two-pipeline split workable.

### Layer contracts

- **Bronze — schema-resilient ingest.** Steam stores the raw payload as a single `review_json STRING`. IGDB ingests every source column (with explicit excludes via a config dict list), casts `ArrayType` to STRING pre-write, and sets `delta.schema.autoMerge.enabled = true` at session level so new fields land without DDL. Steam Bronze MERGEs only the batches the scraper has landed in `/Files/Steam/Reviews/`, polling `steam.loadControlReviews WHERE is_loaded = 0` for the read set — this decouples scrape cadence from ingest cadence (the scraper runs hourly throughout the day to stay well under Steam's rate limits; Bronze fires once daily and picks up whatever has accumulated).
- **Silver — clean, enrich, score.** Reviews: parse JSON, English-only, dedup `(app_id, recommendationid)`, 8-step text-cleaning chain for VADER readiness, engineered quality columns (`isVaderEligible`, `hasCredibleText`, etc.), VADER as `pandas_udf`. IGDB: dim and bridge build, broadcast join to `externalGames` to resolve `gameKey` into Steam Reviews.
- **Gold — derive, aggregate, serve.** Review-grain signal columns and per-game-normalised `reviewInfluenceScore`; game-grain influence-weighted aggregates with empirical-Bayes shrinkage. All metrics flat — no complex types ([adr-001](../adrs/adr-001-dimensional-gold-over-array-obt.md)); presentation logic lives in serving views ([adr-004](../adrs/adr-004-percentiles-in-views.md), [adr-008](../adrs/adr-008-store-wide-expose-narrow.md)).

**Common contract.** Every layer: MERGE keyed on natural or surrogate hash, `whenMatchedUpdateAll` (or explicit column mapping where run-id semantics matter) only when row hash differs. SCD Type 1, no destructive ops, and an `insert_run_id` / `update_run_id` lineage column on every row.

### Environments

| Env | Lakehouse | Audit schema | Scale |
|---|---|---|---|
| dev  | `IGDBAnalytics_Dev` | `dev`   | ~7M reviews |
| prod | `IGDBAnalytics`     | `steam` | ~71M reviews at Bronze |

Both lakehouses share the same Fabric trial F-capacity (one Spark cluster at a time). The split into separate lakehouses + audit schemas is deliberate: it lets the production scrape keep accumulating reviews uninterrupted while dev iteration continues, instead of partitioning a single lakehouse with a `load_type` flag and risking dev runs polluting prod state. Steam Reviews notebooks switch via a single `environment` parameter; the older IGDB notebooks predate the pattern and use hardcoded names (see §What's not in the repo).

### Run-time profile (prod, 2026-04-23)

> **71.1M reviews end-to-end in ≈ 2h 28m** on a single 8-core Fabric trial F-cluster.
> Bronze 40m → Silver 1h 29m (VADER + demoji `pandas_udf`) → Gold review 12m → game-grain Gold 2m. A 120s "poverty wait" sits between every Spark notebook because trial capacity allows only one cluster at a time.

### Control plane

The audit warehouse (`IGDBAudit`, separate Fabric SQL Warehouse, accessed via pyodbc + PBI OAuth) holds `loadControlReviews` (per-execution log), `versionControl` (CDF watermarks per Delta table), and `loadOrchestratorReviews` (game prioritisation queue). Watermark and orchestration reads do not require a Spark cluster — see [adr-002](../adrs/adr-002-cdf-incremental-audit-warehouse.md).

## Data model

Gold is dimensional, not array-OBT — Fabric's SQL analytics endpoint cannot surface complex types ([adr-001](../adrs/adr-001-dimensional-gold-over-array-obt.md)). All facts are flat; M:M dimensions live in bridge views over Silver. The Gold layer stores wide and exposes narrow through serving views ([adr-008](../adrs/adr-008-store-wide-expose-narrow.md)).

### Schema map

How each entity traverses the layers (until [issue #9](../diagrams/) lands a real diagram):

| Entity | Bronze | Silver | Gold |
|---|---|---|---|
| **Steam reviews** | `bronze.steamReviews` (raw JSON STRING) | `silver.steamReviews` (parsed, cleaned, scored) | `gold.factReviews`, `gold.vw_factReviews` |
| **Games** | `bronze.games` | `silver.games` | `gold.factGameScores` (synthesised at game grain), `gold.vw_factGameScores`, `gold.vw_dimGames`, `gold.vw_gameCatalogue` |
| **Genres / Themes / Platforms** *(parallel structure)* | `bronze.{genres, themes, platforms}` (+ `bronze.platform_types`) | `silver.{genres, themes, platforms}`; `silver.bridgeGame{Genres, Themes, Platforms}` | `gold.vw_dim{Genre, Theme, Platform}`, `gold.vw_agg{Genres, Themes, Platforms}`, `gold.vw_game{Genres, Themes, Platforms}` |
| **External games** *(IGDB↔Steam join)* | `bronze.external_games`, `bronze.external_game_sources` | `silver.externalGames` (resolves `gameKey` for Steam reviews) | — (consumed as Silver join input only) |

**Object-type conventions.** Everything in Gold except `factReviews` and `factGameScores` is a view; there are no materialised dim, bridge, or agg tables in Gold (view performance is sufficient at this scale, and a single view definition is the single source of truth for filtering and labelling logic).

- `vw_dim*` are thin projections over Silver — every consumer hits the same view, so any future filter or label change lands in one place.
- `vw_game{Genres, Themes, Platforms}` are M:M lookup views built off `silver.bridgeGame*` with `LEFT JOIN + COALESCE(..., 'Unknown')` so every game appears in every lookup view, even those with no IGDB metadata.
- `vw_agg{Genres, Themes, Platforms}` are aggregate views at game × dim grain — they roll up `factGameScores` measures by dim, no materialisation required.
- `vw_factReviews` and `vw_factGameScores` are presentation views (rounding, scaling, labelling, tier bands).
- `factReviews` is review-grain Delta clustered by `gameKey`; `factGameScores` is game-grain Delta with no clustering (~30k rows).

### Field lineage — review grain (Bronze → Gold review)

| Layer | Column | Logic |
|---|---|---|
| Bronze | `review_json` | Raw Steam API payload, stored as `STRING` (schema-resilient) |
| Silver | `reviewRaw` | `review_json:review` extracted, dedup on `(app_id, recommendationid)`, English-only |
| Silver | `reviewCleaned` | 8-step regex chain: demojize → BBCode strip → ASCII-art / URL strip → heart-suit substitution → demoji-colon strip → whitespace collapse → trim |
| Silver | `isVaderEligible` | `len > 1 AND (asciiRatio ≥ 0.15 OR uniqueWordRatio = 1) AND uniqueWordRatio ≥ 0.1 AND hasCredibleText` |
| Silver | `sentimentCompound` | VADER `pandas_udf` returning `struct{pos, compound, neu, neg}`; NULL when not eligible |
| Gold (review) | `sentimentSignal` | `abs(sentimentCompound)` when eligible; **NULL otherwise — no fallback to `voteSignal`** |
| Gold (review) | `reviewInfluenceScore` | Weighted blend of `communitySignal`, `lengthSignal`, `emotionalSignal`, `playtimeSignal`, `sentimentSignal`; per-game normalised ([adr-007](../adrs/adr-007-per-game-normalisation.md)) |

> **Scoring logic** — VADER eligibility and text-cleaning rationale, the `reviewInfluenceScore` formula, influence-weighted aggregation, Bayesian shrinkage with empirical priors, and tier calibration — lives in [scoring-model.md](scoring-model.md), an analytical-engineering subdocument.

## Engineering patterns

Three categories live here. **ADR-linked** items have full context at the linked ADR; **decision-linked** items have one-line rationales in [decisions.md](../decisions/decisions.md); items with no link are **showcase patterns** — operational technique that didn't displace an alternative (so isn't really a "decision") but earns its space because it's the engineering itself, not just the framing of it.

### Idempotency and change tracking

- **MERGE on surrogate hash** — `whenMatchedUpdateAll(t.hash != s.hash)` everywhere; SCD Type 1, no destructive ops, re-runs are no-ops by construction.
- **CDF incremental with audit-warehouse watermark** — Gold v3 ran 15,505 inserts in 2.85m vs 5.74m for the v1 70.9M-row full load. _([adr-002](../adrs/adr-002-cdf-incremental-audit-warehouse.md))_
- **Per-game normalisation has CDF write-amplification** — 15,505 incoming reviews triggered 131,800 row updates in `gold.factReviews`; per-game `max_votesUp` and `playtimeSignal = percent_rank()` shift when new reviews land, so every affected row re-hashes. Acceptable trade — global normalisation would distort small games — but worth knowing for incremental-load metrics. _([adr-007](../adrs/adr-007-per-game-normalisation.md))_
- **Audit-write skipped on no-op merges** — Fabric does not record no-op operations in Delta history; otherwise the audit log would carry phantom rows. _([decisions.md](../decisions/decisions.md))_
- **Explicit MERGE column mapping for `run_id` lineage** — `whenMatchedUpdateAll` would smear lineage; `insert_run_id` must be preserved on the update branch and set to NULL on insert, `update_run_id` does the inverse. Every MERGE writes the column map out by hand because the convenience methods don't allow per-column branch logic.
- **`_change_type` filter on CDF reads** — without filtering to `('insert', 'update_postimage')`, every update doubles via `update_preimage` rows. The most common silent CDF bug.
- **Hash column excludes identity columns by design** — every Silver/Gold `hash` covers content columns only. Including `insert_run_id` / `update_run_id` would force a false update on every re-read of unchanged rows.

### Performance under scale

- **Liquid clustering on Bronze, Silver, and Gold review** — cluster keys match the dominant predicate (Bronze: `recommendationid` for MERGE; Silver: `reviewKey` for MERGE; Gold review: `gameKey` for `factGameScores` group-by). 15k skewed `app_id` values — Hive partitioning would create 15k tiny-file folders, Z-Order rewrites all files on every OPTIMIZE. _([adr-006](../adrs/adr-006-liquid-clustering.md))_
- **Adaptive salting on hot keys** — `salt = floor(rand() * 32)` only when a `gameKey` exceeds 50,000 reviews. Counter-Strike (2.5M reviews) creates pathological GROUP BY skew; uniform salting wastes shuffle on cold keys. Salting reduces — doesn't eliminate — skew: post-salting the `factReviews` MERGE still shows a 24× max/median per-task ratio in Spark UI. _([adr-005](../adrs/adr-005-adaptive-salting.md))_
- **OPTIMIZE in a separate scheduled maintenance notebook** — inlining with MERGE creates locking contention; liquid clustering only rewrites unclustered files since the last run anyway. _([decisions.md](../decisions/decisions.md))_
- **VADER as `pandas_udf` over Arrow batches** — eliminates JVM↔Python serialisation overhead per row; ran 71M reviews in ~89m on the 8-core trial. The DAG shows two `ArrowEvalPython` stages back-to-back: demoji UDF, then VADER UDF.
- **Broadcast joins for small lookup tables** — `broadcast(silver.externalGames)` on the `gameKey` resolution, `broadcast(audit_executions)` for the per-execution lookup. Eliminates the shuffle stage on the small side of N-vs-71M joins.
- **Write-side file skew is data shape, not a bug** — `gold.factReviews` ships with 1.1 GB vs 180 MB files because Counter-Strike packs into one cluster. Repartitioning would add a shuffle stage with no read benefit; liquid clustering handles read-time skipping regardless.

### Modelling discipline

- **Dimensional Gold over array-OBT** — Fabric's SQL endpoint can't surface complex types; arrays would be invisible to T-SQL and Power BI Direct Lake. _([adr-001](../adrs/adr-001-dimensional-gold-over-array-obt.md))_
- **Store wide, expose narrow** — full precision lives in the fact, presentation moves to the view. Adding a Delta column is expensive; adding a view column is cheap. _([adr-008](../adrs/adr-008-store-wide-expose-narrow.md))_
- **Percentile and tier columns excluded from facts** — percentiles are non-additive and cohort-dependent; a `WHERE` against a percentile-bearing fact silently produces wrong percentile-of-percentile. _([adr-004](../adrs/adr-004-percentiles-in-views.md))_
- **Empirical Bayes priors derived from data** — `smoothedIGDBRating` flatlined at 57–62 with a textbook 0.5 prior because the actual population mean is 0.68; `voteRating` keeps 0.5 because it's a genuine indifference point. Math lives in [scoring-model.md](scoring-model.md). _([adr-003](../adrs/adr-003-empirical-bayes-priors.md))_
- **Per-game normalisation for `reviewInfluenceScore` components** — Counter-Strike's absolute max would dwarf niche games; `max_votesUp` scoped per `gameKey` keeps small-audience reviews comparable within their own context. _([adr-007](../adrs/adr-007-per-game-normalisation.md))_
- **Tier and label columns dropped from aggregate-grain views** — at aggregate grain everything averages to A/B; raw rating numbers carry more cross-genre information. _([decisions.md](../decisions/decisions.md))_
- **Platform aggregation scoped to IGDB only** — no canonical IGDB↔Steam platform mapping exists, and refusing to fabricate one is the call. _([decisions.md](../decisions/decisions.md))_

### Schema resilience

- **Bronze `steamReviews` stores the review as a single `review_json STRING`** — Steam payloads evolve; no Bronze DDL changes when fields are added, parsing happens in Silver via `from_json`. _([decisions.md](../decisions/decisions.md))_
- **IGDB Bronze: session-level `autoMerge` + `ArrayType` → STRING pre-write** — Bronze absorbs new IGDB columns without pipeline failure; arrays-as-JSON-string satisfy the SQL-endpoint constraint.
- **Silver engineered quality columns** — `isVaderEligible`, `hasCredibleText`, `containsBugReport`, `wordLengthRatio`, `asciiRatio`, `uniqueWordRatio`. Closes the Goat-review loophole: 8000-char `GoatGoat...` strings pass naive length and ASCII filters but fail `wordLengthRatio BETWEEN 2 AND 15`.
- **SHA-256 surrogate keys via `concat_ws('|', …)`** — `reviewKey = sha2(concat_ws('|', eId, steamId), 256)`. The explicit `|` separator avoids collisions like `eId=71+steamId=7` vs `eId=7+steamId=17`. The surrogate hash is distinct from the content `hash` column that drives MERGE idempotency.

### Extraction and control plane

- **Steam API extractor on plain Jupyter, not Spark** — pure-Python API fetch doesn't justify a Spark cluster; under one-cluster-at-a-time trial capacity, every avoided spin-up matters. _([adr-002](../adrs/adr-002-cdf-incremental-audit-warehouse.md))_
- **Audit warehouse over pyodbc with PBI bearer token** — IN-clauses chunked at ~1,000 rows to stay under SQL Server's 2,100-parameter ceiling; no Spark cluster spin-up to read or write audit. _([adr-002](../adrs/adr-002-cdf-incremental-audit-warehouse.md))_
- **`tenacity` retry + HTTP-403 `kill_switch`** — `wait_random_exponential(multiplier=1, max=600)` up to 5 attempts on rate-limits and 5xx, but a 403 trips a `threading.Event` that aborts every worker thread. Steam treats 403 as "your IP is suspect"; aborting before more requests fire is the difference between a rate-limit cooldown and a ban.
- **High-water-mark cursor early-exit** — Steam returns `recent`-sorted; once `reviews[0].timestamp_created <= high_water_mark`, every subsequent review in the page is older. Exit the batch loop instead of paging through known data.
- **Pipeline `run_id` propagated end-to-end ("Option A" wiring)** — pipeline-level parameter flows into every notebook and is persisted as `inserted_run_id` / `updated_run_id` on every Gold row plus every `versionControl` audit entry. One id traces a row from Bronze ingest through Silver and Gold for any post-mortem.
- **PARAMETERS cell for runtime overrides** — `load_type`, `environment`, `salt_threshold`, `salt_factor` swappable from the pipeline without code edits. Steam pipelines only; the older IGDB notebooks predate this pattern.

### Operational gotchas

Reference docs in [Docs/references/](../references/) carry the actionable detail. These pointers exist so a reader of `overview.md` knows what's there:

- **`sentimentSignal` NULL-no-fallback contract** — downstream weighted aggregates need `NULLIF` guards; a fallback to vote signal would muddle two semantically distinct signals. _([references/sentiment-vader-quirks.md](../references/sentiment-vader-quirks.md))_
- **VADER eligibility logic** — Steam mislabels non-English reviews as English; the eligibility flags filter emoji-only, template-heavy, and no-space reviews before VADER runs. _([references/sentiment-vader-quirks.md](../references/sentiment-vader-quirks.md))_
- **`table_changes()` Spark SQL parser workaround** — column refs in subqueries attach to the outer table, not `table_changes()`; aliasing doesn't help. Workaround: collect to Python, build an explicit IN-list. _([references/spark-quirks.md](../references/spark-quirks.md))_
- **Liquid-clustering DDL ordering** — `CLUSTER BY (col)` must come before `TBLPROPERTIES` at create time; `ALTER TABLE ... CLUSTER BY` is unsupported in Fabric — clusters are immutable post-create. _([references/fabric_gotchas.md](../references/fabric_gotchas.md))_
- **`SHOW CREATE TABLE` blocked on Delta in Fabric** — also: Spark views are invisible to the SQL endpoint (separate catalogs). Schema reproduction needs DataFrame introspection or DDL extraction. _([references/fabric_gotchas.md](../references/fabric_gotchas.md))_

## What's not in the repo

Two categories: deliberate scope limits, and first-iteration code worth flagging. Listed for completeness — these aren't caveats, they're the honest perimeter.

### Out of scope by design

- **No semantic layer.** Fields like `sentimentSignal`, `weightedSentimentRating`, the S–F tier columns, and label fields are computed in Gold and exposed via serving views — not authored as DAX measures in a Power BI semantic model. The cleaner production answer for presentation logic is a semantic layer with proper measures over the fact tables. The trade-off was scope, not pattern preference: the project was scoped to PySpark/Fabric end-to-end, and lifting presentation into a separate semantic layer would have added another tool surface to the build.
- **No time-series, no DLC grouping, no patch/version segmentation.** The grain stays one row per game, static. Adding any of these would require IGDB fields not currently fetched and a non-trivial schema change.
- **No cross-platform IGDB↔Steam analysis.** No canonical mapping between IGDB platform ids and Steam app ids exists, and refusing to fabricate one is the call. `vw_aggPlatforms` is IGDB-only as a result.

### First-iteration code, flagged honestly

- **IGDB Bronze and Silver notebooks (`NB_1_Bronze`, `NB_2_Silver`) are the first Python written on this project.** They predate the patterns the Steam notebooks use — no `environment` parameter, no PARAMETERS-cell runtime overrides, ad-hoc credential handling. They work and shipped to prod; they're not the showcase code.
- **Pre-public-push scrub is non-negotiable.** IGDB Bronze credentials (`Client-ID`, `Authorization: Bearer`) are hardcoded in the parameters cell. `CLAUDE.md` and `GEMINI.md` (personal-context files) were committed to history despite the gitignore. Both need sanitisation and a git-history rewrite before any public push.
- **Architecture diagrams** in `Docs/architecture/diagrams/` are blocked on [issue #9](../diagrams/) and not yet complete. Until they land, the entity × layer table in §Data model is the closest thing.

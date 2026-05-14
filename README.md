# Altanwir

*Steam reviews carry two signals at once: the recommend-vote thumb, and the words. A medallion lakehouse over 71M reviews × IGDB scores both. The text-sentiment leaderboard reads cozy and short-narrative; the recommend-vote leaderboard reads AAA.*

## Architecture

<a href="Docs/architecture/diagrams/architecture.png"><img src="Docs/architecture/diagrams/architecture-summary.png" alt="Medallion pipeline summary: sources to serving views"></a>

*Full pipeline + legend: [`overview.md#diagram`](Docs/architecture/overview.md#diagram).*

## The why

71M Steam reviews × IGDB, processed end-to-end in 2h 28m on a single 8-core Fabric trial cluster. The pipeline scores each review on two axes (recommend-vote and VADER text-sentiment), influence-weights them at review grain, and applies shrinkage so a 5-review indie does not outrank a 50,000-review behemoth (formulas in [`scoring-model.md`](Docs/architecture/scoring-model.md)).

## The how + key decisions

A Bronze→Silver→Gold medallion: schema-resilient JSON ingest, then an 8-step text-cleaning chain feeding VADER through engineered data-quality gates (`isVaderEligible`, `hasCredibleText`), then per-review signals and game-grain aggregation with performance tuning at 71M-review scale. CDF incremental from Silver onward, gated by watermarks held in a separate Fabric SQL Warehouse that handles audit and observability off the Spark plane. Four decisions carry the engineering identity; the silver VADER + cleaning work is logged as **D-11** in [`decisions.md`](Docs/decisions.md).

| ADR | One-line gist |
|---|---|
| [adr-001](Docs/adrs/adr-001-dimensional-gold-over-array-obt.md) | Dimensional Gold over array-OBT. Fabric's SQL endpoint cannot surface complex types, so the model goes Kimball-aligned star schema all the way down. |
| [adr-002](Docs/adrs/adr-002-cdf-incremental-audit-warehouse.md) | CDF incremental with a separate audit warehouse. Watermark reads do not spin up a Spark cluster, and the audit plane carries the observability story. |
| [adr-003](Docs/adrs/adr-003-empirical-bayes-priors.md) | Data-derived priors for the shrinkage step. A flat 0.5 prior flatlined `smoothedIGDBRating` at 57-62 across genres; the actual population mean is ~0.67. |
| [adr-005](Docs/adrs/adr-005-adaptive-salting.md) | Adaptive salting on hot keys. Spark performance tuning on Counter-Strike's 2.5M-review skew; uniform salting wastes shuffle on cold keys. |

Full set in [`Docs/adrs/`](Docs/adrs/) (8 ADRs).

## What surfaced

The pipeline's two-axis design produces a per-game text-sentiment score alongside the recommend-vote score. Both are smoothed toward the dataset average; both land on a 0-100 scale in `vw_factGameScores` as `weightedSentimentRating` and `weightedVoteRating` (formulas in [`scoring-model.md`](Docs/architecture/scoring-model.md)). The two often disagree, and the disagreement is structured.

Three finding docs walk what surfaces:

- [`sentiment-vote-alignment.md`](Docs/findings/sentiment-vote-alignment.md): the per-game gap between the two axes, with recognizable extremes (Doom −21.16 on the angry-text side, Starfield +16.64 on the mild-text side) and the theme-grain pattern behind them.
- [`where-the-gap-grows.md`](Docs/findings/where-the-gap-grows.md): the gap grows monotonically with audience size. In games with 100k+ reviews, the two letter grades disagree on 71% of titles.
- [`what-sentimentrating-reveals.md`](Docs/findings/what-sentimentrating-reveals.md): the smoothed text-sentiment leaderboard reads cozy and short-narrative (A Short Hike 96.75, Tiny Glade 96.21), with the same shape at theme and genre grain.

Three more in [`Docs/findings/`](Docs/findings/) (protest-reviews, edge-cases, funny).

## How to navigate

- [`Docs/architecture/overview.md`](Docs/architecture/overview.md): full architectural writeup. Diagram + legend, field-lineage table (Bronze → Gold), engineering-pattern catalogue all live here.
- [`Docs/architecture/scoring-model.md`](Docs/architecture/scoring-model.md): VADER eligibility, `reviewInfluenceScore` formula, the shrinkage step, tier calibration.
- [`Docs/findings/`](Docs/findings/): six analytical writeups (sentiment-vote-alignment, what-sentimentrating-reveals, where-the-gap-grows, protest-reviews, edge-cases, funny).
- [`Docs/adrs/`](Docs/adrs/): eight ADRs in Context / Decision / Rationale / Trade-offs / Reversibility shape.
- [`Docs/decisions.md`](Docs/decisions.md): lightweight architectural calls that didn't warrant a full ADR (11 rows including D-11 for the VADER scorer choice).
- [`DuckDB/`](DuckDB/): active analytics layer over Gold parquet exports. Contains the harness, the agent orientation primer that scaffolded the analytics loop, and the agentic-analytics methodology doc.

## Tech stack

Lakehouse on Azure (Microsoft Fabric). The Spark + Delta + Parquet patterns are directly portable to Databricks.

- **Cloud / platform**: Microsoft Fabric on Azure
- **Compute**: Apache Spark (PySpark)
- **Storage / format**: Delta Lake, Apache Parquet
- **Languages**: Python, SQL
- **Sentiment**: VADER (vaderSentiment)
- **Query layer**: DuckDB (post-Fabric portability)
- **Orchestration**: Microsoft Fabric Data Factory
- **Audit plane**: Fabric SQL Warehouse (off-Spark)

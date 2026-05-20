# Labs

Exploratory labs from early in the project, before the pipeline settled into its current shape. Each one was a place to try a tool or a technique in isolation. The working pipeline lives in Fabric and [`../DuckDB/`](../DuckDB/); these are the workings-out behind it, kept for the record.

The folder numbers mark the order the folders were created, not the order things were learned. Lab02 in particular spans both the earliest experiments and the latest.

## Lab00_duckdb: local DuckDB exploration

The first sandbox: the DuckDB CLI pointed straight at files, no server.

- Landed the full **Kaggle Steam datasets** as raw CSVs: ~6.4M reviews ([`andrewmvd/steam-reviews`](https://www.kaggle.com/datasets/andrewmvd/steam-reviews), `reviews.csv`, ~2 GB) and ~123k games ([`fronkongames/steam-games-dataset`](https://www.kaggle.com/datasets/fronkongames/steam-games-dataset), `games.csv`, ~390 MB), plus smaller hand-cut samples (`sampleReviews.csv` at ~100k rows, `sGames.csv` at 100) for fast iteration.
- `SELECT` directly over CSV and Parquet with no load step, including wildcard reads across many files at once.
- Federated querying against the cloud lakehouse: `delta_scan` reading remote Delta tables over an `az login` credential chain, with no connection strings or passwords.
- The "yoink": `COPY` a slice of a cloud table down to a local file to keep working offline at zero cloud cost (`query.sql` is the leftover `azReviews` probe).
- Ran queries from `.sql` files through the CLI so the logic stayed in version control instead of an ad-hoc REPL.

The query layer that later became [`../DuckDB/`](../DuckDB/) grew out of this.

## Lab01_dbt: dbt-on-DuckDB proof of concept

A working dbt-core project (`steam_intelligence`) on the DuckDB adapter, modelling the Kaggle Steam data end to end.

- **Sources, wired two ways** with a dev/prod switch: local CSVs read directly through the `dbt_external_tables` package (paths set via project vars) for dev, the Fabric `raw` schema for prod.
- **Staging:**
  - `stgReviews`: drops null and blank reviews, deduplicates with `row_number()` over `(app_id, review_text)`, mints a surrogate `reviewId` from an md5 hash, and derives `isPositive` / `isVoted` flags.
  - `stgGames`: renames a 38-column Steam export (a Jinja loop generates the `_c00..` aliases for the prod source), parses release dates with `try_strptime`, and splits the delimited language, platform, genre, and tag strings into arrays.
- **Marts:** `dimGames` (game dimension), `fctReviews` (review fact), and `fctGamingAnalytics`, a one-big-table joining the two on `gameId`.
- **Quality and parameterization:** `unique` + `not_null` tests on the keys, an optional `release_date` var to narrow the build, and generated docs / lineage.

It stayed a proof of concept. The real transforms live in Fabric/Spark and the DuckDB layer; this is where the dbt workflow got rehearsed, not the live model layer.

## Lab02_Fabric: playground and tutorial notebooks

The Microsoft Fabric workspace where the cloud Spark work got rehearsed: two lakehouses (`SteamAnalytics`, `IGDBAnalytics`) and a set of notebooks. The deepest of the three labs.

- **Delta / lakehouse mechanics** (`Playground` notebook, on the Steam data):
  - time travel through the `_delta_log` (`VERSION AS OF`, `RESTORE` after a delete),
  - schema evolution via `ALTER ... ADD COLUMNS`,
  - `OPTIMIZE` for file compaction.
- **IGDB API ingestion** (tutorial notebooks 01–04), built up step by step:
  - a bare connection test first: an apicalypse query (`fields name, total_rating; limit 10;`) sent with `requests.post`, response turned into a Spark DataFrame,
  - then casting and rounding (`from_unixtime` dates, rounded ratings) and a first `mergeSchema` append experiment,
  - then a paginated bulk pull (500 rows per request, offset loop, rate-limit aware) landed as a bronze table,
  - with idempotent `MERGE INTO` upserts keyed on an md5 change-hash so re-runs don't duplicate. The `platforms` table got the same treatment.
- **Spark optimization** (Tutorial04): inflated the games table to ~1.6M rows with a `unionAll` loop, exploded the `platforms` array, then compared join strategies: a plain SQL join timed out near 30 minutes on the shuffle, while a `broadcast` join finished in one second. Salting (a random salt key plus a cross-joined lookup) broke up the hot platform key.

The IGDB ingestion and the salting pattern here fed the production Fabric pipeline.

---

A fourth lab, `Lab03_duckdb_gold`, graduated out of `Labs/` entirely: it became the active analytics layer at [`../DuckDB/`](../DuckDB/). These three are the experiments that stayed experiments.

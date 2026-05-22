# DuckDB: active analytics layer

After the Fabric trial expired, the parquet exports were loaded into a local DuckDB catalog so analysis could keep running without a live cluster. The harness (`init.duckdb.sql`) sets up DuckDB views matching the Fabric SQL Endpoint view definitions, so analyses run locally without a live Fabric/Spark dependency.

## What's in this folder

| File | For whom | Purpose |
|---|---|---|
| `init.duckdb.sql` | Engineer | Sets up the schemas and views; runs one time to setup and any time view alteration is needed |
| `README.md` | Engineer | Overview, setup, what's in the database |
| [`agent-orientation-primer.md`](agent-orientation-primer.md) | Agent (or curious engineer) | Column meanings, naming conventions, architecture orientation |
| [`agentic-analytics.md`](agentic-analytics.md) | Engineer | The methodology: how findings were produced via the agent-driven loop |
| [`query-rules.md`](query-rules.md) | Agent (read first) | Must-follow patterns for queries against `gold.*` views |

**Agents working in this folder: read `query-rules.md` before composing queries.**

## How it works

`init.duckdb.sql` is a harness. View bodies are the single source of truth in [`Fabric/IGDBAnalytics.SQLEndpoint/gold/Views/`](../Fabric/IGDBAnalytics.SQLEndpoint/gold/Views/). The harness sets up matching schemas + base-table views over parquet, then `.read`s each Fabric `.sql` file in dependency order. Edits to those files propagate by re-baking the database.

### Database

*This is a slimmed down version of the full architecture found in [`overview.md`](../Docs/architecture/overview.md)*

**Schema `silver`**, base-table views over parquet:

| View | Purpose |
|---|---|
| `silver.games` | IGDB Game ratings |
| `silver.genres` / `silver.platforms` / `silver.themes` | IGDB taxonomy |
| `silver.bridgegamegenres` / `silver.bridgegameplatforms` / `silver.bridgegamethemes` | Many-to-many bridges |
| `silver.externalgames` | Steam appid mapping |
| `silver.steamreviews` | Cleaned steam review grain |

**Schema `gold`**, base-table views over parquet:

| View | Purpose |
|---|---|
| `gold.factreviews` | Per-review fact (~71M rows) |
| `gold.factgamescores` | Per-game fact, includes igdb scores and aggregated review ratings |

> [!WARNING]
> `silver.steamreviews`, `gold.factreviews` and `gold.vw_factReviews` need the about 50 GB of space for the review-grain parquet data, which is not committed. They are defined by the harness but error when queried locally.

**Schema `gold`**, ported from Fabric T-SQL via `.read`:

| View | Purpose |
|---|---|
| `gold.vw_dimGames` / `vw_dimGenre` / `vw_dimPlatform` / `vw_dimTheme` | Dimensions |
| `gold.vw_gameGenres` / `vw_gamePlatforms` / `vw_gameThemes` | Joined dim views with `Unknown` coalescing |
| `gold.vw_factGameScores` | Per-game with rounded scores, tier ladders, Steam-style rating labels |
| `gold.vw_factReviews` | Per-review with playtime/sentiment buckets, joined to `gameName` |
| `gold.vw_aggGenres` / `vw_aggPlatforms` / `vw_aggThemes` | Genre/platform/theme rollups (weighted means) |
| `gold.vw_gameCatalogue` | Flat catalog of game × genre × theme × platform |

### Explicitly excluded

- **Bronze layer.** Raw IGDB and review JSON. Out of scope for analytics.
- **Audit warehouse.** `audit/loadControlReviews/`, `audit/loadOrchestratorReviews/`, `audit/versionControl/`. Operational metadata, not relevant to queries.
- **The .duckdb file itself.** Git ignored in `DuckDB/altanwir-gold.duckdb`
- **The review-grain tables.** `silver.steamreviews` (~31 GB) and `gold.factreviews` (~20 GB), plus `silver.externalgames`, the deprecated `gold.gaminganalytics`, and the `bronze` / `audit` trees, are gitignored for size. Querying `gold.vw_factReviews`, `gold.factreviews`, or `silver.steamreviews` locally errors. Everything the game-grain views need is committed under `DuckDB/data/`.

## Setup

Non review-grain findings can be reproduced from a fresh clone with the same harness:

1. **Install the DuckDB CLI.** See [duckdb.org/docs/installation](https://duckdb.org/docs/installation/).
2. **Start a DuckDB session in terminal:**

```bash
cd DuckDB
duckdb           # this runs in-memory; optionally pass a filename to persist (i.e. duckdb game-reviews.duckdb)
```

> [!NOTE]
> Launch DuckDB from this `DuckDB/` folder. `.read` and `read_parquet` in `init.duckdb.sql` resolve paths against the working directory, so `data/` and `../Fabric/` only line up when `DuckDB/` is where you launched it.

3. **Then build the views and run any queries**:

```sql
.read init.duckdb.sql
```

```sql
-- sample query
SELECT gameName, round(sentimentVoteAlignment, 2) AS alignment, totalReviews,
       weightedSentimentRating, weightedSentimentTier, steamVoteRating, steamRatingLabel
FROM gold.vw_factGameScores
WHERE gameName IN ('Doom', 'Starfield')
  AND sentimentVoteAlignment IS NOT NULL
ORDER BY alignment;
```

```sql
-- see all tables
show all tables;
-- detail view of a table
describe <schema.table>;
```

For a one-shot run, the below builds the views and prints the result without an interactive session.

```bash
duckdb -c ".read init.duckdb.sql" -c "<query>"
```

The one thing a clone won't have is the review-grain tables. `silver.steamreviews` (~31 GB) and `gold.factreviews` (~20 GB) are too large to commit, so `gold.vw_factReviews` and the related base views error when queried.

To point an agent at this data, prompt it with [`agent-orientation-primer.md`](agent-orientation-primer.md) as the canonical orientation.

> [!NOTE]
> DuckDB folds unquoted identifiers to lowercase, so `gold.factGameScores` and `gold.factgamescores` resolve to the same view.
>
> The MSSQL VS Code extension red-squiggles valid DuckDB syntax (`CREATE OR REPLACE VIEW`, `.read`, etc.); those errors are spurious for this file.

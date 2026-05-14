# DuckDB — active analytics layer

Active analytics layer for ad-hoc queries over Gold parquet exports. The harness (`init.duckdb.sql`) sets up DuckDB views matching the Fabric SQL Endpoint definitions, so analyses run locally without a live Fabric/Spark dependency.

## What's in this folder

| File | For whom | Purpose |
|---|---|---|
| `init.duckdb.sql` | Anyone with the parquet exports | Sets up the schemas and views; run once after opening the .duckdb file |
| `README.md` | Human reviewer | This file — overview, setup, what's in the database |
| [`agent-orientation-primer.md`](agent-orientation-primer.md) | Agent (or curious human) | Column meanings, morpheme conventions, architecture orientation |
| [`agentic-analytics.md`](agentic-analytics.md) | Human reviewer | The methodology — how findings were produced via the agent-driven loop |
| [`query-rules.md`](query-rules.md) | Agent (read first) | Must-follow patterns for queries against `gold.*` views |

**Agents working in this folder: read `query-rules.md` before composing queries.**

## Architecture

`init.duckdb.sql` is a *harness*. View bodies are the single source of truth in [`Fabric/IGDBAnalytics.SQLEndpoint/gold/Views/`](../Fabric/IGDBAnalytics.SQLEndpoint/gold/Views/) — the harness sets up matching schemas + base-table views over parquet, then `.read`s each Fabric `.sql` file in dependency order. Edits to those files propagate by re-baking the database.

## What's in the database

**Schema `silver`** — base-table views over parquet:

| View | Purpose |
|---|---|
| `silver.games` | IGDB Game ratings |
| `silver.genres` / `silver.platforms` / `silver.themes` | IGDB taxonomy |
| `silver.bridgegamegenres` / `silver.bridgegameplatforms` / `silver.bridgegamethemes` | Many-to-many bridges |
| `silver.externalgames` | Steam appid mapping |
| `silver.steamreviews` | Cleaned steam review grain |

**Schema `gold`** — base-table views over parquet:

| View | Purpose |
|---|---|
| `gold.factreviews` | Per-review fact (~71M rows) |
| `gold.factgamescores` | Per-game fact, includes igdb scores and aggregated review ratings |

**Schema `gold`** — ported from Fabric T-SQL via `.read`:

| View | Purpose |
|---|---|
| `gold.vw_dimGames` / `vw_dimGenre` / `vw_dimPlatform` / `vw_dimTheme` | Dimensions |
| `gold.vw_gameGenres` / `vw_gamePlatforms` / `vw_gameThemes` | Joined dim views with `Unknown` coalescing |
| `gold.vw_factGameScores` | Per-game with rounded scores, tier ladders, Steam-style rating labels |
| `gold.vw_factReviews` | Per-review with playtime/sentiment buckets, joined to `gameName` |
| `gold.vw_aggGenres` / `vw_aggPlatforms` / `vw_aggThemes` | Genre/platform/theme rollups (weighted means) |
| `gold.vw_gameCatalogue` | Flat catalog of game × genre × theme × platform |

## What's *not* here

- **Bronze layer** — raw IGDB and review JSON. Out of scope for analytics.
- **Audit warehouse** — `audit/loadControlReviews/`, `audit/loadOrchestratorReviews/`, `audit/versionControl/`. Operational metadata, not relevant to queries.
- **The .duckdb file itself** — lives in scratch (OneDrive-synced), never in the repo:
  `G:\Work\Altanwir-scratch\Lab03_duckdb_gold\altanwir-gold.duckdb`
- **The parquet exports** (~50GB) — live at `G:\Work\IGDB-Blitz\IGDB-exports\` (OneDrive-synced, never committed).

## How the harness runs

A DuckDB session is opened against a local `.duckdb` catalog file, then `.read G:/Work/Altanwir/DuckDB/init.duckdb.sql` recreates the schemas and views in one pass. Queries use schema-qualified names, e.g. `SELECT * FROM gold.vw_factGameScores ORDER BY sentimentVoteAlignment ASC LIMIT 8`.

The `.duckdb` catalog file, the `duckdb.exe` binary, and the parquet exports all live outside the repo (the binary and catalog file in scratch; the exports under `G:\Work\IGDB-Blitz\IGDB-exports\`). The harness is the artifact that's committed; the data and binary are not part of this portfolio.

## Notes

- DuckDB folds unquoted identifiers to lowercase, so `gold.factGameScores` and `gold.factgamescores` resolve to the same view.
- The MSSQL VS Code extension red-squiggles valid DuckDB syntax (`CREATE OR REPLACE VIEW`, `.read`, etc.) — those errors are spurious for this file.

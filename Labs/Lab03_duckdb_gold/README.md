# Lab03 — DuckDB on Gold parquet

Post-Fabric query layer over the Gold parquet exports. Closes [#42](https://github.com/Nhilas/Altanwir/issues/42).

Purpose: ad-hoc analytics and portfolio drill-downs without a live Fabric/Spark dependency. **Not** a full Fabric snapshot — only the slice useful for downstream analysis.

## Architecture

`init.duckdb.sql` is a *harness*. View bodies are the single source of truth in [`Fabric/IGDBAnalytics.SQLEndpoint/gold/Views/`](../../Fabric/IGDBAnalytics.SQLEndpoint/gold/Views/) — the harness sets up matching schemas + base-table views over parquet, then `.read`s each Fabric `.sql` file in dependency order. Edits to those files propagate by re-baking the database.

## What's in the database

**Schema `silver`** — base-table views over parquet:
| View | Purpose |
|---|---|
| `silver.games` | Game master |
| `silver.genres` / `silver.platforms` / `silver.themes` | IGDB taxonomy |
| `silver.bridgegamegenres` / `silver.bridgegameplatforms` / `silver.bridgegamethemes` | Many-to-many bridges |
| `silver.externalgames` | Steam appid mapping |
| `silver.steamreviews` | Cleaned review grain (pre-VADER) |

**Schema `gold`** — base-table views over parquet:
| View | Purpose |
|---|---|
| `gold.factreviews` | Per-review fact (~71M rows) |
| `gold.factgamescores` | Per-game fact |

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
- **`gold.gaminganalytics`** — legacy OBT predecessor, not used.
- **The .duckdb file itself** — lives in scratch (OneDrive-synced), never in the repo:
  `G:\Work\Altanwir-scratch\Lab03_duckdb_gold\altanwir-gold.duckdb`
- **The parquet exports** (~50GB) — live at `G:\Work\IGDB-Blitz\IGDB-exports\` (OneDrive-synced, never committed).

## Setup

1. If your parquet exports aren't at the default path, find-and-replace `G:/Work/IGDB-Blitz/IGDB-exports/` in `init.duckdb.sql`.
2. From PowerShell, open (or create) the database:

   ```powershell
   .\duckdb.exe G:\Work\Altanwir-scratch\Lab03_duckdb_gold\altanwir-gold.duckdb
   ```

3. At the `D ` prompt, bake the views:

   ```
   .read G:/Work/Altanwir/Labs/Lab03_duckdb_gold/init.duckdb.sql
   ```

4. Query (use schema-qualified names):

   ```sql
   SELECT gameName, sentimentVoteAlignment, totalReviews, steamRatingLabel
   FROM gold.vw_factGameScores
   ORDER BY sentimentVoteAlignment ASC
   LIMIT 8;
   ```

## Notes

- DuckDB folds unquoted identifiers to lowercase, so `gold.factGameScores` and `gold.factgamescores` resolve to the same view.
- The MSSQL VS Code extension red-squiggles valid DuckDB syntax (`CREATE OR REPLACE VIEW`, `.read`, etc.) — those errors are spurious for this file.

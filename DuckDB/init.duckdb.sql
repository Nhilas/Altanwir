-- =============================================================================
-- Altanwir Gold — DuckDB query layer
-- Issue #42: post-Fabric ad-hoc analytics over Gold parquet exports.
-- =============================================================================
--
-- This file is DuckDB SQL, not T-SQL. The MSSQL extension may flag valid
-- DuckDB syntax (CREATE OR REPLACE VIEW, .read, no GO terminator) as errors —
-- those warnings are spurious for this file.
--
-- ARCHITECTURE ----------------------------------------------------------------
-- This script is a *harness*. The view definitions themselves are NOT
-- duplicated here — they live in the Fabric SQL Endpoint folder:
--   Fabric/IGDBAnalytics.SQLEndpoint/gold/Views/vw_*.sql
-- Those files are auto-generated from Fabric and are the single source of
-- truth. This harness:
--   1. Wipes the silver/gold schemas (idempotency)
--   2. Recreates base-table views over parquet exports, with names matching
--      what the Fabric T-SQL files expect (silver.games, gold.factgamescores,
--      etc.)
--   3. .read's each Fabric view file in dependency order
--
-- PATHS ----------------------------------------------------------------------
-- Launch DuckDB from THIS folder (DuckDB/). Paths resolve against the process
-- working directory, not the script location:
--   Parquet base tables: data/<schema>/<table>/part-*.parquet   (i.e. DuckDB/data/)
--   Fabric view DDLs:     ../Fabric/IGDBAnalytics.SQLEndpoint/gold/Views/*.sql
--
-- The game-grain slice is committed in-repo (DuckDB/data/). The review-grain
-- tables are not shipped (size): querying gold.factreviews, silver.steamreviews,
-- or gold.vw_factReviews locally errors with a missing-file message. Every other
-- view runs. The view DEFINITIONS stay here so this harness remains a faithful
-- mirror of Fabric.
-- -----------------------------------------------------------------------------


-- =============================================================================
-- Step 1 — Create schemas
-- =============================================================================

CREATE OR REPLACE SCHEMA silver;
CREATE OR REPLACE SCHEMA gold;


-- =============================================================================
-- Step 2 — Silver base-table views (over parquet)
-- =============================================================================

-- Dimension sources
CREATE OR REPLACE VIEW silver.games AS
SELECT * FROM read_parquet('data/silver/games/*.parquet');

CREATE OR REPLACE VIEW silver.genres AS
SELECT * FROM read_parquet('data/silver/genres/*.parquet');

CREATE OR REPLACE VIEW silver.platforms AS
SELECT * FROM read_parquet('data/silver/platforms/*.parquet');

CREATE OR REPLACE VIEW silver.themes AS
SELECT * FROM read_parquet('data/silver/themes/*.parquet');

-- Bridge tables (game ↔ genre/platform/theme)
CREATE OR REPLACE VIEW silver.bridgegamegenres AS
SELECT * FROM read_parquet('data/silver/bridgegamegenres/*.parquet');

CREATE OR REPLACE VIEW silver.bridgegameplatforms AS
SELECT * FROM read_parquet('data/silver/bridgegameplatforms/*.parquet');

CREATE OR REPLACE VIEW silver.bridgegamethemes AS
SELECT * FROM read_parquet('data/silver/bridgegamethemes/*.parquet');

-- External-id mapping (IGDB ↔ Steam)
CREATE OR REPLACE VIEW silver.externalgames AS
SELECT * FROM read_parquet('data/silver/externalgames/*.parquet');

-- Review fact source
CREATE OR REPLACE VIEW silver.steamreviews AS
SELECT * FROM read_parquet('data/silver/steamreviews/*.parquet');


-- =============================================================================
-- Step 3 — Gold base-table views (over parquet)
-- =============================================================================

-- Review-grain fact (~71M rows)
CREATE OR REPLACE VIEW gold.factreviews AS
SELECT * FROM read_parquet('data/gold/factreviews/*.parquet');

-- Game-grain aggregated fact
CREATE OR REPLACE VIEW gold.factgamescores AS
SELECT * FROM read_parquet('data/gold/factGameScores/*.parquet');


-- =============================================================================
-- Step 4 — .read each Fabric T-SQL view file in dependency order.
--
-- Tier 1: dims (depend on silver base only)
-- Tier 2: game-bridge views (depend on tier 1 + silver bridges)
-- Tier 3: facts (depend on gold base + tier 1)
-- Tier 4: aggs + catalogue (depend on tier 2/3)
-- =============================================================================

-- Tier 1: dims
.read ../Fabric/IGDBAnalytics.SQLEndpoint/gold/Views/vw_dimGames.sql
.read ../Fabric/IGDBAnalytics.SQLEndpoint/gold/Views/vw_dimGenre.sql
.read ../Fabric/IGDBAnalytics.SQLEndpoint/gold/Views/vw_dimPlatform.sql
.read ../Fabric/IGDBAnalytics.SQLEndpoint/gold/Views/vw_dimTheme.sql

-- Tier 2: game-bridge views
.read ../Fabric/IGDBAnalytics.SQLEndpoint/gold/Views/vw_gameGenres.sql
.read ../Fabric/IGDBAnalytics.SQLEndpoint/gold/Views/vw_gamePlatforms.sql
.read ../Fabric/IGDBAnalytics.SQLEndpoint/gold/Views/vw_gameThemes.sql

-- Tier 3: facts
.read ../Fabric/IGDBAnalytics.SQLEndpoint/gold/Views/vw_factGameScores.sql
.read ../Fabric/IGDBAnalytics.SQLEndpoint/gold/Views/vw_factReviews.sql

-- Tier 4: aggs + catalogue
.read ../Fabric/IGDBAnalytics.SQLEndpoint/gold/Views/vw_aggGenres.sql
.read ../Fabric/IGDBAnalytics.SQLEndpoint/gold/Views/vw_aggPlatforms.sql
.read ../Fabric/IGDBAnalytics.SQLEndpoint/gold/Views/vw_aggThemes.sql
.read ../Fabric/IGDBAnalytics.SQLEndpoint/gold/Views/vw_gameCatalogue.sql

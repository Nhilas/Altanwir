# Lakehouse Overview

> Queries: [05_Lakehouse_Overview_Queries.sql](05_Lakehouse_Overview_Queries.sql)

| Filename | Description |
|----------|-------------|
| G01_lakehouse_tables.png | Complete inventory of all tables in the lakehouse organized by schema: bronze layer tables (steamreviews, games, genres, platforms, etc.), silver layer tables (steamreviews, games, genres, platforms, and bridge tables), and gold layer tables (factreviews, factgamescores, gaminganalytics). |
| G02_lakehouse_views.png | Full list of gold business views (vw_* objects) including aggregation views (vw_aggGenres, vw_aggPlatforms, vw_aggThemes), dimension views (vw_dimGames, vw_dimGenre, vw_dimPlatform, vw_dimTheme), fact views (vw_factGameScores, vw_factReviews), and bridge views (vw_gameGenres, vw_gamePlatforms, vw_gameThemes). |
| G03_audit_tables_views.png | Audit warehouse objects in IGDBAudit: load control tables (loadControlReviews, loadOrchestratorReviews), version control metadata (versionControl), and audit views for monitoring (loadReviews, loadReviewStats). |

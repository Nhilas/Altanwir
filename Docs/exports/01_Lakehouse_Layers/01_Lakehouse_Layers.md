# Lakehouse Layers

For each layer, you'll find three types of views:
- **_detail.png** — table schema, sample rows, and row counts
- **_history.png** — Delta version history (operations, timestamps, versions)
- **_cdf_changes.png** — Change Data Feed (CDF) showing inserted/updated/deleted rows between versions

## Files

| Filename | Layer | Description |
|----------|-------|-------------|
| B01_bronze_cdf_changes.png | Bronze | Change Data Feed output showing 71.1M initial ingest (version 1) plus incremental updates and upserts across versions. |
| B01_bronze_detail.png | Bronze | Table schema, sample rows, and metadata for the Bronze landing table: 24.74 GB, 30 files, clustering on `recommendationid`. |
| B01_bronze_history.png | Bronze | Version history showing CREATE TABLE (v0), initial MERGE of 71.1M rows (v1, 36.91 min), OPTIMIZE (v2), and incremental MERGE updates (v3). |
| B02_silver_cdf_changes.png | Silver | Change Data Feed showing 70.9M initial ingest (version 1) plus incremental inserts and updates across versions. |
| B02_silver_detail.png | Silver | Table schema, sample rows, and metadata for the Silver layer: 33.89 GB, 110 files, V-Order enabled, clustering on `reviewKey`. |
| B02_silver_history.png | Silver | Version history showing CREATE TABLE (v0), initial MERGE of 70.9M rows (v1, 82.44 min), OPTIMIZE (v2), and incremental MERGE with updates (v3). |
| B03_gold_cdf_changes.png | Gold | Change Data Feed for the Gold layer showing factReviews inserts and updates across versions. |
| B03_gold_factreviews_detail.png | Gold | Table schema, sample rows, and metadata for factReviews: 20.83 GB, 31 files, Liquid Clustering on `gameKey`, V-Order enabled. |
| B03_gold_factreviews_history.png | Gold | Version history showing CREATE TABLE (v0), initial MERGE of 70.9M rows (v1, 5.74 min), OPTIMIZE (v2), and incremental MERGE with targeted updates (v3). |
| B04_gold_factgamescores_detail.png | Gold | Table schema, sample rows, and metadata for factGameScores: 0.01 GB, 2 files, 30,145 rows (one row per game). |
| B04_gold_factgamescores_history.png | Gold | Version history showing CREATE TABLE (v0), initial MERGE of 29,910 rows (v1, 0.63 min), incremental MERGE updates (v2–v4). |

## Notebooks

| Notebook | Description |
|----------|-------------|
| NB_Delta_Details.Notebook | Used to generate the B01–B04 Delta detail, version history, and CDF screenshots via Fabric lakehouse explorer. |

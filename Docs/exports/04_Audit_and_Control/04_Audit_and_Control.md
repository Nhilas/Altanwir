# 04_Audit_and_Control

> Queries: [04_Audit_and_Control_Queries.sql](04_Audit_and_Control_Queries.sql)

| Filename | Description |
|----------|-------------|
| C01_versioncontrol.png | Audit table tracking Delta table versions across pipeline executions, showing run IDs, change types (MERGE/INSERT/etc.), commit versions, row counts (inserted/updated), and audit metadata for each medallion layer table. |
| C02_loadcontrol.png | Detailed load control table with one row per pipeline run, capturing execution metadata: start/end timestamps, duration, status, rows processed, retrieved cursor position, and load confirmation flags. |
| C03_loadcontrol_summary.png | Aggregated view of load control metrics: execution counts and total reviews by execution type (initial/incremental) and status (success/failed/empty), with average duration per execution. |
| C04_top_games_by_reviews.png | Analytics chart showing top games ranked by total review count, including app ID, game name, load metadata (priority, execution count), review ranges (earliest/latest), and last execution timestamp. |

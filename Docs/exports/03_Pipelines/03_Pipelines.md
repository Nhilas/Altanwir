# 03_Pipelines

| Filename | Description |
|----------|-------------|
| F01_pl_steam_reviews_dag.png | DAG for the Steam Reviews medallion pipeline showing the complete dependency chain from Bronze ingestion through Silver transformation to Gold analytics tables, with "Trial Wait 120s" activities between stages to manage Fabric trial capacity constraints. |
| F02_pl_igdb_dag.png | DAG for the IGDB (game metadata) ingestion pipeline with conditional logic for change detection, routing to Silver cleaning and Gold updates only when source data has changed. |
| F03_pl_steam_reviews_incremental.png | Incremental variant of the Steam Reviews pipeline showing the API fetch → Bronze merge → Silver transform → Gold update sequence for delta loads, including row retrieval and cursor management. |

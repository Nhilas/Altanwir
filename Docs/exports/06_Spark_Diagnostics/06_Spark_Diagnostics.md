# 06_Spark_Diagnostics

## Recommended Reading Order

Follow this sequence to trace the investigation:

1. **E00_full_review_load.png** — High-level overview of the entire 71M-row pipeline run
2. **E01_jobs_diagnostics.png** — Summary of hot jobs and identified skew patterns
3. **E01_job\*.png / E01_job\*_stage\*.png** — Per-job drill-downs (jobs 15, 22, 25 with their critical stages)
4. **E02_\*.png** — Follow-up checks and bronze/silver write verification
5. **E03_\*.png** — Deep dive into UDF execution and Stage 39 behavior at production scale

## Screenshots

| Filename | Pass | Description |
|----------|------|-------------|
| E00_full_review_load.png | E00 | End-to-end pipeline run timing for the full 71M review load, showing activity sequence and wall-clock durations across bronze/silver/gold layers with inter-stage wait activities. |
| E01_gold_71m_jobs.png | E01 | Jobs view filtered to gold layer transformations on the 71M run, showing job IDs, task counts, durations, and data volumes for factReviews and related operations. |
| E01_jobs_diagnostics.png | E01 | Diagnostic summary flagging hot stages and skew patterns; identifies data skew (Max/Median ratios) and time skew in Stage 41 (factReviews MERGE) and adjacent stages. |
| E01_job15.png | E01 | Drill-down on Spark job 15, showing DAG structure and Stage 23 details including task count, input/shuffle metrics, and execution summary. |
| E01_job15_stage23.png | E01 | Task-level metrics for Stage 23 within job 15; shows duration, input, and shuffle write distributions across 259 tasks with per-executor aggregation. |
| E01_job22.png | E01 | Drill-down on Spark job 22, intermediate projection stage with 259 tasks, input of 4.6 GiB, and shuffle write of 183.6 MiB. |
| E01_job22_stage35.png | E01 | Task-level execution summary for Stage 35 within job 22; shows wide variation in task duration and per-executor task distribution. |
| E01_job25.png | E01 | Drill-down on Spark job 25 (final factReviews MERGE operation), showing Stage 41 with 259 tasks, 13.3 GiB input, and 19.2 GiB shuffle write. |
| E01_job25_stage41.png | E01 | Task-level metrics for Stage 41 within job 25; demonstrates significant skew with max task duration 23s vs median 0.2s, and per-executor load imbalance (Executor 1: 223 tasks, Executors 2-3: 28 and 8 tasks). |
| E02_bronze_reviews_71m.png | E02 | Bronze write operation (Livy ID a29723ae...) showing wall-clock timing (40m 3s) and four jobs: Job 15 (read, 34m 31s), Job 22 (validation), Job 24 (shuffle), Job 25 (write, 57s) processing 71.1M rows. |
| E02_job15.png | E02 | Re-check of job 15 from bronze ingestion; shows Stage 19 with 22848 tasks, 89.8 GiB input, and 35 min wall-clock duration confirming the "small files problem" in bronze ingest. |
| E02_job25.png | E02 | Re-check of job 25 from silver transformations; Stage 33 (DeltaOptimizedWriter) with 142 tasks, 58s duration, and 23 GiB output confirming silver write performance. |
| E02_job25_stage33.png | E02 | Detailed task metrics for Stage 33 silver writer; shows output and shuffle read distributions across 142 tasks with per-executor timing (Executor 2: 73 tasks, Executor 3: 69 tasks). |
| E03_job25_stage39.png | E03 | Production-scale task metrics for Stage 39 (VADER UDF application); shows significant per-task variation (2.4–10 min durations) with shuffle read/GC time tracking across Executors 2, 3, and 4. |
| E03_silver_71m_job25_udfs.png | E03 | DAG visualization of nested UDF and CodeGen stages in job 25, Stage 39; displays two distinct ArrowEvalPython stages for VADER sentiment analysis embedded in the Spark execution graph. |

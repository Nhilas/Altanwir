# Altanwir — Saturday Pre-Expiry Plan

**Today:** Apr 25, 2026
**Trial dies:** ~15:30 local
**Time available:** ~4 hours (start 11:00)
**Hard stop:** 15:00 — last 30 min is for trial-dying-weirdness buffer

---

## Pre-flight (15 min, 11:00–11:15)

- [x] Open Fabric → SQL endpoint of `IGDBAnalytics` workspace
- [x] Open second tab → `IGDBAudit` warehouse query editor
- [x] Open OneLake Viewer → confirm sync icon is "fully downloaded" not "syncing"
  - it's not
- [x] Create folder `/docs/screenshots/` in the repo
- [x] Open Notepad/VS Code with this plan visible while you work
- [x] Optional: rename the Fabric tabs ("Lakehouse SQL", "Audit WH", "Lakehouse Items", "Pipeline Runs") so you don't get lost

---

## Section A — SQL Endpoint Screenshots

**Dies at 15:30. Highest priority.**

For each query: run it, screenshot the result grid, save as `screenshots/A##_name.png`.

### A01 — Top-20 factGameScores (the headline result)

**File:** `A01_factGameScores_top20.png`

```sql
SELECT TOP 20
    gameKey,
    gameName,
    totalReviews,
    pctVotedUp,
    voteRating,
    weightedVote,
    weightedSentiment,
    steamLabel,
    weightedVoteTier,
    weightedSentimentTier,
    sentimentVoteAlignment
FROM gold.vw_factGameScores
WHERE totalReviews >= 1000
ORDER BY weightedSentiment DESC;
```

**Why it matters:** S-tier shows up here. Stardew, Portal 2, Hades-style results. The "small curated games beat AAAs" finding.

---

### A02 — sentimentVoteAlignment NEGATIVE tail (Ultrakill territory)

**File:** `A02_alignment_negative_top10.png`

```sql
SELECT TOP 10
    gameKey,
    gameName,
    totalReviews,
    pctVotedUp,
    weightedVote,
    weightedSentiment,
    sentimentVoteAlignment
FROM gold.vw_factGameScores
WHERE totalReviews >= 50000
ORDER BY sentimentVoteAlignment ASC;
```

**Why it matters:** This is the README lede. "Players write angry-positive reviews about rage games and horror games." Ultrakill, Postal, FNAF, Sekiro.

---

### A03 — sentimentVoteAlignment POSITIVE tail (Starfield territory)

**File:** `A03_alignment_positive_top10.png`

```sql
SELECT TOP 10
    gameKey,
    gameName,
    totalReviews,
    pctVotedUp,
    weightedVote,
    weightedSentiment,
    sentimentVoteAlignment
FROM gold.vw_factGameScores
WHERE totalReviews >= 50000
ORDER BY sentimentVoteAlignment DESC;
```

**Why it matters:** The other half of the lede. Disappointing AAAs, scams. Starfield, Civ VII, Payday 3, The Day Before.

---

### A04 — Genre rankings

**File:** `A04_aggGenres.png`

```sql
SELECT
    genreName,
    gameCount,
    avgWeightedSentiment,
    avgWeightedVote,
    weightedSmoothedIGDBRating,
    avgSentimentVoteAlignment
FROM gold.vw_aggGenres
WHERE gameCount >= 20
ORDER BY avgWeightedSentiment DESC;
```

**Why it matters:** Puzzle/Visual Novel on top, Shooter/Fighting on bottom. Tracks intuition. Proves the pipeline produces intuitively correct cross-grain aggregations.

---

### A05 — Theme rankings (Horror as cross-grain replication)

**File:** `A05_aggThemes.png`

```sql
SELECT
    themeName,
    gameCount,
    avgWeightedSentiment,
    avgWeightedVote,
    weightedSmoothedIGDBRating,
    avgSentimentVoteAlignment
FROM gold.vw_aggThemes
WHERE gameCount >= 20
ORDER BY avgSentimentVoteAlignment ASC;
```

**Why it matters:** Horror at the bottom with avgSentimentVoteAlignment ≈ -9.23. Same pattern as Ultrakill at game grain — text more negative than votes. Cross-grain replication of the per-game finding.

---

### A06 — Tier distribution (proves the recalibration)

**File:** `A06_tier_distribution.png`

```sql
SELECT
    weightedSentimentTier,
    COUNT(*) AS games,
    AVG(weightedSentiment) AS avg_sentiment,
    MIN(weightedSentiment) AS min_sentiment,
    MAX(weightedSentiment) AS max_sentiment
FROM gold.vw_factGameScores
WHERE weightedSentimentTier <> 'Insufficient Data'
GROUP BY weightedSentimentTier
ORDER BY MIN(weightedSentiment) DESC;
```

**Why it matters:** Shows the leaner S→D distribution after recalibration. Empirical Bayes + threshold tuning visible in one shot.

---

### A07 — Insufficient Data threshold (proves the gating works)

**File:** `A07_insufficient_data.png`

```sql
SELECT
    CASE WHEN totalReviews < 10 THEN 'Insufficient' ELSE 'Sufficient' END AS bucket,
    COUNT(*) AS games,
    MIN(totalReviews) AS min_reviews,
    MAX(totalReviews) AS max_reviews
FROM gold.factGameScores
GROUP BY CASE WHEN totalReviews < 10 THEN 'Insufficient' ELSE 'Sufficient' END;
```

**Why it matters:** Shows you're not letting noise into the rankings. Defensive engineering visible.

---

### A08 — Volume vs quality (the "review count pulls toward mean" finding)

**File:** `A08_volume_vs_quality.png`

```sql
SELECT
    CASE
        WHEN totalReviews < 100 THEN '0-99'
        WHEN totalReviews < 1000 THEN '100-999'
        WHEN totalReviews < 10000 THEN '1k-10k'
        WHEN totalReviews < 100000 THEN '10k-100k'
        ELSE '100k+'
    END AS review_volume_bucket,
    COUNT(*) AS games,
    AVG(weightedSentiment) AS avg_sentiment,
    AVG(weightedVote) AS avg_vote
FROM gold.factGameScores
WHERE totalReviews >= 10
GROUP BY CASE
        WHEN totalReviews < 100 THEN '0-99'
        WHEN totalReviews < 1000 THEN '100-999'
        WHEN totalReviews < 10000 THEN '1k-10k'
        WHEN totalReviews < 100000 THEN '10k-100k'
        ELSE '100k+'
    END
ORDER BY MIN(totalReviews);
```

**Why it matters:** Shows higher review volumes regress toward mean — the "S-tier dominated by small curated games" insight is structural, not random.

---

### A09 — Single-game deep-dive (Ultrakill or Starfield)

**File:** `A09_games_detail.png` (or your pick)

```sql
SELECT *
FROM gold.vw_factGameScores
WHERE gameName LIKE '%Ultrakill%'
   OR gameName LIKE '%Starfield%'
   OR gameName LIKE '%Stardew%'
   OR gameName LIKE '%Portal 2%'
ORDER BY gameName;
```

**Why it matters:** All the columns visible at once for famous games. Useful for README detail walkthrough.

---

## Section B — Delta History / CDF Proof

**Dies at 15:30.**

### B01 — Bronze Delta history (CDF enabled)

**File:** `B01_bronze_history.png`

```sql
DESCRIBE HISTORY IGDBAnalytics.bronze.steamreviews;
```

**What to look for:** `operation` column showing MERGE entries. Property change for `delta.enableChangeDataFeed = true` if it shows up. Yesterday's 77M MERGE should be visible.

---

### B02 — Silver Delta history

**File:** `B02_silver_history.png`

```sql
DESCRIBE HISTORY IGDBAnalytics.silver.steamreviews;
```

**What to look for:** Liquid clustering operations, MERGEs, version progression.

---

### B03 — Gold factReviews history

**File:** `B03_gold_factreviews_history.png`

```sql
DESCRIBE HISTORY IGDBAnalytics.gold.factReviews;
```

---

### B04 — Gold factGameScores history

**File:** `B04_gold_factgamescores_history.png`

```sql
DESCRIBE HISTORY IGDBAnalytics.gold.factGameScores;
```

---

### B05 — CDF proof on Bronze (table_changes call)

**File:** `B05_bronze_cdf_changes.png`

```sql
-- Replace 3 with whatever recent version had inserts
SELECT
    _change_type,
    _commit_version,
    _commit_timestamp,
    COUNT(*) AS rows_affected
FROM table_changes('IGDBAnalytics.bronze.steamreviews', 0)
GROUP BY _change_type, _commit_version, _commit_timestamp
ORDER BY _commit_version DESC;
```

**What to look for:** insert / update_postimage / update_preimage rows by version. Proves CDF works end-to-end. **This screenshot is irreplaceable** — `table_changes()` requires Spark.

---

## Section C — Audit Warehouse

**Dies at 15:30.**

### C01 — versionControl populated

**File:** `C01_versioncontrol.png`

```sql
SELECT TOP 20
    table_name,
    run_id,
    change_type,
    commit_version,
    commit_timestamp,
    rows_inserted,
    rows_updated,
    latest_source_version
FROM steam.versionControl
ORDER BY commit_timestamp DESC;
```

**Why it matters:** Proves the audit pattern works end-to-end. Version tracking via pyodbc, not Spark — interview talking point about avoiding cluster spinup for watermark checks.

---

### C02 — loadControlReviews execution audit

**File:** `C02_loadcontrol.png`

```sql
SELECT TOP 20
    app_id,
    run_id,
    execution_id,
    execution_type,
    execution_status,
    retrieved_reviews,
    is_loaded,
    execution_duration
FROM steam.loadControlReviews
WHERE retrieved_reviews > 0
ORDER BY execution_start_time DESC;
```

**Why it matters:** Shows the orchestrator-level audit trail. Concurrent extraction visible (multiple games, overlapping execution windows).

---

### C03 — Aggregate stats by execution_type

**File:** `C03_loadcontrol_summary.png`

```sql
SELECT
    execution_type,
    execution_status,
    COUNT(*) AS executions,
    SUM(retrieved_reviews) AS total_reviews,
    AVG(execution_duration) AS avg_duration_seconds
FROM steam.loadControlReviews
GROUP BY execution_type, execution_status
ORDER BY execution_type, execution_status;
```

**Why it matters:** One-screenshot summary of the entire scrape effort. The "77M reviews extracted across N executions" stat lives here.

---

## Section D — View DDL Bulk Extraction

**Dies at 15:30. Run ONCE, save the output, paste into your repo.**

### D01 — Extract ALL view definitions

Run this in the **SQL endpoint** (not the warehouse):

```sql
SELECT
    s.name AS schema_name,
    o.name AS view_name,
    m.definition
FROM sys.sql_modules m
JOIN sys.objects o ON m.object_id = o.object_id
JOIN sys.schemas s ON o.schema_id = s.schema_id
WHERE o.type = 'V'
  AND s.name IN ('gold', 'silver', 'bronze')
ORDER BY s.name, o.name;
```

**Procedure:**

1. Run the query
2. Click into each `definition` cell, copy the full text
3. Save each as `Labs/Lab02_Fabric/sql_views/<schema>/<viewname>.sql`
4. The 13 views from your deployment doc:
   - `vw_dimGames`, `vw_dimGenre`, `vw_dimPlatform`, `vw_dimTheme`
   - `vw_gameCatalogue`, `vw_gameGenres`, `vw_gamePlatforms`, `vw_gameThemes`
   - `vw_factReviews`, `vw_factGameScores`
   - `vw_aggGenres`, `vw_aggPlatforms`, `vw_aggThemes`

**Fallback if `sys.sql_modules` doesn't work in SQL endpoint:**

```sql
SELECT OBJECT_DEFINITION(OBJECT_ID('gold.vw_factGameScores'));
```

Run once per view. Slower but reliable.

**Backup:** screenshot the result grid as `D01_view_ddls.png` so you have a visual record even if copy-paste fails.

---

## Section E — Spark UI / History Server

**Dies with the cluster (15:30).**

### E01 — Yesterday's Silver 77M run, salted DAG

- Open `Monitor` → `Pipeline run history` → Apr 23 prod run
- Open the Silver activity → Spark UI → Jobs tab
- Screenshot the long-running stage with adaptive salting visible
- **File:** `E01_silver_77m_dag.png`

### E02 — Adaptive salting evidence (skew metrics tab)

- From the same job, navigate to the Stages tab
- Find the stage with the GROUP BY operation
- Open `Summary metrics` for that stage
- Screenshot showing post-salt task distribution (max ≈ median, low skewness)
- **File:** `E02_silver_skew_resolved.png`
- **Note**: I am showing Jobs 15 (the pre-salt stage), Job 22 -> salting and group by MAYBE, Job 25 -> final merge (not useful but who knows)

### E03 — Bronze 77M MERGE timing

- Navigate to Apr 23 Bronze run
- Screenshot the Jobs tab showing the MERGE stage
- **File:** `E03_bronze_77m_merge.png`
- **Note**: I also got the stage with 22k tasks to show the small files problem. This is Job 15. It shows I know about it and I accepted the I/O throttling in favor of the safety of having all the reviews batches in the data lake.  This is also the only stage (job 15 stage 19) with skew
  - and Job 25 stage 33 (what i think is the merge which is what this asked for)

### E04 — factGameScores 30k synthesis

- Navigate to Apr 23 NB_Gold_Game_Scores run
- Screenshot the small fast job (proves the design choice — facts cheap to recompute)
- **File:** `E04_factgamescores_synthesis.png`
- cba, it's visible that it takes 2 mins. we can just say "it's got skew but it's acceptable because it takes 2 min to run on 70m reviews spare me"
- instead i did the 70m silver run and the 1hr udf stage showing some skew. it shows that despite it being an udf it still ran fast ish

---

## Section F — Pipeline DAG Screenshots

**Probably survives 7-day grace, but capture anyway.**

### F01 — pl_Steam_Reviews_Medallion DAG

- Open the pipeline in Fabric UI
- Screenshot the canvas showing Bronze → change-gate → Silver → wait → Gold → wait → factGameScores
- **File:** `F01_pl_steam_reviews_dag.png`

### F02 — pl_IGDB_Medallion DAG

- Same approach
- **File:** `F02_pl_igdb_dag.png`

### F03 — Pipeline run history (success rate evidence)

- Pipelines → Run history view
- Screenshot showing the ~77M successful run, recent runs
- **File:** `F03_pipeline_run_history.png`
- uh yeah we're not showing recent runs, see F04. LOL xD I got the run for the full load that went through in 1 go. the incremental was a disaster, and IGDB took time too, they got ugly

---

## Section G — Lakehouse Explorer

**Probably survives 7-day grace.**

### G01 — Tables tree

- Open `IGDBAnalytics` lakehouse → Explorer view
- Expand bronze, silver, gold schemas — show all tables
- **File:** `G01_lakehouse_tables.png`

### G02 — Views tree (SQL endpoint)

- Switch to SQL endpoint view → expand the schema tree
- Show all 13 views grouped by schema
- **File:** `G02_lakehouse_views.png`

### G03 — One table's properties pane (e.g., silver.steamreviews)

- Click silver.steamreviews → properties → liquid clustering keys visible
- **File:** `G03_silver_table_props.png`
- **Note:** skipped in favor of audit screenie `G03_audit_tables_views.png`

---

## Section H — Parquet Export (Hail Mary survival path)

**Run before 15:00 if at all possible.** Lets you query data forever via DuckDB.

### H01 — Export factGameScores

In a notebook (small enough to be safe even on a flaky cluster):

```python
spark.read.format("delta").load(
    "abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Tables/gold/factGameScores"
).write.mode("overwrite").parquet(
    "abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Files/exports/factGameScores"
)
```

Then drag the parquet files out of OneLake Viewer → OneDrive folder.

### H02 — Export factReviews (LARGE — only if time)

Same pattern. Multi-GB. Skip if cluster is being weird.

### H03 — Export agg views as materialized parquet

```python
spark.sql("SELECT * FROM gold.vw_aggGenres").write.mode("overwrite").parquet(
    "abfss://Altanwir@onelake.dfs.fabric.microsoft.com/IGDBAnalytics.Lakehouse/Files/exports/aggGenres"
)
```

Same for `vw_aggThemes`, `vw_aggPlatforms`, `vw_factGameScores`.

**These are gold for post-trial DuckDB analysis.** No Fabric needed after that.

---

## Time blocks

| Time | Section | Notes |
|---|---|---|
| 11:00–11:15 | Pre-flight | All tabs open, plan visible |
| 11:15–11:45 | **Section D first** — view DDL extraction. Lose this and the views are gone forever | Single highest-leverage 30 min of the day |
| 11:45–12:45 | Section A — SQL endpoint screenshots (A01–A09) | Run them in order, screenshot each |
| 12:45–13:00 | Section B — DESCRIBE HISTORY (B01–B05) | Quick wins |
| 13:00–13:30 | **LUNCH. STEP AWAY FROM SCREEN.** | Do not skip |
| 13:30–13:50 | Section C — audit warehouse (C01–C03) | Different tab/connection |
| 13:50–14:20 | Section H — parquet exports (H01, H03 minimum) | Kicks off; runs in background |
| 14:20–14:45 | Section E — Spark UI from yesterday's run history | Click around the history server |
| 14:45–15:00 | Sections F + G — pipeline DAGs + lakehouse explorer | Easy mode, low cognitive load |
| **15:00 — HARD STOP** | Buffer | Last 30 min for "oh shit I missed X" |

---

## Permission to Stop — Saturday Apr 25

When all of these are checked, **you are done. Close the laptop. Trial dies. Your portfolio survives.**

- [ ] **PtS 1.** Section D complete — all view DDLs extracted to `/Labs/Lab02_Fabric/sql_views/`
- [ ] **PtS 2.** Section A screenshots A01, A02, A03, A04 captured (the 4 most important)
- [ ] **PtS 3.** Section B screenshots B01 + B05 captured (CDF proof minimum)
- [ ] **PtS 4.** Section C screenshot C01 captured (versionControl evidence)
- [ ] **PtS 5.** Section H — at least one parquet export landed in OneDrive

**Bonus credit (not required):**

- [ ] Sections E + F + G fully captured
- [ ] All 9 A-section screenshots
- [ ] All 5 B-section screenshots

---

## Fog Day version

If brain is fried mid-day, the absolute minimum to ship is:

1. **Section D** — view DDLs (no replacement for this, do it FIRST)
2. **A01** — top-20 factGameScores
3. **A02 + A03** — Ultrakill/Starfield split
4. **B01** — bronze DESCRIBE HISTORY
5. **C01** — versionControl
6. **H01** — factGameScores parquet export

That's a complete portfolio. Everything else is gravy.

---

## Notes

- **If a query fails:** check column names in your actual schema. The queries above assume column names from the summaries — you know the real names. Adjust and move on.
- **If the SQL endpoint hangs:** wait 30 seconds, refresh the tab, retry. Trial weirdness peaks today. Don't fight it — switch to a different section.
- **If the cluster won't spin for parquet export:** skip Section H. The screenshots are the priority.
- **Save screenshots immediately to local disk**, not OneDrive sync. Sync can lag and you don't want to find out at 15:35.
- **OneLake Viewer:** check the sync status one more time at 14:30. If anything is stuck, click "always keep on this device" again.

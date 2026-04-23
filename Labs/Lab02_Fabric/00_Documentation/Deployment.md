# Altanwir — Prod Deploy Checklist

**Date:** April 23, 2026
**Deadline:** Fabric trial expires ~April 25 (≈48h window)
**Scope:** Steam Reviews branch is net-new in prod. IGDB Bronze + Silver already populated, not touching.
**Data volume:** ~77M reviews across JSON batch files, all for games present in IGDB (no join-based slimming expected).

---

## Reality check

- VADER on 77M rows via `pandas_udf` on trial capacity is the only real wildcard. Adaptive salting is the insurance policy.
- Two Gold Delta tables (`factReviews`, `factGameScores`). Everything else in Gold is a view.
- `pl_Steam_API` stays as-is, runs on its own schedule.
- `pl_IGDB_Medallion` is the one we're extending. `NB_3_Gold` is obsolete and gets removed.

---

## Phase 1 — Audit warehouse prep (≈20 min, low mana)

The Bronze Steam Reviews notebook reads from these tables and will fail cold-start without them.

- [x] **1.1** Add `is_loaded` column to `IGDBAudit.steam.loadControlReviews`
  - `ALTER TABLE steam.loadControlReviews ADD is_loaded BIT NOT NULL DEFAULT 0`
  - Existing rows default to 0 → every unprocessed execution becomes a Bronze candidate (correct behaviour for first run)
- [x] **1.2** Create `IGDBAudit.steam.versionControl`
  - CTAS from `dev.versionControl` if possible: `CREATE TABLE steam.versionControl AS SELECT * FROM dev.versionControl WHERE 1=0`
  - Otherwise build from DDL — spot-check schema matches dev exactly (Silver and factReviews CDF depend on this)
- [x] **1.3** Confirm `steam.loadOrchestratorReviews` exists and is populated (should be — just verify)
- [x] **1.4** Smoke query: `SELECT COUNT(*) FROM steam.loadControlReviews WHERE is_loaded = 0 AND retrieved_reviews > 0`
  - This is your Bronze workload preview. Number should roughly reflect the 77M review scope.

**Phase 1 done when:** all 4 boxes checked, smoke count is sane.

---

## Phase 2 — Lakehouse DDL (≈30–45 min, low mana, mechanical)

Order matters: Delta tables first, then views (T-SQL needs base tables resolvable).

### 2a — Delta tables

- [x] **2.1** Run `NB_DDL` with `environment=prod`
  - Creates: `bronze.steamReviews`, `silver.steamReviews`, `gold.factReviews`, `gold.factGameScores`
- [x] **2.2** Verify each table exists and schema is correct
  - `DESCRIBE TABLE IGDBAnalytics.gold.factGameScores` etc.
  - `SELECT COUNT(*) FROM IGDBAnalytics.gold.factReviews` → returns 0 cleanly

### 2b — SQL Endpoint views (13 total)

Run the saved view DDLs against the prod SQL Endpoint.

- [ ] **2.3** Dim views
  - [ ] `vw_dimGames`
  - [ ] `vw_dimGenre`
  - [ ] `vw_dimPlatform`
  - [ ] `vw_dimTheme`
- [ ] **2.4** Lookup views
  - [ ] `vw_gameCatalogue`
  - [ ] `vw_gameGenres`
  - [ ] `vw_gamePlatforms`
  - [ ] `vw_gameThemes`
- [ ] **2.5** Fact views
  - [ ] `vw_factReviews`
  - [ ] `vw_factGameScores`
- [ ] **2.6** Agg views
  - [ ] `vw_aggGenres`
  - [ ] `vw_aggPlatforms`
  - [ ] `vw_aggThemes`
- [x] **2.7** Smoke: `SELECT TOP 1 * FROM vw_factGameScores` — errors cleanly or returns empty. Same for one agg view.

**Phase 2 done when:** 4 Delta tables + 13 views exist and are queryable (empty is fine).

---

## Phase 3 — Pipeline wiring (≈1–1.5h, medium mana, Tool Hell risk)

**Option A confirmed** — extending `pl_IGDB_Medallion` so `run_id` auditing flows through both audit tables and internal `inserted_run_id` / `updated_run_id` columns.

### 3a — Clean up obsolete

- [x] **3.1** Remove `NB_3_Gold` activity from `pl_IGDB_Medallion` (obsolete OBT logic, star schema has replaced it)

### 3b — Add Steam Reviews branch

- [x] **3.2** Add `NB_Steam_Reviews_Bronze` activity
  - Load type: **incremental only** (reads `loadControlReviews` for `is_loaded = 0 AND retrieved_reviews > 0`)
  - Marks each processed row (1 row = 1 execution) as `is_loaded = 1` on success
- [x] **3.3** Add change gate after Bronze: "did any rows process?"
  - Same pattern as existing IGDB change detection
- [x] **3.4** Add `NB_Steam_Reviews_Silver` activity
  - Shares `load_type` parameter with Gold: `FULL` / `RELOAD` / `INCREMENTAL`
  - `FULL` = truncate + load, `RELOAD` = upsert everything, `INCREMENTAL` = CDF pattern
- [x] **3.5** Add `NB_Steam_Reviews_Gold` activity (builds `gold.factReviews`)
  - Dependency on Silver success
  - Same `load_type` parameter as Silver
- [x] **3.6** Keep "Poverty Wait 120s" between heavy Spark activities (trial-capacity single-cluster limit)

### 3c — Synthesis step

- [x] **3.7** Add final `NB_Gold_factGameScores` activity
  - Load type: **full only** (30k rows, reads `silver.games` + `gold.factReviews`)
  - Liquid clustering on `gameKey` matters here
  - Fires if **either** IGDB branch OR Steam Reviews branch ran
  - Gate: `If Condition` with `@or(activity('IGDB_Silver').output..., activity('Steam_Gold').output...)`
  - This is the fiddliest bit — budget 20 min for the expression

### 3d — Validation

- [x] **3.8** Pipeline validates without errors in Fabric UI
- [ ] **3.9** Smoke-run with `environment=dev` first if unsure — catches wiring bugs before prod

**Phase 3 done when:** pipeline shows full DAG, validates, optionally smoke-runs on dev.

---

## Phase 4 — Execute on prod (≈4–8h wall clock, 30 min active attention)

- [x] **4.1** Kick off `pl_IGDB_Medallion` with `environment=prod`, `run_mode=FULL`
  - Log start time
- [x] **4.2** Bronze Steam Reviews processes all `is_loaded = 0` rows → ~77M into `bronze.steamReviews`
- [x] **4.3** Silver Reviews — **the VADER gauntlet**
  - Watch Spark UI for skew
  - Adaptive salting should hold
  - This is where you need to be physically near the keyboard
- [x] **4.4** Gold `factReviews` builds (should breeze relative to Silver)
- [x] **4.5** Gold `factGameScores` builds (30k rows, quick)
- [x] **4.6** Sanity queries:
  - [x] `SELECT COUNT(*) FROM gold.factReviews` → ~77M-ish
  - [x] `SELECT COUNT(*) FROM gold.factGameScores` → ~30k
  - [x] `SELECT gameName, totalReviews FROM vw_factGameScores ORDER BY totalReviews DESC LIMIT 20` → Stardew/Portal/Hades/CS territory

**Phase 4 done when:** both Gold tables populated, sanity queries return sane data (OR Hatch A documented).

---

## Phase 5 — Screenshot blitz (parallel to Phase 4, fog-day-friendly)

Capture while Spark works. Save to `/docs/screenshots/`, prefix with phase order for README later.

- [ ] **5.1** `01_lakehouse_explorer_tables.png` — 4 Delta tables visible
- [ ] **5.2** `02_lakehouse_explorer_views.png` — 13 views visible under SQL Endpoint
- [ ] **5.3** `03_pipeline_dag.png` — `pl_IGDB_Medallion` full DAG with both branches
- [ ] **5.4** `04_pipeline_run_history.png` — successful prod run timestamps
- [ ] **5.5** `05_spark_ui_silver_reviews.png` — DAG / skew story
- [ ] **5.6** `06_factgamescores_top20.png` — top 20 by totalReviews
- [ ] **5.7** `07_agggenres_top10.png` — ranked by `avgWeightedSentiment`
- [ ] **5.8** `08_sentiment_vote_alignment.png` — cultural-patterns top/bottom 10
- [ ] **5.9** `09_audit_wh_run_id.png` — prod `run_id` row in IGDBAudit
- [ ] **5.10** Aim for **≥5 captured** (PtS bar). All 10 would be ideal, not required.

---

## Escape hatches

### Hatch A — Targeted reload (chosen path if Silver melts)

- Silver and Gold Reviews both accept `targeted_reload` natively
- Control Bronze via `is_loaded` flag manipulation in `loadControlReviews`
  - Set `is_loaded = 1` on lower-priority games (e.g., tail of the review-count distribution) to exclude them from the run
  - Re-run Bronze with targeted scope
- Still a legitimate 30–40M-scale story, fully documented

### Hatch B — Silver without VADER, two-pass

- Write Silver with minimal schema first (no sentiment columns)
- Second notebook adds VADER only to top-N most-reviewed games
- Falls back to Hatch A scope-wise

### Hatch C — Dev screenshots + ADR

- If Fabric trial dies mid-run
- Use dev environment screenshots
- Write ADR explaining trial-capacity constraints and what would happen in real prod
- This IS a legitimate engineer story, not a failure

---

## Post-deadline backlog (NOT today)

- [ ] Commit SQL Endpoint view DDLs to GitHub
  - Suggested path: `Labs/Lab02_Fabric/sql_views/`
  - One `.sql` file per view
  - Pairs naturally with README work (lots of analytical logic lives in these views)
- [ ] Rename `NB_1_Bronze` → `NB_IGDB_Bronze`, `NB_2_Silver` → `NB_IGDB_Silver`
  - VS Code + Fabric Data Engineering extension rename tar pit
  - Cosmetic only — will not block anything

---

## Permission to Stop — today

When all 5 are checked, you're done. README drafting is a bonus round, not a requirement.

- [x] **PtS 1.** `steam.versionControl` exists AND `steam.loadControlReviews.is_loaded` column added
- [x] **PtS 2.** Prod lakehouse DDL deployed: 4 Delta tables + 13 views all queryable
- [x] **PtS 3.** `pl_IGDB_Medallion` extended with Steam Reviews branch + `factGameScores` synthesis step, wiring validated
- [x] **PtS 4.** Prod pipeline run completed on full 77M (OR documented Hatch A), `gold.factGameScores` populated with ~30k rows
- [ ] **PtS 5.** ≥5 screenshots in `/docs/screenshots/` covering lakehouse, Spark UI, gold queries, agg view, audit WH

---
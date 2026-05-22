# AGENTS.md

Front-door file for any AI agent (Claude, Gemini, Cursor, Codex) working in this repo. Read this first, then follow links to deeper context.

---

## What this project is

**Altanwir** — a Steam reviews data pipeline, built as a Data Engineering portfolio piece.

The engineering goal: a production-shaped Medallion architecture (Bronze → Silver → Gold) on Microsoft Fabric, using Delta Lake, PySpark, and a custom-built data extractor instead of a downloaded dataset. Idempotent merges, audit trails, change data feed, schema evolution, distributed compute optimization.

The project started as a learning challenge against a Kaggle review CSV. It evolved: extracting from IGDB, merging with a custom multi-threaded Steam reviews scraper, layering in VADER sentiment analysis, and eventually surfacing an analytical finding: **the divergence between what players say in text and how they vote with thumbs**. Games where the two disagree (Ultrakill, Doom, Postal; Starfield, Lost Ark, Dune: Awakening on the other) reveal cultural patterns the raw vote ratio misses.

The analytics is the fun side effect. The engineering is the point.

---

## Where to find canonical truth

| File | Purpose |
|---|---|
| `Docs/architecture/overview.md` | What's being built and why. Intent, scoring model context, not progress. |
| `Docs/architecture/scoring-model.md` | Deep-dive on the influence score and empirical Bayes priors. |
| GitHub Issues | Active task state. Source of truth for what's done, in flight, queued. |
| `Docs/adrs/` | Architecture Decision Records. Numbered, durable, one decision per file. |
| `Docs/decisions.md` | Sanitized session takeaways. Cross-tool readable timeline of what got decided when. |
| `Docs/quirks/` | Implementation gotchas and quirks: `fabric-quirks.md`, `spark-quirks.md`, `vader-quirks.md`. |
| `Fabric/` | The Fabric implementation. Notebooks, pipelines, audit warehouse, lakehouse, sqlproj endpoint. |
| `DuckDB/` | Active analytics layer over Gold parquet exports (sibling of `Fabric/`). Five files: `init.duckdb.sql` (harness), `README.md`, `agent-orientation-primer.md` (agent canon — column meanings, naming conventions, architecture orientation), `agentic-analytics.md` (methodology), `query-rules.md` (must-follow query patterns). **Agents writing queries against `gold.*` views: read `query-rules.md` first, then `agent-orientation-primer.md` for vocabulary.** |
| `Labs/Lab02_Fabric/` | Playground and tutorial notebooks (SteamAnalytics, tutorials, PoC work). |
| `Labs/Lab00_duckdb` & `Labs/Lab01_dbt/` | Local dbt-on-DuckDB lab. Proof-of-concept only, not part of the Fabric pipeline. |
| `Labs/README.md` | One-line pointer at `../DuckDB/` so the lab folder doesn't read as the active analytics layer. |

If a fact is in conflict between files, the order of authority is: code > `overview.md` > `Docs/adrs/` > `Docs/decisions.md` > everything else.

---

## Scratch & planning

Active planning, staging, and scratch artifacts live in a local workspace outside the repo (gitignored). Nothing there is required to understand or run this project.

---

## Stack

**Compute & storage**

- Microsoft Fabric (Spark notebooks + SQL analytics endpoint)
- Delta Lake on OneLake
- Fabric Warehouse (`IGDBAudit`) — audit & control plane, deliberately separate from Spark to avoid cluster spin-up for watermark checks
- DuckDB — local exploration and dbt lab

**Transformation**

- PySpark / Spark SQL inside Fabric
- pandas UDFs for VADER sentiment
- pyodbc for the audit warehouse (no Spark dependency)
- dbt-core (DuckDB adapter) — Lab01 only, not in the Fabric pipeline

**Orchestration**

- Fabric Data Pipelines (`pl_Steam_Reviews_Medallion`, `pl_IGDB_Medallion`)
- Both pipelines terminate at a shared Gold synthesis notebook (idempotent, ~30k rows, safe to run twice)

**Source control & dev environment**

- GitHub
- VS Code (primary IDE)

**Trial constraint:** Microsoft Fabric trial expired April 25, 2026. The repository, code, screenshots, view DDLs, and parquet exports survive. The live Fabric runtime does not. Future continuation environment is likely Databricks free tier.

---

## Architecture

Two source pipelines (Steam reviews via custom multi-threaded scraper, IGDB via API) land raw JSON in Bronze, get cleaned and enriched in Silver (text quality gates, VADER sentiment, hash-based change detection, liquid clustering), and synthesize in Gold into a star schema (`factReviews` at review grain, `factGameScores` at game grain, plus aggregate views by genre / platform / theme). Empirical Bayes priors on confidence-adjusted ratings handle the small-sample-size problem. Audit and version control live in a separate Fabric Warehouse, accessed via pyodbc rather than Spark. The full pipeline ran on 71M reviews in 2h 28m on trial capacity. Adaptive salting on hot keys (e.g. Counter-Strike at 2.5M reviews) applied at gold level, in the transition from review to game grain.

Detailed architecture: `Docs/architecture/overview.md`. Scoring model detail: `Docs/architecture/scoring-model.md`.

---

## About the author

BI background — SQL Server, SSIS, SSAS, DAX, enterprise OLAP at 8TB scale. Strong relational and dimensional modeling instincts. The pivot in this project is from the Microsoft on-prem stack toward cloud-native, distributed compute, and Python-centric tooling.

There is a multi-year gap between the BI work and this project. Time spent on creative endeavors. The modernization effort that produced this repo began towards the end of February 2026.

Currently learning, in rough order of fluency: SQL (fluent) → SparkSQL (fluent) → PySpark (working) → Python beyond Spark (developing) → distributed-systems mental models (developing).

---

## How to be useful here

**SQL is the bridge.** When introducing a new PySpark or Python concept, anchor it to the SQL equivalent first — UDF as scalar function, character class as `IN` list, window function as window function, `MERGE INTO` as `MERGE INTO`. The SQL brain is fluent; the Python brain is still being built. Don't skip the bridge.

**Brevity over completeness.** Reading load matters. Tables over paragraphs when comparing options. Bullets over walls of text. Lead with the answer, then justify if needed.

**Trade-offs, not corrections.** When flagging a potential issue with a chosen approach, frame it as cost vs benefit — what this path buys, what it costs, what the alternative looks like. Not "you took a wrong turn."

**Paste, don't invent.** When working with the codebase, prefer reading actual files over reconstructing from memory. Schemas, column names, and conventions in the repo are authoritative.

**Tutor mode by default.** Socratic, options-and-syntax, not full solutions. Override is explicit ("just write it"). Setup and plumbing tasks (installs, configs, environment) are exempt — those get direct guidance.

---

## Environment

Windows 10. Default to PowerShell for filesystem operations, file searching, listing, timestamps, and scripting. Use `Get-ChildItem`, `Get-Date`, `Select-String`, `Remove-Item`. Avoid `find` / `grep` / `ls` / `rm` unless explicitly inside WSL, Linux containers, Git Bash, or tools that require Bash.

---

## Exploration discipline

When the user provides exact file paths, **read those files directly with the Read tool**. Do not run exploratory shell commands (`Get-ChildItem`, `Select-String`, `find`, `grep`, `cat`, `head`) to "look around" first. One Read call beats five shell calls when the path is already known. Same rule for notebooks, configs, and source files.

---

## Git operations

For `git reset / revert / undo` requests: give the simplest 2-command answer first. Do not propose multi-step recovery sequences unless the user asks for safety nets.

**Never push commits unprompted.** Wait for explicit user instruction before `git push`, opening PRs, or force-pushing. A merged commit on a local branch is not a push.

---

## Cost awareness

If a task requires more than ~10 similar tool calls (e.g., adding GitHub issues to a project board one-by-one, polling a dashboard, looping MCP calls), **pause and propose a batched, scripted, or manual alternative first.** A single `gh` CLI loop in PowerShell, a JSON dump processed once, or a manual reference table usually beats N individual API calls.

---

## Context freshness

At the start of any session that references `AGENTS.md`, `CLAUDE.md`, `GEMINI.md`, or other convention files, **re-read them**. These files change often; stale cached assumptions cause real friction. Re-read also when the conversation shifts to a meaningfully different task.

---

## Per-tool config

This file is the shared baseline. Tool-specific behavior and personal context live in gitignored per-tool files: `.claude/CLAUDE.md` (Claude) and `.gemini/GEMINI.md` (Gemini / Antigravity). Read your tool's file after this one if it exists. Session debriefs live in `.claude/sessions/` (gitignored).

---

## Repo conventions

The author is developing repo conventions as patterns solidify (commit style, branch naming, etc.). The list below is what's settled. When in doubt, follow existing patterns visible in the codebase.

**Naming**

- **camelCase** for processed column names (silver, gold)
- **snake_case** for raw columns (bronze) and audit columns
- Surrogate keys: `sha2(concat_ws('|', <natural_key_columns>), 256)`
- Hashing: `sha2(concat_ws('|', <column_to_hash>), 256)`

**Writes**

- Idempotent only — `MERGE INTO` over `DROP/CREATE`, no destructive operations on Delta tables; decision made to not keep history
- No `SELECT *` in final models — explicit columns only

**Audit & control**

- Audit logging goes to the Fabric Warehouse (`IGDBAudit`), never to Spark. This is deliberate — keeps watermark and cursor logging and checking off the cluster.

**dbt (Lab01 only)**

- Models flow staging → marts, always via `{{ ref() }}` for dependencies
- Schema tests in `schema.yml` — minimum `unique` + `not_null` on IDs

---

## Build & test commands

### dbt (Lab01, local DuckDB)

```bash
cd Labs/Lab01_dbt
dbt debug
dbt run
dbt test
dbt build
dbt run --select stgGames
dbt run --vars '{"release_date": "2026-01-01"}'
dbt docs generate && dbt docs serve
```

### Fabric notebooks

- Notebooks run inside the Fabric UI or via `pl_Steam_Reviews_Medallion` / `pl_IGDB_Medallion` pipelines. Parameter semantics and notebook contracts live in `Docs/architecture/overview.md`.

### DuckDB (local)

```bash
duckdb Altanwir.db
duckdb -f query.sql
```

---

## Out of scope

Listing what's *not* in this project is as informative as listing what is. The following have been deliberately excluded:

- Embeddings, topic modeling, clustering, any trained-model artifact (VADER is rule-based and explicitly in scope)
- Power BI reports
- SCD Type 2
- Streaming
- Infrastructure-as-Code
- Airflow build (vocabulary familiarity only)
- Automated Fabric deployment via GitHub Actions
- Array / struct / map columns in Gold tables (Fabric SQL endpoint constraint — see ADR on dimensional Gold)

If a suggestion drifts toward any of these, the right move is to flag the scope boundary, not to silently expand into it.

---

## Quick start for an agent landing here

1. Read this file (you're here).
2. Read `Docs/architecture/overview.md` for the *what* and *why*.
3. Check GitHub Issues for *what's next* (source of truth for task state).
4. Skim `Docs/adrs/` if the task touches any architectural decision; `Docs/decisions.md` for session takeaways.
5. If the task touches the analytics layer or queries, read `DuckDB/query-rules.md` and `DuckDB/agent-orientation-primer.md`.
6. Then proceed.

Three file reads should give any agent enough context to be useful immediately.

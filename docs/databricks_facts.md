# Databricks facts (collected from the workspace)

Collected on 2026-10-25 through the Databricks REST API (Jobs 2.1, Repos, Workspace) and the SQL
Statement Execution API on the workspace's serverless SQL warehouse. Read-only. All times are UTC.
No credentials are stored in this file.

## Workspace

| Item | Value |
|---|---|
| Edition | Databricks Free Edition, serverless only, daily compute limit |
| Region (from billing SKUs) | AWS us-east-2 (Ohio) |
| Catalog | `workspace` |
| Project schemas | `pkwiki_raw`, `pkwiki_bronze`, `pkwiki_silver`, `pkwiki_ops` (plus `pkwiki_drift_bronze`, `pkwiki_drift_silver`, `pkwiki_drift_ops` for the schema drift demo) |
| Git folder | `/Workspace/Users/<user>/pk-wiki-integrity-lakehouse` (repo id 1932041229346639, branch `main`, head `4f8216c` "readme: results from the runs on databricks") |
| Job | `pkwiki-lakehouse-daily`, job id 628071350539448, schedule `0 0 21 * * ?` UTC, PAUSED, max 1 concurrent run |
| SQL warehouse | "Serverless Starter Warehouse", id `9af9b3eed023027a`, serverless PRO, 2X-Small, 1 cluster, auto-stop 10 min, Photon on |

Where the code is read from:

- The job uses `git_source` (GitHub `main`) with notebook paths relative to the repo root
  (`notebooks/01_bronze_ingest`, `notebooks/02_silver_unify_and_mask`,
  `notebooks/03_silver_scd2_and_reverts`). So a job run checks out the latest commit on GitHub
  `main`; it does not read the workspace Git folder.
- The one-time runs on 2026-10-11 (`dbx-full-1` and the rest) ran the notebooks from the Git folder:
  `/Workspace/Users/<user>/pk-wiki-integrity-lakehouse/notebooks/<name>` (source WORKSPACE).

## Run history (all runs in the workspace)

22 runs, all on 2026-10-11 between 17:08 and 17:38 UTC, all SUCCESS: 21 one-time notebook runs
plus 1 run of the job. "Setup s" is the serverless environment start, "Exec s" the notebook
execution. "Jobs DBU" is from `system.billing.usage`, matched on `usage_metadata.job_run_id`.

| Start (UTC) | Run id | Kind | Notebook / task | Parameters | Setup s | Exec s | Run total s | Jobs DBU | Result |
|---|---|---|---|---|---|---|---|---|---|
| 2026-10-11 17:08:08 | 248163739010657 | one-time run | 00_setup | catalog=workspace, prefix=pkwiki, seed_samples=yes | 4 | 99 | 103.8 | 0.2178 | SUCCESS |
| 2026-10-11 17:10:55 | 676284436159547 | one-time run | 01_bronze_ingest | load_type=full, snapshot=2026-08, run_id=dbx-full-1 | 3 | 81 | 84.7 | 0.2191 | SUCCESS |
| 2026-10-11 17:12:38 | 451949132960041 | one-time run | 02_silver_unify_and_mask | load_type=full, snapshot=2026-08, run_id=dbx-full-1 | 3 | 80 | 84.1 | 0.3132 | SUCCESS |
| 2026-10-11 17:14:12 | 1000409131072632 | one-time run | 03_silver_scd2_and_reverts | load_type=full, snapshot=2026-08, run_id=dbx-full-1 | 3 | 43 | 47.4 | 0.2228 | SUCCESS |
| 2026-10-11 17:15:19 | 855912292957346 | one-time run | 01_bronze_ingest | load_type=incremental, start_date=2026-09-27, run_id=dbx-incr-1 | 2 | 61 | 63.8 | 0.1914 | SUCCESS |
| 2026-10-11 17:16:30 | 697081709966488 | one-time run | 02_silver_unify_and_mask | load_type=incremental, start_date=2026-09-27, run_id=dbx-incr-1 | 3 | 55 | 59.4 | 0.1941 | SUCCESS |
| 2026-10-11 17:17:43 | 210867658144266 | one-time run | 03_silver_scd2_and_reverts | load_type=incremental, start_date=2026-09-27, run_id=dbx-incr-1 | 2 | 66 | 69.2 | 0.2088 | SUCCESS |
| 2026-10-11 17:18:56 | 320852091696540 | one-time run | 01_bronze_ingest | load_type=full, snapshot=2026-08, run_id=dbx-full-2 | 4 | 55 | 59.6 | 0.1452 | SUCCESS |
| 2026-10-11 17:20:07 | 802369272050054 | one-time run | 02_silver_unify_and_mask | load_type=full, snapshot=2026-08, run_id=dbx-full-2 | 2 | 63 | 65.7 | 0.1953 | SUCCESS |
| 2026-10-11 17:21:18 | 219135878082543 | one-time run | 03_silver_scd2_and_reverts | load_type=full, snapshot=2026-08, run_id=dbx-full-2 | 2 | 42 | 44.6 | 0.1497 | SUCCESS |
| 2026-10-11 17:22:07 | 874237958442746 | one-time run | 01_bronze_ingest | load_type=incremental, run_id=dbx-incr-2-standard | 2 | 59 | 62.4 | 0.1817 | SUCCESS |
| 2026-10-11 17:23:18 | 966217278291750 | one-time run | 02_silver_unify_and_mask | load_type=incremental, run_id=dbx-incr-2-standard | 2 | 61 | 63.7 | 0.1450 | SUCCESS |
| 2026-10-11 17:24:29 | 361069843802487 | one-time run | 03_silver_scd2_and_reverts | load_type=incremental, run_id=dbx-incr-2-standard | 4 | 55 | 59.7 | 0.1297 | SUCCESS |
| 2026-10-11 17:26:16 | 946593231059215 | one-time run | 00_setup | catalog=workspace, prefix=pkwiki_drift, seed_samples=no | 3 | 53 | 57.0 | 0.1317 | SUCCESS |
| 2026-10-11 17:27:40 | 327579688267055 | one-time run | 01_bronze_ingest | prefix=pkwiki_drift, load_type=incremental, start_date=2026-09-28, wikis=pswiki, run_id=dbx-drift-incr | 3 | 39 | 42.6 | 0.1506 | SUCCESS |
| 2026-10-11 17:28:29 | 365962047235342 | one-time run | 02_silver_unify_and_mask | prefix=pkwiki_drift, load_type=incremental, start_date=2026-09-28, wikis=pswiki, run_id=dbx-drift-incr | 3 | 46 | 50.2 | 0.1446 | SUCCESS |
| 2026-10-11 17:29:29 | 73633849369882 | one-time run | 03_silver_scd2_and_reverts | prefix=pkwiki_drift, load_type=incremental, start_date=2026-09-28, wikis=pswiki, run_id=dbx-drift-incr | 2 | 48 | 51.1 | 0.1578 | SUCCESS |
| 2026-10-11 17:30:30 | 132015228047116 | one-time run | 01_bronze_ingest | prefix=pkwiki_drift, load_type=full, snapshot=2026-08, wikis=pswiki, run_id=dbx-drift-full | 2 | 37 | 39.7 | 0.1804 | SUCCESS |
| 2026-10-11 17:31:21 | 863528647979101 | one-time run | 02_silver_unify_and_mask | prefix=pkwiki_drift, load_type=full, snapshot=2026-08, wikis=pswiki, run_id=dbx-drift-full | 4 | 56 | 61.3 | 0.2047 | SUCCESS |
| 2026-10-11 17:32:33 | 259756001839097 | one-time run | 03_silver_scd2_and_reverts | prefix=pkwiki_drift, load_type=full, snapshot=2026-08, wikis=pswiki, run_id=dbx-drift-full | 2 | 42 | 45.0 | 0.1860 | SUCCESS |
| 2026-10-11 17:33:49 | 381774305356598 | one-time run | 99_checks | defaults | 3 | 53 | 57.2 | 0.2821 | SUCCESS |
| 2026-10-11 17:35:05 | 365321304344769 | job run | bronze (01_bronze_ingest) | job defaults (load_type=incremental) | 6 | 55 | 148.8 (whole run) | 0.5029 (whole run) | SUCCESS |
| | | | silver (02_silver_unify_and_mask) | job defaults (load_type=incremental) | 2 | 44 | | | SUCCESS |
| | | | scd2 (03_silver_scd2_and_reverts) | job defaults (load_type=incremental) | 2 | 37 | | | SUCCESS |

Run page of the job run:
`https://dbc-f8a0f45c-f7da.cloud.databricks.com/?o=7474660113881069#job/628071350539448/run/365321304344769`

### Per logical pipeline run

| Pipeline run | Tasks | Wall time s (sum of runs) | Exec s | Jobs DBU |
|---|---|---|---|---|
| 00_setup (`pkwiki`, seed samples) | 1 | 103.8 | 99 | 0.2178 |
| `dbx-full-1` (full, snapshot 2026-08) | 3 | 216.2 | 204 | 0.7551 |
| `dbx-incr-1` (incremental, start_date 2026-09-27) | 3 | 192.4 | 182 | 0.5943 |
| `dbx-full-2` (same full load again, no-op) | 3 | 169.9 | 160 | 0.4902 |
| `dbx-incr-2-standard` (daily incremental, no-op) | 3 | 185.8 | 175 | 0.4564 |
| 00_setup (`pkwiki_drift`) | 1 | 57.0 | 53 | 0.1317 |
| `dbx-drift-incr` (pswiki only) | 3 | 143.9 | 133 | 0.4530 |
| `dbx-drift-full` (pswiki only) | 3 | 145.9 | 135 | 0.5711 |
| 99_checks | 1 | 57.2 | 53 | 0.2821 |
| Job run 365321304344769 (bronze, silver, scd2 as one job, incremental, no-op) | 3 | 148.8 | 136 | 0.5029 |
| **Total** | 24 | **1420.9** | | **4.4546** |

Observations for the FinOps section:

- The same no-op incremental run took 185.8 s as three separate one-time runs and 148.8 s as one
  multi-task job run (about 20% less wall time), because the job reuses one serverless environment
  across its tasks. DBU was about the same (0.456 vs 0.503), so the gain is latency, not credits.
- A full load of the sample (20,000 dump rows) took 216 s and 0.76 DBU the first time and 170 s and
  0.49 DBU on the idempotent re-run (0 rows written).
- Serverless setup is small (2 to 6 s per task). Most time is Spark execution on a tiny data set,
  so run time is dominated by fixed per-statement overhead (Delta commits, MERGE planning, audit
  writes), not by data volume.

## Row counts (2026-10-25)

| Table | Rows |
|---|---|
| `workspace.pkwiki_bronze.mediawiki_history` | 20,000 |
| `workspace.pkwiki_bronze.recentchanges` | 17,405 |
| `workspace.pkwiki_silver.revision` | 26,169 |
| `workspace.pkwiki_silver.page` (SCD2, all versions) | 9,162 (9,147 current) |
| `workspace.pkwiki_silver.editor` (SCD2, all versions) | 542 (542 current) |
| `workspace.pkwiki_silver.page_event` | 4,067 |
| `workspace.pkwiki_silver.user_event` | 230 |
| `workspace.pkwiki_ops.pipeline_execution_logs` | 53 |
| `workspace.pkwiki_ops.quarantine` | 0 |
| `workspace.pkwiki_ops.schema_drift_log` | 0 |
| `workspace.pkwiki_drift_bronze.mediawiki_history` | 6 |
| `workspace.pkwiki_drift_bronze.recentchanges` | 6 |
| `workspace.pkwiki_drift_silver.revision` | 4 |
| `workspace.pkwiki_drift_ops.pipeline_execution_logs` | 15 |
| `workspace.pkwiki_drift_ops.schema_drift_log` | 6 |

### Audit log summary (`pkwiki_ops.pipeline_execution_logs`, grouped by run and layer)

| run_id | Layer | Load type | Steps | Sum duration_sec | Rows read | Inserted | Updated | Quarantined |
|---|---|---|---|---|---|---|---|---|
| dbx-full-1 | raw_to_bronze | full | 4 | 54.6 | 20,000 | 20,000 | 0 | 0 |
| dbx-full-1 | bronze_to_silver | full | 4 | 48.1 | 20,000 | 20,000 | 0 | 0 |
| dbx-full-1 | silver_scd2 | full | 2 | 19.6 | 26,793 | 3,208 | 0 | 0 |
| dbx-incr-1 | raw_to_bronze | incremental | 4 | 38.5 | 17,405 | 17,405 | 0 | 0 |
| dbx-incr-1 | bronze_to_silver | incremental | 4 | 28.4 | 10,466 | 10,466 | 0 | 0 |
| dbx-incr-1 | silver_scd2 | incremental | 3 | 41.0 | 27,206 | 6,496 | 17 | 0 |
| dbx-full-2 | raw_to_bronze | full | 4 | 34.6 | 20,000 | 0 | 0 | 0 |
| dbx-full-2 | bronze_to_silver | full | 4 | 34.9 | 20,000 | 0 | 0 | 0 |
| dbx-full-2 | silver_scd2 | full | 2 | 20.2 | 26,793 | 0 | 0 | 0 |
| dbx-incr-2-standard | raw_to_bronze | incremental | 4 | 30.4 | 17,405 | 0 | 0 | 0 |
| dbx-incr-2-standard | bronze_to_silver | incremental | 4 | 30.3 | 10,466 | 0 | 0 | 0 |
| dbx-incr-2-standard | silver_scd2 | incremental | 3 | 28.7 | 27,206 | 0 | 0 | 0 |
| 365321304344769 (job run) | raw_to_bronze | incremental | 4 | 30.9 | 17,405 | 0 | 0 | 0 |
| 365321304344769 (job run) | bronze_to_silver | incremental | 4 | 29.6 | 10,466 | 0 | 0 | 0 |
| 365321304344769 (job run) | silver_scd2 | incremental | 3 | 25.7 | 27,206 | 0 | 0 | 0 |

All 53 steps have status SUCCESS. The job run logs `run_id` as the numeric Databricks run id (the
job parameter default is `{{job.run_id}}`); the one-time runs log the names passed in.

## Billing (`system.billing.usage`)

The table is readable from this workspace (219 records). The latest `usage_date` and
`ingestion_date` in it are 2026-10-15, so usage after that date (including this collection) is not
visible yet. List prices come from `system.billing.list_prices`. Free Edition charges nothing, so
USD figures are what the same usage would cost at list price.

### By day and product

| Date | Product | SKU | Unit | Quantity |
|---|---|---|---|---|
| 2026-09-07 | GENIE | GENIE_FREE_USAGE | DBU | 96.1941 |
| 2026-09-07 | INTERACTIVE (serverless notebooks) | PREMIUM_ALL_PURPOSE_SERVERLESS_COMPUTE_US_EAST_OHIO | DBU | 3.5976 |
| 2026-09-07 | PREDICTIVE_OPTIMIZATION | PREMIUM_JOBS_SERVERLESS_COMPUTE_US_EAST_OHIO | DBU | 0.1362 |
| 2026-09-10 | PREDICTIVE_OPTIMIZATION | PREMIUM_JOBS_SERVERLESS_COMPUTE_US_EAST_OHIO | DBU | 0.1210 |
| 2026-10-11 | JOBS (the 22 runs above) | PREMIUM_JOBS_SERVERLESS_COMPUTE_US_EAST_OHIO | DBU | 4.4547 |
| 2026-10-11 | SQL (warehouse 9af9b3eed023027a) | PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST_OHIO | DBU | 2.4437 |
| 2026-10-11 | PREDICTIVE_OPTIMIZATION | PREMIUM_JOBS_SERVERLESS_COMPUTE_US_EAST_OHIO | DBU | 0.2003 |
| 2026-10-11 | NETWORKING | PUBLIC_CONNECTIVITY_DATA_PROCESSED_US_EAST_OHIO | GB | 0.0021 |
| 2026-10-13 | PREDICTIVE_OPTIMIZATION | PREMIUM_JOBS_SERVERLESS_COMPUTE_US_EAST_OHIO | DBU | 0.1634 |
| 2026-10-14 | PREDICTIVE_OPTIMIZATION | PREMIUM_JOBS_SERVERLESS_COMPUTE_US_EAST_OHIO | DBU | 0.1334 |
| 2026-09-07 to 2026-10-15 | DEFAULT_STORAGE | PREMIUM_DATABRICKS_STORAGE_US_EAST_OHIO | DSU | 1.5981 in total (0.008 to 1.09 per day) |

The 2026-09-07 rows come from an earlier lab notebook in the same workspace, not from this project.

### DBU at list price (all dates)

| SKU | DBU | List USD per DBU | List USD |
|---|---|---|---|
| PREMIUM_JOBS_SERVERLESS_COMPUTE_US_EAST_OHIO (jobs plus predictive optimization) | 5.2090 | 0.35 | 1.82 |
| PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST_OHIO | 2.4437 | 0.70 | 1.71 |
| PREMIUM_ALL_PURPOSE_SERVERLESS_COMPUTE_US_EAST_OHIO | 3.5976 | 0.75 | 2.70 |
| GENIE_FREE_USAGE | 96.1941 | free SKU | 0 |

Project pipeline compute on 2026-10-11 (all 22 runs): 4.4546 DBU, about 1.56 USD at list price,
for about 23.7 minutes of serverless job time. The one SQL warehouse session that day used
2.44 DBU (about 1.71 USD), more than half of the jobs total, which is why the queries for this
report were batched into a few warehouse sessions.


## Phase 3: Gold layer runs, first pass (2026-10-25), SUPERSEDED

> **Superseded.** The Gold numbers in this section come from commit `f1d3261`, before the
> surrogate key fix (`3d5673e`). Back then `sk()` returned signed xxhash64 values, so about half the
> real page and editor versions got negative keys and facts that pointed at them were treated as
> unknown members. Use the section "Gold after the key fix" at the end of this file for current
> numbers. The run durations, the EXPLAIN plan shape and the Query Profile steps below are still
> valid.

Code: branch `phase3-gold-wip`, commit `f1d3261` ("gold layer: star schema, merge refresh after
silver, audit logging (wip)"). The workspace Git folder was switched to that branch for these runs
(Repos API, repo id 1932041229346639). The job `pkwiki-lakehouse-daily` was not changed and is
still PAUSED; it gets the `gold` task after the merge to `main`.

`00_setup` was not re-run: `runner.run_gold` calls `create_layers(spark, cfg, ("gold", "ops"))`,
which creates the `pkwiki_gold` schema, the 11 Gold tables and `pkwiki_ops.gold_watermark` (with
the informational PK and FK constraints) on the first Gold run.

Both runs were one-time multi-task runs on serverless jobs compute (`runs/submit`, notebooks from
the Git folder). A last read-only `checks` task (a helper notebook outside the repo) collected row
counts, `DESCRIBE DETAIL`, `DESCRIBE HISTORY`, the audit rows and the plan on the same compute, so
no SQL warehouse session was needed.

### Runs and per-task durations

| Run | Databricks run id | Task | Notebook | Parameters | Setup s | Exec s | Result |
|---|---|---|---|---|---|---|---|
| `dbx-gold-full-1` | 537104133346022 (334.9 s total) | gold | 04_gold_star | load_type=full, run_id=dbx-gold-full-1 | 4 | 291 | SUCCESS |
| | | checks | gold_checks (read-only) | | 2 | 36 | SUCCESS |
| `dbx-gold-incr-1` | 901398887650123 (390.9 s total) | bronze | 01_bronze_ingest | load_type=incremental, start_date=2026-10-25, run_id=dbx-gold-incr-1 | 3 | 66 | SUCCESS |
| | | silver | 02_silver_unify_and_mask | same | 2 | 50 | SUCCESS |
| | | scd2 | 03_silver_scd2_and_reverts | same | 2 | 55 | SUCCESS |
| | | gold | 04_gold_star | same | 2 | 173 | SUCCESS |
| | | checks | gold_checks (read-only) | | 1 | 32 | SUCCESS |

Run pages:

- `dbx-gold-full-1`, whole run: https://dbc-f8a0f45c-f7da.cloud.databricks.com/?o=7474660113881069#job/604616113073500/run/537104133346022
- `dbx-gold-full-1`, Gold task (builds `fact_revision` and the aggregates): https://dbc-f8a0f45c-f7da.cloud.databricks.com/?o=7474660113881069#job/604616113073500/run/386774278628102
- `dbx-gold-incr-1`, whole run: https://dbc-f8a0f45c-f7da.cloud.databricks.com/?o=7474660113881069#job/1122166254975470/run/901398887650123
- `dbx-gold-incr-1`, Gold task: https://dbc-f8a0f45c-f7da.cloud.databricks.com/?o=7474660113881069#job/1122166254975470/run/397206758106845

Of the 291 s of the full Gold task, the audited steps account for 140.6 s (99.2 s for the 11 table
loads, 41.4 s for the four OPTIMIZE steps). The rest is the first-run table creation with its PK
and FK `ALTER TABLE` statements, the imports and the row count display at the end of the notebook.
On the incremental run the audited Gold steps took 126.3 s (95.8 s loads, 30.5 s OPTIMIZE) of
173 s. DBU per run is not available yet: `system.billing.usage` still ends at 2026-10-15.

### New data for the incremental run

The landing volume only had `ingest_date=2026-09-27`, which is already in Silver, so a re-run of
it would write 0 rows and prove nothing. We ran the extractor (step 4 of the execution guide) for
the last 24 hours, from 2026-10-24 15:02 to 2026-10-25 15:02 UTC, and copied its output to
`/Volumes/workspace/pkwiki_raw/landing/recentchanges/load_type=incremental/ingest_date=2026-10-25/`:

| Wiki | Recent changes | File size |
|---|---|---|
| urwiki | 11,994 | 8.4 MB |
| sdwiki | 817 | 0.53 MB |
| pnbwiki | 85 | 56 KB |
| pswiki | 6 | 3.7 KB |

### Gold row counts, before and after the incremental run

| Table | After `dbx-gold-full-1` | After `dbx-gold-incr-1` | Delta |
|---|---|---|---|
| `pkwiki_gold.dim_date` | 10,957 | 10,957 | 0 |
| `pkwiki_gold.dim_wiki` | 4 | 4 | 0 (1 row updated) |
| `pkwiki_gold.dim_change_type` | 12 | 12 | 0 |
| `pkwiki_gold.dim_page` (SCD2) | 9,166 | 13,859 | +4,693 |
| `pkwiki_gold.dim_editor` (SCD2) | 546 | 577 | +31 |
| `pkwiki_gold.fact_revision` | 26,169 | 32,102 | +5,933 |
| `pkwiki_gold.fact_page_event` | 4,067 | 7,786 | +3,719 |
| `pkwiki_gold.agg_wiki_daily` | 720 | 727 | +7 |
| `pkwiki_gold.agg_revert_latency` | 67 | 69 | +2 |
| `pkwiki_gold.agg_edit_wars` | 1 | 1 | 0 |
| `pkwiki_gold.agg_editor_monthly` | 121 | 126 | +5 |
| `pkwiki_ops.gold_watermark` | 4 | 4 | 0 |

Silver for comparison: `revision` 26,169 to 32,102 (+5,933), `page_event` 4,067 to 7,786
(+3,719), `page` 9,162 to 13,855 (+4,693), `editor` 542 to 573 (+31), `user_event` 230 to 242
(+12). The facts match Silver row for row; `dim_page` and `dim_editor` are Silver plus one unknown
member per wiki (4 rows).

### Audit rows in `pkwiki_ops.pipeline_execution_logs`

`dbx-gold-full-1` (15 rows, all SUCCESS):

| Layer | Target | Duration s | Rows read | Inserted | Updated | Source |
|---|---|---|---|---|---|---|
| silver_to_gold | dim_date | 15.2 | 10,957 | 10,957 | 0 | generated calendar |
| silver_to_gold | dim_wiki | 7.4 | 4 | 4 | 0 | static list + silver.revision |
| silver_to_gold | dim_change_type | 3.9 | 12 | 12 | 0 | static list |
| silver_to_gold | dim_page | 9.3 | 9,162 | 9,166 | 0 | silver.page (full) |
| silver_to_gold | dim_editor | 10.2 | 542 | 546 | 0 | silver.editor (full) |
| silver_to_gold | fact_revision | 12.9 | 26,169 | 26,169 | 0 | silver.revision (full) |
| silver_to_gold | fact_page_event | 10.2 | 4,067 | 4,067 | 0 | silver.page_event (full) |
| silver_to_gold | agg_wiki_daily | 7.2 | 26,169 | 720 | 0 | gold facts, all days |
| silver_to_gold | agg_revert_latency | 6.2 | 231 | 67 | 0 | gold facts, all months |
| silver_to_gold | agg_edit_wars | 7.5 | 18,747 | 1 | 0 | gold facts, all days |
| silver_to_gold | agg_editor_monthly | 9.2 | 15,536 | 121 | 0 | gold facts, all months |
| gold_optimize | fact_revision | 11.3 | | | | date_key, wiki_key, page_key |
| gold_optimize | fact_page_event | 8.0 | | | | date_key, wiki_key, page_key |
| gold_optimize | dim_page | 11.5 | | | | wiki_key, page_id |
| gold_optimize | dim_editor | 10.6 | | | | wiki_key, editor_hash |

`dbx-gold-incr-1` (26 rows, all SUCCESS):

| Layer | Target | Duration s | Rows read | Inserted | Updated | Source |
|---|---|---|---|---|---|---|
| raw_to_bronze | recentchanges (urwiki) | 17.8 | 11,994 | 11,994 | 0 | landing ingest_date=2026-10-25 |
| raw_to_bronze | recentchanges (pnbwiki) | 7.9 | 85 | 85 | 0 | landing ingest_date=2026-10-25 |
| raw_to_bronze | recentchanges (sdwiki) | 8.0 | 817 | 817 | 0 | landing ingest_date=2026-10-25 |
| raw_to_bronze | recentchanges (pswiki) | 8.7 | 6 | 6 | 0 | landing ingest_date=2026-10-25 |
| bronze_to_silver | revision | 8.4 | 5,933 | 5,933 | 0 | bronze.recentchanges 2026-10-25 |
| bronze_to_silver | page_event | 9.7 | 3,719 | 3,719 | 0 | bronze.recentchanges 2026-10-25 |
| bronze_to_silver | user_event | 7.6 | 12 | 12 | 0 | bronze.recentchanges 2026-10-25 |
| bronze_to_silver | quarantine | 6.2 | 0 | 0 | 0 | bronze.recentchanges 2026-10-25 |
| silver_scd2 | revision (reverts) | 14.3 | 5,982 | 0 | 5 | silver.revision 2026-10-25 |
| silver_scd2 | page | 13.6 | 5,981 | 4,693 | 2 | silver.revision 2026-10-25 |
| silver_scd2 | editor | 10.7 | 5,933 | 31 | 0 | silver.revision 2026-10-25 |
| silver_to_gold | dim_date | 4.7 | 10,957 | 0 | 0 | generated calendar |
| silver_to_gold | dim_wiki | 5.2 | 4 | 0 | 1 | static list + silver.revision |
| silver_to_gold | dim_change_type | 3.8 | 12 | 0 | 0 | static list |
| silver_to_gold | dim_page | 12.6 | 4,695 | 4,693 | 2 | silver.page (incremental) |
| silver_to_gold | dim_editor | 12.0 | 31 | 31 | 0 | silver.editor (incremental) |
| silver_to_gold | fact_revision | 16.3 | 21,497 | 5,933 | 2 | silver.revision (incremental) |
| silver_to_gold | fact_page_event | 13.7 | 6,343 | 3,719 | 0 | silver.page_event (incremental) |
| silver_to_gold | agg_wiki_daily | 7.3 | 14,996 | 7 | 0 | gold facts, 4 touched days |
| silver_to_gold | agg_revert_latency | 6.6 | 21 | 2 | 0 | gold facts, 2 months |
| silver_to_gold | agg_edit_wars | 5.9 | 14,996 | 0 | 0 | gold facts, 4 touched days |
| silver_to_gold | agg_editor_monthly | 7.7 | 19,959 | 5 | 1 | gold facts, all months |
| gold_optimize | fact_revision | 7.6 | | | | date_key, wiki_key, page_key |
| gold_optimize | fact_page_event | 6.8 | | | | date_key, wiki_key, page_key |
| gold_optimize | dim_page | 8.1 | | | | wiki_key, page_id |
| gold_optimize | dim_editor | 8.0 | | | | wiki_key, editor_hash |

The incremental Gold run only recomputed the aggregates for the 4 `date_key`s the new fact rows
touched (and the 2 months for `agg_revert_latency`), not the whole history.

### Liquid clustering and OPTIMIZE

`DESCRIBE DETAIL` after both runs:

| Table | clusteringColumns | numFiles after full | numFiles after incremental | Size after incremental |
|---|---|---|---|---|
| `fact_revision` | date_key, wiki_key, page_key | 1 | 1 | 2.04 MB |
| `fact_page_event` | date_key, wiki_key, page_key | 1 | 1 | 0.54 MB |
| `dim_page` | wiki_key, page_id | 1 | 1 | 1.20 MB |
| `dim_editor` | wiki_key, editor_hash | 1 | 1 | 62 KB |
| other 7 Gold tables | none (small, not clustered) | 1 to 8 | 1 to 8 | 4 to 28 KB |

`DESCRIBE HISTORY` shows, per clustered table, a `CLUSTER BY` operation (first run only, for
example `newClusteringColumns: date_key,wiki_key,page_key` on `fact_revision` at 15:06:10 UTC)
followed by `OPTIMIZE`. The OPTIMIZE after the full run recorded `clusterBy` with the clustering
columns on all four tables and compacted `dim_page` from 11 files to 1 and `dim_editor` from 12 to
1 (the facts were already one file each). After the incremental MERGE it compacted the new files
again: `fact_revision` 2 to 1, `fact_page_event` 2 to 1, `dim_page` 5 to 1, `dim_editor` 5 to 1.
On that second OPTIMIZE the history entry of the two facts shows `clusterBy: []` while the two
dims show their columns; `DESCRIBE DETAIL` still lists the clustering columns on all four, so the
tables stay liquid clustered. Because `CLUSTER BY` is only set when the columns differ, the second
run went straight to OPTIMIZE.

### Plan of the main fact aggregation (`agg_wiki_daily`)

`EXPLAIN FORMATTED` of `pipeline.gold.wiki_daily_frame(spark, cfg)` (all days), run on serverless
jobs compute on 2026-10-25 against the Gold tables after `dbx-gold-full-1`. What it shows:

- Everything runs in Photon (`PhotonScan`, `PhotonGroupingAgg`, `PhotonShuffledHashJoin`).
- `dim_editor` is small, so the left join to `fact_revision` is a broadcast hash join (the
  `EXECUTOR_BROADCAST` exchange in node 5); `fact_revision` is not shuffled for the join.
- The two `countDistinct` measures (`active_editors`, `active_human_editors`) make Spark add an
  `Expand` and a second aggregation level (nodes 9 to 19): first by (wiki_key, date_key,
  editor_hash), then by (wiki_key, date_key). That is the most expensive part of the plan.
- Only the 13 columns the aggregation needs are read from `fact_revision` (column pruning in the
  `ReadSchema` of node 1).
- `fact_page_event` is aggregated separately (nodes 20 to 25) and the two results are joined with
  a full outer shuffled hash join on (wiki_key, date_key) (node 26).
- `AdaptiveSparkPlan` means AQE can still change join strategies and partition counts at run time.

<details>
<summary>Full EXPLAIN FORMATTED output</summary>

```
== Physical Plan ==
AdaptiveSparkPlan (31)
+- == Initial Plan ==
   PhotonResultStage (30)
   +- PhotonColumnarToRow (29)
      +- PhotonProject (28)
         +- PhotonProject (27)
            +- PhotonShuffledHashJoin FullOuter (26)
               :- PhotonGroupingAgg (19)
               :  +- PhotonShuffleExchangeSource (18)
               :     +- PhotonShuffleMapStage (17)
               :        +- PhotonShuffleExchangeSink (16)
               :           +- PhotonGroupingAgg (15)
               :              +- PhotonGroupingAgg (14)
               :                 +- PhotonShuffleExchangeSource (13)
               :                    +- PhotonShuffleMapStage (12)
               :                       +- PhotonShuffleExchangeSink (11)
               :                          +- PhotonGroupingAgg (10)
               :                             +- PhotonExpand (9)
               :                                +- PhotonProject (8)
               :                                   +- PhotonBroadcastHashJoin LeftOuter (7)
               :                                      :- PhotonScan parquet workspace.pkwiki_gold.fact_revision (1)
               :                                      +- PhotonShuffleExchangeSource (6)
               :                                         +- PhotonShuffleMapStage (5)
               :                                            +- PhotonShuffleExchangeSink (4)
               :                                               +- PhotonProject (3)
               :                                                  +- PhotonScan parquet workspace.pkwiki_gold.dim_editor (2)
               +- PhotonGroupingAgg (25)
                  +- PhotonShuffleExchangeSource (24)
                     +- PhotonShuffleMapStage (23)
                        +- PhotonShuffleExchangeSink (22)
                           +- PhotonGroupingAgg (21)
                              +- PhotonScan parquet workspace.pkwiki_gold.fact_page_event (20)


(1) PhotonScan parquet workspace.pkwiki_gold.fact_revision
Output [13]: [wiki_key#53261, date_key#53262, editor_key#53265L, bytes_added#53276L, bytes_removed#53277L, seconds_to_revert#53278L, is_bot_edit#53279, is_anonymous_edit#53280, is_page_creation#53282, is_new_article#53283, is_revert#53284, is_reverted#53285, is_vandalism_suspect#53286]
Location: PreparedDeltaFileIndex [s3://dbstorage-prod-rw3fj/uc/37b00a9a-74fd-42ad-997d-3e5d098e319d/a26ac749-9a09-40e1-a784-c77e93981c56/__unitystorage/catalogs/d1bde834-391a-4ad0-bbbb-13e124c7730e/tables/229c04ca-0735-4378-a36f-f15acabb00b1]
ReadSchema: struct<wiki_key:int,date_key:int,editor_key:bigint,bytes_added:bigint,bytes_removed:bigint,seconds_to_revert:bigint,is_bot_edit:boolean,is_anonymous_edit:boolean,is_page_creation:boolean,is_new_article:boolean,is_revert:boolean,is_reverted:boolean,is_vandalism_suspect:boolean>

(2) PhotonScan parquet workspace.pkwiki_gold.dim_editor
Output [3]: [editor_key#53292L, editor_hash#53296, is_bot#53298]
Location: PreparedDeltaFileIndex [s3://dbstorage-prod-rw3fj/uc/37b00a9a-74fd-42ad-997d-3e5d098e319d/a26ac749-9a09-40e1-a784-c77e93981c56/__unitystorage/catalogs/d1bde834-391a-4ad0-bbbb-13e124c7730e/tables/f72b563b-042e-4fbb-a55e-38e6566862b7]
ReadSchema: struct<editor_key:bigint,editor_hash:string,is_bot:boolean>

(3) PhotonProject
Input [3]: [editor_key#53292L, editor_hash#53296, is_bot#53298]
Arguments: [editor_key#53292L, editor_hash#53296, is_bot#53298 AS editor_is_bot#53211]

(4) PhotonShuffleExchangeSink
Input [3]: [editor_key#53292L, editor_hash#53296, editor_is_bot#53211]
Arguments: SinglePartition

(5) PhotonShuffleMapStage
Input [3]: [editor_key#53292L, editor_hash#53296, editor_is_bot#53211]
Arguments: EXECUTOR_BROADCAST, [id=#31753]

(6) PhotonShuffleExchangeSource
Input [3]: [editor_key#53292L, editor_hash#53296, editor_is_bot#53211]
Arguments: false

(7) PhotonBroadcastHashJoin
Left keys [1]: [editor_key#53265L]
Right keys [1]: [editor_key#53292L]
Join type: LeftOuter
Join condition: None

(8) PhotonProject
Input [16]: [wiki_key#53261, date_key#53262, editor_key#53265L, bytes_added#53276L, bytes_removed#53277L, seconds_to_revert#53278L, is_bot_edit#53279, is_anonymous_edit#53280, is_page_creation#53282, is_new_article#53283, is_revert#53284, is_reverted#53285, is_vandalism_suspect#53286, editor_key#53292L, editor_hash#53296, editor_is_bot#53211]
Arguments: [wiki_key#53261, date_key#53262, bytes_added#53276L, bytes_removed#53277L, seconds_to_revert#53278L, is_bot_edit#53279, is_anonymous_edit#53280, is_page_creation#53282, is_new_article#53283, is_revert#53284, is_reverted#53285, is_vandalism_suspect#53286, CASE WHEN (editor_key#53265L >= 0) THEN editor_hash#53296 END AS editor_hash#53313, editor_is_bot#53211]

(9) PhotonExpand
Input [14]: [wiki_key#53261, date_key#53262, bytes_added#53276L, bytes_removed#53277L, seconds_to_revert#53278L, is_bot_edit#53279, is_anonymous_edit#53280, is_page_creation#53282, is_new_article#53283, is_revert#53284, is_reverted#53285, is_vandalism_suspect#53286, editor_hash#53313, editor_is_bot#53211]
Arguments: [[wiki_key#53261, date_key#53262, null, 0, null, CASE WHEN NOT is_bot_edit#53279 THEN 1 ELSE 0 END, CASE WHEN is_bot_edit#53279 THEN 1 ELSE 0 END, CASE WHEN is_anonymous_edit#53280 THEN 1 ELSE 0 END, CASE WHEN is_new_article#53283 THEN 1 ELSE 0 END, CASE WHEN is_page_creation#53282 THEN 1 ELSE 0 END, CASE WHEN is_revert#53284 THEN 1 ELSE 0 END, CASE WHEN is_reverted#53285 THEN 1 ELSE 0 END, CASE WHEN is_vandalism_suspect#53286 THEN 1 ELSE 0 END, CASE WHEN is_reverted#53285 THEN seconds_to_revert#53278L END, bytes_added#53276L, bytes_removed#53277L], [wiki_key#53261, date_key#53262, editor_hash#53313, 1, NOT coalesce(editor_is_bot#53211, is_bot_edit#53279), null, null, null, null, null, null, null, null, null, null, null]]

(10) PhotonGroupingAgg
Input [16]: [wiki_key#53261, date_key#53262, editor_hash#53495, gid#53494, (NOT coalesce(editor_is_bot, workspace.pkwiki_gold.fact_revision.is_bot_edit))#53496, CASE WHEN (NOT workspace.pkwiki_gold.fact_revision.is_bot_edit) THEN 1 ELSE 0 END#53499, CASE WHEN workspace.pkwiki_gold.fact_revision.is_bot_edit THEN 1 ELSE 0 END#53500, CASE WHEN workspace.pkwiki_gold.fact_revision.is_anonymous_edit THEN 1 ELSE 0 END#53501, CASE WHEN workspace.pkwiki_gold.fact_revision.is_new_article THEN 1 ELSE 0 END#53502, CASE WHEN workspace.pkwiki_gold.fact_revision.is_page_creation THEN 1 ELSE 0 END#53503, CASE WHEN workspace.pkwiki_gold.fact_revision.is_revert THEN 1 ELSE 0 END#53504, CASE WHEN workspace.pkwiki_gold.fact_revision.is_reverted THEN 1 ELSE 0 END#53505, CASE WHEN workspace.pkwiki_gold.fact_revision.is_vandalism_suspect THEN 1 ELSE 0 END#53506, CASE WHEN workspace.pkwiki_gold.fact_revision.is_reverted THEN workspace.pkwiki_gold.fact_revision.seconds_to_revert END#53507L, workspace.pkwiki_gold.fact_revision.bytes_added#53508L, workspace.pkwiki_gold.fact_revision.bytes_removed#53509L]
Arguments: [wiki_key#53261, date_key#53262, editor_hash#53495, gid#53494], [partial_max((NOT coalesce(editor_is_bot, workspace.pkwiki_gold.fact_revision.is_bot_edit))#53496) AS max#53587, partial_count(1) AS count#53459L, partial_sum(CASE WHEN (NOT workspace.pkwiki_gold.fact_revision.is_bot_edit) THEN 1 ELSE 0 END#53499) AS sum#53589L, partial_sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_bot_edit THEN 1 ELSE 0 END#53500) AS sum#53591L, partial_sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_anonymous_edit THEN 1 ELSE 0 END#53501) AS sum#53593L, partial_sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_new_article THEN 1 ELSE 0 END#53502) AS sum#53595L, partial_sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_page_creation THEN 1 ELSE 0 END#53503) AS sum#53597L, partial_sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_revert THEN 1 ELSE 0 END#53504) AS sum#53599L, partial_sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_reverted THEN 1 ELSE 0 END#53505) AS sum#53601L, partial_sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_vandalism_suspect THEN 1 ELSE 0 END#53506) AS sum#53603L, partial_percentile(CASE WHEN workspace.pkwiki_gold.fact_revision.is_reverted THEN workspace.pkwiki_gold.fact_revision.seconds_to_revert END#53507L, 0.5, 1, 0, 0, false) AS buf#53605, partial_sum(workspace.pkwiki_gold.fact_revision.bytes_added#53508L) AS sum#53607L, partial_sum(workspace.pkwiki_gold.fact_revision.bytes_removed#53509L) AS sum#53609L], [max#53586, count#53458L, sum#53588L, sum#53590L, sum#53592L, sum#53594L, sum#53596L, sum#53598L, sum#53600L, sum#53602L, buf#53604, sum#53606L, sum#53608L], [wiki_key#53261, date_key#53262, editor_hash#53495, gid#53494, max#53587, count#53459L, sum#53589L, sum#53591L, sum#53593L, sum#53595L, sum#53597L, sum#53599L, sum#53601L, sum#53603L, buf#53605, sum#53607L, sum#53609L], false

(11) PhotonShuffleExchangeSink
Input [17]: [wiki_key#53261, date_key#53262, editor_hash#53495, gid#53494, max#53587, count#53459L, sum#53589L, sum#53591L, sum#53593L, sum#53595L, sum#53597L, sum#53599L, sum#53601L, sum#53603L, buf#53605, sum#53607L, sum#53609L]
Arguments: hashpartitioning(wiki_key#53261, date_key#53262, editor_hash#53495, gid#53494, 16)

(12) PhotonShuffleMapStage
Input [17]: [wiki_key#53261, date_key#53262, editor_hash#53495, gid#53494, max#53587, count#53459L, sum#53589L, sum#53591L, sum#53593L, sum#53595L, sum#53597L, sum#53599L, sum#53601L, sum#53603L, buf#53605, sum#53607L, sum#53609L]
Arguments: ENSURE_REQUIREMENTS, [id=#31764]

(13) PhotonShuffleExchangeSource
Input [17]: [wiki_key#53261, date_key#53262, editor_hash#53495, gid#53494, max#53587, count#53459L, sum#53589L, sum#53591L, sum#53593L, sum#53595L, sum#53597L, sum#53599L, sum#53601L, sum#53603L, buf#53605, sum#53607L, sum#53609L]
Arguments: false

(14) PhotonGroupingAgg
Input [17]: [wiki_key#53261, date_key#53262, editor_hash#53495, gid#53494, max#53587, count#53459L, sum#53589L, sum#53591L, sum#53593L, sum#53595L, sum#53597L, sum#53599L, sum#53601L, sum#53603L, buf#53605, sum#53607L, sum#53609L]
Arguments: [wiki_key#53261, date_key#53262, editor_hash#53495, gid#53494], [finalmerge_max(merge max#53587) AS max((NOT coalesce(editor_is_bot, workspace.pkwiki_gold.fact_revision.is_bot_edit)))#53497, finalmerge_count(merge count#53459L) AS count(1)#53401L, finalmerge_sum(merge sum#53589L) AS sum(CASE WHEN (NOT workspace.pkwiki_gold.fact_revision.is_bot_edit) THEN 1 ELSE 0 END)#53402L, finalmerge_sum(merge sum#53591L) AS sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_bot_edit THEN 1 ELSE 0 END)#53403L, finalmerge_sum(merge sum#53593L) AS sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_anonymous_edit THEN 1 ELSE 0 END)#53404L, finalmerge_sum(merge sum#53595L) AS sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_new_article THEN 1 ELSE 0 END)#53405L, finalmerge_sum(merge sum#53597L) AS sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_page_creation THEN 1 ELSE 0 END)#53406L, finalmerge_sum(merge sum#53599L) AS sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_revert THEN 1 ELSE 0 END)#53409L, finalmerge_sum(merge sum#53601L) AS sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_reverted THEN 1 ELSE 0 END)#53410L, finalmerge_sum(merge sum#53603L) AS sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_vandalism_suspect THEN 1 ELSE 0 END)#53411L, finalmerge_percentile(merge buf#53605) AS percentile(CASE WHEN workspace.pkwiki_gold.fact_revision.is_reverted THEN workspace.pkwiki_gold.fact_revision.seconds_to_revert END, 0.5, 1)#53412, finalmerge_sum(merge sum#53607L) AS sum(workspace.pkwiki_gold.fact_revision.bytes_added)#53413L, finalmerge_sum(merge sum#53609L) AS sum(workspace.pkwiki_gold.fact_revision.bytes_removed)#53414L], [max((NOT coalesce(editor_is_bot, workspace.pkwiki_gold.fact_revision.is_bot_edit)))#53497, count(1)#53401L, sum(CASE WHEN (NOT workspace.pkwiki_gold.fact_revision.is_bot_edit) THEN 1 ELSE 0 END)#53402L, sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_bot_edit THEN 1 ELSE 0 END)#53403L, sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_anonymous_edit THEN 1 ELSE 0 END)#53404L, sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_new_article THEN 1 ELSE 0 END)#53405L, sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_page_creation THEN 1 ELSE 0 END)#53406L, sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_revert THEN 1 ELSE 0 END)#53409L, sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_reverted THEN 1 ELSE 0 END)#53410L, sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_vandalism_suspect THEN 1 ELSE 0 END)#53411L, percentile(CASE WHEN workspace.pkwiki_gold.fact_revision.is_reverted THEN workspace.pkwiki_gold.fact_revision.seconds_to_revert END, 0.5, 1)#53412, sum(workspace.pkwiki_gold.fact_revision.bytes_added)#53413L, sum(workspace.pkwiki_gold.fact_revision.bytes_removed)#53414L], [wiki_key#53261, date_key#53262, editor_hash#53495, gid#53494, max((NOT coalesce(editor_is_bot, workspace.pkwiki_gold.fact_revision.is_bot_edit)))#53497 AS (NOT coalesce(editor_is_bot, workspace.pkwiki_gold.fact_revision.is_bot_edit))#53498, count(1)#53401L AS count(1)#53510L, sum(CASE WHEN (NOT workspace.pkwiki_gold.fact_revision.is_bot_edit) THEN 1 ELSE 0 END)#53402L AS sum(CASE WHEN (NOT workspace.pkwiki_gold.fact_revision.is_bot_edit) THEN 1 ELSE 0 END)#53512L, sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_bot_edit THEN 1 ELSE 0 END)#53403L AS sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_bot_edit THEN 1 ELSE 0 END)#53514L, sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_anonymous_edit THEN 1 ELSE 0 END)#53404L AS sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_anonymous_edit THEN 1 ELSE 0 END)#53516L, sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_new_article THEN 1 ELSE 0 END)#53405L AS sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_new_article THEN 1 ELSE 0 END)#53518L, sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_page_creation THEN 1 ELSE 0 END)#53406L AS sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_page_creation THEN 1 ELSE 0 END)#53520L, sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_revert THEN 1 ELSE 0 END)#53409L AS sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_revert THEN 1 ELSE 0 END)#53522L, sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_reverted THEN 1 ELSE 0 END)#53410L AS sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_reverted THEN 1 ELSE 0 END)#53524L, sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_vandalism_suspect THEN 1 ELSE 0 END)#53411L AS sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_vandalism_suspect THEN 1 ELSE 0 END)#53526L, percentile(CASE WHEN workspace.pkwiki_gold.fact_revision.is_reverted THEN workspace.pkwiki_gold.fact_revision.seconds_to_revert END, 0.5, 1)#53412 AS percentile(CASE WHEN workspace.pkwiki_gold.fact_revision.is_reverted THEN workspace.pkwiki_gold.fact_revision.seconds_to_revert END, 0.5D, 1L)#53528, sum(workspace.pkwiki_gold.fact_revision.bytes_added)#53413L AS sum(workspace.pkwiki_gold.fact_revision.bytes_added)#53530L, sum(workspace.pkwiki_gold.fact_revision.bytes_removed)#53414L AS sum(workspace.pkwiki_gold.fact_revision.bytes_removed)#53532L], true

(15) PhotonGroupingAgg
Input [17]: [wiki_key#53261, date_key#53262, editor_hash#53495, gid#53494, (NOT coalesce(editor_is_bot, workspace.pkwiki_gold.fact_revision.is_bot_edit))#53498, count(1)#53510L, sum(CASE WHEN (NOT workspace.pkwiki_gold.fact_revision.is_bot_edit) THEN 1 ELSE 0 END)#53512L, sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_bot_edit THEN 1 ELSE 0 END)#53514L, sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_anonymous_edit THEN 1 ELSE 0 END)#53516L, sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_new_article THEN 1 ELSE 0 END)#53518L, sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_page_creation THEN 1 ELSE 0 END)#53520L, sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_revert THEN 1 ELSE 0 END)#53522L, sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_reverted THEN 1 ELSE 0 END)#53524L, sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_vandalism_suspect THEN 1 ELSE 0 END)#53526L, percentile(CASE WHEN workspace.pkwiki_gold.fact_revision.is_reverted THEN workspace.pkwiki_gold.fact_revision.seconds_to_revert END, 0.5D, 1L)#53528, sum(workspace.pkwiki_gold.fact_revision.bytes_added)#53530L, sum(workspace.pkwiki_gold.fact_revision.bytes_removed)#53532L]
Arguments: [wiki_key#53261, date_key#53262], [partial_first(count(1)#53510L, true) AS (first#53536L, valueSet#53537) FILTER (WHERE (gid#53494 = 0)), partial_first(sum(CASE WHEN (NOT workspace.pkwiki_gold.fact_revision.is_bot_edit) THEN 1 ELSE 0 END)#53512L, true) AS (first#53540L, valueSet#53541) FILTER (WHERE (gid#53494 = 0)), partial_first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_bot_edit THEN 1 ELSE 0 END)#53514L, true) AS (first#53544L, valueSet#53545) FILTER (WHERE (gid#53494 = 0)), partial_first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_anonymous_edit THEN 1 ELSE 0 END)#53516L, true) AS (first#53548L, valueSet#53549) FILTER (WHERE (gid#53494 = 0)), partial_first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_new_article THEN 1 ELSE 0 END)#53518L, true) AS (first#53552L, valueSet#53553) FILTER (WHERE (gid#53494 = 0)), partial_first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_page_creation THEN 1 ELSE 0 END)#53520L, true) AS (first#53556L, valueSet#53557) FILTER (WHERE (gid#53494 = 0)), partial_count(editor_hash#53495) AS count#53559L FILTER (WHERE (gid#53494 = 1)), partial_count(editor_hash#53495) AS count#53561L FILTER (WHERE ((gid#53494 = 1) AND (NOT coalesce(editor_is_bot, workspace.pkwiki_gold.fact_revision.is_bot_edit))#53498)), partial_first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_revert THEN 1 ELSE 0 END)#53522L, true) AS (first#53564L, valueSet#53565) FILTER (WHERE (gid#53494 = 0)), partial_first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_reverted THEN 1 ELSE 0 END)#53524L, true) AS (first#53568L, valueSet#53569) FILTER (WHERE (gid#53494 = 0)), partial_first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_vandalism_suspect THEN 1 ELSE 0 END)#53526L, true) AS (first#53572L, valueSet#53573) FILTER (WHERE (gid#53494 = 0)), partial_first(percentile(CASE WHEN workspace.pkwiki_gold.fact_revision.is_reverted THEN workspace.pkwiki_gold.fact_revision.seconds_to_revert END, 0.5D, 1L)#53528, true) AS (first#53576, valueSet#53577) FILTER (WHERE (gid#53494 = 0)), partial_first(sum(workspace.pkwiki_gold.fact_revision.bytes_added)#53530L, true) AS (first#53580L, valueSet#53581) FILTER (WHERE (gid#53494 = 0)), partial_first(sum(workspace.pkwiki_gold.fact_revision.bytes_removed)#53532L, true) AS (first#53584L, valueSet#53585) FILTER (WHERE (gid#53494 = 0))], [first#53534L, valueSet#53535, first#53538L, valueSet#53539, first#53542L, valueSet#53543, first#53546L, valueSet#53547, first#53550L, valueSet#53551, first#53554L, valueSet#53555, count#53558L, count#53560L, first#53562L, valueSet#53563, first#53566L, valueSet#53567, first#53570L, valueSet#53571, first#53574, valueSet#53575, first#53578L, valueSet#53579, first#53582L, ... 1 more fields], [wiki_key#53261, date_key#53262, first#53536L, valueSet#53537, first#53540L, valueSet#53541, first#53544L, valueSet#53545, first#53548L, valueSet#53549, first#53552L, valueSet#53553, first#53556L, valueSet#53557, count#53559L, count#53561L, first#53564L, valueSet#53565, first#53568L, valueSet#53569, first#53572L, valueSet#53573, first#53576, valueSet#53577, first#53580L, ... 3 more fields], false

(16) PhotonShuffleExchangeSink
Input [28]: [wiki_key#53261, date_key#53262, first#53536L, valueSet#53537, first#53540L, valueSet#53541, first#53544L, valueSet#53545, first#53548L, valueSet#53549, first#53552L, valueSet#53553, first#53556L, valueSet#53557, count#53559L, count#53561L, first#53564L, valueSet#53565, first#53568L, valueSet#53569, first#53572L, valueSet#53573, first#53576, valueSet#53577, first#53580L, valueSet#53581, first#53584L, valueSet#53585]
Arguments: hashpartitioning(wiki_key#53261, date_key#53262, 16)

(17) PhotonShuffleMapStage
Input [28]: [wiki_key#53261, date_key#53262, first#53536L, valueSet#53537, first#53540L, valueSet#53541, first#53544L, valueSet#53545, first#53548L, valueSet#53549, first#53552L, valueSet#53553, first#53556L, valueSet#53557, count#53559L, count#53561L, first#53564L, valueSet#53565, first#53568L, valueSet#53569, first#53572L, valueSet#53573, first#53576, valueSet#53577, first#53580L, valueSet#53581, first#53584L, valueSet#53585]
Arguments: ENSURE_REQUIREMENTS, [id=#31772]

(18) PhotonShuffleExchangeSource
Input [28]: [wiki_key#53261, date_key#53262, first#53536L, valueSet#53537, first#53540L, valueSet#53541, first#53544L, valueSet#53545, first#53548L, valueSet#53549, first#53552L, valueSet#53553, first#53556L, valueSet#53557, count#53559L, count#53561L, first#53564L, valueSet#53565, first#53568L, valueSet#53569, first#53572L, valueSet#53573, first#53576, valueSet#53577, first#53580L, valueSet#53581, first#53584L, valueSet#53585]
Arguments: false

(19) PhotonGroupingAgg
Input [28]: [wiki_key#53261, date_key#53262, first#53536L, valueSet#53537, first#53540L, valueSet#53541, first#53544L, valueSet#53545, first#53548L, valueSet#53549, first#53552L, valueSet#53553, first#53556L, valueSet#53557, count#53559L, count#53561L, first#53564L, valueSet#53565, first#53568L, valueSet#53569, first#53572L, valueSet#53573, first#53576, valueSet#53577, first#53580L, valueSet#53581, first#53584L, valueSet#53585]
Arguments: [wiki_key#53261, date_key#53262], [finalmerge_first(merge first#53536L, valueSet#53537) AS first(count(1))#53511L, finalmerge_first(merge first#53540L, valueSet#53541) AS first(sum(CASE WHEN (NOT workspace.pkwiki_gold.fact_revision.is_bot_edit) THEN 1 ELSE 0 END))#53513L, finalmerge_first(merge first#53544L, valueSet#53545) AS first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_bot_edit THEN 1 ELSE 0 END))#53515L, finalmerge_first(merge first#53548L, valueSet#53549) AS first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_anonymous_edit THEN 1 ELSE 0 END))#53517L, finalmerge_first(merge first#53552L, valueSet#53553) AS first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_new_article THEN 1 ELSE 0 END))#53519L, finalmerge_first(merge first#53556L, valueSet#53557) AS first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_page_creation THEN 1 ELSE 0 END))#53521L, finalmerge_count(merge count#53559L) AS count(editor_hash)#53407L, finalmerge_count(merge count#53561L) AS count(editor_hash)#53408L, finalmerge_first(merge first#53564L, valueSet#53565) AS first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_revert THEN 1 ELSE 0 END))#53523L, finalmerge_first(merge first#53568L, valueSet#53569) AS first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_reverted THEN 1 ELSE 0 END))#53525L, finalmerge_first(merge first#53572L, valueSet#53573) AS first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_vandalism_suspect THEN 1 ELSE 0 END))#53527L, finalmerge_first(merge first#53576, valueSet#53577) AS first(percentile(CASE WHEN workspace.pkwiki_gold.fact_revision.is_reverted THEN workspace.pkwiki_gold.fact_revision.seconds_to_revert END, 0.5D, 1L))#53529, finalmerge_first(merge first#53580L, valueSet#53581) AS first(sum(workspace.pkwiki_gold.fact_revision.bytes_added))#53531L, finalmerge_first(merge first#53584L, valueSet#53585) AS first(sum(workspace.pkwiki_gold.fact_revision.bytes_removed))#53533L], [first(count(1))#53511L, first(sum(CASE WHEN (NOT workspace.pkwiki_gold.fact_revision.is_bot_edit) THEN 1 ELSE 0 END))#53513L, first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_bot_edit THEN 1 ELSE 0 END))#53515L, first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_anonymous_edit THEN 1 ELSE 0 END))#53517L, first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_new_article THEN 1 ELSE 0 END))#53519L, first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_page_creation THEN 1 ELSE 0 END))#53521L, count(editor_hash)#53407L, count(editor_hash)#53408L, first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_revert THEN 1 ELSE 0 END))#53523L, first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_reverted THEN 1 ELSE 0 END))#53525L, first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_vandalism_suspect THEN 1 ELSE 0 END))#53527L, first(percentile(CASE WHEN workspace.pkwiki_gold.fact_revision.is_reverted THEN workspace.pkwiki_gold.fact_revision.seconds_to_revert END, 0.5D, 1L))#53529, first(sum(workspace.pkwiki_gold.fact_revision.bytes_added))#53531L, first(sum(workspace.pkwiki_gold.fact_revision.bytes_removed))#53533L], [wiki_key#53261, date_key#53262, coalesce(first(count(1))#53511L, 0) AS edits#53350L, first(sum(CASE WHEN (NOT workspace.pkwiki_gold.fact_revision.is_bot_edit) THEN 1 ELSE 0 END))#53513L AS human_edits#53351L, first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_bot_edit THEN 1 ELSE 0 END))#53515L AS bot_edits#53352L, first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_anonymous_edit THEN 1 ELSE 0 END))#53517L AS anonymous_edits#53353L, first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_new_article THEN 1 ELSE 0 END))#53519L AS new_articles#53354L, first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_page_creation THEN 1 ELSE 0 END))#53521L AS pages_created#53355L, count(editor_hash)#53407L AS active_editors#53356L, count(editor_hash)#53408L AS active_human_editors#53357L, first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_revert THEN 1 ELSE 0 END))#53523L AS reverts#53358L, first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_reverted THEN 1 ELSE 0 END))#53525L AS reverted_edits#53359L, first(sum(CASE WHEN workspace.pkwiki_gold.fact_revision.is_vandalism_suspect THEN 1 ELSE 0 END))#53527L AS vandalism_suspects#53360L, first(percentile(CASE WHEN workspace.pkwiki_gold.fact_revision.is_reverted THEN workspace.pkwiki_gold.fact_revision.seconds_to_revert END, 0.5D, 1L))#53529 AS median_seconds_to_revert#53361, first(sum(workspace.pkwiki_gold.fact_revision.bytes_added))#53531L AS bytes_added#53362L, first(sum(workspace.pkwiki_gold.fact_revision.bytes_removed))#53533L AS bytes_removed#53363L], true

(20) PhotonScan parquet workspace.pkwiki_gold.fact_page_event
Output [3]: [wiki_key#53385, date_key#53386, change_type_key#53390]
Location: PreparedDeltaFileIndex [s3://dbstorage-prod-rw3fj/uc/37b00a9a-74fd-42ad-997d-3e5d098e319d/a26ac749-9a09-40e1-a784-c77e93981c56/__unitystorage/catalogs/d1bde834-391a-4ad0-bbbb-13e124c7730e/tables/65bfbfe9-6c32-4d36-862f-283e0c1abe02]
ReadSchema: struct<wiki_key:int,date_key:int,change_type_key:int>

(21) PhotonGroupingAgg
Input [3]: [wiki_key#53385, date_key#53386, change_type_key#53390]
Arguments: [wiki_key#53385, date_key#53386], [partial_sum(CASE WHEN (change_type_key#53390 = 13) THEN 1 ELSE 0 END) AS sum#53489L, partial_sum(CASE WHEN (change_type_key#53390 = 14) THEN 1 ELSE 0 END) AS sum#53491L, partial_sum(CASE WHEN (change_type_key#53390 = 12) THEN 1 ELSE 0 END) AS sum#53493L], [sum#53488L, sum#53490L, sum#53492L], [wiki_key#53385, date_key#53386, sum#53489L, sum#53491L, sum#53493L], false

(22) PhotonShuffleExchangeSink
Input [5]: [wiki_key#53385, date_key#53386, sum#53489L, sum#53491L, sum#53493L]
Arguments: hashpartitioning(wiki_key#53385, date_key#53386, 16)

(23) PhotonShuffleMapStage
Input [5]: [wiki_key#53385, date_key#53386, sum#53489L, sum#53491L, sum#53493L]
Arguments: ENSURE_REQUIREMENTS, [id=#31781]

(24) PhotonShuffleExchangeSource
Input [5]: [wiki_key#53385, date_key#53386, sum#53489L, sum#53491L, sum#53493L]
Arguments: false

(25) PhotonGroupingAgg
Input [5]: [wiki_key#53385, date_key#53386, sum#53489L, sum#53491L, sum#53493L]
Arguments: [wiki_key#53385, date_key#53386], [finalmerge_sum(merge sum#53489L) AS sum(CASE WHEN (change_type_key = 13) THEN 1 ELSE 0 END)#53415L, finalmerge_sum(merge sum#53491L) AS sum(CASE WHEN (change_type_key = 14) THEN 1 ELSE 0 END)#53416L, finalmerge_sum(merge sum#53493L) AS sum(CASE WHEN (change_type_key = 12) THEN 1 ELSE 0 END)#53417L], [sum(CASE WHEN (change_type_key = 13) THEN 1 ELSE 0 END)#53415L, sum(CASE WHEN (change_type_key = 14) THEN 1 ELSE 0 END)#53416L, sum(CASE WHEN (change_type_key = 12) THEN 1 ELSE 0 END)#53417L], [wiki_key#53385, date_key#53386, sum(CASE WHEN (change_type_key = 13) THEN 1 ELSE 0 END)#53415L AS pages_deleted#53364L, sum(CASE WHEN (change_type_key = 14) THEN 1 ELSE 0 END)#53416L AS pages_restored#53365L, sum(CASE WHEN (change_type_key = 12) THEN 1 ELSE 0 END)#53417L AS pages_moved#53366L], true

(26) PhotonShuffledHashJoin
Left keys [2]: [wiki_key#53261, date_key#53262]
Right keys [2]: [wiki_key#53385, date_key#53386]
Join type: FullOuter
Join condition: None

(27) PhotonProject
Input [21]: [wiki_key#53261, date_key#53262, edits#53350L, human_edits#53351L, bot_edits#53352L, anonymous_edits#53353L, new_articles#53354L, pages_created#53355L, active_editors#53356L, active_human_editors#53357L, reverts#53358L, reverted_edits#53359L, vandalism_suspects#53360L, median_seconds_to_revert#53361, bytes_added#53362L, bytes_removed#53363L, wiki_key#53385, date_key#53386, pages_deleted#53364L, pages_restored#53365L, pages_moved#53366L]
Arguments: [coalesce(wiki_key#53261, wiki_key#53385) AS wiki_key#53420, coalesce(date_key#53262, date_key#53386) AS date_key#53421, coalesce(edits#53350L, 0) AS edits#53438L, coalesce(human_edits#53351L, 0) AS human_edits#53439L, coalesce(bot_edits#53352L, 0) AS bot_edits#53440L, coalesce(anonymous_edits#53353L, 0) AS anonymous_edits#53441L, coalesce(new_articles#53354L, 0) AS new_articles#53442L, coalesce(pages_created#53355L, 0) AS pages_created#53443L, coalesce(active_editors#53356L, 0) AS active_editors#53444L, coalesce(active_human_editors#53357L, 0) AS active_human_editors#53445L, coalesce(reverts#53358L, 0) AS reverts#53446L, coalesce(reverted_edits#53359L, 0) AS reverted_edits#53447L, coalesce(vandalism_suspects#53360L, 0) AS vandalism_suspects#53448L, median_seconds_to_revert#53361, coalesce(bytes_added#53362L, 0) AS bytes_added#53449L, coalesce(bytes_removed#53363L, 0) AS bytes_removed#53450L, coalesce(pages_deleted#53364L, 0) AS pages_deleted#53451L, coalesce(pages_restored#53365L, 0) AS pages_restored#53452L, coalesce(pages_moved#53366L, 0) AS pages_moved#53453L]

(28) PhotonProject
Input [19]: [wiki_key#53420, date_key#53421, edits#53438L, human_edits#53439L, bot_edits#53440L, anonymous_edits#53441L, new_articles#53442L, pages_created#53443L, active_editors#53444L, active_human_editors#53445L, reverts#53446L, reverted_edits#53447L, vandalism_suspects#53448L, median_seconds_to_revert#53361, bytes_added#53449L, bytes_removed#53450L, pages_deleted#53451L, pages_restored#53452L, pages_moved#53453L]
Arguments: [wiki_key#53420, date_key#53421, edits#53438L, human_edits#53439L, bot_edits#53440L, anonymous_edits#53441L, new_articles#53442L, pages_created#53443L, active_editors#53444L, active_human_editors#53445L, reverts#53446L, reverted_edits#53447L, vandalism_suspects#53448L, median_seconds_to_revert#53361, bytes_added#53449L, bytes_removed#53450L, pages_deleted#53451L, pages_restored#53452L, pages_moved#53453L, CASE WHEN (edits#53438L > 0) THEN (cast(bot_edits#53440L as double) / cast(edits#53438L as double)) END AS bot_edit_share#53455, (bytes_added#53449L - bytes_removed#53450L) AS net_bytes#53457L]

(29) PhotonColumnarToRow
Input [21]: [wiki_key#53420, date_key#53421, edits#53438L, human_edits#53439L, bot_edits#53440L, anonymous_edits#53441L, new_articles#53442L, pages_created#53443L, active_editors#53444L, active_human_editors#53445L, reverts#53446L, reverted_edits#53447L, vandalism_suspects#53448L, median_seconds_to_revert#53361, bytes_added#53449L, bytes_removed#53450L, pages_deleted#53451L, pages_restored#53452L, pages_moved#53453L, bot_edit_share#53455, net_bytes#53457L]

(30) PhotonResultStage
Input [21]: [wiki_key#53420, date_key#53421, edits#53438L, human_edits#53439L, bot_edits#53440L, anonymous_edits#53441L, new_articles#53442L, pages_created#53443L, active_editors#53444L, active_human_editors#53445L, reverts#53446L, reverted_edits#53447L, vandalism_suspects#53448L, median_seconds_to_revert#53361, bytes_added#53449L, bytes_removed#53450L, pages_deleted#53451L, pages_restored#53452L, pages_moved#53453L, bot_edit_share#53455, net_bytes#53457L]

(31) AdaptiveSparkPlan
Output [21]: [wiki_key#53420, date_key#53421, edits#53438L, human_edits#53439L, bot_edits#53440L, anonymous_edits#53441L, new_articles#53442L, pages_created#53443L, active_editors#53444L, active_human_editors#53445L, reverts#53446L, reverted_edits#53447L, vandalism_suspects#53448L, median_seconds_to_revert#53361, bytes_added#53449L, bytes_removed#53450L, pages_deleted#53451L, pages_restored#53452L, pages_moved#53453L, bot_edit_share#53455, net_bytes#53457L]
Arguments: isFinalPlan=false


== Photon Explanation ==
The query is fully supported by Photon.
== Optimizer Statistics (table names per statistics state) ==
  missing = 
  partial = 
  full    = dim_editor, fact_page_event, fact_revision
```

</details>

### How to take the DAG screenshot (Query Profile on serverless)

Serverless compute has no classic Spark UI (no cluster, so no Spark UI, driver logs or
event timeline to open). Databricks shows a Query Profile per Spark query instead: the same
operator DAG with rows, time and memory per node, and Photon shown per operator.

1. Open the Gold task run page of the incremental run:
   https://dbc-f8a0f45c-f7da.cloud.databricks.com/?o=7474660113881069#job/1122166254975470/run/397206758106845
   (or the full run's Gold task, linked above).
2. The page shows the notebook with its cell outputs. Scroll to the cell `run = runner.run_gold(...)`.
   Under the cell, open the Spark jobs / "See performance" link. It lists every query the cell ran.
3. Pick the `MERGE INTO workspace.pkwiki_gold.agg_wiki_daily` query (the aggregation) or the
   `MERGE INTO workspace.pkwiki_gold.fact_revision` query (the fact load) and click it to open the
   Query Profile.
4. Switch the profile to the graph view, expand it to full screen, and turn on "Show metrics" to
   see rows and time per operator. Take the screenshot there. The node names match the
   `EXPLAIN FORMATTED` output above (PhotonScan, PhotonBroadcastHashJoin, PhotonGroupingAgg,
   PhotonShuffledHashJoin).
5. Another way in: left sidebar, Query History, set the compute filter to serverless compute for
   jobs (or notebooks), filter by the time of the run (2026-10-25 15:11 to 15:15 UTC for the
   Gold task of `dbx-gold-incr-1`, 15:02 to 15:08 UTC for `dbx-gold-full-1`), open the MERGE statement, then "See query profile".

Save that screenshot as `docs/figures/spark_ui_dag.png` and rebuild the PDF (see `docs/SCREENSHOT-STEPS.md`).


## Gold run `dbx-gold-full-2` on commit `81e7e8f` (2026-10-25), SUPERSEDED

> **Superseded** by the key fix section below. Kept as the record of the `agg_edit_wars` change.

Commit `81e7e8f`: `agg_edit_wars` keeps every page-day with 2 or more reverts (new columns
`is_edit_war` and `reverted_edits`), and `merge_gold` runs schema evolution before each MERGE.
Run 746131132461536: the Gold task took 3 s setup + 206 s exec (249.1 s including the checks
task), SUCCESS. `agg_edit_wars` went from 1 to 18 rows (17 inserted, 1 updated), the MERGE added
the new columns to the existing table, and every other Gold table kept its count (all other steps
wrote 0 rows). 15 audit rows, all SUCCESS.

## Gold after the key fix (commit `3d5673e`, 2026-10-25), CURRENT

Commit `3d5673e`: `sk()` clears the sign bit of xxhash64, so every real surrogate key is >= 0 and
only the unknown members are negative (-1 urwiki, -2 pnbwiki, -3 sdwiki, -4 pswiki). A full refresh
also deletes dimension rows whose key changed.

### Before the fix (Delta time travel to 2026-10-25 15:20 UTC)

| Check | Value |
|---|---|
| `dim_page` keys below -4 | 7,060 of 13,859 |
| `dim_editor` keys below -4 | 274 of 577 |
| `fact_revision` rows on an unknown page / editor | 20,122 (62.68%) / 12,143 (37.83%) |
| `fact_page_event` rows on an unknown page / editor | 4,008 (51.48%) / 1,252 (16.08%) |

### Runs

One multi-task run on serverless jobs compute, run id 57688778010155 (577.2 s in total), all tasks
SUCCESS. The full Gold refresh ran first and the incremental pass ran after it. That order matters:
the fix changes about half of the `revision_key` values, so an incremental run before the full one
would have inserted duplicate facts.

| Task | run_id in the audit log | Notebook | Parameters | Setup s | Exec s |
|---|---|---|---|---|---|
| gold_full | dbx-gold-full-3 | 04_gold_star | load_type=full | 4 | 167 |
| checks_full | | gold_checks (read-only) | | 1 | 38 |
| bronze | dbx-gold-incr-2 | 01_bronze_ingest | load_type=incremental, start_date=2026-10-25 | 2 | 56 |
| silver | dbx-gold-incr-2 | 02_silver_unify_and_mask | same | 1 | 53 |
| scd2 | dbx-gold-incr-2 | 03_silver_scd2_and_reverts | same | 1 | 50 |
| gold_incr | dbx-gold-incr-2 | 04_gold_star | same | 2 | 162 |
| checks | | gold_checks (read-only) | | 1 | 33 |

- Run page: https://dbc-f8a0f45c-f7da.cloud.databricks.com/?o=7474660113881069#job/88997456277403/run/57688778010155
- Gold full task: https://dbc-f8a0f45c-f7da.cloud.databricks.com/?o=7474660113881069#job/88997456277403/run/58743378244069
- Gold incremental task: https://dbc-f8a0f45c-f7da.cloud.databricks.com/?o=7474660113881069#job/88997456277403/run/949335926789860

What `dbx-gold-full-3` did, from the audit log:

- `dim_page`: 7,060 rows re-keyed (7,060 inserted, 7,060 deleted).
- `dim_editor`: 274 rows re-keyed.
- `fact_revision`: 16,097 inserted, 9,700 updated (facts whose page or editor now resolves),
  16,097 deleted.
- `fact_page_event`: 3,921 inserted, 2,248 updated, 3,921 deleted.
- Aggregates: `agg_wiki_daily` 418 updated; `agg_revert_latency` 16 inserted, 13 updated,
  26 deleted; `agg_edit_wars` 17 updated; `agg_editor_monthly` 34 inserted, 76 updated.

Fact and dimension row counts stayed the same, as expected for a re-key.

New data for `dbx-gold-incr-2`: a second extractor pull covering 2026-10-25 15:02:22 to 15:46:11
UTC (urwiki 44, sdwiki 121, pnbwiki 3, pswiki 0 changes; the empty pswiki file was not uploaded),
added to the same `ingest_date=2026-10-25` folder. Bronze re-read the whole day and inserted only
the 168 new rows.

### Row counts

| Table | After `dbx-gold-full-3` | After `dbx-gold-incr-2` | Delta |
|---|---|---|---|
| `dim_date` | 10,957 | 10,957 | 0 |
| `dim_wiki` | 4 | 4 | 0 |
| `dim_change_type` | 12 | 12 | 0 |
| `dim_page` | 13,859 | 13,886 | +27 |
| `dim_editor` | 577 | 578 | +1 |
| `fact_revision` | 32,102 | 32,144 | +42 |
| `fact_page_event` | 7,786 | 7,799 | +13 |
| `agg_wiki_daily` | 727 | 727 | 0 (3 updated) |
| `agg_revert_latency` | 59 | 59 | 0 |
| `agg_edit_wars` | 18 | 18 | 0 |
| `agg_editor_monthly` | 160 | 160 | 0 (4 updated) |
| `pkwiki_ops.gold_watermark` | 4 | 4 | 0 |

Silver after the incremental: `revision` 32,144, `page_event` 7,799, `page` 13,882, `editor` 574,
`user_event` 244. The facts equal Silver row for row, and the two dims are Silver plus the 4 unknown
members. The incremental Gold run recomputed the aggregates for 1 touched day (2026-10-25).
Audit rows: 15 for `dbx-gold-full-3` and 26 for `dbx-gold-incr-2`, all SUCCESS.
`pkwiki_ops.pipeline_execution_logs` now has 150 rows.

### Key integrity (after `dbx-gold-incr-2`)

| Key | Min | Keys below -4 | Negative (unknown members) | Rows |
|---|---|---|---|---|
| `dim_page.page_key` | -4 | 0 | 4 | 13,886 |
| `dim_editor.editor_key` | -4 | 0 | 4 | 578 |
| `fact_revision.revision_key` | 316491819817217 | 0 | 0 | 32,144 |
| `fact_revision.page_key` | -4 | 0 | 7,422 | 32,144 |
| `fact_revision.editor_key` | 16334835762443724 | 0 | 0 | 32,144 |
| `fact_page_event.page_event_key` | 2122806329554343 | 0 | 0 | 7,799 |
| `fact_page_event.page_key` | -4 | 0 | 15 | 7,799 |
| `fact_page_event.editor_key` | -4 | 0 | 5 | 7,799 |
| `agg_edit_wars.page_key` | 3453034368350180938 | 0 | 0 | 18 |

- FK orphans: a LEFT ANTI JOIN of every fact key to its dimension (page, editor, wiki, date, PKT
  date, change type), plus `agg_edit_wars` to page, wiki and date, found **0 orphans in all 15
  checks**.
- Duplicate surrogate keys in `dim_page`, `dim_editor`, `fact_revision`, `fact_page_event`: 0.
- Duplicate business keys: `fact_revision` has 32,144 rows and 32,144 distinct (wiki_key, rev_id);
  `fact_page_event` has 7,799 rows and 7,799 distinct (wiki_key, event_key).

Unknown member share:

| Fact | Unknown page | Unknown editor |
|---|---|---|
| `fact_revision` | 7,422 (23.09%) | 0 (0.00%) |
| `fact_page_event` | 15 (0.19%) | 5 (0.06%) |

All 7,422 `fact_revision` rows on an unknown page come from the history dump (2002 to 2006: pswiki
3,146, sdwiki 2,688, urwiki 1,588), and every one of them has `page_id` NULL and `is_deleted` true.
They are revisions of pages that were later deleted, and the dump does not keep their page, so the
unknown member is the correct target. Every revision that has a `page_id` resolves to a page; among
the 15,038 API revisions none is on an unknown member.

### Aggregates

`agg_editor_monthly`: 160 rows covering 2002-11 to 2026-10, 745 editor rows, 175 active, 32,144
edits (equal to `fact_revision`). Recent months:

| Wiki | Month | Group | Editors | Active | Very active | New | Edits |
|---|---|---|---|---|---|---|---|
| urwiki | 2026-09 | bot | 7 | 7 | 4 | 7 | 6,706 |
| urwiki | 2026-09 | human | 32 | 12 | 3 | 31 | 2,223 |
| urwiki | 2026-10 | bot | 9 | 7 | 3 | 2 | 1,014 |
| urwiki | 2026-10 | human | 20 | 8 | 4 | 20 | 4,634 |
| sdwiki | 2026-09 | bot | 2 | 1 | 0 | 2 | 21 |
| sdwiki | 2026-09 | human | 5 | 2 | 0 | 5 | 108 |
| sdwiki | 2026-10 | bot | 1 | 1 | 1 | 0 | 181 |
| sdwiki | 2026-10 | human | 5 | 3 | 0 | 5 | 98 |
| pnbwiki | 2026-09 | bot | 1 | 0 | 0 | 1 | 3 |
| pnbwiki | 2026-10 | bot | 1 | 1 | 0 | 0 | 33 |
| pnbwiki | 2026-10 | human | 2 | 1 | 0 | 2 | 10 |
| pswiki | 2026-09 | human | 2 | 0 | 0 | 2 | 2 |
| pswiki | 2026-10 | bot | 1 | 0 | 0 | 1 | 2 |
| pswiki | 2026-10 | human | 2 | 0 | 0 | 2 | 3 |

For the last two days, active editors in `agg_wiki_daily` were compared with a recount from
`fact_revision` joined to `dim_editor` (distinct `editor_hash`). They match on every row:

| Wiki | Day | Edits | active_editors | active_human_editors | Recount |
|---|---|---|---|---|---|
| urwiki | 2026-10-24 | 280 | 16 | 10 | 16 |
| urwiki | 2026-10-25 | 5,368 | 21 | 13 | 21 |
| sdwiki | 2026-10-24 | 152 | 4 | 3 | 4 |
| sdwiki | 2026-10-25 | 127 | 5 | 4 | 5 |
| pnbwiki | 2026-10-24 | 25 | 2 | 1 | 2 |
| pnbwiki | 2026-10-25 | 18 | 3 | 2 | 3 |
| pswiki | 2026-10-24 | 5 | 3 | 2 | 3 |

`agg_edit_wars`: 18 contested page-days, and both new columns are present and filled. 6 have
`is_edit_war` true (26 reverts, 26 reverted edits) and 12 have it false (32 reverts, 36 reverted
edits).

### Power BI column check

Every column the Power BI model reads (the `PrepareTable` column list of each table in
`dashboard/powerbi/databricks/Knowledge Integrity Monitor.SemanticModel/definition/tables/*.tmdl`)
was selected from the live tables on SQL warehouse `9af9b3eed023027a`. All 14 queries succeeded, so
no column is missing. That matters because `PrepareTable` uses `MissingField.UseNull`: a missing
column would not fail the refresh, it would just load as blanks.

| Table | Model columns | Rows | Columns that are entirely NULL |
|---|---|---|---|
| `pkwiki_gold.agg_edit_wars` | 13 | 18 | none |
| `pkwiki_gold.agg_editor_monthly` | 10 | 160 | none |
| `pkwiki_gold.agg_revert_latency` | 9 | 59 | none |
| `pkwiki_gold.agg_wiki_daily` | 21 | 727 | none |
| `pkwiki_gold.dim_change_type` | 4 | 12 | none |
| `pkwiki_gold.dim_date` | 15 | 10,957 | none |
| `pkwiki_gold.dim_editor` | 13 | 578 | `valid_to` (every editor version is still current) |
| `pkwiki_gold.dim_page` | 14 | 13,886 | none |
| `pkwiki_gold.dim_wiki` | 8 | 4 | none |
| `pkwiki_gold.fact_page_event` | 14 | 7,799 | none |
| `pkwiki_gold.fact_revision` | 29 | 32,144 | none |
| `pkwiki_ops.pipeline_execution_logs` | 16 | 150 | `error_message` (no failed step) |
| `pkwiki_ops.quarantine` | 7 | 0 | all (table is empty) |
| `pkwiki_ops.schema_drift_log` | 10 | 0 | all (table is empty) |

The model reads every key as a 64-bit integer, and with the sign bit cleared all keys fit.

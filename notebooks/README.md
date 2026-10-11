# Notebooks

Databricks source-format notebooks (`.py` with `# COMMAND ----------` cells). Open the repo as a Git folder in Databricks and they show up as normal notebooks. They are thin wrappers, the actual code is in `pipeline/` so the same thing can run locally with `scripts/run_local.py`.

| Notebook | What it does | When |
|---|---|---|
| `00_setup` | schemas, landing volume, all tables, optional copy of `sample_data/raw` into the volume | once (safe to rerun) |
| `01_bronze_ingest` | raw files -> bronze (insert-only, idempotent) | every run, step 1 |
| `02_silver_unify_and_mask` | bronze -> silver: cast, merge dump + api, mask PII, quarantine | every run, step 2 |
| `03_silver_scd2_and_reverts` | revert detection + SCD2 `silver.page` / `silver.editor` | every run, step 3 |
| `99_checks` | logs, idempotency proof, duplicate keys, quarantine/drift, row counts | whenever |

01-03 take the same widgets (`load_type`, `snapshot`, `start_date`, `end_date`, `wikis`, `bronze_mode`, `run_id`, `catalog`, `prefix`, `landing_root`), the daily job in `jobs/` passes them as job parameters. Each one ends by showing its rows from `ops.pipeline_execution_logs`.

Examples:
- daily run: leave everything at the defaults
- backfill: `start_date=2026-09-20`, `end_date=2026-09-27`
- full load: `load_type=full`, `snapshot=2026-08`

The silver notebook needs the PII salt secret, see the first cells of `00_setup`.

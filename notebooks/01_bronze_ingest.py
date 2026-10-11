# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - raw -> bronze
# MAGIC Reads the landed files from the volume into `bronze.mediawiki_history` (full load) or `bronze.recentchanges` (incremental).
# MAGIC Rows are only inserted if their `record_hash` isn't in that partition yet, so running the same day twice adds nothing.
# MAGIC
# MAGIC Params: `load_type` full/incremental, `snapshot` YYYY-MM (full only), `start_date` / `end_date` for a backfill (leave both empty for the normal daily run = two newest landed days), `wikis` (`all` or e.g. `urwiki,sdwiki`), `bronze_mode` (`rebuild` deletes the partitions first), `run_id` (the job passes `{{job.run_id}}`, empty = new id).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("prefix", "pkwiki")
dbutils.widgets.text("landing_root", "")
dbutils.widgets.dropdown("load_type", "incremental", ["incremental", "full"])
dbutils.widgets.text("snapshot", "")
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")
dbutils.widgets.text("wikis", "all")
dbutils.widgets.dropdown("bronze_mode", "append", ["append", "rebuild"])
dbutils.widgets.text("run_id", "")

# COMMAND ----------

import os
import sys

repo_root = os.path.abspath("..")
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from pipeline import runner

# COMMAND ----------

params = {k: dbutils.widgets.get(k) for k in runner.DEFAULTS}
run = runner.run_bronze(spark, params)

# COMMAND ----------

display(runner.last_run_summary(spark, params, run["run_id"]))

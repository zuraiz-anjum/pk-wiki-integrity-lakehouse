# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - SCD2 dimensions and reverts
# MAGIC Runs after 02 for the same batch. For api edits it works out which edits were reverts / got reverted (sha1 matches an earlier version of the page), then updates the SCD Type 2 tables `silver.page` and `silver.editor` (old versions get closed, never overwritten).
# MAGIC
# MAGIC Same params as 01 (`bronze_mode` is ignored here).

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
run = runner.run_silver_scd2(spark, params)

# COMMAND ----------

display(runner.last_run_summary(spark, params, run["run_id"]))

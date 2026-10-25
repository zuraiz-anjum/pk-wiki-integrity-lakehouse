# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - gold star schema
# MAGIC Runs after 03. Refreshes the Gold star schema in `<prefix>_gold` from Silver: the dimensions (`dim_date`, `dim_wiki`, `dim_change_type`, SCD2 `dim_page` and `dim_editor`), the two facts (`fact_revision`, `fact_page_event`) and the four aggregates (`agg_wiki_daily`, `agg_revert_latency`, `agg_edit_wars`, `agg_editor_monthly`).
# MAGIC
# MAGIC Incremental runs only pick up the Silver rows written since the last Gold run (watermark in `<prefix>_ops.gold_watermark`), and the aggregates are recomputed for the days that changed. `load_type=full` merges against all of Silver and removes rows Silver removed. Every write is a MERGE, so running it twice changes nothing. The tables are created on the first run, no need to rerun 00_setup.
# MAGIC
# MAGIC Same params as 01-03. Gold always covers every wiki, so `wikis`, `start_date`, `end_date` and `bronze_mode` don't change anything here.

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
run = runner.run_gold(spark, params, on_databricks=True)

# COMMAND ----------

display(runner.last_run_summary(spark, params, run["run_id"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row counts per Gold table

# COMMAND ----------

from pipeline.delta_utils import table_name
from pipeline.schemas import TABLES

cfg, *_ = runner.resolve(params)
counts = [(name, spark.table(table_name(cfg, key)).count())
          for key, (layer, name, *_) in TABLES.items() if layer == "gold"]
display(spark.createDataFrame(counts, "gold_table string, row_count bigint"))

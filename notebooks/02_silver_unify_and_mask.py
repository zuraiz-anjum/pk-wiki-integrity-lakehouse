# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - bronze -> silver
# MAGIC Casts the bronze rows, puts the dump and the api into the same three tables (`silver.revision`, `page_event`, `user_event`), hashes editor names and masks IPs/emails in comments.
# MAGIC Rows that don't pass the checks go to `ops.quarantine`. Everything is a MERGE, so a rerun of the same batch changes nothing.
# MAGIC
# MAGIC Same params as 01 (`bronze_mode` is ignored here). Needs the `pkwiki/pii_salt` secret, see 00_setup.

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

from pipeline import pii, runner

# COMMAND ----------

params = {k: dbutils.widgets.get(k) for k in runner.DEFAULTS}
salt = pii.get_salt(dbutils)
run = runner.run_silver(spark, params, salt)

# COMMAND ----------

display(runner.last_run_summary(spark, params, run["run_id"]))

# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - setup
# MAGIC Creates the four schemas (`<prefix>_raw`, `_bronze`, `_silver`, `_ops`), the landing volume and every Delta table from `pipeline/schemas.py`.
# MAGIC Safe to run again, tables that already exist are left alone.
# MAGIC
# MAGIC Params: `catalog`, `prefix` (use something like `pkwiki_test` for a throwaway copy), `seed_samples` = yes copies `sample_data/raw` into the landing volume so there is something to load on day one.

# COMMAND ----------

# MAGIC %md
# MAGIC ### PII salt (once per workspace)
# MAGIC Silver hashes editor names with a salt that we keep out of the repo. Create the secret once with the Databricks CLI:
# MAGIC ```
# MAGIC databricks secrets create-scope pkwiki
# MAGIC databricks secrets put-secret pkwiki pii_salt
# MAGIC ```
# MAGIC (the second command asks for the value). Don't change it later or the same editor gets a new key.
# MAGIC
# MAGIC For a quick test without the scope you can run `os.environ["PII_SALT"] = "..."` in the silver notebook before it reads the salt.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("prefix", "pkwiki")
dbutils.widgets.dropdown("seed_samples", "no", ["yes", "no"])

# COMMAND ----------

import os
import shutil
import sys

# notebook runs from notebooks/, the package is one level up
repo_root = os.path.abspath("..")
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from pipeline import setup
from pipeline.config import Config

# COMMAND ----------

cfg = Config(dbutils.widgets.get("catalog"), dbutils.widgets.get("prefix"))
setup.create_all(spark, cfg, on_databricks=True)

# COMMAND ----------

if dbutils.widgets.get("seed_samples") == "yes":
    try:
        setup.seed_samples(repo_root, cfg)
    except shutil.Error as e:
        # copytree also copies permissions/mtimes, which the volume doesn't always allow.
        # plain file copies are enough
        print(f"copytree complained ({len(e.args[0])} errors), copying files one by one")
        src = os.path.join(repo_root, "sample_data", "raw")
        for folder, _, files in os.walk(src):
            out = os.path.join(cfg.landing_root, os.path.relpath(folder, src))
            os.makedirs(out, exist_ok=True)
            for name in files:
                shutil.copyfile(os.path.join(folder, name), os.path.join(out, name))
        print(f"copied sample files to {cfg.landing_root}")

# COMMAND ----------

schemas = ", ".join(f"'{cfg.prefix}_{l}'" for l in ("raw", "bronze", "silver", "ops"))
display(spark.sql(f"""
    SELECT table_schema, table_name, table_type
    FROM {cfg.catalog}.information_schema.tables
    WHERE table_schema IN ({schemas})
    ORDER BY table_schema, table_name
"""))

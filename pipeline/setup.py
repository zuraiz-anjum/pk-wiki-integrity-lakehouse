"""Creates schemas, the landing volume and every table (from schemas.py). Safe to run again."""
import os
import shutil

from pipeline.config import Config
from pipeline.delta_utils import create_table, init_session, table_name
from pipeline.schemas import TABLES


def create_all(spark, cfg: Config, on_databricks=True):
    init_session(spark)
    for layer in ("raw", "bronze", "silver", "ops"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema(layer)}")
    if on_databricks:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.schema('raw')}.landing")

    for key, (layer, name, schema, parts, pk) in TABLES.items():
        full, created = create_table(spark, cfg, key)
        print(("created " if created else "exists  ") + full)
        if created and on_databricks:
            # informational PK in unity catalog (not enforced, but shows up in the catalog
            # explorer and BI tools pick it up). the NOT NULL that create_table puts on the key
            # columns *is* enforced, here and locally
            try:
                spark.sql(f"ALTER TABLE {full} ADD CONSTRAINT pk_{name} PRIMARY KEY ({', '.join(pk)})")
            except Exception as e:
                print(f"  (no PK constraint on {full}: {type(e).__name__})")


def seed_samples(repo_root, cfg: Config):
    """Copy sample_data/raw from the repo into the landing volume so the pipeline has something
    to read on day one."""
    src = os.path.join(repo_root, "sample_data", "raw")
    # copyfile, not the default copy2: the /Volumes FUSE mount can refuse copystat
    shutil.copytree(src, cfg.landing_root, dirs_exist_ok=True, copy_function=shutil.copyfile)
    n = sum(len(files) for _, _, files in os.walk(cfg.landing_root))
    print(f"{n} files under {cfg.landing_root}")


def drop_all(spark, cfg: Config):
    """Only for resetting a test copy (prefix pkwiki_test etc.)."""
    for key in TABLES:
        spark.sql(f"DROP TABLE IF EXISTS {table_name(cfg, key)}")

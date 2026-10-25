"""Creates schemas, the landing volume and every table (from schemas.py). Safe to run again."""
import os
import shutil

from pipeline.config import Config
from pipeline.delta_utils import create_table, init_session, table_name
from pipeline.schemas import GOLD_FKS, TABLES

LAYERS = ("raw", "bronze", "silver", "gold", "ops")


def add_constraints(spark, cfg, key):
    """Informational PK (and for gold, FK) constraints in unity catalog. Not enforced, but they
    show up in the catalog explorer and BI tools pick up the relationships from them. The NOT NULL
    that create_table puts on the key columns *is* enforced, here and locally."""
    layer, name, schema, parts, pk = TABLES[key]
    full = cfg.table(layer, name)
    try:
        spark.sql(f"ALTER TABLE {full} ADD CONSTRAINT pk_{name} PRIMARY KEY ({', '.join(pk)})")
    except Exception as e:
        print(f"  (no PK constraint on {full}: {type(e).__name__})")
    for col, ref_key, ref_col in GOLD_FKS.get(key, []):
        ref = table_name(cfg, ref_key)
        try:
            spark.sql(f"ALTER TABLE {full} ADD CONSTRAINT fk_{name}_{col} FOREIGN KEY ({col}) "
                      f"REFERENCES {ref} ({ref_col})")
        except Exception as e:
            print(f"  (no FK {name}.{col} -> {ref}: {type(e).__name__})")


def create_layers(spark, cfg: Config, layers, on_databricks=True, verbose=True):
    """Create the missing tables of the given layers. Returns the keys of the new ones."""
    for layer in layers:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema(layer)}")
    new = []
    # dims come before facts in TABLES, so the FK targets exist by the time a fact is created
    for key, (layer, *_) in TABLES.items():
        if layer not in layers:
            continue
        full, created = create_table(spark, cfg, key)
        if created or verbose:
            print(("created " if created else "exists  ") + full)
        if created:
            new.append(key)
            if on_databricks:
                add_constraints(spark, cfg, key)
    return new


def create_all(spark, cfg: Config, on_databricks=True):
    init_session(spark)
    for layer in LAYERS:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema(layer)}")
    if on_databricks:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.schema('raw')}.landing")
    create_layers(spark, cfg, ("bronze", "silver", "gold", "ops"), on_databricks)


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
    # facts before dims, unity catalog won't drop a table another one has a FK to
    for key in reversed(list(TABLES)):
        spark.sql(f"DROP TABLE IF EXISTS {table_name(cfg, key)}")

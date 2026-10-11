"""Small Delta helpers shared by bronze, silver and ops code."""
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from pipeline.schemas import TABLES


def init_session(spark):
    # the dump timestamps have no zone, they are UTC. Without this a cluster in another
    # timezone would shift every event.
    spark.conf.set("spark.sql.session.timeZone", "UTC")


def table_name(cfg, key):
    layer, name, *_ = TABLES[key]
    return cfg.table(layer, name)


def table_exists(spark, full_name):
    try:
        spark.sql(f"DESCRIBE TABLE {full_name}").limit(1).collect()
        return True
    except Exception:
        return False


def create_table(spark, cfg, key):
    """Create an empty Delta table from the schema in schemas.py if it isn't there yet."""
    layer, name, schema, partitions, _ = TABLES[key]
    full = cfg.table(layer, name)
    if table_exists(spark, full):
        return full, False
    # not createDataFrame([], schema).saveAsTable: that makes every column nullable. the builder
    # keeps nullable=False as a real NOT NULL (Delta enforces it on every write) and the comments
    b = DeltaTable.createIfNotExists(spark).tableName(full).addColumns(schema)
    if partitions:
        b = b.partitionedBy(*partitions)
    b.execute()
    return full, True


def _missing_fields(target, incoming, prefix=""):
    """Fields in incoming that target doesn't have, as (dotted.path, type) pairs."""
    have = {f.name: f for f in target.fields}
    out = []
    for fld in incoming.fields:
        path = f"{prefix}{fld.name}"
        if fld.name not in have:
            out.append((path, fld.dataType))
        elif isinstance(fld.dataType, StructType) and isinstance(have[fld.name].dataType, StructType):
            out += _missing_fields(have[fld.name].dataType, fld.dataType, path + ".")
    return out


def evolve_schema(spark, full_name, incoming_schema):
    """Add columns that exist in the dataframe but not in the table (mergeSchema, done by hand
    so it also works for MERGE on serverless). Returns the list of columns added."""
    current = spark.table(full_name).schema
    added = []
    for path, dtype in _missing_fields(current, incoming_schema):
        quoted = ".".join(f"`{p}`" for p in path.split("."))
        spark.sql(f"ALTER TABLE {full_name} ADD COLUMNS ({quoted} {dtype.simpleString()})")
        added.append(path)
    return added


def _history_metrics(spark, full_name):
    row = spark.sql(f"DESCRIBE HISTORY {full_name} LIMIT 1").collect()[0]
    m = row["operationMetrics"] or {}
    if row["operation"] != "MERGE":
        return 0, 0, 0
    return (int(m.get("numTargetRowsInserted", 0)), int(m.get("numTargetRowsUpdated", 0)),
            int(m.get("numTargetRowsDeleted", 0)))


def run_merge(spark, full_name, merge_sql):
    """Run a MERGE INTO statement and return (inserted, updated, deleted)."""
    result = spark.sql(merge_sql)
    cols = set(result.columns)
    if {"num_inserted_rows", "num_updated_rows", "num_deleted_rows"} <= cols:
        r = result.collect()[0]
        return int(r["num_inserted_rows"]), int(r["num_updated_rows"]), int(r["num_deleted_rows"])
    return _history_metrics(spark, full_name)  # older delta doesn't return metrics from MERGE


def insert_columns(schema):
    names = [f.name for f in schema.fields]
    return ", ".join(f"`{n}`" for n in names), ", ".join(f"s.`{n}`" for n in names)


def update_set(schema, skip=()):
    return ", ".join(f"t.`{f.name}` = s.`{f.name}`" for f in schema.fields if f.name not in skip)


def conform(df, schema):
    """Select exactly the contract columns in contract order, casting to the contract type.
    Anything extra is dropped, anything missing fails loudly."""
    return df.select([F.col(f.name).cast(f.dataType).alias(f.name) for f in schema.fields])


def sql_list(values):
    return ", ".join("'" + v.replace("'", "''") + "'" for v in values)

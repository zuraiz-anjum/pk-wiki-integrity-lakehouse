"""
Raw files -> Bronze.

Bronze keeps the payload as it arrived. The only things we add are lineage columns and drift
markers. Loads are idempotent: the write is a MERGE that only inserts rows whose record_hash
isn't in that partition yet, so re-running a day (or a whole backfill) inserts nothing new.
"""
import os

from pyspark.sql import functions as F

from pipeline import schemas as sc
from pipeline.audit import StepSkipped, audit_step
from pipeline.delta_utils import conform, evolve_schema, insert_columns, run_merge, sql_list, table_name

HISTORY_FIELD_COUNT = len(sc.HISTORY_COLUMN_NAMES)


def _has_files(path):
    if not os.path.isdir(path):
        return False
    return any(not n.startswith((".", "_")) for n in os.listdir(path))


def _merge_insert_only(spark, cfg, key, df, partition_filter):
    target = table_name(cfg, key)
    added = evolve_schema(spark, target, df.schema)
    if added:
        print(f"schema evolved on {target}: added {added}")
    view = f"src_{key}"
    df.createOrReplaceTempView(view)
    cols, vals = insert_columns(df.schema)
    pk = sc.TABLES[key][4]
    on = " AND ".join(f"t.`{c}` = s.`{c}`" for c in pk)
    sql = f"""
        MERGE INTO {target} AS t
        USING {view} AS s
        ON {on} AND {partition_filter}
        WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})
    """
    return run_merge(spark, target, sql)


def log_drift(spark, cfg, rows_df, run):
    """rows_df: source, batch_ref, wiki, drift_type, field_name, row_count, sample_value, action_taken.
    Merged so a rerun of the same batch doesn't log the same drift twice."""
    if rows_df.limit(1).count() == 0:
        return 0
    target = table_name(cfg, "drift_log")
    df = (rows_df.withColumn("first_seen_run_id", F.lit(run["run_id"]))
          .withColumn("load_timestamp", F.current_timestamp()))
    df = conform(df, sc.SCHEMA_DRIFT_LOG)
    df.createOrReplaceTempView("src_drift")
    cols, vals = insert_columns(df.schema)
    pk = sc.TABLES["drift_log"][4]
    on = " AND ".join(f"t.`{c}` = s.`{c}`" for c in pk)
    run_merge(spark, target, f"""
        MERGE INTO {target} AS t USING src_drift AS s ON {on}
        WHEN MATCHED THEN UPDATE SET t.row_count = s.row_count, t.load_timestamp = s.load_timestamp
        WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})
    """)
    for r in df.collect():
        print(f"  drift: {r.drift_type} {r.field_name} ({r.row_count} rows) -> {r.action_taken}")
    return df.count()


# --------------------------------------------------------------------------------------------
# full load: MediaWiki history dump
# --------------------------------------------------------------------------------------------
def read_history(spark, path):
    return (spark.read.format("csv")
            .schema(sc.HISTORY_RAW_SCHEMA)          # explicit, no inferSchema
            .option("sep", "\t")
            .option("header", "false")
            .option("quote", "")                    # the dump never quotes, a " is just a character
            .option("mode", "PERMISSIVE")           # bad rows go to _corrupt_record instead of failing
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .option("recursiveFileLookup", "true")  # picks up .tsv and .tsv.bz2 alike
            .load(path)
            .withColumn("source_file", F.col("_metadata.file_path")))


def history_drift(df, snapshot, wiki):
    """Rows that didn't split into 78 fields. If the dump suddenly gets a 79th column every row
    lands here, gets quarantined in silver, and we see it in the drift log instead of a crash."""
    return (df.where(F.col("_corrupt_record").isNotNull())
            .withColumn("n", F.size(F.split("_corrupt_record", "\t")))
            .where(F.col("n") != HISTORY_FIELD_COUNT)
            .groupBy("n").agg(F.count("*").alias("row_count"),
                              F.first(F.substring("_corrupt_record", 1, 300)).alias("sample_value"))
            .select(F.lit("mediawiki_history").alias("source"), F.lit(snapshot).alias("batch_ref"),
                    F.lit(wiki).alias("wiki"), F.lit("column_count").alias("drift_type"),
                    F.concat(F.col("n").cast("string"), F.lit(f" fields instead of {HISTORY_FIELD_COUNT}"))
                    .alias("field_name"),
                    "row_count", "sample_value",
                    F.lit("kept in bronze._corrupt_record, quarantined in silver").alias("action_taken")))


def load_history_snapshot(spark, cfg, run, snapshot, wikis):
    target = table_name(cfg, "bronze_history")
    for wiki in wikis:
        path = cfg.history_path(snapshot, wiki)
        with audit_step(spark, cfg, run, "raw_to_bronze", target, path, reraise=False) as m:
            if not _has_files(path):
                raise StepSkipped("nothing landed here")
            raw = read_history(spark, path)
            hashed_line = F.concat_ws("\t", *[F.coalesce(F.col(c), F.lit("")) for c in sc.HISTORY_COLUMN_NAMES])
            df = (raw.withColumn("record_hash",
                                 F.sha2(F.coalesce(F.col("_corrupt_record"), hashed_line), 256))
                  .withColumn("snapshot", F.lit(snapshot))
                  .withColumn("wiki", F.lit(wiki))
                  .withColumn("batch_id", F.lit(run["run_id"]))
                  .withColumn("load_timestamp", F.current_timestamp())
                  .dropDuplicates(["record_hash"]))
            df = conform(df, sc.BRONZE_HISTORY)
            m["rows_read"] = df.count()
            m["rows_quarantined"] = df.where("_corrupt_record IS NOT NULL").count()
            log_drift(spark, cfg, history_drift(df, snapshot, wiki), run)
            part = f"t.snapshot = '{snapshot}' AND t.wiki = '{wiki}'"
            m["rows_inserted"], m["rows_updated"], m["rows_deleted"] = \
                _merge_insert_only(spark, cfg, "bronze_history", df, part)


# --------------------------------------------------------------------------------------------
# incremental: recent changes API (jsonl)
# --------------------------------------------------------------------------------------------
_RECORD_FIELDS = sc.RC_RECORD_SCHEMA.fields


def _typed_record(loose_rec):
    """Rebuild the record with the declared types from the all-string parse. try_cast gives null
    instead of an exception, which we then spot as a mismatch."""
    parts = []
    for fld in _RECORD_FIELDS:
        raw = loose_rec[fld.name]
        t = fld.dataType.simpleString()
        if t.startswith(("array", "map")):
            parts.append(F.from_json(raw, fld.dataType).alias(fld.name))
        elif t == "string":
            parts.append(raw.alias(fld.name))
        else:
            parts.append(F.expr(f"try_cast(`_l`.`record`.`{fld.name}` AS {t})").alias(fld.name))
    return F.struct(*parts)


def _mismatches():
    checks = []
    for fld in _RECORD_FIELDS:
        if fld.dataType.simpleString() == "string":
            continue
        raw = F.col("_l.record")[fld.name]
        typed = F.col("record")[fld.name]
        checks.append(F.when(raw.isNotNull() & typed.isNull(), F.lit(fld.name)))
    return F.filter(F.array(*checks), lambda x: x.isNotNull())


def read_rc(spark, path):
    known_rec = F.array(*[F.lit(f.name) for f in _RECORD_FIELDS])
    known_env = F.array(*[F.lit(n) for n in sc.RC_ENVELOPE_FIELDS])
    df = (spark.read.option("recursiveFileLookup", "true").text(path)
          .withColumn("source_file", F.col("_metadata.file_path"))
          .where(F.length(F.trim("value")) > 0)
          .withColumn("_l", F.from_json("value", sc.RC_LINE_SCHEMA_LOOSE))
          .withColumn("record", _typed_record(F.col("_l.record")))
          .withColumn("type_mismatch_fields", _mismatches())
          .withColumn("unknown_fields", F.array_union(
              F.array_except(F.coalesce(F.expr("json_object_keys(get_json_object(value, '$.record'))"),
                                        F.array().cast("array<string>")), known_rec),
              F.transform(F.array_except(F.coalesce(F.expr("json_object_keys(value)"),
                                                    F.array().cast("array<string>")), known_env),
                          lambda k: F.concat(F.lit("envelope."), k)))))
    bad_json = F.col("_l").isNull() | F.col("_l.record").isNull()
    return (df.withColumn("_corrupt_record",
                          F.when(bad_json | (F.size("type_mismatch_fields") > 0), F.col("value")))
            .select(F.col("_l._wiki").alias("env_wiki"), F.col("_l._source").alias("source_api"),
                    F.col("_l._ingested_at").alias("ingested_at"),
                    F.col("_l._window_start").alias("window_start"),
                    F.when(~bad_json, F.col("record")).alias("record"),
                    F.col("value").alias("raw_json"), "_corrupt_record", "type_mismatch_fields",
                    "unknown_fields", F.sha2("value", 256).alias("record_hash"), "source_file"))


def rc_drift(df, ingest_date, wiki):
    base = [F.lit("recentchanges").alias("source"), F.lit(ingest_date).alias("batch_ref"),
            F.lit(wiki).alias("wiki")]
    new = (df.select(F.explode("unknown_fields").alias("field_name"), "raw_json")
           .groupBy("field_name").agg(F.count("*").alias("row_count"),
                                      F.first(F.substring("raw_json", 1, 300)).alias("sample_value"))
           .select(*base, F.lit("new_field").alias("drift_type"), "field_name", "row_count",
                   "sample_value", F.lit("kept in bronze.raw_json, add it to schemas.py to load it")
                   .alias("action_taken")))
    mism = (df.select(F.explode("type_mismatch_fields").alias("field_name"), "raw_json")
            .groupBy("field_name").agg(F.count("*").alias("row_count"),
                                       F.first(F.substring("raw_json", 1, 300)).alias("sample_value"))
            .select(*base, F.lit("type_mismatch").alias("drift_type"), "field_name", "row_count",
                    "sample_value", F.lit("row flagged in _corrupt_record, quarantined in silver")
                    .alias("action_taken")))
    bad = (df.where(F.col("record").isNull())
           .agg(F.count("*").alias("row_count"), F.first(F.substring("raw_json", 1, 300)).alias("sample_value"))
           .where("row_count > 0")
           .select(*base, F.lit("malformed_json").alias("drift_type"), F.lit("<line>").alias("field_name"),
                   "row_count", "sample_value", F.lit("quarantined in silver").alias("action_taken")))
    return new.unionByName(mism).unionByName(bad)


def load_recentchanges(spark, cfg, run, ingest_dates, wikis):
    target = table_name(cfg, "bronze_rc")
    for d in ingest_dates:
        for wiki in wikis:
            path = cfg.rc_path(d, wiki)
            with audit_step(spark, cfg, run, "raw_to_bronze", target, path, reraise=False) as m:
                if not _has_files(path):
                    raise StepSkipped("nothing landed here")
                df = (read_rc(spark, path)
                      .withColumn("ingest_date", F.lit(d))
                      .withColumn("wiki", F.lit(wiki))
                      .withColumn("batch_id", F.lit(run["run_id"]))
                      .withColumn("load_timestamp", F.current_timestamp())
                      .dropDuplicates(["record_hash"]))
                df = conform(df, sc.BRONZE_RC)
                m["rows_read"] = df.count()
                m["rows_quarantined"] = df.where("_corrupt_record IS NOT NULL").count()
                log_drift(spark, cfg, rc_drift(df, d, wiki), run)
                part = f"t.ingest_date = '{d}' AND t.wiki = '{wiki}'"
                m["rows_inserted"], m["rows_updated"], m["rows_deleted"] = \
                    _merge_insert_only(spark, cfg, "bronze_rc", df, part)


def rebuild_partitions(spark, cfg, key, partition_sql):
    """For the rare case where the parse itself changed (new field added to schemas.py) and old
    bronze rows should be re-read from the raw files. Deletes the partition, the normal load
    then re-inserts it."""
    target = table_name(cfg, key)
    spark.sql(f"DELETE FROM {target} WHERE {partition_sql}")


def wikis_sql(wikis):
    return sql_list(wikis)

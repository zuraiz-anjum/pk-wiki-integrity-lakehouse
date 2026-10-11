"""
Second silver step, runs after silver.py for the same batch:

1. revert detection for api edits. The dump already tells us what was reverted; the api doesn't,
   so we do it ourselves: an edit whose sha1 matches one of the previous 15 versions of the page
   restored that version, and every edit in between got reverted.
2. SCD Type 2 dimensions silver.page (title / namespace / redirect / deleted) and
   silver.editor (editor_type / is_bot). Old versions are closed (valid_to, is_current=false),
   never overwritten.

Both are MERGEs with change detection, so rerunning a batch is a no-op.
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

from pipeline import schemas as sc
from pipeline.audit import audit_step
from pipeline.delta_utils import conform, insert_columns, run_merge, table_name
from pipeline.pii import MASKED_RX

REVERT_LOOKBACK_REVISIONS = 15   # same idea as the window wikimedia uses for identity reverts
REVERT_LOOKBACK_DAYS = 30        # don't scan a page's whole history every day


def batch_filter(load_type, refs, wikis):
    ref = F.col("source_ref").isin(*refs)
    src = F.col("source") == ("dump" if load_type == "full" else "api")
    return ref & src & F.col("wiki").isin(*wikis)


# ----------------------------------------------------------------------------- reverts
def detect_reverts(spark, cfg, run, refs, wikis, source_param):
    rev_t = table_name(cfg, "silver_revision")
    with audit_step(spark, cfg, run, "silver_scd2", rev_t + " (reverts)", source_param,
                     reraise=False) as m:
        revs = spark.table(rev_t)
        batch = revs.where(batch_filter("incremental", refs, wikis))
        bounds = batch.agg(F.min("rev_ts").alias("lo")).collect()[0]
        if bounds.lo is None:
            return
        touched = batch.select("wiki", "page_id").distinct()
        hist = (revs.join(touched, ["wiki", "page_id"])
                .where(F.col("rev_ts") >= F.lit(bounds.lo) - F.expr(f"INTERVAL {REVERT_LOOKBACK_DAYS} DAYS"))
                .where(F.col("sha1").isNotNull())
                .select("wiki", "page_id", "rev_id", "rev_ts", "sha1", "source"))
        w = Window.partitionBy("wiki", "page_id").orderBy("rev_ts", "rev_id")
        hist = hist.withColumn("rn", F.row_number().over(w))
        m["rows_read"] = hist.count()

        a, b = hist.alias("a"), hist.alias("b")
        # a restores b: same text, at least one edit in between, within the lookback
        matches = (a.join(b, (F.col("a.wiki") == F.col("b.wiki")) & (F.col("a.page_id") == F.col("b.page_id"))
                          & (F.col("a.sha1") == F.col("b.sha1")) & (F.col("b.rn") < F.col("a.rn") - 1)
                          & (F.col("a.rn") - F.col("b.rn") <= REVERT_LOOKBACK_REVISIONS))
                   .groupBy("a.wiki", "a.page_id", "a.rev_id", "a.rn", "a.rev_ts", "a.source")
                   .agg(F.max("b.rn").alias("base_rn")))
        reverts = matches.select("wiki", "page_id", F.col("rev_id").alias("revert_rev_id"),
                                 F.col("rn").alias("revert_rn"), F.col("rev_ts").alias("revert_ts"), "base_rn",
                                 F.col("source").alias("revert_source"))

        c = hist.alias("c")
        undone = (c.join(reverts.alias("r"), (F.col("c.wiki") == F.col("r.wiki"))
                         & (F.col("c.page_id") == F.col("r.page_id"))
                         & (F.col("c.rn") > F.col("r.base_rn")) & (F.col("c.rn") < F.col("r.revert_rn")))
                  # first revert wins if an edit was undone more than once
                  .withColumn("k", F.row_number().over(
                      Window.partitionBy("c.wiki", "c.rev_id").orderBy("r.revert_rn")))
                  .where("k = 1")
                  .select(F.col("c.wiki").alias("wiki"), F.col("c.rev_id").alias("rev_id"),
                          F.col("r.revert_rev_id").alias("reverted_by_rev_id"),
                          (F.col("r.revert_ts").cast("long") - F.col("c.rev_ts").cast("long")).alias("seconds_to_revert"),
                          F.col("r.revert_source").alias("revert_source")))

        flags = (reverts.select("wiki", F.col("revert_rev_id").alias("rev_id"), F.lit(True).alias("set_revert"),
                                F.lit(None).cast("boolean").alias("set_reverted"),
                                F.lit(None).cast("bigint").alias("reverted_by_rev_id"),
                                F.lit(None).cast("bigint").alias("seconds_to_revert"))
                 .unionByName(undone.where("revert_source = 'api'")  # dump-vs-dump is already in the dump
                              .select("wiki", "rev_id", F.lit(None).cast("boolean").alias("set_revert"),
                                      F.lit(True).alias("set_reverted"), "reverted_by_rev_id", "seconds_to_revert"))
                 .groupBy("wiki", "rev_id")
                 .agg(F.max("set_revert").alias("set_revert"), F.max("set_reverted").alias("set_reverted"),
                      F.max("reverted_by_rev_id").alias("reverted_by_rev_id"),
                      F.max("seconds_to_revert").alias("seconds_to_revert")))
        flags.createOrReplaceTempView("src_revert_flags")
        # flags only ever go false -> true, so running this twice can't flip anything back.
        # the api often already tagged the edit mw-reverted, then we still want to fill in who
        # reverted it and how fast
        m["rows_inserted"], m["rows_updated"], m["rows_deleted"] = run_merge(spark, rev_t, f"""
            MERGE INTO {rev_t} AS t
            USING src_revert_flags AS s
            ON t.wiki = s.wiki AND t.rev_id = s.rev_id
            WHEN MATCHED AND ((s.set_revert AND NOT t.is_revert) OR (s.set_reverted AND NOT t.is_reverted)
                              OR (s.reverted_by_rev_id IS NOT NULL AND t.reverted_by_rev_id IS NULL))
              THEN UPDATE SET
                t.is_revert = t.is_revert OR coalesce(s.set_revert, false),
                t.is_reverted = t.is_reverted OR coalesce(s.set_reverted, false),
                t.reverted_by_rev_id = coalesce(t.reverted_by_rev_id, s.reverted_by_rev_id),
                t.seconds_to_revert = coalesce(t.seconds_to_revert, s.seconds_to_revert),
                t.load_timestamp = current_timestamp()
        """)


# -------------------------------------------------------------------------------- scd2
def _versions(states, keys, attrs, ts_col):
    """states: one row per observation (keys, ts, attrs; attrs may be null = 'no info').
    Forward-fill attrs, then keep only the rows where something actually changed."""
    w = Window.partitionBy(*keys).orderBy(ts_col).rowsBetween(Window.unboundedPreceding, 0)
    filled = states.select(*keys, ts_col, *[F.last(a, ignorenulls=True).over(w).alias(a) for a in attrs])
    filled = filled.withColumn("attr_hash", F.sha2(F.to_json(F.struct(*attrs)), 256))
    o = Window.partitionBy(*keys).orderBy(ts_col)
    changed = (filled.withColumn("_prev", F.lag("attr_hash").over(o))
               .where(F.col("_prev").isNull() | (F.col("_prev") != F.col("attr_hash")))
               .drop("_prev"))
    return changed.withColumnRenamed(ts_col, "valid_from")


def _scd2_merge(spark, cfg, run, key, versions, keys, sk_name, extra_cols=()):
    target = table_name(cfg, key)
    schema = sc.TABLES[key][2]
    cur = spark.table(target).where("is_current").select(
        *keys, F.col("attr_hash").alias("cur_hash"), F.col("valid_from").alias("cur_from"))
    v = versions.join(cur, list(keys), "left")

    # keys we've never seen: insert their whole history from this batch
    o = Window.partitionBy(*keys).orderBy("valid_from")
    brand_new = (v.where(F.col("cur_hash").isNull())
                 .withColumn("valid_to", F.lead("valid_from").over(o))
                 .withColumn("is_current", F.col("valid_to").isNull()))
    # keys we have: only the latest state matters, and only if it's newer and different
    latest = (v.where(F.col("cur_hash").isNotNull())
              .withColumn("_rn", F.row_number().over(Window.partitionBy(*keys).orderBy(F.col("valid_from").desc())))
              .where("_rn = 1").drop("_rn")
              .where((F.col("attr_hash") != F.col("cur_hash")) & (F.col("valid_from") > F.col("cur_from")))
              .withColumn("valid_to", F.lit(None).cast("timestamp"))
              .withColumn("is_current", F.lit(True)))
    new_rows = brand_new.unionByName(latest).drop("cur_hash", "cur_from")
    new_rows = (new_rows.withColumn(sk_name, F.sha2(F.concat_ws("|", *[F.col(k).cast("string") for k in keys],
                                                                 F.col("valid_from").cast("string")), 256))
                .withColumn("batch_id", F.lit(run["run_id"]))
                .withColumn("load_timestamp", F.current_timestamp()))
    new_rows = conform(new_rows, schema)

    # classic scd2 merge trick: changed keys go in twice. once with a merge key, which matches
    # the current version and closes it, and once (in new_rows) with a null merge key, which
    # can never match and so gets inserted as the new current version
    close = (latest.select(*keys, F.col("valid_from").alias("close_at"))
             .withColumn("_merge_key", F.concat_ws("|", *[F.col(k).cast("string") for k in keys])))
    staged = (new_rows.withColumn("_merge_key", F.lit(None).cast("string"))
              .withColumn("close_at", F.lit(None).cast("timestamp"))
              .unionByName(close, allowMissingColumns=True))
    view = f"src_{key}"
    staged.createOrReplaceTempView(view)
    cols, vals = insert_columns(schema)
    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    return new_rows.count(), run_merge(spark, target, f"""
        MERGE INTO {target} AS t
        USING {view} AS s
        ON {on} AND t.is_current AND s._merge_key IS NOT NULL
        WHEN MATCHED THEN UPDATE SET
            t.is_current = false, t.valid_to = s.close_at, t.load_timestamp = current_timestamp()
        WHEN NOT MATCHED AND s._merge_key IS NULL THEN INSERT ({cols}) VALUES ({vals})
    """)


def scd2_pages(spark, cfg, run, load_type, refs, wikis, source_param):
    target = table_name(cfg, "silver_page")
    with audit_step(spark, cfg, run, "silver_scd2", target, source_param, reraise=False) as m:
        f = batch_filter(load_type, refs, wikis)
        revs = spark.table(table_name(cfg, "silver_revision")).where(f).select(
            "wiki", "page_id", F.col("rev_ts").alias("ts"), "page_title", "page_namespace",
            F.col("page_is_redirect").alias("is_redirect"),
            F.when(F.col("is_deleted"), F.lit(True)).alias("is_deleted"))
        ev = spark.table(table_name(cfg, "silver_page_event")).where(f & (F.col("page_id") > 0))
        moves = ev.where("event_type = 'move'").select(
            "wiki", "page_id", F.col("event_ts").alias("ts"), F.col("new_title").alias("page_title"),
            F.lit(None).cast("int").alias("page_namespace"), F.lit(None).cast("boolean").alias("is_redirect"),
            F.lit(None).cast("boolean").alias("is_deleted"))
        dels = ev.where(F.col("event_type").isin("delete", "restore")).select(
            "wiki", "page_id", F.col("event_ts").alias("ts"), F.lit(None).cast("string").alias("page_title"),
            F.lit(None).cast("int").alias("page_namespace"), F.lit(None).cast("boolean").alias("is_redirect"),
            (F.col("event_type") == "delete").alias("is_deleted"))
        states = revs.unionByName(moves).unionByName(dels).where(F.col("page_id").isNotNull())
        attrs = ["page_title", "page_namespace", "is_redirect", "is_deleted"]
        versions = _versions(states, ["wiki", "page_id"], attrs, "ts") \
            .withColumn("is_deleted", F.coalesce("is_deleted", F.lit(False)))
        m["rows_read"] = states.count()
        n, (m["rows_inserted"], m["rows_updated"], m["rows_deleted"]) = _scd2_merge(
            spark, cfg, run, "silver_page", versions, ["wiki", "page_id"], "page_sk")


def scd2_editors(spark, cfg, run, load_type, refs, wikis, source_param):
    target = table_name(cfg, "silver_editor")
    with audit_step(spark, cfg, run, "silver_scd2", target, source_param, reraise=False) as m:
        f = batch_filter(load_type, refs, wikis)
        # account level, not edit level: only bot/system accounts keep their name, so an unhashed
        # key means bot. a human using the flood flag stays "registered" here (their edits are
        # still marked is_bot in silver.revision)
        hashed = F.col("editor_key").rlike(MASKED_RX)
        etype = F.when(~hashed, "bot").when(F.col("editor_type") == "bot", "registered").otherwise(F.col("editor_type"))
        states = (spark.table(table_name(cfg, "silver_revision")).where(f & F.col("editor_key").isNotNull())
                  .select("wiki", "editor_key", F.col("rev_ts").alias("ts"), etype.alias("editor_type"),
                          (~hashed).alias("is_bot"), "editor_registration_ts"))
        reg = states.groupBy("wiki", "editor_key").agg(F.min("editor_registration_ts").alias("registration_ts"))
        versions = _versions(states.drop("editor_registration_ts"), ["wiki", "editor_key"],
                             ["editor_type", "is_bot"], "ts").join(reg, ["wiki", "editor_key"], "left")
        m["rows_read"] = states.count()
        n, (m["rows_inserted"], m["rows_updated"], m["rows_deleted"]) = _scd2_merge(
            spark, cfg, run, "silver_editor", versions, ["wiki", "editor_key"], "editor_sk")


def run_silver_history(spark, cfg, run, load_type, refs, wikis):
    source_param = f"silver.revision source_ref={refs[0]}..{refs[-1]} wiki={','.join(wikis)}"
    if load_type == "incremental":
        detect_reverts(spark, cfg, run, refs, wikis, source_param)
    scd2_pages(spark, cfg, run, load_type, refs, wikis, source_param)
    scd2_editors(spark, cfg, run, load_type, refs, wikis, source_param)

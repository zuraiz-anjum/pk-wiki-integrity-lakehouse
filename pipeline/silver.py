"""
Bronze -> Silver: cast, unify the two sources, mask PII, quarantine what doesn't fit, MERGE.

Both sources end up in the same three tables:
    silver.revision    one row per edit                     PK (wiki, rev_id)
    silver.page_event  creations, moves, deletions, ...     PK (wiki, event_key)
    silver.user_event  account creations, renames, blocks   PK (wiki, event_key)

Merge rules
  * rerunning the same batch changes nothing: rows are only updated when row_hash differs
  * the dump wins over the api for the same revision (it has the final revert flags etc.)
  * a full load of snapshot YYYY-MM replaces api rows from before the end of that month for the
    wikis in the batch (WHEN NOT MATCHED BY SOURCE ... DELETE), api rows after it are kept
"""
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from pipeline import pii
from pipeline import schemas as sc
from pipeline.audit import StepSkipped, audit_step
from pipeline.config import PKT, WIKIS, snapshot_cutoff
from pipeline.delta_utils import (conform, insert_columns, run_merge, sql_list, table_name,
                                  update_set)

REVERT_TAGS = ["mw-rollback", "mw-undo", "mw-manual-revert"]
USER_LOG_TYPES = ["newusers", "renameuser", "block", "rights"]
NOT_HASHED = {"row_hash", "batch_id", "load_timestamp", "source_ref"}


# ------------------------------------------------------------------------------- helpers
def tc(col, dtype):
    """try_cast: bad values become null instead of killing the job (ANSI mode is on on serverless)."""
    return F.expr(f"try_cast({col} AS {dtype})")


@F.udf(StringType())
def base36_to_hex(s):
    # the dump stores sha1 in base 36 (31 chars), the api in hex (40 chars). we need one format
    # to compare them when looking for reverts
    if not s:
        return None
    try:
        return format(int(s, 36), "040x")
    except ValueError:
        return None


def split_list(col):
    return F.when(F.col(col).isNull() | (F.col(col) == ""), F.lit(None).cast("array<string>")) \
        .otherwise(F.split(F.col(col), ","))


def normalise_title(title, ns):
    """api titles have the localized namespace prefix and spaces, the dump has neither."""
    no_prefix = F.when(ns != 0, F.regexp_replace(title, r"^[^:]+:", "")).otherwise(title)
    return F.regexp_replace(no_prefix, " ", "_")


def error_if(cond, msg):
    return F.when(cond, F.lit(msg))


def collect_errors(*checks):
    return F.filter(F.array(*checks), lambda x: x.isNotNull())


def short_hash(*cols):
    return F.substring(F.sha2(F.concat_ws("|", *[F.coalesce(c.cast("string"), F.lit("")) for c in cols]), 256), 1, 24)


def log_key(log_id, event_type):
    # one log entry can produce two dump rows (a move that leaves a redirect is a move + a
    # create with the same log id), so the type is part of the key
    return F.concat(F.lit("log:"), log_id.cast("string"), F.lit(":"), event_type)


def with_row_hash(df, schema):
    cols = [F.col(f.name) for f in schema.fields if f.name not in NOT_HASHED and f.name in df.columns]
    return df.withColumn("row_hash", F.sha2(F.to_json(F.struct(*cols)), 256))


def pkt(ts):
    return F.from_utc_timestamp(ts, PKT)


# ------------------------------------------------------------------- dump (full load)
def history_frames(b, salt):
    """bronze.mediawiki_history slice -> (revisions, page events, user events, rejects)"""
    wiki_ok = F.col("wiki_db").isin(*WIKIS) & (F.col("wiki_db") == F.col("wiki"))
    ts = tc("event_timestamp", "timestamp")
    common_errors = [error_if(~wiki_ok | F.col("wiki_db").isNull(), "wiki_db doesn't match the folder"),
                     error_if(ts.isNull(), "bad event_timestamp")]
    raw_line = F.coalesce(F.col("_corrupt_record"),
                          F.concat_ws("\t", *[F.coalesce(F.col(c), F.lit("")) for c in sc.HISTORY_COLUMN_NAMES]))
    b = b.withColumn("_ts", ts).withColumn("_raw_line", raw_line)

    # rows the reader couldn't split, and entities we don't know about, go straight to quarantine
    rejects = b.where(F.col("_corrupt_record").isNotNull()
                      | ~F.coalesce(F.col("event_entity"), F.lit("")).isin("revision", "page", "user")) \
        .select(F.lit("bronze.mediawiki_history").alias("source_table"), "record_hash", "wiki",
                F.col("snapshot").alias("batch_ref"),
                F.array(F.when(F.col("_corrupt_record").isNotNull(),
                               F.concat(F.lit("malformed row: "),
                                        F.size(F.split("_corrupt_record", "\t")).cast("string"),
                                        F.lit(" fields")))
                        .otherwise(F.concat(F.lit("unknown event_entity "),
                                            F.coalesce("event_entity", F.lit("null"))))).alias("reasons"),
                F.col("_raw_line").alias("raw_payload"))
    b = b.where(F.col("_corrupt_record").isNull())

    performer = F.coalesce(F.col("event_user_text"), F.col("event_user_text_historical"))
    p_type = pii.editor_type(performer,
                             F.coalesce(tc("event_user_is_anonymous", "boolean"), F.lit(False)),
                             F.coalesce(tc("event_user_is_temporary", "boolean"), F.lit(False)),
                             F.coalesce(F.col("event_user_is_bot_by_historical"), F.lit("")) != "")
    b = b.withColumn("_ptype", p_type).withColumn("_pkey", pii.mask_name(performer, F.col("_ptype"), salt))

    ns_hist = tc("page_namespace_historical", "int")
    title = pii.mask_user_page_title(F.col("page_title_historical"), ns_hist, salt)

    # --- revisions
    r = b.where("event_entity = 'revision'")
    rev_id = tc("revision_id", "bigint")
    rev = r.select(
        F.col("wiki_db").alias("wiki"), rev_id.alias("rev_id"), tc("page_id", "bigint").alias("page_id"),
        tc("revision_parent_id", "bigint").alias("parent_rev_id"),
        F.col("_ts").alias("rev_ts"), pkt(F.col("_ts")).alias("rev_ts_pkt"), F.to_date("_ts").alias("rev_date"),
        title.alias("page_title"), ns_hist.alias("page_namespace"),
        tc("page_is_redirect", "boolean").alias("page_is_redirect"),
        F.col("_pkey").alias("editor_key"), F.col("_ptype").alias("editor_type"),
        (F.col("_ptype") == "bot").alias("is_bot"),
        F.coalesce(tc("event_user_registration_timestamp", "timestamp"),
                   tc("event_user_creation_timestamp", "timestamp")).alias("editor_registration_ts"),
        tc("revision_text_bytes", "bigint").alias("bytes"),
        tc("revision_text_bytes_diff", "bigint").alias("bytes_diff"),
        base36_to_hex("revision_text_sha1").alias("sha1"),
        tc("revision_minor_edit", "boolean").alias("is_minor"),
        (F.coalesce(tc("revision_parent_id", "bigint"), F.lit(0)) == 0).alias("is_page_creation"),
        pii.mask_text(F.col("event_comment")).alias("comment_masked"),
        split_list("revision_tags").alias("tags"),
        F.coalesce(tc("revision_is_identity_revert", "boolean"), F.lit(False)).alias("is_revert"),
        F.coalesce(tc("revision_is_identity_reverted", "boolean"), F.lit(False)).alias("is_reverted"),
        tc("revision_first_identity_reverting_revision_id", "bigint").alias("reverted_by_rev_id"),
        tc("revision_seconds_to_identity_revert", "bigint").alias("seconds_to_revert"),
        (F.coalesce(tc("revision_is_deleted_by_page_deletion", "boolean"), F.lit(False))
         | F.coalesce(tc("page_is_deleted", "boolean"), F.lit(False))).alias("is_deleted"),
        F.lit("dump").alias("source"), F.col("snapshot").alias("source_ref"),
        F.lit(1).alias("_priority"), F.col("load_timestamp").alias("_order"),
        "record_hash", "snapshot", "_raw_line",
        collect_errors(*common_errors,
                       error_if(rev_id.isNull(), "bad revision_id"),
                       error_if(F.col("page_id").isNotNull() & tc("page_id", "bigint").isNull(), "bad page_id"),
                       error_if(F.col("revision_text_bytes").isNotNull()
                                & tc("revision_text_bytes", "bigint").isNull(), "bad revision_text_bytes"))
        .alias("_errors"))

    # --- page events
    p = b.where("event_entity = 'page'")
    log_id = tc("event_log_id", "bigint")
    page = p.select(
        F.col("wiki_db").alias("wiki"),
        F.when(log_id.isNotNull(), log_key(log_id, F.col("event_type")))
        .otherwise(F.concat(F.lit("h:"), short_hash(F.col("wiki_db"), F.col("page_id"), F.col("event_type"),
                                                     F.col("event_timestamp"), F.col("page_title_historical"))))
        .alias("event_key"),
        log_id.alias("log_id"), tc("page_id", "bigint").alias("page_id"),
        F.col("event_type").alias("event_type"),
        F.col("_ts").alias("event_ts"), pkt(F.col("_ts")).alias("event_ts_pkt"),
        ns_hist.alias("page_namespace"), title.alias("page_title"),
        # the dump only knows the title after the move
        F.when(F.col("event_type") == "move", title).alias("new_title"),
        F.col("_pkey").alias("editor_key"), F.col("_ptype").alias("editor_type"),
        pii.mask_text(F.col("event_comment")).alias("comment_masked"),
        F.lit("dump").alias("source"), F.col("snapshot").alias("source_ref"),
        F.lit(1).alias("_priority"), F.col("load_timestamp").alias("_order"),
        "record_hash", "snapshot", "_raw_line",
        collect_errors(*common_errors).alias("_errors"))

    # --- user events (target is the account the event is about)
    u = b.where("event_entity = 'user'")
    target = F.coalesce(F.col("user_text_historical"), F.col("user_text"))
    t_type = pii.editor_type(target,
                             F.coalesce(tc("user_is_anonymous", "boolean"), F.lit(False)),
                             F.coalesce(tc("user_is_temporary", "boolean"), F.lit(False)),
                             F.coalesce(F.col("user_is_bot_by_historical"), F.lit("")) != "")
    user = u.withColumn("_ttype", t_type).select(
        F.col("wiki_db").alias("wiki"),
        F.when(log_id.isNotNull(), log_key(log_id, F.col("event_type")))
        .otherwise(F.concat(F.lit("h:"), short_hash(F.col("wiki_db"), F.col("user_id"), F.col("event_type"),
                                                     F.col("event_timestamp"))))
        .alias("event_key"),
        log_id.alias("log_id"), F.col("event_type"),
        F.col("_ts").alias("event_ts"), pkt(F.col("_ts")).alias("event_ts_pkt"),
        F.col("_pkey").alias("performer_key"), F.col("_ptype").alias("performer_type"),
        pii.mask_name(target, F.col("_ttype"), salt).alias("target_key"), F.col("_ttype").alias("target_type"),
        F.lit(None).cast("string").alias("block_duration"),
        split_list("user_blocks_historical").alias("block_flags"),
        split_list("user_groups_historical").alias("user_groups"),
        pii.mask_text(F.col("event_comment")).alias("comment_masked"),
        F.lit("dump").alias("source"), F.col("snapshot").alias("source_ref"),
        F.lit(1).alias("_priority"), F.col("load_timestamp").alias("_order"),
        "record_hash", "snapshot", "_raw_line",
        collect_errors(*common_errors).alias("_errors"))

    return rev, page, user, rejects


# ------------------------------------------------------------------ api (incremental)
def rc_frames(b, salt):
    rec = F.col("record")
    b = b.where(F.col("record").isNotNull() & F.col("_corrupt_record").isNull())
    ts = tc("record.timestamp", "timestamp")
    errs = [error_if(F.coalesce(F.col("env_wiki"), F.lit("")) != F.col("wiki"), "envelope wiki doesn't match the folder"),
            error_if(ts.isNull(), "bad timestamp")]
    name = rec["user"]
    ptype = pii.editor_type(name, F.coalesce(rec["anon"], F.lit(False)), F.coalesce(rec["temp"], F.lit(False)),
                            F.coalesce(rec["bot"], F.lit(False)))
    b = (b.withColumn("_ts", ts).withColumn("_ptype", ptype)
         .withColumn("_pkey", pii.mask_name(name, F.col("_ptype"), salt))
         .withColumn("_title", pii.mask_user_page_title(normalise_title(rec["title"], rec["ns"]), rec["ns"], salt))
         .withColumn("_order", F.coalesce(tc("ingested_at", "timestamp"), F.col("load_timestamp"))))
    keep = ["record_hash", "raw_json", "ingest_date", "_order"]

    # --- revisions (edits and page creations)
    r = b.where(rec["type"].isin("edit", "new"))
    rev = r.select(
        F.col("wiki"), rec["revid"].alias("rev_id"), rec["pageid"].alias("page_id"),
        rec["old_revid"].alias("parent_rev_id"),
        F.col("_ts").alias("rev_ts"), pkt(F.col("_ts")).alias("rev_ts_pkt"), F.to_date("_ts").alias("rev_date"),
        F.col("_title").alias("page_title"), rec["ns"].alias("page_namespace"),
        rec["redirect"].alias("page_is_redirect"),
        F.col("_pkey").alias("editor_key"), F.col("_ptype").alias("editor_type"),
        (F.col("_ptype") == "bot").alias("is_bot"),
        F.lit(None).cast("timestamp").alias("editor_registration_ts"),
        rec["newlen"].alias("bytes"), (rec["newlen"] - F.coalesce(rec["oldlen"], F.lit(0))).alias("bytes_diff"),
        F.lower(rec["sha1"]).alias("sha1"),
        rec["minor"].alias("is_minor"), (rec["type"] == "new").alias("is_page_creation"),
        pii.mask_text(rec["comment"]).alias("comment_masked"), rec["tags"].alias("tags"),
        F.coalesce(F.arrays_overlap(rec["tags"], F.array(*[F.lit(t) for t in REVERT_TAGS])), F.lit(False))
        .alias("is_revert"),
        F.coalesce(F.array_contains(rec["tags"], "mw-reverted"), F.lit(False)).alias("is_reverted"),
        F.lit(None).cast("bigint").alias("reverted_by_rev_id"),
        F.lit(None).cast("bigint").alias("seconds_to_revert"),
        F.lit(False).alias("is_deleted"),
        F.lit("api").alias("source"), F.col("ingest_date").alias("source_ref"), F.lit(2).alias("_priority"),
        *keep,
        collect_errors(*errs, error_if(F.coalesce(rec["revid"], F.lit(0)) <= 0, "missing revid")).alias("_errors"))

    # --- page events: creations from type=new, the rest from the log
    lt, la = rec["logtype"], rec["logaction"]
    p = b.where((rec["type"] == "new") | ((rec["type"] == "log") & ~lt.isin(*USER_LOG_TYPES)))
    etype = (F.when(rec["type"] == "new", "create")
             .when(lt.isin("move", "protect", "upload", "merge", "create"), lt)
             .when((lt == "delete") & (la == "delete"), "delete")
             .when((lt == "delete") & (la == "restore"), "restore")
             .otherwise(F.concat(F.lit("other:"), lt, F.lit("/"), la)))
    tgt_ns = tc("record.logparams['target_ns']", "int")
    new_title = pii.mask_user_page_title(normalise_title(rec["logparams"]["target_title"], tgt_ns), tgt_ns, salt)
    page = p.withColumn("_etype", etype).select(
        F.col("wiki"),
        F.when(rec["logid"].isNotNull(), log_key(rec["logid"], F.col("_etype")))
        .otherwise(F.concat(F.lit("h:"), short_hash(F.col("wiki"), rec["pageid"], F.col("_etype"), F.col("_ts"),
                                                     F.col("_title"))))
        .alias("event_key"),
        rec["logid"].alias("log_id"), rec["pageid"].alias("page_id"), F.col("_etype").alias("event_type"),
        F.col("_ts").alias("event_ts"), pkt(F.col("_ts")).alias("event_ts_pkt"),
        rec["ns"].alias("page_namespace"), F.col("_title").alias("page_title"),
        F.when(lt == "move", new_title).alias("new_title"),
        F.col("_pkey").alias("editor_key"), F.col("_ptype").alias("editor_type"),
        pii.mask_text(rec["comment"]).alias("comment_masked"),
        F.lit("api").alias("source"), F.col("ingest_date").alias("source_ref"), F.lit(2).alias("_priority"),
        *keep, collect_errors(*errs).alias("_errors"))

    # --- user events
    u = b.where((rec["type"] == "log") & lt.isin(*USER_LOG_TYPES))
    target = F.when(lt == "renameuser", rec["logparams"]["olduser"]) \
        .otherwise(F.regexp_extract(normalise_title(rec["title"], rec["ns"]), r"^([^/]+)", 1))
    ttype = pii.editor_type(target, F.lit(False), F.lit(False), F.lit(False))
    utype = (F.when(lt == "newusers", "create").when(lt == "renameuser", "rename")
             .when((lt == "block") & (la == "unblock"), "unblock").when(lt == "block", "block")
             .when(lt == "rights", "altergroups"))
    user = u.withColumn("_ttype", ttype).withColumn("_etype", utype).select(
        F.col("wiki"), log_key(rec["logid"], F.col("_etype")).alias("event_key"),
        rec["logid"].alias("log_id"), F.col("_etype").alias("event_type"),
        F.col("_ts").alias("event_ts"), pkt(F.col("_ts")).alias("event_ts_pkt"),
        F.col("_pkey").alias("performer_key"), F.col("_ptype").alias("performer_type"),
        pii.mask_name(target, F.col("_ttype"), salt).alias("target_key"), F.col("_ttype").alias("target_type"),
        rec["logparams"]["duration"].alias("block_duration"),
        F.from_json(rec["logparams"]["flags"], "array<string>").alias("block_flags"),
        F.from_json(rec["logparams"]["newgroups"], "array<string>").alias("user_groups"),
        pii.mask_text(rec["comment"]).alias("comment_masked"),
        F.lit("api").alias("source"), F.col("ingest_date").alias("source_ref"), F.lit(2).alias("_priority"),
        *keep, collect_errors(*errs, error_if(rec["logid"].isNull(), "log event without logid")).alias("_errors"))
    return rev, page, user


def rc_rejects(b):
    """Lines bronze already flagged (bad json / wrong types)."""
    bad = b.where(F.col("record").isNull() | F.col("_corrupt_record").isNotNull())
    return bad.select(
        F.lit("bronze.recentchanges").alias("source_table"), "record_hash", "wiki",
        F.col("ingest_date").alias("batch_ref"),
        F.when(F.col("record").isNull(), F.array(F.lit("malformed json")))
        .otherwise(F.transform("type_mismatch_fields", lambda c: F.concat(F.lit("wrong type for "), c)))
        .alias("reasons"),
        F.col("raw_json").alias("raw_payload"))


# ---------------------------------------------------------------------------- merges
def split_good_bad(df, source_table, batch_ref_col, payload_col):
    bad = df.where(F.size("_errors") > 0).select(
        F.lit(source_table).alias("source_table"), "record_hash", "wiki",
        F.col(batch_ref_col).alias("batch_ref"), F.col("_errors").alias("reasons"),
        payload_col.alias("raw_payload"))
    return df.where(F.size("_errors") == 0), bad


def dedupe(df, keys):
    """same event can show up twice (overlapping api windows, dump + api). dump first, then newest."""
    w = Window.partitionBy(*keys).orderBy(F.col("_priority").asc(), F.col("_order").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1")


def merge_silver(spark, cfg, key, df, run, handover=None):
    """MERGE a batch into a silver table. handover = (wikis, cutoff_ts, ts_col) for full loads."""
    _, _, schema, _, pk = sc.TABLES[key]
    target = table_name(cfg, key)
    df = with_row_hash(dedupe(df, pk), schema) \
        .withColumn("batch_id", F.lit(run["run_id"])) \
        .withColumn("load_timestamp", F.current_timestamp())
    df = conform(df, schema)
    view = f"src_{key}"
    df.createOrReplaceTempView(view)
    on = " AND ".join(f"t.`{c}` = s.`{c}`" for c in pk)
    cols, vals = insert_columns(schema)
    handover_sql = ""
    if handover:
        wikis, cutoff, ts_col = handover
        handover_sql = (f"WHEN NOT MATCHED BY SOURCE AND t.source = 'api' AND t.wiki IN ({sql_list(wikis)}) "
                        f"AND t.{ts_col} < TIMESTAMP '{cutoff}' THEN DELETE")
    sql = f"""
        MERGE INTO {target} AS t
        USING {view} AS s
        ON {on}
        WHEN MATCHED AND t.row_hash <> s.row_hash AND (s.source = 'dump' OR t.source = 'api')
          THEN UPDATE SET {update_set(schema, skip=set(pk))}
        WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})
        {handover_sql}
    """
    return df.count(), run_merge(spark, target, sql)


def merge_quarantine(spark, cfg, bad, run):
    target = table_name(cfg, "quarantine")
    df = conform(bad.withColumn("batch_id", F.lit(run["run_id"]))
                 .withColumn("load_timestamp", F.current_timestamp())
                 .dropDuplicates(["source_table", "record_hash"]), sc.QUARANTINE)
    df.createOrReplaceTempView("src_quarantine")
    cols, vals = insert_columns(sc.QUARANTINE)
    return df.count(), run_merge(spark, target, f"""
        MERGE INTO {target} AS t USING src_quarantine AS s
        ON t.source_table = s.source_table AND t.record_hash = s.record_hash
        WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})
    """)


def _load(spark, cfg, run, frames, rejects, source_param, handover=None):
    """frames: [(table_key, df_with__errors, source_table_name, batch_ref_col, payload_col)]"""
    all_bad = rejects
    for key, df, src_table, ref_col, payload in frames:
        good, bad = split_good_bad(df, src_table, ref_col, payload)
        all_bad = all_bad.unionByName(bad)
        ts_col = "rev_ts" if key == "silver_revision" else "event_ts"
        with audit_step(spark, cfg, run, "bronze_to_silver", table_name(cfg, key), source_param,
                        reraise=False) as m:
            m["rows_quarantined"] = bad.count()
            n, (m["rows_inserted"], m["rows_updated"], m["rows_deleted"]) = merge_silver(
                spark, cfg, key, good, run, handover=(handover[0], handover[1], ts_col) if handover else None)
            m["rows_read"] = n + m["rows_quarantined"]
    with audit_step(spark, cfg, run, "bronze_to_silver", table_name(cfg, "quarantine"), source_param,
                    reraise=False) as m:
        m["rows_read"], (m["rows_inserted"], m["rows_updated"], m["rows_deleted"]) = \
            merge_quarantine(spark, cfg, all_bad, run)
        m["rows_quarantined"] = m["rows_read"]


def silver_from_history(spark, cfg, run, snapshot, wikis, salt):
    src = table_name(cfg, "bronze_history")
    b = spark.table(src).where((F.col("snapshot") == snapshot) & F.col("wiki").isin(*wikis))
    source_param = f"{src} snapshot={snapshot} wiki={','.join(wikis)}"
    if b.limit(1).count() == 0:
        with audit_step(spark, cfg, run, "bronze_to_silver", "silver.*", source_param):
            raise StepSkipped("bronze has nothing for this snapshot, run 01_bronze_ingest first")
        return
    rev, page, user, rejects = history_frames(b, salt)
    tbl = "bronze.mediawiki_history"
    frames = [("silver_revision", rev, tbl, "snapshot", F.col("_raw_line")),
              ("silver_page_event", page, tbl, "snapshot", F.col("_raw_line")),
              ("silver_user_event", user, tbl, "snapshot", F.col("_raw_line"))]
    _load(spark, cfg, run, frames, rejects, source_param, handover=(wikis, snapshot_cutoff(snapshot)))


def silver_from_recentchanges(spark, cfg, run, ingest_dates, wikis, salt):
    src = table_name(cfg, "bronze_rc")
    b = spark.table(src).where(F.col("ingest_date").isin(*ingest_dates) & F.col("wiki").isin(*wikis))
    source_param = f"{src} ingest_date={ingest_dates[0]}..{ingest_dates[-1]} wiki={','.join(wikis)}"
    if b.limit(1).count() == 0:
        with audit_step(spark, cfg, run, "bronze_to_silver", "silver.*", source_param):
            raise StepSkipped("bronze has nothing for these dates")
        return
    rev, page, user = rc_frames(b, salt)
    tbl = "bronze.recentchanges"
    frames = [("silver_revision", rev, tbl, "ingest_date", F.col("raw_json")),
              ("silver_page_event", page, tbl, "ingest_date", F.col("raw_json")),
              ("silver_user_event", user, tbl, "ingest_date", F.col("raw_json"))]
    # categorize events (category membership changes, ~half the feed) are skipped on purpose
    _load(spark, cfg, run, frames, rc_rejects(b), source_param)
